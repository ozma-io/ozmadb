module FunWithFlags.FunDB.Operations.Context

open System
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Flatten
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.Operations.ContextCache
open FunWithFlags.FunDB.Operations.SaveRestore
module SQL = FunWithFlags.FunDB.SQL.AST
module SaveRestore = FunWithFlags.FunDB.Operations.SaveRestore

type UserViewErrorInfo =
    | UVENotFound
    | UVEAccessDenied
    | UVEResolve of string
    | UVEExecute of string
    | UVEArguments of string
    with
        member this.Message =
            match this with
            | UVENotFound -> "User view not found"
            | UVEAccessDenied -> "User view access denied"
            | UVEResolve msg -> msg
            | UVEExecute msg -> msg
            | UVEArguments msg -> msg

type EntityErrorInfo =
    | EENotFound
    | EEAccessDenied
    | EEArguments of string
    | EEExecute of string
    with
        member this.Message =
            match this with
            | EENotFound -> "Entity not found"
            | EEAccessDenied -> "Entity access denied"
            | EEArguments msg -> msg
            | EEExecute msg -> msg

type SaveErrorInfo =
    | SEAccessDenied
    | SENotFound
    with
        member this.Message =
            match this with
            | SEAccessDenied -> "Dump access denied"
            | SENotFound -> "Schema not found"

type RestoreErrorInfo =
    | REAccessDenied
    | REPreloaded
    with
        member this.Message =
            match this with
            | REAccessDenied -> "Restore access denied"
            | REPreloaded -> "Cannot restore preloaded schemas"

type RequestErrorInfo =
    | REUserNotFound
    | RENoRole
    with
        member this.Message =
            match this with
            | REUserNotFound -> "User not found"
            | RENoRole -> "Access denied"

type RequestException (info : RequestErrorInfo) =
    inherit Exception(info.Message)

    member this.Info = info

let rec private printException (e : exn) : string =
    if isNull e.InnerException then
        e.Message
    else
        sprintf "%s: %s" e.Message (printException e.InnerException)

let private decodeValue (constrFunc : 'A -> FieldValue option) (isNullable : bool) (tok: JToken) : FieldValue option =
    if tok.Type = JTokenType.Null then
        if isNullable then
            Some FNull
        else
            None
    else
        constrFunc <| tok.ToObject()

let private parseExprArgument (fieldExprType : FieldExprType) : bool -> JToken -> FieldValue option =
    let decodeValueStrict f = decodeValue (f >> Some)
    match fieldExprType with
    | FETArray SFTString -> decodeValueStrict FStringArray
    | FETArray SFTInt -> decodeValueStrict FIntArray
    | FETArray SFTDecimal -> decodeValueStrict FDecimalArray
    | FETArray SFTBool -> decodeValueStrict FBoolArray
    | FETArray SFTDateTime -> decodeValueStrict FDateTimeArray
    | FETArray SFTDate -> decodeValueStrict FDateArray
    | FETArray SFTJson -> decodeValueStrict FJsonArray
    | FETArray SFTUserViewRef -> decodeValueStrict FUserViewRefArray
    | FETScalar SFTString -> decodeValueStrict FString
    | FETScalar SFTInt -> decodeValueStrict FInt
    | FETScalar SFTDecimal -> decodeValueStrict FDecimal
    | FETScalar SFTBool -> decodeValueStrict FBool
    | FETScalar SFTDateTime -> decodeValueStrict FDateTime
    | FETScalar SFTDate -> decodeValueStrict FDate
    | FETScalar SFTJson -> decodeValueStrict FJson
    | FETScalar SFTUserViewRef -> decodeValueStrict FUserViewRef

type RawArguments = Map<string, JToken>

let private convertArgument (fieldType : FieldType<_, _>) (isNullable : bool) (tok : JToken) : FieldValue option =
    match fieldType with
    | FTType feType -> parseExprArgument feType isNullable tok
    | FTReference (ref, where) -> decodeValue (FInt >> Some) isNullable tok
    | FTEnum values ->
        let checkAndEncode v =
            if Set.contains v values then
                Some <| FString v
            else
                None
        decodeValue checkAndEncode isNullable tok

let private convertEntityArguments (rawArgs : RawArguments) (entity : ResolvedEntity) : Result<EntityArguments, string> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind (fieldName.ToString()) rawArgs with
        | None -> Ok None
        | Some value ->
            match convertArgument field.fieldType field.isNullable value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.fieldType fieldName
            | Some arg -> Ok (Some (fieldName, arg))
    match entity.columnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

type UserViewSource =
    | UVAnonymous of string
    | UVNamed of ResolvedUserViewRef

[<NoComparison>]
type RequestParams =
    { cacheStore : ContextCacheStore
      userName : UserName
      isRoot : bool
      disableACL : bool
      language : string
    }

[<NoComparison>]
type RoleType =
    | RTRoot
    | RTRole of FlatRole

let private getRole = function
    | RTRoot -> None
    | RTRole role -> Some role

type RequestContext private (opts : RequestParams, ctx : IContext, rawUserId : int option, roleType : RoleType) =
    let { cacheStore = cacheStore; userName = userName; language = language; isRoot = isRoot } = opts
    let logger = cacheStore.LoggerFactory.CreateLogger<RequestContext>()
    let mutable needMigration = false

    let transactionTime = DateTimeOffset.UtcNow
    let userId =
        match rawUserId with
        | None -> FNull
        | Some id -> FInt id
    let globalArguments =
        [ (FunQLName "lang", FString language)
          (FunQLName "user", FString userName)
          (FunQLName "user_id", userId)
          (FunQLName "transaction_time", FDateTime transactionTime)
        ] |> Map.ofSeq
    do
        assert (globalArgumentTypes |> Map.toSeq |> Seq.forall (fun (name, _) -> Map.containsKey name globalArguments))

    let resolveSource (source : UserViewSource) : Task<Result<ResolvedUserView, UserViewErrorInfo>> = task {
        match source with
        | UVAnonymous query ->
            try
                let! anon = ctx.GetAnonymousView query
                return Ok anon
            with
            | :? UserViewResolveException as err -> return Error <| UVEResolve err.Message
        | UVNamed ref ->
            match ctx.State.userViews.Find ref with
            | None -> return Error UVENotFound
            | Some (Error err) ->
                logger.LogError(err.error, "Requested user view {uv} is broken", ref.ToString())
                return Error (UVEResolve err.error.Message)
            | Some (Ok cached) -> return Ok cached
    }

    let convertViewArguments (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ArgumentValues, string> =
        let findArgument name (arg : CompiledArgument) =
            match name with
            | PLocal (FunQLName lname) ->
                match Map.tryFind lname rawArgs with
                | Some argStr ->
                    match convertArgument arg.fieldType false argStr with
                    | None -> Error <| sprintf "Cannot convert argument %O to type %O" name fieldType
                    | Some arg -> Ok arg
                | _ -> Error <| sprintf "Argument not found: %O" name
            | PGlobal gname -> Ok (Map.find gname globalArguments)
        compiled.query.arguments.types |> Map.traverseResult findArgument

    static member Create (opts : RequestParams) : Task<RequestContext> = task {
        let! ctx = opts.cacheStore.GetCache()
        try
            let lowerUserName = opts.userName.ToLowerInvariant()
            // FIXME: SLOW!    
            let! rawUser =
                ctx.Connection.System.Users
                    .Include("Role")
                    .Include("Role.Schema")
                    .Where(fun user -> user.Name.ToLowerInvariant() = lowerUserName)
                    .SingleOrDefaultAsync()    
            let userId = if isNull rawUser then None else Some rawUser.Id
            let roleType =
                if opts.isRoot then
                    RTRoot
                else
                    match rawUser with
                    | null -> raise <| RequestException REUserNotFound
                    | user when user.IsRoot || opts.disableACL -> RTRoot
                    | user when isNull user.Role -> raise <| RequestException RENoRole
                    | user ->
                        let role = ctx.State.permissions.FindRole { schema = FunQLName user.Role.Schema.Name; name = FunQLName user.Role.Name } |> Option.get
                        RTRole role
            return new RequestContext(opts, ctx, userId, roleType)
        with
        | e ->
            ctx.Dispose ()
            return reraise' e
    }

    interface IDisposable with
        member this.Dispose () =
            (ctx :> IDisposable).Dispose()

    member this.UserName = userName
    member this.Language = language
    member this.Context = ctx
    member this.CacheStore = cacheStore

    member this.Commit () : Task<Result<unit, string>> =
        task {
            try
                if needMigration then
                    do! ctx.Migrate ()
                else
                    let! _ = ctx.Connection.System.SaveChangesAsync ()
                    do! ctx.Connection.Commit ()
                return Ok ()
            with
            | :? DbUpdateException
            | :? ContextException as ex ->
                logger.LogError(ex, "Failed to commit")
                return Error <| printException ex
        }

    member this.GetUserViewInfo (source : UserViewSource) =
        task {
            match! resolveSource source with
            | Error e -> return Error e
            | Ok uv ->
                try
                    match roleType with
                    | RTRoot -> ()
                    | RTRole role -> checkRoleViewExpr ctx.State.layout role uv.compiled
                    return Ok uv
                with
                | :? PermissionsViewException as err ->
                    logger.LogError(err, "Access denied to user view info")
                    return Error UVEAccessDenied
        }

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) : Task<Result<ResolvedUserView * ExecutedViewExpr, UserViewErrorInfo>> =
        task {
            match! resolveSource source with
            | Error e -> return Error e
            | Ok uv ->
                let restricted =
                    match roleType with
                    | RTRoot -> uv.compiled
                    | RTRole role -> applyRoleViewExpr ctx.State.layout role uv.compiled
                let getResult info (res : ExecutedViewExpr) = task {
                    return (uv, { res with rows = Array.ofSeq res.rows })
                }
                match convertViewArguments rawArgs restricted with
                | Error msg -> return Error <| UVEArguments msg
                | Ok arguments ->
                    try
                        let! r = runViewExpr ctx.Connection.Query restricted arguments getResult
                        return Ok r
                    with
                    | :? PermissionsViewException as err ->
                        logger.LogError(err, "Access denied to user view")
                        return Error UVEAccessDenied
        }

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.State.layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args ->
                    try
                        do! insertEntity ctx.Connection.Query globalArguments (getRole roleType) entityRef entity args
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTimeOffset.UtcNow,
                                Type = "insert",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable(), // FIXME: set id
                                Details = args.ToString()
                            )
                        ignore <| ctx.Connection.System.Events.Add(event)
                        // FIXME: better handling of this
                        if entityRef.schema = funSchema then
                            needMigration <- true
                        return Ok ()
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to insert entry")
                        return Error (EEExecute <| printException ex)
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        return Error EEAccessDenied
        }

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (id : int) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.State.layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args when Map.isEmpty args -> return Ok ()
                | Ok args ->
                    try
                        do! updateEntity ctx.Connection.Query globalArguments (getRole roleType) entityRef entity id args
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTimeOffset.UtcNow,
                                Type = "update",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Details = args.ToString()
                            )
                        ignore <| ctx.Connection.System.Events.Add(event)
                        if entityRef.schema = funSchema then
                            needMigration <- true
                        return Ok ()
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to update entry")
                        return Error (EEExecute <| printException ex)
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        return Error EEAccessDenied
        }

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (id : int) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.State.layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                try
                    do! deleteEntity ctx.Connection.Query globalArguments (getRole roleType) entityRef entity id
                    let event =
                        EventEntry (
                            TransactionTimestamp = transactionTime,
                            Timestamp = DateTimeOffset.UtcNow,
                            Type = "delete",
                            UserName = userName,
                            SchemaName = entityRef.schema.ToString(),
                            EntityName = entityRef.name.ToString(),
                            EntityId = Nullable id,
                            Details = ""
                        )
                    ignore <| ctx.Connection.System.Events.Add(event)
                    if entityRef.schema = funSchema then
                        needMigration <- true
                    return Ok ()
                with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to delete entry")
                        return Error (EEExecute <| printException ex)
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        return Error EEAccessDenied
        }

    member this.SaveSchema (name : SchemaName) : Task<Result<SchemaDump, SaveErrorInfo>> =
        task {
            if roleType <> RTRoot then
                return Error SEAccessDenied
            else
                try
                    let! schema = saveSchema ctx.Connection.System name
                    return Ok schema
                with
                | :? SaveSchemaException as ex ->
                    match ex.Info with
                    | SaveRestore.SENotFound -> return Error SENotFound
        }

    member this.RestoreSchema (name : SchemaName) (dump : SchemaDump) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            if roleType <> RTRoot then
                return Error REAccessDenied
            else if Map.containsKey name cacheStore.Preload.schemas then
                return Error REPreloaded
            else
                let! modified = restoreSchema ctx.Connection.System name dump
                let event =
                    EventEntry (
                        TransactionTimestamp = transactionTime,
                        Timestamp = DateTimeOffset.UtcNow,
                        Type = "restore_schema",
                        UserName = userName,
                        SchemaName = name.ToString(),
                        EntityName = null,
                        EntityId = Nullable(),
                        Details = dump.ToString()
                    )
                ignore <| ctx.Connection.System.Events.Add(event)
                if modified then
                    needMigration <- true
                return Ok ()
        }