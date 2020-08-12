module FunWithFlags.FunDB.Operations.Context

open System
open System.IO
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open System.Linq
open NpgsqlTypes
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.Operations.ContextCache
open FunWithFlags.FunDB.Operations.SaveRestore
module SQL = FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDBSchema.System

[<SerializeAsObject("error")>]
type UserViewErrorInfo =
    | [<CaseName("not_found")>] UVENotFound
    | [<CaseName("access_denied")>] UVEAccessDenied
    | [<CaseName("resolution")>] UVEResolution of Details : string
    | [<CaseName("execution")>] UVEExecution of Details : string
    | [<CaseName("arguments")>] UVEArguments of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | UVENotFound -> "User view not found"
            | UVEAccessDenied -> "User view access denied"
            | UVEResolution msg -> "User view typecheck failed"
            | UVEExecution msg -> "User view execution failed"
            | UVEArguments msg -> "Invalid user view arguments"

[<SerializeAsObject("error")>]
type EntityErrorInfo =
    | [<CaseName("not_found")>] EENotFound
    | [<CaseName("access_denied")>] EEAccessDenied
    | [<CaseName("arguments")>] EEArguments of Details : string
    | [<CaseName("execution")>] EEExecution of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | EENotFound -> "Entity not found"
            | EEAccessDenied -> "Entity access denied"
            | EEArguments msg -> "Invalid operation arguments"
            | EEExecution msg -> "Operation execution failed"

[<SerializeAsObject("error")>]
type SaveErrorInfo =
    | [<CaseName("access_denied")>] SEAccessDenied
    | [<CaseName("not_found")>] SENotFound
    with
        [<DataMember>]
        member this.Message =
            match this with
            | SEAccessDenied -> "Dump access denied"
            | SENotFound -> "Schema not found"

[<SerializeAsObject("error")>]
type RestoreErrorInfo =
    | [<CaseName("access_denied")>] REAccessDenied
    | [<CaseName("preloaded")>] REPreloaded
    | [<CaseName("invalid_format")>] REInvalidFormat of Message : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | REAccessDenied -> "Restore access denied"
            | REPreloaded -> "Cannot restore preloaded schemas"
            | REInvalidFormat msg -> "Invalid data format"

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

type RawArguments = Map<string, JToken>

let private convertEntityArguments (rawArgs : RawArguments) (entity : ResolvedEntity) : Result<EntityArguments, string> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind (fieldName.ToString()) rawArgs with
        | None -> Ok None
        | Some value ->
            match parseValueFromJson field.fieldType field.isNullable value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.fieldType fieldName
            | Some arg -> Ok (Some (fieldName, arg))
    match entity.columnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

[<SerializeAsObject("type")>]
type UserViewSource =
    | [<CaseName("anonymous")>] UVAnonymous of Query : string
    | [<CaseName("named")>] UVNamed of Ref : ResolvedUserViewRef

[<NoEquality; NoComparison>]
type RequestParams =
    { CacheStore : ContextCacheStore
      UserName : UserName
      IsRoot : bool
      Language : string
      CancellationToken : CancellationToken
    }

[<NoEquality; NoComparison>]
type UserViewEntriesResult =
    { Info : UserViewInfo
      Result : ExecutedViewExpr
    }

[<NoEquality; NoComparison>]
type UserViewInfoResult =
    { Info : UserViewInfo
      PureAttributes : ExecutedAttributeMap
      PureColumnAttributes : ExecutedAttributeMap array
    }

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseName("insert")>] TInsertEntity of Entity : ResolvedEntityRef * Entries : RawArguments
    | [<CaseName("update")>] TUpdateEntity of Entity : ResolvedEntityRef * Id : int * Entries : RawArguments
    | [<CaseName("delete")>] TDeleteEntity of Entity : ResolvedEntityRef * Id : int

[<SerializeAsObject("type")>]
type TransactionOpResult =
    | [<CaseName("insert")>] TRInsertEntity of Id : int
    | [<CaseName("update")>] TRUpdateEntity
    | [<CaseName("delete")>] TRDeleteEntity

[<NoEquality; NoComparison>]
type Transaction =
    { Operations : TransactionOp[]
    }

[<NoEquality; NoComparison>]
type TransactionResult =
    { Results : TransactionOpResult[]
    }

type TransactionError =
    { Error : EntityErrorInfo
      Operation : int
    }

[<NoEquality; NoComparison>]
type RoleType =
    | RTRoot
    | RTRole of ResolvedRole

let isRootRole : RoleType -> bool = function
    | RTRoot -> true
    | RTRole _ -> false

let private getRole = function
    | RTRoot -> None
    | RTRole role -> Some role

type RequestContext private (opts : RequestParams, ctx : IContext, rawUserId : int option, roleType : RoleType) =
    let { CacheStore = cacheStore; UserName = userName; CancellationToken = cancellationToken } = opts
    let logger = cacheStore.LoggerFactory.CreateLogger<RequestContext>()
    let mutable needMigration = false

    let transactionTime = DateTime.UtcNow
    let userId =
        match rawUserId with
        | None -> FNull
        | Some id -> FInt id
    let globalArguments =
        [ (FunQLName "lang", FString opts.Language)
          (FunQLName "user", FString opts.UserName)
          (FunQLName "user_id", userId)
          (FunQLName "transaction_time", FDateTime <| NpgsqlDateTime transactionTime)
        ] |> Map.ofList
    do
        assert (globalArgumentTypes |> Map.toSeq |> Seq.forall (fun (name, _) -> Map.containsKey name globalArguments))

    let resolveSource (source : UserViewSource) (recompileQuery : bool) : Task<Result<PrefetchedUserView, UserViewErrorInfo>> = task {
        match source with
        | UVAnonymous query ->
            try
                let! anon =
                    if not recompileQuery then
                        ctx.GetAnonymousView query
                    else
                        ctx.ResolveAnonymousView None query
                return Ok anon
            with
            | :? UserViewResolveException as err ->
                logger.LogError(err, "Failed to resolve anonymous user view: {uv}", query)
                return Error <| UVEResolution (printException err)
        | UVNamed ref ->
            let recompileView query = task {
                try
                    let! anon = ctx.ResolveAnonymousView (Some ref.schema) query
                    return Ok anon
                with
                | :? UserViewResolveException as err ->
                    logger.LogError(err, "Failed to recompile user view {uv}", ref.ToString())
                    return Error <| UVEResolution (printException err)
            }
            match ctx.State.UserViews.Find ref with
            | None -> return Error UVENotFound
            | Some (Error err) ->
                if not recompileQuery then
                    logger.LogError(err.error, "Requested user view {uv} is broken", ref.ToString())
                    return Error <| UVEResolution (printException err.error)
                else
                    return! recompileView err.source.Query
            | Some (Ok cached) ->
                if not recompileQuery then
                    return Ok cached
                else
                    let! query =
                        ctx.Transaction.System.UserViews
                            .Where(fun uv -> uv.Schema.Name = ref.schema.ToString() && uv.Name = ref.name.ToString())
                            .Select(fun uv -> uv.Query)
                            .FirstAsync()
                    return! recompileView query
    }

    let convertViewArguments (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ArgumentValues, string> =
        let findArgument (name, arg : CompiledArgument) =
            match name with
            | PLocal (FunQLName lname) ->
                match Map.tryFind lname rawArgs with
                | Some argStr ->
                    match parseValueFromJson arg.fieldType false argStr with
                    | None -> Error <| sprintf "Cannot convert argument %O to type %O" name fieldType
                    | Some arg -> Ok (Some (name, arg))
                | _ -> Ok None
            | PGlobal gname -> Ok (Some (name, Map.find gname globalArguments))
        compiled.query.arguments.types |> Map.toSeq |> Seq.traverseResult findArgument |> Result.map (Seq.catMaybes >> Map.ofSeq)

    static member Create (opts : RequestParams) : Task<RequestContext> = task {
        let! ctx = opts.CacheStore.GetCache(opts.CancellationToken)
        try
            let lowerUserName = opts.UserName.ToLowerInvariant()
            // FIXME: SLOW!
            let! rawUsers =
                ctx.Transaction.System.Users
                    .Include("Role")
                    .Include("Role.Schema")
                    .ToListAsync(opts.CancellationToken)
            let rawUser = rawUsers |> Seq.filter (fun user -> user.Name.ToLowerInvariant() = lowerUserName) |> Seq.first
            let userId = rawUser |> Option.map (fun u -> u.Id)
            let roleType =
                if opts.IsRoot then
                    RTRoot
                else
                    match rawUser with
                    | None -> raise <| RequestException REUserNotFound
                    | Some user when user.IsRoot -> RTRoot
                    | Some user when isNull user.Role -> raise <| RequestException RENoRole
                    | Some user ->
                        match ctx.State.Permissions.Find { schema = FunQLName user.Role.Schema.Name; name = FunQLName user.Role.Name } |> Option.get with
                        | Ok role -> RTRole role
                        | Error e -> raise <| RequestException RENoRole
            return new RequestContext(opts, ctx, userId, roleType)
        with
        | e ->
            ctx.Dispose ()
            return reraise' e
    }

    interface IDisposable with
        member this.Dispose () =
            (ctx :> IDisposable).Dispose()

    member this.UserName = opts.UserName
    member this.Language = opts.Language
    member this.Context = ctx
    member this.CacheStore = cacheStore
    member this.CancellationToken = cancellationToken

    member this.Commit () : Task<Result<unit, string>> =
        task {
            try
                if needMigration then
                    do! ctx.Migrate ()
                else
                    do! ctx.Transaction.Commit ()
                return Ok ()
            with
            | :? DbUpdateException
            | :? ContextException as ex ->
                logger.LogError(ex, "Failed to commit")
                return Error <| printException ex
        }

    member this.GetUserViewInfo (source : UserViewSource) (recompileQuery : bool) : Task<Result<UserViewInfoResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source recompileQuery with
            | Error e -> return Error e
            | Ok uv ->
                try
                    match roleType with
                    | RTRoot -> ()
                    | RTRole role -> checkRoleViewExpr ctx.State.Layout role uv.UserView.compiled
                    return Ok { Info = uv.Info
                                PureAttributes = uv.PureAttributes.Attributes
                                PureColumnAttributes = uv.PureAttributes.ColumnAttributes
                              }
                with
                | :? PermissionsViewException as err ->
                    logger.LogError(err, "Access denied to user view info")
                    let event =
                        EventEntry (
                            TransactionTimestamp = transactionTime,
                            Timestamp = DateTime.UtcNow,
                            Type = "info",
                            UserName = userName,
                            EntityId = Nullable(),
                            Error = "access_denied",
                            Details = sprintf "Failed to get info for %O: %s" source (printException err)
                        )
                    do! ctx.WriteEvent event
                    return Error UVEAccessDenied
        }

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) (recompileQuery : bool) : Task<Result<UserViewEntriesResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source recompileQuery with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let restricted =
                        match roleType with
                        | RTRoot -> uv.UserView.compiled
                        | RTRole role ->
                            applyRoleViewExpr ctx.State.Layout role uv.UserView.compiled
                    let getResult info (res : ExecutedViewExpr) = task {
                        return (uv, { res with Rows = Array.ofSeq res.Rows })
                    }
                    match convertViewArguments rawArgs restricted with
                    | Error msg ->
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "select",
                                UserName = userName,
                                EntityId = Nullable(),
                                Error = "arguments",
                                Details = sprintf "Invalid arguments for %O: %s" source msg
                            )
                        do! ctx.WriteEvent event
                        return Error <| UVEArguments msg
                    | Ok arguments ->
                            let! (uv, res) = runViewExpr ctx.Connection.Query restricted arguments cancellationToken getResult
                            return Ok { Info = uv.Info
                                        Result = res
                                      }
                with
                | :? PermissionsViewException as err ->
                    logger.LogError(err, "Access denied to user view")
                    let event =
                        EventEntry (
                            TransactionTimestamp = transactionTime,
                            Timestamp = DateTime.UtcNow,
                            Type = "select",
                            UserName = userName,
                            EntityId = Nullable(),
                            Error = "access_denied",
                            Details = sprintf "Failed to execute %O: %s" source (printException err)
                        )
                    do! ctx.WriteEvent event
                    return Error UVEAccessDenied
        }

    member this.GetEntityInfo (entityRef : ResolvedEntityRef) : Task<Result<SerializedEntity, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                try
                    let res = getEntityInfo ctx.State.Layout (getRole roleType) entityRef entity
                    return Ok res
                with
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "entity_info",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable(),
                                Error = "access_denied",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EEAccessDenied
        }

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Task<Result<int, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args ->
                    try
                        let! newId = insertEntity ctx.Connection.Query globalArguments ctx.State.Layout (getRole roleType) entityRef args cancellationToken
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "insert",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable newId,
                                Details = args.ToString()
                            )
                        ignore <| ctx.Transaction.System.Events.Add(event)
                        // FIXME: better handling of this
                        if entityRef.schema = funSchema then
                            needMigration <- true
                        return Ok newId
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to insert entry")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "insert",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable(),
                                Error = "execution",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error (EEExecution <| printException ex)
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "insert",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable(),
                                Error = "access_denied",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EEAccessDenied
        }

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (id : int) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args when Map.isEmpty args -> return Ok ()
                | Ok args ->
                    try
                        do! updateEntity ctx.Connection.Query globalArguments ctx.State.Layout (getRole roleType) entityRef id args cancellationToken
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "update",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Details = args.ToString()
                            )
                        ignore <| ctx.Transaction.System.Events.Add(event)
                        if entityRef.schema = funSchema then
                            needMigration <- true
                        return Ok ()
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to update entry")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "update",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "execution",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error (EEExecution <| printException ex)
                    | :? EntityNotFoundException as ex ->
                        logger.LogError(ex, "Not found")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "update",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "not_found",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EENotFound
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "update",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "access_denied",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EEAccessDenied
        }

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (id : int) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                try
                    do! deleteEntity ctx.Connection.Query globalArguments ctx.State.Layout (getRole roleType) entityRef id cancellationToken
                    let event =
                        EventEntry (
                            TransactionTimestamp = transactionTime,
                            Timestamp = DateTime.UtcNow,
                            Type = "delete",
                            UserName = userName,
                            SchemaName = entityRef.schema.ToString(),
                            EntityName = entityRef.name.ToString(),
                            EntityId = Nullable id,
                            Details = ""
                        )
                    ignore <| ctx.Transaction.System.Events.Add(event)
                    if entityRef.schema = funSchema then
                        needMigration <- true
                    return Ok ()
                with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to delete entry")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "delete",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "execution",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error (EEExecution <| printException ex)
                    | :? EntityNotFoundException as ex ->
                        logger.LogError(ex, "Not found")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "delete",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "not_found",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EENotFound
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        let event =
                            EventEntry (
                                TransactionTimestamp = transactionTime,
                                Timestamp = DateTime.UtcNow,
                                Type = "delete",
                                UserName = userName,
                                SchemaName = entityRef.schema.ToString(),
                                EntityName = entityRef.name.ToString(),
                                EntityId = Nullable id,
                                Error = "access_denied",
                                Details = printException ex
                            )
                        do! ctx.WriteEvent event
                        return Error EEAccessDenied
        }

    member this.RunTransaction (transaction : Transaction) : Task<Result<TransactionResult, TransactionError>> =
        task {
            let handleOne (i, op) =
                task {
                    let! res =
                        match op with
                        | TInsertEntity (ref, entries) ->
                            Task.map (Result.map TRInsertEntity) <| this.InsertEntity ref entries
                        | TUpdateEntity (ref, id, entries) ->
                            Task.map (Result.map (fun _ -> TRUpdateEntity)) <| this.UpdateEntity ref id entries
                        | TDeleteEntity (ref, id) ->
                            Task.map (Result.map (fun _ -> TRDeleteEntity)) <| this.DeleteEntity ref id
                    return Result.mapError (fun err -> (i, err)) res
                }
            match! transaction.Operations |> Seq.indexed |> Seq.traverseResultTaskSync handleOne with
            | Ok results ->
                return Ok { Results = Array.ofSeq results }
            | Error (i, err) ->
                return Error { Error = err
                               Operation = i
                             }
        }

    member this.SaveSchema (name : SchemaName) : Task<Result<SchemaDump, SaveErrorInfo>> =
        task {
            if not (isRootRole roleType) then
                return Error SEAccessDenied
            else
                try
                    let! schema = saveSchema ctx.Transaction.System name cancellationToken
                    return Ok schema
                with
                | :? SaveSchemaException as ex ->
                    match ex.Info with
                    | SaveRestore.SENotFound -> return Error SENotFound
        }

    member this.SaveZipSchema (name : SchemaName) : Task<Result<Stream, SaveErrorInfo>> =
        task {
            match! this.SaveSchema name with
            | Error e -> return Error e
            | Ok dump ->
                let stream = new MemoryStream()
                schemasToZipFile (Map.singleton name dump) stream
                ignore <| stream.Seek(0L, SeekOrigin.Begin)
                return Ok (stream :> Stream)
        }

    member this.RestoreSchema (name : SchemaName) (dump : SchemaDump) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            if not (isRootRole roleType) then
                return Error REAccessDenied
            else if Map.containsKey name cacheStore.Preload.Schemas then
                return Error REPreloaded
            else
                let! modified = restoreSchema ctx.Transaction.System name dump cancellationToken
                let event =
                    EventEntry (
                        TransactionTimestamp = transactionTime,
                        Timestamp = DateTime.UtcNow,
                        Type = "restore_schema",
                        UserName = userName,
                        SchemaName = name.ToString(),
                        EntityId = Nullable(),
                        Details = dump.ToString()
                    )
                do! ctx.WriteEvent event
                if modified then
                    needMigration <- true
                return Ok ()
        }

    member this.RestoreZipSchema (name : SchemaName) (stream : Stream) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            let maybeDumps =
                try
                    Ok <| schemasFromZipFile stream
                with
                | :? RestoreSchemaException as e -> Error (REInvalidFormat <| printException e)
            match Result.map (Map.toList) maybeDumps with
            | Error e -> return Error e
            | Ok [(dumpName, dump)] when name = dumpName -> return! this.RestoreSchema name dump
            | Ok _ -> return Error (REInvalidFormat <| sprintf "Archive should only contain directory %O" name)
        }