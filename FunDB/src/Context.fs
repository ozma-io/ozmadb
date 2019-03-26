module FunWithFlags.FunDB.Context

open System
open System.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Entity
open FunWithFlags.FunDB.ContextCache

module SQL = FunWithFlags.FunDB.SQL.AST

let private parseExprArgument (fieldExprType : FieldExprType) (str : string) : FieldValue option =
    let decodeArray constrFunc convertFunc =
            str.Split(',') |> Seq.traverseOption convertFunc |> Option.map (Array.ofSeq >> constrFunc)

    let decodeDateTime = tryIntInvariant >> Option.map (int64 >> DateTimeOffset.FromUnixTimeSeconds)

    match fieldExprType with
    // FIXME: breaks strings with commas!
    | FETArray SFTString -> failwith "Not supported yet"
    | FETArray SFTInt -> decodeArray FIntArray tryIntInvariant
    | FETArray SFTDecimal -> decodeArray FDecimalArray tryDecimalInvariant
    | FETArray SFTBool -> decodeArray FBoolArray tryBool
    | FETArray SFTDateTime -> decodeArray FDateTimeArray decodeDateTime
    | FETArray SFTDate -> decodeArray FDateArray decodeDateTime
    | FETScalar SFTString ->
        // FIXME: lossy!
        if str = "" then Some <| FNull else Some <| FString str
    | FETScalar SFTInt -> Option.map FInt <| tryIntInvariant str
    | FETScalar SFTDecimal -> Option.map FDecimal <| tryDecimalInvariant str
    | FETScalar SFTBool -> Option.map FBool <| tryBool str
    | FETScalar SFTDateTime -> Option.map FDateTime <| decodeDateTime str
    | FETScalar SFTDate -> Option.map FDate <| decodeDateTime str

type RawArguments = Map<string, string>

let private convertArgument (fieldType : FieldType<_, _>) (str : string) : FieldValue option =
    // Magic value that means NULL
    if str = "\x00" then
        Some FNull
    else
        match fieldType with
        | FTType feType -> parseExprArgument feType str
        | FTReference (_, _) -> Option.map FInt <| tryIntInvariant str
        | FTEnum values -> Some <| FString str

let private convertEntityArguments (rawArgs : RawArguments) (entity : ResolvedEntity) : Result<EntityArguments, string> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind (fieldName.ToString()) rawArgs with
        | None -> Ok None
        | Some value ->
            match convertArgument field.fieldType value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.fieldType fieldName
            | Some arg -> (fieldName, arg) |> Some |> Ok
    match entity.columnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

type UserViewRef =
    | UVAnonymous of string
    | UVNamed of string

[<NoComparison>]
type RequestParams =
    { cacheStore : ContextCacheStore
      userName : UserName
      isRoot : bool
      language : string
    }

type RequestError =
    | REUserNotFound
    with
        member this.Message =
            match this with
            | REUserNotFound -> "User not found"

exception RequestException of info : RequestError with
    override this.Message = this.info.Message

type RequestContext (opts : RequestParams) =
    let { cacheStore = cacheStore; userName = userName; language = language; isRoot = isRoot } = opts
    let conn = new DatabaseConnection(cacheStore.ConnectionString)
    let cache = cacheStore.GetCache(conn)

    let isLocalRoot =
        if isRoot then isRoot else
            let lowerUserName = userName.ToLowerInvariant()
            // FIXME: SLOW!
            match conn.System.Users.Where(fun user -> user.Name.ToLowerInvariant() = lowerUserName).SingleOrDefault() with
            | null -> raise <| RequestException REUserNotFound
            | user -> user.IsRoot
    let transactionTime = DateTimeOffset.UtcNow
    let globalArguments =
        [ ("lang", FString language)
          ("user", FString userName)
          ("transaction_time", FDateTime transactionTime)
        ] |> Map.ofSeq
    do
        assert (globalArgumentTypes |> Map.toSeq |> Seq.forall (fun (name, _) -> Map.containsKey name globalArguments))

    let convertViewArguments (rawArgs : RawArguments) (resolved : ResolvedViewExpr) (compiled : CompiledViewExpr) : Result<ViewArguments, string> =
        let findArgument name (arg : CompiledArgument) =
            match name with
            | PLocal lname ->
                match Map.tryFind lname rawArgs with
                | Some argStr ->
                    match convertArgument arg.fieldType argStr with
                    | None -> Error <| sprintf "Cannot convert argument %O to type %O" name fieldType
                    | Some arg -> Ok arg
                | _ -> Error <| sprintf "Argument not found: %O" name
            | PGlobal gname -> Ok (Map.find gname globalArguments)
        compiled.arguments |> Map.traverseResult findArgument

    let getArguments resolved compiled = compiled.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
    let noFixup resolved compiled = Ok compiled

    interface IDisposable with
        member this.Dispose () =
            (conn :> IDisposable).Dispose()

    member this.Connection = conn
    member this.UserName = userName
    member this.Language = language
    member this.Cache = cache
    member this.CacheStore = cacheStore

    member this.GetUserViewInfo (uv : UserViewRef) : Result<CachedUserView, UserViewErrorInfo> =
        match uv with
        | UVAnonymous query -> buildCachedUserView conn.Query cache.layout query noFixup getArguments (fun info res -> info)
        | UVNamed name ->
            match Map.tryFind name cache.userViews with
            | None -> Error UVENotFound
            | Some cached -> Ok cached

    member this.GetUserView (uv : UserViewRef) (rawArgs : RawArguments) : Result<CachedUserView * ExecutedViewExpr, UserViewErrorInfo> =
        let filterInfo (info : MergedViewInfo) =
            if isLocalRoot then
                info
            else
                { info with
                      mainEntity = None
                      columns = info.columns |> Array.map (fun col -> { col with mainField = None })
                }

        match uv with
        | UVAnonymous query ->
            let getResult cached res = (cached, { res with rows = Array.ofSeq res.rows })
            buildCachedUserView conn.Query cache.layout query noFixup (convertViewArguments rawArgs) getResult
        | UVNamed name ->
            match Map.tryFind name cache.userViews with
            | None -> Error UVENotFound
            | Some cached ->
                match convertViewArguments rawArgs cached.resolved cached.compiled with
                | Error msg -> Error <| UVEArguments msg
                | Ok arguments ->
                    try
                        let getResult info (res : ExecutedViewExpr) = ({ cached with info = filterInfo cached.info }, { res with rows = Array.ofSeq res.rows })
                        Ok <| runViewExpr conn.Query cached.compiled arguments getResult
                    with
                    | ViewExecutionException err -> Error <| UVEExecute err

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        // FIXME
        if not isLocalRoot && entityRef.schema <> FunQLName "user" then
            Error <| EEAccessDenied
        else if entityRef.schema = funSchema && entityRef.name = funEvents then
            Error <| EEAccessDenied
        else
            match cache.layout.FindEntity(entityRef) with
            | None -> Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> Error <| EEArguments str
                | Ok args ->
                    try
                        insertEntity conn.Query entityRef entity args
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
                        ignore <| conn.System.Events.Add(event)
                        // FIXME: better handling of this
                        if entityRef.schema = funSchema then
                            cacheStore.Migrate(conn)
                        ignore <| conn.System.SaveChanges()
                        conn.EnsureCommit()
                        Ok ()
                    with
                    | EntityExecutionException msg ->
                        eprintfn "Entity execution exception: %s" msg
                        Error <| EEExecute msg
                    | ContextException e ->
                        eprintfn "Context exception: %O" e
                        Error <| EEExecute e.Message

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (id : int) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        if not isLocalRoot && entityRef.schema <> FunQLName "user" then
            Error <| EEAccessDenied
        else if entityRef.schema = funSchema && entityRef.name = funEvents then
            Error <| EEAccessDenied
        else
            match cache.layout.FindEntity(entityRef) with
            | None -> Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> Error <| EEArguments str
                | Ok args when Map.isEmpty args -> Ok ()
                | Ok args ->
                    try
                        updateEntity conn.Query entityRef entity id args
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
                        ignore <| conn.System.Events.Add(event)
                        if entityRef.schema = funSchema then
                            cacheStore.Migrate(conn)
                        ignore <| conn.System.SaveChanges()
                        conn.EnsureCommit()
                        Ok ()
                    with
                    | EntityExecutionException msg ->
                        eprintfn "Entity execution exception: %s" msg
                        Error <| EEExecute msg
                    | ContextException e ->
                        eprintfn "Context exception: %O" e
                        Error <| EEExecute e.Message

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (id : int) : Result<unit, EntityErrorInfo> =
        if not isLocalRoot then
            Error <| EEAccessDenied
        else if entityRef.schema = funSchema && entityRef.name = funEvents then
            Error <| EEAccessDenied
        else
            match cache.layout.FindEntity(entityRef) with
            | None -> Error EENotFound
            | Some entity ->
                try
                    deleteEntity conn.Query entityRef entity id
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
                    ignore <| conn.System.Events.Add(event)
                    if entityRef.schema = funSchema then
                        cacheStore.Migrate(conn)
                    ignore <| conn.System.SaveChanges()
                    conn.EnsureCommit()
                    Ok ()
                with
                | EntityExecutionException msg ->
                    eprintfn "Entity execution exception: %s" msg
                    Error <| EEExecute msg
                | ContextException e ->
                    eprintfn "Context exception: %O" e
                    Error <| EEExecute e.Message
