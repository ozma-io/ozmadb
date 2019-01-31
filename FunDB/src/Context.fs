module FunWithFlags.FunDB.Context

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Entity
open FunWithFlags.FunDB.ContextCache

module SQL = FunWithFlags.FunDB.SQL.AST

let private parseExprArgument (fieldExprType : FieldExprType) (str : string) : FieldValue option =
    let decodeArray constrFunc convertFunc =
            str.Split(',') |> Seq.traverseOption convertFunc |> Option.map (Array.ofSeq >> constrFunc)

    match fieldExprType with
    // FIXME: breaks strings with commas!
    | FETArray SFTString -> failwith "Not supported yet"
    | FETArray SFTInt -> decodeArray FIntArray tryIntInvariant
    | FETArray SFTBool -> decodeArray FBoolArray tryBool
    | FETArray SFTDateTime -> decodeArray FDateTimeArray tryDateTimeOffsetInvariant
    | FETArray SFTDate -> decodeArray FDateArray tryDateInvariant
    | FETScalar SFTString -> Some <| FString str
    | FETScalar SFTInt -> Option.map FInt <| tryIntInvariant str
    | FETScalar SFTBool -> Option.map FBool <| tryBool str
    | FETScalar SFTDateTime -> Option.map FDateTime <| tryDateTimeOffsetInvariant str
    | FETScalar SFTDate -> Option.map FDate <| tryDateInvariant str

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

let private addCondition (expr : string) (resolved : ResolvedViewExpr) (compiled : CompiledViewExpr) : Result<CompiledViewExpr, string> =
    match parse tokenizeFunQL conditionClause expr with
    | Error msg -> Error msg
    | Ok rawExpr ->
        let maybeResolved =
            try
                Ok <| resolveAddedCondition resolved rawExpr
            with
            | ViewResolveException err -> Error err
        Result.map (compileAddedCondition compiled) maybeResolved

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

type RequestContext (cacheStore : ContextCacheStore, userName : UserName, language : string) =
    let conn = new DatabaseConnection(cacheStore.ConnectionString)
    let cache = cacheStore.GetCache(conn)
    let globalArguments =
        [ ("lang", FString language)
          ("user", FString userName)
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

    interface IDisposable with
        member this.Dispose () =
            (conn :> IDisposable).Dispose()

    member this.Connection = conn
    member this.UserName = userName
    member this.Language = language
    member this.Cache = cache
    member this.CacheStore = cacheStore

    member this.GetUserViewInfo (uv : UserViewRef) : Result<CachedUserView, UserViewErrorInfo> =
        let getArguments resolved compiled = compiled.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
        let noFixup resolved compiled = Ok compiled
        match uv with
        | UVAnonymous query -> buildCachedUserView conn.Query cache.layout query noFixup getArguments (fun info res -> info)
        | UVNamed name ->
            match Map.tryFind name cache.userViews with
            | None -> Error UVENotFound
            | Some cached -> Ok cached

    member this.GetUserView (uv : UserViewRef) (rawCondition : string option) (rawArgs : RawArguments) : Result<CachedUserView * ExecutedViewExpr, UserViewErrorInfo> =
        match uv with
        | UVAnonymous query ->
            let maybeAddCondition =
                match rawCondition with
                | None -> fun resolved compiled -> Ok compiled
                | Some expr -> addCondition expr
            let getResult cached res = (cached, { res with rows = Array.ofSeq res.rows })
            buildCachedUserView conn.Query cache.layout query maybeAddCondition (convertViewArguments rawArgs) getResult
        | UVNamed name ->
            match Map.tryFind name cache.userViews with
            | None -> Error UVENotFound
            | Some cached ->
                match convertViewArguments rawArgs cached.resolved cached.compiled with
                | Error msg -> Error <| UVEArguments msg
                | Ok arguments ->
                    let newCompiledRes =
                        match rawCondition with
                        | None -> Ok cached.compiled
                        | Some expr -> addCondition expr cached.resolved cached.compiled
                    match newCompiledRes with
                    | Error msg -> Error <| UVEFixup msg
                    | Ok newCompiled ->
                        try
                            let getResult info res = (cached, { res with rows = Array.ofSeq res.rows })
                            Ok <| runViewExpr conn.Query newCompiled arguments getResult
                        with
                        | ViewExecutionError err -> Error <| UVEExecute err

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        // FIXME
        if userName <> rootUserName && entityRef.schema <> FunQLName "user" then
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
                        // Optimize
                        if entityRef.schema = FunQLName "public" then
                            cacheStore.Migrate(conn)
                        conn.EnsureCommit()
                        Ok ()
                    with
                    | EntityExecutionException msg -> Error <| EEExecute msg

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (id : int) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        if userName <> rootUserName && entityRef.schema <> FunQLName "user" then
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
                        if entityRef.schema = funSchema then
                            cacheStore.Migrate(conn)
                        conn.EnsureCommit()
                        Ok ()
                    with
                    | EntityExecutionException msg -> Error <| EEExecute msg

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (id : int) : Result<unit, EntityErrorInfo> =
        if userName <> rootUserName then
            Error <| EEAccessDenied
        else
            match cache.layout.FindEntity(entityRef) with
            | None -> Error EENotFound
            | Some entity ->
                try
                    deleteEntity conn.Query entityRef entity id
                    if entityRef.schema = funSchema then
                        cacheStore.Migrate(conn)
                    conn.EnsureCommit()
                    Ok ()
                with
                | EntityExecutionException msg -> Error <| EEExecute msg
