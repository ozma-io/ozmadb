module FunWithFlags.FunDB.Context

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
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
    match fieldType with
        | FTType feType -> parseExprArgument feType str
        | FTReference (_, _) -> Option.map FInt <| tryIntInvariant str
        | FTEnum values -> Some <| FString str

let private convertViewArguments (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ViewArguments, string> =
    let findArgument name (arg : CompiledArgument) =
        match Map.tryFind (name.ToString()) rawArgs with
            | Some argStr ->
                match convertArgument arg.fieldType argStr with
                    | None -> Error <| sprintf "Cannot convert argument %O to type %O" name fieldType
                    | Some arg -> Ok arg
            | _ -> Error <| sprintf "Argument not found: %O" name
    compiled.arguments |> Map.traverseResult findArgument

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

type RequestContext (cacheStore : ContextCacheStore, userName : UserName) =
    let conn = new DatabaseConnection(cacheStore.ConnectionString)
    let cache = cacheStore.GetCache(conn)

    interface IDisposable with
        member this.Dispose () =
            (conn :> IDisposable).Dispose()

    member this.Connection = conn
    member this.UserName = userName
    member this.Cache = cache
    member this.CacheStore = cacheStore

    member this.GetUserViewInfo (uv : UserViewRef) : Result<CachedUserView, UserViewErrorInfo> =
        let getArguments viewExpr = viewExpr.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
        match uv with
            | UVAnonymous query -> buildCachedUserView conn.Query cache.layout query getArguments (fun info res -> info)
            | UVNamed name ->
                match Map.tryFind name cache.userViews with
                    | None -> Error UVENotFound
                    | Some cached -> Ok cached

    member this.GetUserView (uv : UserViewRef) (rawArgs : RawArguments) : Result<CachedUserView * ExecutedViewExpr, UserViewErrorInfo> =
        match uv with
            | UVAnonymous query -> buildCachedUserView conn.Query cache.layout query (convertViewArguments rawArgs) (fun info res -> (info, { res with rows = Array.ofSeq res.rows }))
            | UVNamed name ->
                match Map.tryFind name cache.userViews with
                    | None -> Error UVENotFound
                    | Some cached ->
                        match convertViewArguments rawArgs cached.compiled with
                            | Error msg -> Error <| UVEArguments msg
                            | Ok arguments ->
                                try
                                    Ok <| runViewExpr conn.Query cached.compiled arguments (fun info res -> (cached, { res with rows = Array.ofSeq res.rows }))
                                with
                                    | ViewExecutionError err -> Error <| UVExecute err

    member this.InsertEntity (entityRef : EntityRef) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        if userName <> rootUserName && Option.isNone entityRef.schema then
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
                                if Option.isNone entityRef.schema then
                                    cacheStore.Migrate(conn)
                                conn.Commit()
                                Ok ()
                            with
                                | EntityExecutionException msg -> Error <| EEExecute msg

    member this.UpdateEntity (entityRef : EntityRef) (id : int) (rawArgs : RawArguments) : Result<unit, EntityErrorInfo> =
        if userName <> rootUserName && Option.isNone entityRef.schema then
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
                                if Option.isNone entityRef.schema then
                                    cacheStore.Migrate(conn)
                                conn.Commit()
                                Ok ()
                            with
                                | EntityExecutionException msg -> Error <| EEExecute msg

    member this.DeleteEntity (entityRef : EntityRef) (id : int) : Result<unit, EntityErrorInfo> =
        if userName <> rootUserName then
            Error <| EEAccessDenied
        else
            match cache.layout.FindEntity(entityRef) with
                | None -> Error EENotFound
                | Some entity ->
                    try
                        deleteEntity conn.Query entityRef entity id
                        if Option.isNone entityRef.schema then
                            cacheStore.Migrate(conn)
                        conn.Commit()
                        Ok ()
                    with
                        | EntityExecutionException msg -> Error <| EEExecute msg