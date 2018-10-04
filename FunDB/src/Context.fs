module FunWithFlags.FunDB.Context

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.SQL.Query

module SQL = FunWithFlags.FunDB.SQL.AST

type UserViewErrorInfo =
    | UVENotFound
    | UVENotUpdating
    | UVEAccessDenied
    | UVEArguments of string
    | UVEParse of string
    | UVEResolve of string
    | UVExecute of string

type ContextBuildErrorInfo =
    | CBELayout of string
    | CBEUserView of UserViewErrorInfo
    | CBEUserViewName of string

exception ContextError of info : ContextBuildErrorInfo with
    override this.Message = this.info.ToString()

type CachedUserView =
    { resolved : ResolvedViewExpr
      compiled : CompiledViewExpr
      info : MergedViewInfo
    }

let private buildUserView (layout : Layout) (expr : string) : Result<ResolvedViewExpr, UserViewErrorInfo> =
    match parse tokenizeFunQL viewExpr expr with
        | Error msg -> Error <| UVEParse msg
        | Ok rawExpr ->
            let maybeExpr =
                try
                    Ok <| resolveViewExpr layout rawExpr
                with
                    | ViewResolveError err -> Error err
            Result.mapError UVEResolve maybeExpr

let private buildCachedUserView
        (conn : QueryConnection)
        (layout : Layout)
        (expr : string)
        (argumentsFunc : CompiledViewExpr -> Result<Map<string, FieldValue>, string>)
        (func : CachedUserView -> ExecutedViewExpr -> 'a)
        : Result<'a, UserViewErrorInfo> =
    match buildUserView layout expr with
        | Error err -> Error err
        | Ok expr ->
            let compiled = compileViewExpr layout expr
            match argumentsFunc compiled with
                | Error msg -> Error <| UVEArguments msg
                | Ok arguments ->
                    try
                        runViewExpr conn compiled arguments <| fun info res ->
                            let cached = { compiled = compiled
                                           resolved = expr
                                           info = mergeViewInfo layout expr info
                                         }
                            Ok <| func cached res
                    with
                        | ViewExecutionError err -> Error <| UVExecute err

let private rebuildUserViews (conn : DatabaseConnection) (layout : Layout) : Map<string, CachedUserView> =
    let buildOne (uv : UserView) =
        if not (goodLayoutName uv.Name) then
            raise (ContextError <| CBEUserViewName uv.Name)
        let getArguments viewExpr = viewExpr.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
        match buildCachedUserView conn.Query layout uv.Query getArguments (fun info res -> info) with
            | Ok cachedUv -> (uv.Name, cachedUv)
            | Error err -> raise (ContextError <| CBEUserView err)
    conn.System.UserViews |> Seq.toArray |> Seq.map buildOne |> Map.ofSeq

let private rebuildLayout (conn : DatabaseConnection) =
    let layoutSource = buildSchemaLayout conn.System
    try
        resolveLayout layoutSource
    with
        | ResolveLayoutError msg -> raise (ContextError <| CBELayout msg)

type CachedRequestContext =
    { layout : Layout
      userViews : Map<string, CachedUserView>
      allowedDatabae : AllowedDatabase
    }

type ContextCacheStore (connectionString : string) =
    let rebuildAll (conn : DatabaseConnection) =
        let layout = rebuildLayout conn
        let uvs = rebuildUserViews conn layout
        { layout = layout
          userViews = uvs
          allowedDatabae = buildAllowedDatabase layout
        }

    member this.ConnectionString = connectionString

    member this.GetCache (conn : DatabaseConnection) =
        // FIXME: actually cache
        rebuildAll conn

    member this.Migrate () =
        failwith "Not implemented"

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

let private convertArgument (fieldType : ParsedFieldType) (str : string) : FieldValue option =
    match fieldType with
        | FTType feType -> parseExprArgument feType str
        | FTReference (entityRef, where) -> Option.map FInt <| tryIntInvariant str
        | FTEnum values -> Some <| FString str

let private convertArguments (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ViewArguments, string> =
    let findArgument name (arg : CompiledArgument) =
        match Map.tryFind (name.ToString()) rawArgs with
            | Some argStr ->
                match convertArgument arg.fieldType argStr with
                    | None -> Error <| sprintf "Cannot convert argument %O to type %O" name fieldType
                    | Some arg -> Ok arg
            | _ -> Error <| sprintf "Argument not found: %O" name
    compiled.arguments |> Map.traverseResult findArgument

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

    member this.GetUserView (uv : UserViewRef, rawArgs : RawArguments) : Result<(CachedUserView * ExecutedViewExpr), UserViewErrorInfo> =
        match uv with
            | UVAnonymous query -> buildCachedUserView conn.Query cache.layout query (convertArguments rawArgs) (fun info res -> (info, { res with rows = Array.ofSeq res.rows }))
            | UVNamed name ->
                match Map.tryFind name cache.userViews with
                    | None -> Error UVENotFound
                    | Some cached ->
                        match convertArguments rawArgs cached.compiled with
                            | Error msg -> Error <| UVEArguments msg
                            | Ok arguments ->
                                try
                                    Ok <| runViewExpr conn.Query cached.compiled arguments (fun info res -> (cached, { res with rows = Array.ofSeq res.rows }))
                                with
                                    | ViewExecutionError err -> Error <| UVExecute err

    (*member this.UpdateUserView (uv : UserViewRef, entityId : int, values : Map<string, SQL.Value>) : Result<unit, UserViewErrorInfo> =
        match uv with
            | UVAnonymous query ->
                match buildUserView cache.layout query with
                    | Error err -> Error err
                    | Ok ({ update = None } as expr) -> Error UVENotUpdating
                    | Ok ({ update = Some update } as expr) ->
                        runUpdateById update values
                        let compiled = compileViewExpr cache.layout expr
                        try
                            runViewExpr conn compiled (argumentsFunc compiled) <| fun info res ->
                                let cached = { compiled = compiled
                                               resolved = expr
                                               info = mergeViewInfo layout expr info
                                             }
                                Ok <| func cached res
                        with
                            | ViewExecutionError err -> Error <| UVExecute err
            | UVNamed name ->
                match Map.tryFind name cache.userViews with
                    | None -> Error UVENotFound
                    | Some cached ->
                        try
                            Ok <| runViewExpr conn.Query cached.compiled arguments (fun info res -> (cached, { res with rows = Array.ofSeq res.rows }))
                        with
                            | ViewExecutionError err -> Error <| UVExecute err*)