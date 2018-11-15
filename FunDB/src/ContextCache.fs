module FunWithFlags.FunDB.ContextCache

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.System
open FunWithFlags.FunDB.Layout.Render
open FunWithFlags.FunDB.Layout.Meta
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST

type UserViewErrorInfo =
    | UVENotFound
    | UVEAccessDenied
    | UVEArguments of string
    | UVEParse of string
    | UVEResolve of string
    | UVExecute of string

type EntityErrorInfo =
    | EENotFound
    | EEAccessDenied
    | EEArguments of string
    | EEExecute of string

type ContextBuildErrorInfo =
    | CBELayout of string
    | CBEUserView of UserViewErrorInfo
    | CBEUserViewName of string

exception ContextException of info : ContextBuildErrorInfo with
    override this.Message = this.info.ToString()

type CachedUserView =
    { resolved : ResolvedViewExpr
      compiled : CompiledViewExpr
      info : MergedViewInfo
      pureAttributes : PureAttributes
    }

let buildUserView (layout : Layout) (expr : string) : Result<ResolvedViewExpr, UserViewErrorInfo> =
    match parse tokenizeFunQL viewExpr expr with
        | Error msg -> Error <| UVEParse msg
        | Ok rawExpr ->
            let maybeExpr =
                try
                    Ok <| resolveViewExpr layout rawExpr
                with
                    | ViewResolveException err -> Error err
            Result.mapError UVEResolve maybeExpr

let buildCachedUserView
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
                                           pureAttributes = getPureAttributes expr compiled res
                                         }
                            Ok <| func cached res
                    with
                        | ViewExecutionError err -> Error <| UVExecute err

let private rebuildUserViews (conn : DatabaseConnection) (layout : Layout) : Map<string, CachedUserView> =
    let buildOne (uv : UserView) =
        if not (goodName uv.Name) then
            raise (ContextException <| CBEUserViewName uv.Name)
        let getArguments viewExpr = viewExpr.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
        match buildCachedUserView conn.Query layout uv.Query getArguments (fun info res -> info) with
            | Ok cachedUv -> (uv.Name, cachedUv)
            | Error err -> raise (ContextException <| CBEUserView err)
    conn.System.UserViews |> Seq.toArray |> Seq.map buildOne |> Map.ofSeq

let private rebuildLayout (conn : DatabaseConnection) =
    let layoutSource = buildSchemaLayout conn.System
    try
        resolveLayout layoutSource
    with
        | ResolveLayoutException msg -> raise (ContextException <| CBELayout msg)

type CachedRequestContext =
    { layout : Layout
      userViews : Map<string, CachedUserView>
      allowedDatabase : AllowedDatabase
    }

type ContextCacheStore (connectionString : string, preloadLayout : SourceLayout option) =
    let rebuildAll (conn : DatabaseConnection) =
        let layout = rebuildLayout conn
        let uvs = rebuildUserViews conn layout
        { layout = layout
          userViews = uvs
          allowedDatabase = buildAllowedDatabase layout
        }

    let systemLayout =
        let internalLayoutSource = buildSystemLayout typeof<SystemContext>
        let systemLayoutSource =
            match preloadLayout with
                | Some layout ->
                    // We ignore system entities modifications.
                    assert (not <| Map.containsKey funSchema layout.schemas)
                    { internalLayoutSource with schemas = Map.union internalLayoutSource.schemas layout.schemas }
                | None -> internalLayoutSource
        resolveLayout systemLayoutSource
    let systemMeta = buildLayoutMeta systemLayout

    let filterSystemMigrations (meta : SQL.DatabaseMeta) =
        { meta with schemas = Map.filter (fun name schema -> Map.containsKey name systemMeta.schemas) meta.schemas }

    let filterNonSystemMigrations (meta : SQL.DatabaseMeta) =
        { meta with schemas = Map.filter (fun name schema -> not <| Map.containsKey name systemMeta.schemas) meta.schemas }

    let filterSystemSchemas (layout: Layout) = Map.filter (fun name schema -> Map.containsKey name systemLayout.schemas) layout.schemas

    do
        using (new DatabaseConnection(connectionString)) <| fun conn ->
            let systemRenderedLayout = renderLayout systemLayout
            assert (resolveLayout systemRenderedLayout = systemLayout)
            
            eprintfn "Migrating system entities to the current version"
            let currentMeta = buildDatabaseMeta conn.Transaction

            let systemMigration = migrateDatabase (filterSystemMigrations currentMeta) systemMeta
            for action in systemMigration do
                eprintfn "Migration step: %O" action
                conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty

            updateLayout conn.System systemRenderedLayout

            eprintfn "Sanity checking current state"
            let state = rebuildAll conn
            assert (filterSystemSchemas state.layout = systemLayout.schemas)

            conn.Commit()

    member this.ConnectionString = connectionString

    member this.GetCache (conn : DatabaseConnection) =
        // FIXME: actually cache
        rebuildAll conn

    member this.Migrate (conn : DatabaseConnection) =
        eprintfn "Starting migration"
        // Cache this too
        // Careful here not to evaluate user views before we do migration
        let layout = rebuildLayout conn
        let wantedMeta = buildLayoutMeta layout
        let currentMeta = buildDatabaseMeta conn.Transaction
        let migration = migrateDatabase (filterNonSystemMigrations currentMeta) (filterNonSystemMigrations wantedMeta)
        for action in migration do
            eprintfn "Migration step: %O" action
            conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
        let state = rebuildAll conn
        ()
