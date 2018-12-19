module FunWithFlags.FunDB.ContextCache

open System.Linq
open Microsoft.EntityFrameworkCore

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
    | UVEExecute of string
    | UVEFixup of string

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
        (fixupCompiledFunc : ResolvedViewExpr -> CompiledViewExpr -> Result<CompiledViewExpr, string>)
        (argumentsFunc : ResolvedViewExpr -> CompiledViewExpr -> Result<Map<string, FieldValue>, string>)
        (func : CachedUserView -> ExecutedViewExpr -> 'a)
        : Result<'a, UserViewErrorInfo> =
    match buildUserView layout expr with
        | Error err -> Error err
        | Ok resolved ->
            let compiledRaw = compileViewExpr layout resolved
            match fixupCompiledFunc resolved compiledRaw with
                | Error err -> Error <| UVEFixup err
                | Ok compiled ->
                    match argumentsFunc resolved compiled with
                        | Error msg -> Error <| UVEArguments msg
                        | Ok arguments ->
                            try
                                runViewExpr conn compiled arguments <| fun info res ->
                                    let cached = { compiled = compiled
                                                   resolved = resolved
                                                   info = mergeViewInfo layout resolved info
                                                   pureAttributes = getPureAttributes resolved compiled res
                                                 }
                                    Ok <| func cached res
                            with
                                | ViewExecutionError err -> Error <| UVEExecute err

let private rebuildUserViews (conn : DatabaseConnection) (layout : Layout) : Map<string, CachedUserView> =
    let buildOne (uv : UserView) =
        if not (goodName uv.Name) then
            raise (ContextException <| CBEUserViewName uv.Name)
        let noFixup resolved compiled = Ok compiled
        let getArguments resolved compiled = compiled.arguments |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType) |> Ok
        match buildCachedUserView conn.Query layout uv.Query noFixup getArguments (fun info res -> info) with
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
      meta : SQL.AST.DatabaseMeta
    }

type ContextCacheStore (connectionString : string, preloadLayout : SourceLayout option) =
    let versionField = "StateVersion"
    let fallbackVersion = 0

    let rebuildWithLayoutAndMeta (conn : DatabaseConnection) (layout : Layout) (meta : SQL.AST.DatabaseMeta) =
        let uvs = rebuildUserViews conn layout
        { layout = layout
          userViews = uvs
          allowedDatabase = buildAllowedDatabase layout
          meta = meta
        }

    let rebuildAll (conn : DatabaseConnection) =
        let layout = rebuildLayout conn
        let meta = buildDatabaseMeta conn.Transaction
        rebuildWithLayoutAndMeta conn layout meta

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

    // Returns a version and whether a rebuild should be force-performed.
    let getCurrentVersion (conn : DatabaseConnection) (bump : bool) =
        match conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefault() with
            | null ->
                let newEntry =
                    new StateValue (
                        Name = versionField,
                        Value = string fallbackVersion
                    )
                ignore <| conn.System.State.Add(newEntry)
                ignore <| conn.System.SaveChanges()
                (true, fallbackVersion)
            | entry ->
                match tryIntInvariant entry.Value with
                    | Some v ->
                        if bump then
                            let newVersion = v + 1
                            entry.Value <- string newVersion
                            ignore <| conn.System.SaveChanges()
                            (true, newVersion)
                        else
                            (false, v)
                    | None ->
                        entry.Value <- string fallbackVersion
                        ignore <| conn.System.SaveChanges()
                        (true, fallbackVersion)

    let mutable cachedState =
        using (new DatabaseConnection(connectionString)) <| fun conn ->
            let systemRenderedLayout = renderLayout systemLayout
            assert (resolveLayout systemRenderedLayout = systemLayout)
            
            eprintfn "Migrating system entities to the current version"
            let currentMeta = buildDatabaseMeta conn.Transaction

            let systemMigration = migrateDatabase (filterSystemMigrations currentMeta) systemMeta
            for action in systemMigration do
                eprintfn "Migration step: %O" action
                conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty

            let layoutChanged = updateLayout conn.System systemRenderedLayout

            let (_, currentVersion) = getCurrentVersion conn layoutChanged

            eprintfn "Building current state"
            let state = rebuildAll conn
            assert (filterSystemSchemas state.layout = systemLayout.schemas)

            conn.Commit()

            (currentVersion, state)

    member this.ConnectionString = connectionString

    member this.GetCache (conn : DatabaseConnection) =
        let (oldVersion, oldState) = cachedState
        let (forceRebuild, currentVersion) = getCurrentVersion conn false
        if forceRebuild || oldVersion <> currentVersion then
            let newState = rebuildAll conn
            cachedState <- (currentVersion, newState)
            newState
        else
            oldState

    // After this no more operations with this connection should be performed as it may commit.
    member this.Migrate (conn : DatabaseConnection) =
        eprintfn "Starting migration"
        // We can do this because migration could be performed only after GetCache in the same transaction.
        let (oldVersion, oldState) = cachedState
        // Careful here not to evaluate user views before we do migration
        let layout = rebuildLayout conn
        let wantedMeta = buildLayoutMeta layout
        let migration = migrateDatabase (filterNonSystemMigrations oldState.meta) (filterNonSystemMigrations wantedMeta)
        for action in migration do
            eprintfn "Migration step: %O" action
            conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
        let newState = rebuildWithLayoutAndMeta conn layout wantedMeta
        if oldState = newState then
            eprintfn "No state change is observed"
        else
            // At this point we are sure there is a valid versionEntry because GetCache should have been called.
            let newVersion = oldVersion + 1
            let versionEntry = conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).First()
            versionEntry.Value <- string newVersion
            ignore <| conn.System.SaveChanges()
            conn.Commit()
            cachedState <- (newVersion, newState)