module FunWithFlags.FunDB.ContextCache

open System.Linq
open System.Globalization
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json
open Jint.Native
open Jint.Runtime.Descriptors
open Jint.Runtime.Interop

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
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
open FunWithFlags.FunDB.JavaScript.AST
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
    | CBEValidation of string

exception ContextException of info : ContextBuildErrorInfo with
    override this.Message = this.info.ToString()

type CachedUserView =
    { resolved : ResolvedViewExpr
      compiled : CompiledViewExpr
      info : MergedViewInfo
      pureAttributes : PureAttributes
    }

// Map of registered global arguments. Should be in sync with RequestContext's globalArguments.
let globalArgumentTypes =
    Map.ofSeq
        [ ("lang", FTType <| FETScalar SFTString)
          ("user", FTType <| FETScalar SFTString)
        ]

let buildUserView (layout : Layout) (expr : string) : Result<ResolvedViewExpr, UserViewErrorInfo> =
    match parse tokenizeFunQL viewExpr expr with
    | Error msg -> Error <| UVEParse msg
    | Ok rawExpr ->
        let maybeExpr =
            try
                Ok <| resolveViewExpr layout globalArgumentTypes rawExpr
            with
            | ViewResolveException err -> Error err
        Result.mapError UVEResolve maybeExpr

let buildCachedUserView
        (conn : QueryConnection)
        (layout : Layout)
        (expr : string)
        (fixupCompiledFunc : ResolvedViewExpr -> CompiledViewExpr -> Result<CompiledViewExpr, string>)
        (argumentsFunc : ResolvedViewExpr -> CompiledViewExpr -> Result<ViewArguments, string>)
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
        if not (goodSystemName uv.Name) then
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

type private SystemViews = Map<string, string>

type CachedRequestContext =
    { layout : Layout
      userViews : Map<string, CachedUserView>
      allowedDatabase : AllowedDatabase
      meta : SQL.AST.DatabaseMeta
      systemViews : SystemViews // Used to verify that they didn't change
    }

type PreloadedSettings =
    { layout : SourceLayout
      migration : string // JS code
    }

type ContextCacheStore (connectionString : string, preloadedSettings : PreloadedSettings) =
    let versionField = "StateVersion"
    let fallbackVersion = 0

    let systemUserViewsFunction = "GetSystemUserViews"

    let migrationEngine =
        let engine = Jint.Engine(fun cfg -> ignore <| cfg.Culture(CultureInfo.InvariantCulture)).Execute(preloadedSettings.migration)
    
        // XXX: reimplement this in JavaScript for performance if we switch to V8
        let jsRenderSqlName (this : JsValue) (args : JsValue[]) =
            args.[0] |> Jint.Runtime.TypeConverter.ToString |> renderSqlName |> JsString :> JsValue
        // Strange that this is needed...       
        let jsRenderSqlNameF = System.Func<JsValue, JsValue[], JsValue>(jsRenderSqlName)
        engine.Global.FastAddProperty("renderSqlName", ClrFunctionInstance(engine, "renderSqlName", jsRenderSqlNameF, 1), true, false, true)

        engine

    let getCurrentSystemUserViews (conn : DatabaseConnection) : SystemViews =
        conn.System.UserViews.Where(fun uv -> uv.Name.StartsWith("__")) |> Seq.map (fun uv -> (uv.Name, uv.Query)) |> Map.ofSeq

    let buildSystemUserViews (layout : Layout) : SystemViews =
        let newViews =
            lock migrationEngine <| fun () ->
                if migrationEngine.Global.HasProperty(systemUserViewsFunction) then
                    // Better way?
                    let jsonParser = Jint.Native.Json.JsonParser(migrationEngine)
                    let jsLayout = layout |> renderLayout |> JsonConvert.SerializeObject |> jsonParser.Parse
                    Some (migrationEngine.Invoke(systemUserViewsFunction, jsLayout))
                else
                    None
        match newViews with
        | None -> Map.empty
        | Some ret ->
            let convertPair (KeyValue (k, v : PropertyDescriptor)) =
                let name = sprintf "__%s" k
                if not (goodSystemName name) then
                    failwith (sprintf "Invalid system user view name: %s" k)
                let query = Jint.Runtime.TypeConverter.ToString(v.Value)
                eprintfn "%s" query
                (name, query)
            ret.AsObject().GetOwnProperties() |> Seq.map convertPair |> Map.ofSeq

    let migrateSystemUserViews (conn : DatabaseConnection) (views : SystemViews) =
        conn.System.UserViews.RemoveRange(conn.System.UserViews.AsTracking().Where(fun x -> x.Name.StartsWith("__")))
        views |> Map.toSeq |> Seq.map (fun (name, query) -> UserView(Name = name, Query = query)) |> conn.System.UserViews.AddRange
        ignore <| conn.System.SaveChanges()
    
    let rebuildWithArgs (conn : DatabaseConnection) (layout : Layout) (meta : SQL.AST.DatabaseMeta) (systemViews : SystemViews) =
        let uvs = rebuildUserViews conn layout
        { layout = layout
          userViews = uvs
          allowedDatabase = buildAllowedDatabase layout
          meta = meta
          systemViews = systemViews
        }

    let rebuildAll (conn : DatabaseConnection) =
        let layout = rebuildLayout conn
        let meta = buildDatabaseMeta conn.Transaction
        let systemViews = buildSystemUserViews layout
        rebuildWithArgs conn layout meta systemViews

    let systemLayout =
        let internalLayoutSource = buildSystemLayout typeof<SystemContext>
        assert (not <| Map.containsKey funSchema preloadedSettings.layout.schemas)
        // We ignore system entities modifications.
        let systemLayoutSource =
            { internalLayoutSource with schemas = Map.union internalLayoutSource.schemas preloadedSettings.layout.schemas }
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
            let newEntry = StateValue (Name = versionField, Value = string fallbackVersion)
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

            eprintfn "Building current layout"
            let layout = rebuildLayout conn
            assert (filterSystemSchemas layout = systemLayout.schemas)
            let meta = buildDatabaseMeta conn.Transaction
            eprintfn "Updating system user views"
            let systemViews = buildSystemUserViews layout
            migrateSystemUserViews conn systemViews
            eprintfn "Building current state"
            let state = rebuildWithArgs conn layout meta systemViews

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
    
        // Validate system entries
        let newSystemLayout = { layout with schemas = layout.schemas |> Map.filter (fun name _ -> Map.containsKey name systemLayout.schemas) }
        if newSystemLayout <> systemLayout then
            raise <| ContextException (CBEValidation "Cannot modify system layout")
        let oldSystemViews = getCurrentSystemUserViews conn
        if oldSystemViews <> oldState.systemViews then
            raise <| ContextException (CBEValidation "Cannot modify system user views")
    
        // Actually migrate
        let wantedMeta = buildLayoutMeta layout
        let migration = migrateDatabase (filterNonSystemMigrations oldState.meta) (filterNonSystemMigrations wantedMeta)
        for action in migration do
            eprintfn "Migration step: %O" action
            conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
        let newSystemViews = buildSystemUserViews layout
        migrateSystemUserViews conn newSystemViews

        // Create and push new state
        let newState = rebuildWithArgs conn layout wantedMeta newSystemViews
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