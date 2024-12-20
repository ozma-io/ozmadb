﻿module OzmaDB.API.ContextCache

open FSharpPlus
open System
open System.Reflection
open System.Data
open System.IO
open System.Linq
open Microsoft.EntityFrameworkCore
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.ObjectPool
open FluidCaching
open Npgsql

open OzmaDBSchema.System
open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Parsing
open OzmaDB.Exception
open OzmaDB.Connection
open OzmaDB.EventLogger
open OzmaDB.Layout.Types
open OzmaDB.Layout.Schema
open OzmaDB.Layout.Resolve
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Compile
open OzmaDB.Permissions.Types
open OzmaDB.Permissions.Schema
open OzmaDB.Permissions.Resolve
open OzmaDB.Attributes.Schema
open OzmaDB.Attributes.Parse
open OzmaDB.Attributes.Resolve
open OzmaDB.Attributes.Merge
open OzmaDB.Triggers.Run
open OzmaDB.Triggers.Schema
open OzmaDB.Triggers.Resolve
open OzmaDB.Triggers.Merge
open OzmaDB.Modules.Load
open OzmaDB.Modules.Schema
open OzmaDB.Modules.Resolve
open OzmaDB.Actions.Types
open OzmaDB.Actions.Schema
open OzmaDB.Actions.Resolve
open OzmaDB.Actions.Run
open OzmaDB.UserViews.Source
open OzmaDB.UserViews.Types
open OzmaDB.UserViews.Resolve
open OzmaDB.UserViews.Update
open OzmaDB.UserViews.Schema
open OzmaDB.UserViews.DryRun
open OzmaDB.UserViews.Generate
open OzmaDB.Layout.Domain
open OzmaDB.Layout.Integrity
open OzmaDB.Operations.Update
open OzmaDB.SQL.Query
open OzmaDB.SQL.Meta
open OzmaDB.SQL.Migration

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.DDL

open OzmaDB.JavaScript.Runtime
open OzmaDB.Operations.Preload
open OzmaDB.Operations.Command
open OzmaDB.API.Types
open OzmaDB.API.JavaScript

type ContextException(details: GenericErrorInfo, innerException: exn) =
    inherit UserException(details.LogMessage, innerException, true)

    new(details: GenericErrorInfo) = ContextException(details, null)

    member this.Details = details

[<NoEquality; NoComparison>]
type private AnonymousUserView =
    { Query: string
      UserView: PrefetchedUserView }

[<NoEquality; NoComparison>]
type private AnonymousCommand =
    { Query: string
      Privileged: bool
      Command: CompiledCommandExpr }

let private databaseField = "DatabaseVersion"
let private versionField = "StateVersion"
let private fallbackVersion = 0
let private migrationLockNumber = 0
let private migrationLockParams = Map.singleton 0 (SQL.VInt migrationLockNumber)

let private assemblyId =
    Assembly.GetExecutingAssembly().ManifestModule.ModuleVersionId

[<NoEquality; NoComparison>]
type private CachedContext =
    { Layout: Layout
      UserViews: PrefetchedUserViews
      Permissions: Permissions
      DefaultAttrs: MergedDefaultAttributes
      Domains: LayoutDomains
      Actions: ResolvedActions
      JSEngine: RuntimeLocal<OzmaJSEngine>
      ActionScripts: RuntimeLocal<PreparedActions>
      Triggers: MergedTriggers
      TriggerScripts: RuntimeLocal<PreparedTriggers>
      SystemViews: SourceUserViews
      UserMeta: SQL.DatabaseMeta }

[<NoEquality; NoComparison>]
type private CachedState =
    { Version: int; Context: CachedContext }

let instanceIsInitialized (conn: DatabaseTransaction) =
    task {
        use pg = createPgCatalogContext conn

        let! stateCount =
            pg.Classes.CountAsync(fun cl ->
                cl.RelName = "state"
                && cl.RelKind = 'r'
                && cl.Namespace.NspName = string funSchema)

        return stateCount > 0
    }

[<NoEquality; NoComparison>]
type ContextCacheParams =
    { LoggerFactory: ILoggerFactory
      // Whether to allow automatically marking objects as broken.
      AllowAutoMark: bool
      Preload: HashedPreload
      ConnectionString: string
      EventLogger: EventLogger }

type ContextCacheStore(cacheParams: ContextCacheParams) =
    let preload = cacheParams.Preload.Preload
    let logger = cacheParams.LoggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values. Also we want to move these somewhere else, too much logic in this class.
    let anonymousViewsCache =
        FluidCache<AnonymousUserView>(
            64,
            TimeSpan.FromSeconds(0.0),
            TimeSpan.FromSeconds(600.0),
            fun () -> DateTime.Now
        )

    let anonymousViewsIndex =
        anonymousViewsCache.AddIndex("byQuery", (fun uv -> uv.Query))

    let anonymousCommandsCache =
        FluidCache<AnonymousCommand>(
            64,
            TimeSpan.FromSeconds(0.0),
            TimeSpan.FromSeconds(600.0),
            fun () -> DateTime.Now
        )

    let anonymousCommandsIndex =
        anonymousCommandsCache.AddIndex("byQuery", (fun uv -> uv.Query))

    let clearCaches () =
        anonymousViewsCache.Clear()
        anonymousCommandsCache.Clear()

    let currentDatabaseVersion = sprintf "%O %s" assemblyId cacheParams.Preload.Hash

    let mutable jsRuntimeParams = noJSRuntimeLimits

    let jsRuntimes =
        let policy =
            { new IPooledObjectPolicy<JSRuntime> with
                member this.Create() = JSRuntime jsRuntimeParams

                member this.Return runtime =
                    not runtime.MemoryLimitCancellationToken.IsCancellationRequested
                    && runtime.Limits = jsRuntimeParams }

        DefaultObjectPool(policy, Environment.ProcessorCount)

    let filterSystemViews (views: SourceUserViews) : SourceUserViews =
        let schemas = filterPreloadedSchemas preload views.Schemas

        // Filter out generated user views, so that later comparison doesn't include them.
        let mapOne schemaName (uvSchema: SourceUserViewsSchema) =
            match uvSchema.GeneratorScript with
            | None -> uvSchema
            | Some script -> { uvSchema with UserViews = Map.empty }

        let pristineSchemas = Map.map mapOne schemas

        { Schemas = pristineSchemas }

    let filterUserLayout (layout: Layout) : Layout =
        { Schemas = filterUserSchemas preload layout.Schemas
          SaveRestoredEntities = layout.SaveRestoredEntities }

    // If `None` cold rebuild is needed.
    let getCurrentVersion (transaction: DatabaseTransaction) (cancellationToken: CancellationToken) =
        task {
            try
                let! versionEntry =
                    transaction.System.State.FirstOrDefaultAsync((fun x -> x.Name = versionField), cancellationToken)

                let version =
                    match versionEntry with
                    | null -> None
                    | entry -> tryIntInvariant entry.Value

                return (transaction, version)
            with :? PostgresException when transaction.Connection.Connection.State = ConnectionState.Open ->
                // Most likely we couldn't execute the statement itself because there is no "state" table yet.
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                let! transaction = DatabaseTransaction.Begin(transaction.Connection)
                return (transaction, None)
        }

    let continueAndGetCurrentVersion (connection: DatabaseConnection) (cancellationToken: CancellationToken) =
        task {
            let! transaction = DatabaseTransaction.Begin(connection)
            return! getCurrentVersion transaction cancellationToken
        }

    let openAndGetCurrentVersion (cancellationToken: CancellationToken) : Task<DatabaseTransaction * int option> =
        openAndCheckTransaction
            cacheParams.LoggerFactory
            cacheParams.ConnectionString
            IsolationLevel.Serializable
            cancellationToken
            (fun ts -> getCurrentVersion ts cancellationToken)

    let ensureAndGetVersion
        (conn: DatabaseTransaction)
        (bump: bool)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            let! versionEntry =
                conn.System.State
                    .AsTracking()
                    .FirstOrDefaultAsync((fun x -> x.Name = versionField), cancellationToken)

            match versionEntry with
            | null ->
                let newEntry = StateValue(Name = versionField, Value = string fallbackVersion)
                ignore <| conn.System.State.Add(newEntry)
                let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                return fallbackVersion
            | entry ->
                match tryIntInvariant entry.Value with
                | Some v ->
                    if bump then
                        let newVersion = v + 1
                        entry.Value <- string newVersion
                        let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                        return newVersion
                    else
                        return v
                | None ->
                    entry.Value <- string fallbackVersion
                    let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                    return fallbackVersion
        }

    let getDatabaseVersion
        (transaction: DatabaseTransaction)
        (cancellationToken: CancellationToken)
        : Task<string option> =
        task {
            let! databaseEntry =
                transaction.System.State.FirstOrDefaultAsync((fun x -> x.Name = databaseField), cancellationToken)

            match databaseEntry with
            | null -> return None
            | entry -> return Some entry.Value
        }

    let ensureDatabaseVersion (conn: DatabaseTransaction) (cancellationToken: CancellationToken) : Task =
        task {
            let! databaseEntry =
                conn.System.State
                    .AsTracking()
                    .FirstOrDefaultAsync((fun x -> x.Name = databaseField), cancellationToken)

            match databaseEntry with
            | null ->
                let newEntry = StateValue(Name = databaseField, Value = currentDatabaseVersion)
                ignore <| conn.System.State.Add(newEntry)
                let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                ()
            | entry when entry.Value <> currentDatabaseVersion ->
                entry.Value <- currentDatabaseVersion
                let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                ()
            | _ -> ()
        }

    let makeJSEngines files =
        RuntimeLocal(fun runtime ->
            let env =
                { Files = files
                  SearchPath = seq { moduleDirectory } }

            OzmaJSEngine(runtime, env))

    let rec finishColdRebuild
        (transaction: DatabaseTransaction)
        (layout: Layout)
        (userMeta: SQL.DatabaseMeta)
        (isChanged: bool)
        (cancellationToken: CancellationToken)
        : Task<CachedState> =
        task {
            let runtime = jsRuntimes.Get()

            try
                let! (currentVersion,
                      jsEngine,
                      mergedAttrs,
                      triggers,
                      mergedTriggers,
                      permissions,
                      actions,
                      sourceViews,
                      userViews) =
                    task {
                        try
                            let systemViews = preloadUserViews preload
                            let! sourceViews = buildSchemaUserViews transaction.System None cancellationToken

                            let sourceViews =
                                { Schemas = Map.union systemViews.Schemas sourceViews.Schemas }: SourceUserViews

                            let! sourceModules = buildSchemaModules transaction.System None cancellationToken
                            let modules = resolveModules layout sourceModules true

                            let jsEngines = makeJSEngines (moduleFiles modules)
                            let jsEngine = jsEngines.GetValue runtime
                            let modules = resolvedLoadedModules modules jsEngine true cancellationToken

                            do!
                                checkBrokenModules
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    modules
                                    cancellationToken

                            let! sourceTriggers = buildSchemaTriggers transaction.System None cancellationToken
                            let triggers = resolveTriggers layout true sourceTriggers
                            let triggerScripts = prepareTriggers jsEngine true triggers cancellationToken
                            let mergedTriggers = mergeTriggers layout triggers

                            do!
                                checkBrokenTriggers
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    triggerScripts
                                    cancellationToken

                            let generatedViews =
                                generateUserViews jsEngine layout mergedTriggers true sourceViews cancellationToken

                            let sourceViews = generatedUserViewsSource sourceViews generatedViews
                            let! userViewsUpdate = updateUserViews transaction.System sourceViews cancellationToken

                            let! sourceAttrs = buildSchemaAttributes transaction.System None cancellationToken
                            let parsedAttrs = parseAttributes true sourceAttrs

                            let defaultAttrs =
                                resolveAttributes layout (generatedViews.Find >> Option.isSome) true parsedAttrs

                            let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                            do!
                                checkBrokenAttributes
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    defaultAttrs
                                    cancellationToken

                            let userViews = resolveUserViews layout mergedAttrs true generatedViews

                            let jsEngine = jsEngines.GetValue runtime

                            let! sourceActions = buildSchemaActions transaction.System None cancellationToken
                            let actions = resolveActions layout true sourceActions
                            let preparedActions = prepareActions jsEngine true actions cancellationToken

                            do!
                                checkBrokenActions
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    preparedActions
                                    cancellationToken

                            let! sourcePermissions = buildSchemaPermissions transaction.System None cancellationToken

                            let permissions =
                                resolvePermissions layout (generatedViews.Find >> Option.isSome) true sourcePermissions

                            do!
                                checkBrokenPermissions
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    permissions
                                    cancellationToken

                            do! deleteDeferredFromUpdate layout transaction userViewsUpdate cancellationToken

                            let! currentVersion =
                                ensureAndGetVersion
                                    transaction
                                    (isChanged || not (updateResultIsEmpty userViewsUpdate))
                                    cancellationToken

                            // To dry-run user views we need to stop the transaction.
                            let! _ = transaction.Commit(cancellationToken)

                            return
                                (currentVersion,
                                 jsEngines,
                                 mergedAttrs,
                                 triggers,
                                 mergedTriggers,
                                 permissions,
                                 actions,
                                 sourceViews,
                                 userViews)
                        with ex ->
                            do! transaction.Rollback()
                            return reraise' ex
                    }

                let! prefetchedViews =
                    dryRunUserViews
                        transaction.Connection.Query
                        layout
                        mergedTriggers
                        true
                        None
                        userViews
                        cancellationToken

                let! (transaction, newCurrentVersion) =
                    continueAndGetCurrentVersion transaction.Connection cancellationToken

                if newCurrentVersion <> Some currentVersion then
                    let! sourceLayout = buildFullSchemaLayout transaction.System preload cancellationToken
                    let layout = resolveLayout false sourceLayout
                    let! userMeta = buildUserDatabaseMeta transaction preload cancellationToken
                    return! finishColdRebuild transaction layout userMeta false cancellationToken
                else
                    try
                        do!
                            checkBrokenUserViews
                                logger
                                cacheParams.AllowAutoMark
                                preload
                                transaction
                                prefetchedViews
                                cancellationToken

                        let! _ = transaction.Commit(cancellationToken)

                        let systemViews = filterSystemViews sourceViews
                        let domains = buildLayoutDomains layout

                        let state =
                            { Layout = layout
                              Permissions = permissions
                              DefaultAttrs = mergedAttrs
                              JSEngine = jsEngine
                              Actions = actions
                              ActionScripts =
                                RuntimeLocal(fun runtime ->
                                    prepareActions (jsEngine.GetValue runtime) false actions cancellationToken)
                              Triggers = mergedTriggers
                              TriggerScripts =
                                RuntimeLocal(fun runtime ->
                                    prepareTriggers (jsEngine.GetValue runtime) false triggers cancellationToken)
                              UserViews = prefetchedViews
                              Domains = domains
                              SystemViews = systemViews
                              UserMeta = userMeta }

                        return
                            { Version = currentVersion
                              Context = state }
                    with ex ->
                        do! transaction.Rollback()
                        return reraise' ex
            finally
                jsRuntimes.Return runtime
        }

    let rec getMigrationLock (transaction: DatabaseTransaction) (cancellationToken: CancellationToken) : Task<bool> =
        task {
            // Try to get a lock. If we fail, wait till someone else releases it and then _restart the transaction and try again_.
            // This is because otherwise transaction gets to see older state of the database.
            let! ret =
                task {
                    try
                        return!
                            transaction.Connection.Query.ExecuteValueQuery
                                "SELECT pg_try_advisory_xact_lock(@0)"
                                migrationLockParams
                                cancellationToken
                    with ex ->
                        do! transaction.Rollback()
                        return reraise' ex
                }

            match ret with
            | None -> return failwith "Impossible"
            | Some(_, _, SQL.VBool true) -> return true
            | _ ->
                try
                    let! _ =
                        transaction.Connection.Query.ExecuteNonQuery
                            "SELECT pg_advisory_xact_lock(@0)"
                            migrationLockParams
                            cancellationToken

                    ()
                with ex ->
                    do! transaction.Rollback()
                    reraise' ex

                do! transaction.Rollback()
                return false
        }

    let coldRebuildFromDatabase
        (transaction: DatabaseTransaction)
        (cancellationToken: CancellationToken)
        : Task<CachedState option> =
        task {
            logger.LogInformation("Starting cold rebuild")

            match! getMigrationLock transaction cancellationToken with
            | false -> return None
            | true ->
                let! (userMeta, layout, isChanged) =
                    task {
                        try
                            let! (isChanged, layout, userMeta) =
                                initialMigratePreload
                                    logger
                                    cacheParams.AllowAutoMark
                                    preload
                                    transaction
                                    cancellationToken

                            do! ensureDatabaseVersion transaction cancellationToken
                            return (userMeta, layout, isChanged)
                        with ex ->
                            do! transaction.Rollback()
                            return reraise' ex
                    }

                let! ret = finishColdRebuild transaction layout userMeta isChanged cancellationToken
                return Some ret
        }

    // Called when state update by another instance is detected. More lightweight than full `coldRebuildFromDatabase`.
    let rec rebuildFromDatabase
        (transaction: DatabaseTransaction)
        (currentVersion: int)
        (cancellationToken: CancellationToken)
        : Task<CachedState option> =
        task {
            let! (layout, mergedAttrs, triggers, mergedTriggers, sourceUvs, userViews) =
                task {
                    try
                        let! sourceLayout = buildFullSchemaLayout transaction.System preload cancellationToken
                        let layout = resolveLayout false sourceLayout


                        let! sourceTriggers = buildSchemaTriggers transaction.System None cancellationToken
                        let triggers = resolveTriggers layout false sourceTriggers
                        let mergedTriggers = mergeTriggers layout triggers

                        let! sourceUvs = buildSchemaUserViews transaction.System None cancellationToken

                        let! sourceAttrs = buildSchemaAttributes transaction.System None cancellationToken
                        let parsedAttrs = parseAttributes false sourceAttrs

                        let defaultAttrs =
                            resolveAttributes layout (sourceUvs.Find >> Option.isSome) false parsedAttrs

                        let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                        let userViews =
                            resolveUserViews layout mergedAttrs false (passthruGeneratedUserViews sourceUvs)

                        // To dry-run user views we need to stop the transaction.
                        let! _ = transaction.Rollback()
                        // `forceAllowBroken` is set to `true` to overcome potential race condition between this call and `forceAllowBroken` update during migration.
                        return (layout, mergedAttrs, triggers, mergedTriggers, sourceUvs, userViews)
                    with ex ->
                        do! transaction.Rollback()
                        return reraise' ex
                }

            let! prefetchedViews =
                dryRunUserViews transaction.Connection.Query layout mergedTriggers true None userViews cancellationToken

            let! (transaction, newCurrentVersion) =
                continueAndGetCurrentVersion transaction.Connection cancellationToken

            if newCurrentVersion <> Some currentVersion then
                match newCurrentVersion with
                | None -> return! coldRebuildFromDatabase transaction cancellationToken
                | Some ver -> return! rebuildFromDatabase transaction ver cancellationToken
            else
                try
                    do!
                        checkBrokenUserViews
                            logger
                            cacheParams.AllowAutoMark
                            preload
                            transaction
                            prefetchedViews
                            cancellationToken

                    let! sourceModules = buildSchemaModules transaction.System None cancellationToken
                    let! sourceActions = buildSchemaActions transaction.System None cancellationToken
                    let! sourcePermissions = buildSchemaPermissions transaction.System None cancellationToken
                    let! userMeta = buildUserDatabaseMeta transaction preload cancellationToken

                    let! _ = transaction.Commit cancellationToken

                    let actions = resolveActions layout false sourceActions

                    let modules = resolveModules layout sourceModules false
                    let jsEngines = makeJSEngines (moduleFiles modules)

                    let actionScripts =
                        RuntimeLocal(fun runtime ->
                            prepareActions (jsEngines.GetValue runtime) false actions cancellationToken)

                    let triggerScripts =
                        RuntimeLocal(fun runtime ->
                            prepareTriggers (jsEngines.GetValue runtime) false triggers cancellationToken)

                    do
                        let myIsolate = jsRuntimes.Get()

                        try
                            ignore <| actionScripts.GetValue myIsolate
                            ignore <| triggerScripts.GetValue myIsolate
                            ()
                        finally
                            jsRuntimes.Return myIsolate

                    let permissions =
                        resolvePermissions
                            layout
                            (prefetchedViews.Find >> Option.bind Result.getOption >> Option.isSome)
                            false
                            sourcePermissions

                    // Another instance has already rebuilt them, so just load them from the database.
                    let systemViews = filterSystemViews sourceUvs
                    let domains = buildLayoutDomains layout

                    let newState =
                        { Layout = layout
                          Permissions = permissions
                          DefaultAttrs = mergedAttrs
                          JSEngine = jsEngines
                          Actions = actions
                          ActionScripts = actionScripts
                          Triggers = mergedTriggers
                          TriggerScripts = triggerScripts
                          UserViews = prefetchedViews
                          Domains = domains
                          SystemViews = systemViews
                          UserMeta = userMeta }

                    return
                        Some
                            { Version = currentVersion
                              Context = newState }
                with ex ->
                    do! transaction.Rollback()
                    return reraise' ex
        }

    let mutable cachedState: CachedState option = None
    // Used to detect simultaneous migrations.
    let cachedStateLock = new SemaphoreSlim 1

    // Run a function that atomically updates database state and release a connection.
    let updateStateAndRelease
        (transaction: DatabaseTransaction)
        (cancellationToken: CancellationToken)
        (next: unit -> Task)
        : Task<bool> =
        task {
            use _ = transaction
            use _ = transaction.Connection
            let! success = cachedStateLock.WaitAsync(0)

            if not success then
                do! cachedStateLock.WaitAsync(cancellationToken)
                ignore <| cachedStateLock.Release()
                return false
            else
                try
                    do! next ()
                    return true
                finally
                    ignore <| cachedStateLock.Release()
        }

    let rec getCachedState (cancellationToken: CancellationToken) : Task<DatabaseConnection * CachedState> =
        task {
            let! (transaction, ret) = openAndGetCurrentVersion cancellationToken

            match (cachedState, ret) with
            | (None, Some ver) ->
                let! _ =
                    updateStateAndRelease transaction cancellationToken
                    <| fun () ->
                        task {
                            let! databaseVersion = getDatabaseVersion transaction cancellationToken

                            if databaseVersion = Some currentDatabaseVersion then
                                match! rebuildFromDatabase transaction ver cancellationToken with
                                | None -> ()
                                | Some newState -> cachedState <- Some newState
                            else
                                match! coldRebuildFromDatabase transaction cancellationToken with
                                | None -> ()
                                | Some newState -> cachedState <- Some newState
                        }

                return! getCachedState cancellationToken
            | (Some oldState, Some ver) ->
                if oldState.Version <> ver then
                    let! _ =
                        updateStateAndRelease transaction cancellationToken
                        <| fun () ->
                            task {
                                transaction.Connection.Connection.UnprepareAll()

                                match! rebuildFromDatabase transaction ver cancellationToken with
                                | None -> ()
                                | Some newState ->
                                    clearCaches ()
                                    cachedState <- Some newState
                            }

                    return! getCachedState cancellationToken
                else
                    do! (transaction :> IAsyncDisposable).DisposeAsync()
                    return (transaction.Connection, oldState)
            | (_, None)
            | (None, _) ->
                let! _ =
                    updateStateAndRelease transaction cancellationToken
                    <| fun () ->
                        task {
                            transaction.Connection.Connection.UnprepareAll()

                            match! coldRebuildFromDatabase transaction cancellationToken with
                            | None -> ()
                            | Some newState ->
                                clearCaches ()
                                cachedState <- Some newState
                        }

                return! getCachedState cancellationToken
        }

    member this.LoggerFactory = cacheParams.LoggerFactory
    member this.Preload = preload
    member this.EventLogger = cacheParams.EventLogger
    member this.ConnectionString = cacheParams.ConnectionString

    member this.JSRuntimeParams
        with get () = jsRuntimeParams
        and set v =
            if v <> jsRuntimeParams then
                jsRuntimeParams <- v

    member this.GetCache(initialCancellationToken: CancellationToken) =
        task {
            // We get the cached state in a separate transaction to minimize locking.
            // This can lead to races with database migrations, but it should only lead to
            // rare errors and no permission issues.
            let! (connection, oldState) = getCachedState initialCancellationToken
            let! transaction = DatabaseTransaction.Begin(connection)

            try
                let mutable isDisposed = false

                let runtime = lazy (jsRuntimes.Get())

                let mutable forceAllowBroken = false

                let mutable scheduledBeforeCommit =
                    OrderedMap.empty: OrderedMap<string, Layout -> Task<Result<unit, GenericErrorInfo>>>

                let runCommitCallbacks layout =
                    task {
                        for cb in OrderedMap.values scheduledBeforeCommit do
                            match! cb layout with
                            | Ok() -> ()
                            | Error e -> raise <| ContextException(e)
                    }

                let mutable cancellationToken = initialCancellationToken
                let mutable commitCancellationToken = initialCancellationToken

                let migrate () =
                    task {
                        let! localSuccess = cachedStateLock.WaitAsync(0)

                        if not localSuccess then
                            raise <| ContextException(GEMigrationConflict)

                        try
                            match!
                                transaction.Connection.Query.ExecuteValueQuery
                                    "SELECT pg_try_advisory_xact_lock(@0)"
                                    migrationLockParams
                                    cancellationToken
                            with
                            | None -> failwith "Impossible"
                            | Some(lockName, lockRet, SQL.VBool true) -> ()
                            | _ -> raise <| ContextException(GEMigrationConflict)

                            logger.LogInformation("Starting migration")
                            // Careful here not to evaluate user views before we do migration.

                            let! sourceLayout = buildSchemaLayout transaction.System None cancellationToken
                            let sourceLayout = applyHiddenLayoutData sourceLayout (preloadLayout preload)

                            if not <| preloadLayoutIsUnchanged sourceLayout preload then
                                raise <| ContextException(GEMigration("Cannot modify preloaded layout"))

                            let layout =
                                try
                                    resolveLayout forceAllowBroken sourceLayout
                                with :? ResolveLayoutException as e when e.IsUserException ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve layout: " + fullUserMessage e),
                                        e
                                    )

                            if forceAllowBroken then
                                do! checkBrokenLayout logger true preload transaction layout cancellationToken

                            // Actually migrate.
                            let (newAssertions, wantedUserMeta) =
                                buildFullLayoutMeta layout (filterUserLayout layout)

                            let migration = planDatabaseMigration oldState.Context.UserMeta wantedUserMeta

                            try
                                do! migrateDatabase transaction.Connection.Query migration cancellationToken
                            with :? QueryExecutionException as e ->
                                raise
                                <| ContextException(
                                    GEMigration("Failed to update the database: " + fullUserMessage e),
                                    e
                                )

                            let oldAssertions =
                                buildAssertions oldState.Context.Layout (filterUserLayout oldState.Context.Layout)

                            let addedAssertions = differenceLayoutAssertions newAssertions oldAssertions

                            try
                                do!
                                    checkAssertions
                                        transaction.Connection.Query
                                        layout
                                        addedAssertions
                                        cancellationToken
                            with :? LayoutIntegrityException as e ->
                                raise <| ContextException(GEMigration(fullUserMessage e), e)

                            let! sourceModules = buildSchemaModules transaction.System None cancellationToken

                            if not <| preloadModulesAreUnchanged sourceModules preload then
                                raise <| ContextException(GEMigration("Cannot modify preloaded modules"))

                            let modules =
                                try
                                    resolveModules layout sourceModules false
                                with :? ResolveModulesException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve modules: " + fullUserMessage e),
                                        e
                                    )

                            let jsEngines = makeJSEngines (moduleFiles modules)

                            let jsEngine =
                                try
                                    jsEngines.GetValue runtime.Value
                                with :? JavaScriptRuntimeException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve modules: " + fullUserMessage e),
                                        e
                                    )

                            let! sourceActions = buildSchemaActions transaction.System None cancellationToken

                            if not <| preloadActionsAreUnchanged sourceActions preload then
                                raise <| ContextException(GEMigration("Cannot modify preloaded actions"))

                            let actions =
                                try
                                    resolveActions layout forceAllowBroken sourceActions
                                with :? ResolveActionsException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve actions: " + fullUserMessage e),
                                        e
                                    )

                            let preparedActions =
                                try
                                    prepareActions jsEngine forceAllowBroken actions cancellationToken
                                with :? ActionRunException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve actions: " + fullUserMessage e),
                                        e
                                    )

                            if forceAllowBroken then
                                do! checkBrokenActions logger true preload transaction preparedActions cancellationToken

                            let! sourceTriggers = buildSchemaTriggers transaction.System None cancellationToken

                            if not <| preloadTriggersAreUnchanged sourceTriggers preload then
                                raise <| ContextException(GEMigration("Cannot modify preloaded triggers"))

                            let triggers =
                                try
                                    resolveTriggers layout forceAllowBroken sourceTriggers
                                with :? ResolveTriggersException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve triggers: " + fullUserMessage e),
                                        e
                                    )

                            let preparedTriggers =
                                try
                                    prepareTriggers jsEngine forceAllowBroken triggers cancellationToken
                                with :? TriggerRunException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve triggers: " + fullUserMessage e),
                                        e
                                    )

                            let mergedTriggers = mergeTriggers layout triggers

                            if forceAllowBroken then
                                do!
                                    checkBrokenTriggers
                                        logger
                                        true
                                        preload
                                        transaction
                                        preparedTriggers
                                        cancellationToken

                            let! sourceUserViews = buildSchemaUserViews transaction.System None cancellationToken

                            if filterSystemViews sourceUserViews <> oldState.Context.SystemViews then
                                raise <| ContextException(GEMigration("Cannot modify preloaded user views"))

                            logger.LogInformation("Updating generated user views")

                            let generatedUserViews =
                                try
                                    generateUserViews
                                        jsEngine
                                        layout
                                        mergedTriggers
                                        forceAllowBroken
                                        sourceUserViews
                                        cancellationToken
                                with :? UserViewGenerateException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to generate user views: " + fullUserMessage e),
                                        e
                                    )

                            let! userViewsUpdate =
                                updateUserViews
                                    transaction.System
                                    (generatedUserViewsSource sourceUserViews generatedUserViews)
                                    cancellationToken

                            do! deleteDeferredFromUpdate layout transaction userViewsUpdate cancellationToken

                            let! sourceAttrs = buildSchemaAttributes transaction.System None cancellationToken

                            if not <| preloadAttributesAreUnchanged sourceAttrs preload then
                                raise
                                <| ContextException(GEMigration("Cannot modify preloaded default attributes"))

                            let parsedAttrs =
                                try
                                    parseAttributes forceAllowBroken sourceAttrs
                                with :? ParseAttributesException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve default attributes: " + fullUserMessage e),
                                        e
                                    )

                            let defaultAttrs =
                                try
                                    resolveAttributes
                                        layout
                                        (generatedUserViews.Find >> Option.isSome)
                                        forceAllowBroken
                                        parsedAttrs
                                with :? ResolveAttributesException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve default attributes: " + fullUserMessage e),
                                        e
                                    )

                            let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                            if forceAllowBroken then
                                do! checkBrokenAttributes logger true preload transaction defaultAttrs cancellationToken

                            let userViews =
                                try
                                    resolveUserViews layout mergedAttrs forceAllowBroken generatedUserViews
                                with :? UserViewResolveException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve user views: " + fullUserMessage e),
                                        e
                                    )

                            let! sourcePermissions = buildSchemaPermissions transaction.System None cancellationToken

                            if not <| preloadPermissionsAreUnchanged sourcePermissions preload then
                                raise <| ContextException(GEMigration("Cannot modify preloaded permissions"))

                            let permissions =
                                try
                                    resolvePermissions
                                        layout
                                        (userViews.Find >> Option.bind Result.getOption >> Option.isSome)
                                        forceAllowBroken
                                        sourcePermissions
                                with :? ResolvePermissionsException as e ->
                                    raise
                                    <| ContextException(
                                        GEMigration("Failed to resolve permissions: " + fullUserMessage e),
                                        e
                                    )

                            if forceAllowBroken then
                                do! checkBrokenPermissions logger true preload transaction permissions cancellationToken

                            let! checkedPrefetchedUserViews =
                                task {
                                    if forceAllowBroken then
                                        return emptyPrefetchedUserViews
                                    else
                                        try
                                            return!
                                                dryRunUserViews
                                                    transaction.Connection.Query
                                                    layout
                                                    mergedTriggers
                                                    false
                                                    (Some false)
                                                    userViews
                                                    cancellationToken
                                        with :? UserViewDryRunException as e ->
                                            return
                                                raise
                                                <| ContextException(
                                                    GEMigration("Failed to resolve user views: " + fullUserMessage e),
                                                    e
                                                )
                                }

                            do! runCommitCallbacks layout

                            // We update state now and check user views _after_ that.
                            // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                            let! newVersion = ensureAndGetVersion transaction true cancellationToken

                            let! _ =
                                transaction.System.State
                                    .AsQueryable()
                                    .Where(fun x -> x.Name = versionField)
                                    .ExecuteUpdateAsync(
                                        (fun x -> x.SetProperty((fun x -> x.Value), (fun x -> string newVersion))),
                                        cancellationToken
                                    )

                            try
                                let! _ = transaction.Commit(cancellationToken)
                                ()
                            with :? DbUpdateException as e ->
                                raise <| ContextException(GEMigration(fullUserMessage e), e)

                            let! prefetchedUserViews =
                                task {
                                    if forceAllowBroken then
                                        let! prefetchedViews =
                                            dryRunUserViews
                                                transaction.Connection.Query
                                                layout
                                                mergedTriggers
                                                true
                                                None
                                                userViews
                                                cancellationToken

                                        let! (transaction, newVersion) =
                                            continueAndGetCurrentVersion transaction.Connection cancellationToken
                                        // In the rare event we have a race condition, don't mark user views as broken -- something might have changed.
                                        // They will be marked later by some other instance.
                                        if newVersion = Some oldState.Version then
                                            do!
                                                checkBrokenUserViews
                                                    logger
                                                    true
                                                    preload
                                                    transaction
                                                    prefetchedViews
                                                    cancellationToken

                                        return prefetchedViews
                                    else
                                        let! uncheckedViews =
                                            dryRunUserViews
                                                transaction.Connection.Query
                                                layout
                                                mergedTriggers
                                                false
                                                (Some true)
                                                userViews
                                                cancellationToken

                                        return mergePrefetchedUserViews checkedPrefetchedUserViews uncheckedViews
                                }

                            let domains = buildLayoutDomains layout

                            let newState =
                                { Layout = layout
                                  Permissions = permissions
                                  DefaultAttrs = mergedAttrs
                                  JSEngine = jsEngines
                                  Actions = actions
                                  ActionScripts =
                                    RuntimeLocal(fun runtime ->
                                        prepareActions (jsEngines.GetValue runtime) false actions cancellationToken)
                                  Triggers = mergedTriggers
                                  TriggerScripts =
                                    RuntimeLocal(fun runtime ->
                                        prepareTriggers (jsEngines.GetValue runtime) false triggers cancellationToken)
                                  UserViews = prefetchedUserViews
                                  Domains = domains
                                  SystemViews = filterSystemViews sourceUserViews
                                  UserMeta = wantedUserMeta }

                            clearCaches ()

                            if not <| Array.isEmpty migration then
                                // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                                NpgsqlConnection.ClearPool(transaction.Connection.Connection)

                            cachedState <-
                                Some
                                    { Version = newVersion
                                      Context = newState }
                        finally
                            ignore <| cachedStateLock.Release()
                    }

                let resolveAnonymousView (isPrivileged: bool) (homeSchema: SchemaName option) (query: string) =
                    task {
                        let findExistingView =
                            oldState.Context.UserViews.Find
                            >> Option.map (Result.map (fun pref -> pref.UserView))

                        let uv =
                            resolveAnonymousUserView
                                oldState.Context.Layout
                                isPrivileged
                                oldState.Context.DefaultAttrs
                                findExistingView
                                homeSchema
                                query

                        return!
                            dryRunAnonymousUserView
                                transaction.Connection.Query
                                oldState.Context.Layout
                                oldState.Context.Triggers
                                uv
                                cancellationToken
                    }

                let getAnonymousView (isPrivileged: bool) (query: string) : Task<PrefetchedUserView> =
                    task {
                        let createNew query =
                            task {
                                let! uv = resolveAnonymousView isPrivileged None query
                                let ret = { UserView = uv; Query = query }
                                return ret
                            }

                        let! ret = anonymousViewsIndex.GetItem(query, ItemCreator(createNew))

                        if not isPrivileged && ret.UserView.UserView.Resolved.Privileged then
                            // Just to throw the exception properly.
                            createNew query |> ignore

                        return ret.UserView
                    }

                let getAnonymousCommand (isPrivileged: bool) (query: string) : Task<CompiledCommandExpr> =
                    task {
                        let createNew query =
                            task {
                                let resolved = resolveCommand oldState.Context.Layout isPrivileged query
                                let compiled = compileCommandExpr oldState.Context.Layout resolved

                                let ret =
                                    { Command = compiled
                                      Privileged = resolved.Privileged
                                      Query = query }

                                return ret
                            }

                        let! ret = anonymousCommandsIndex.GetItem(query, ItemCreator(createNew))

                        if not isPrivileged && ret.Privileged then
                            // Just to throw the exception properly.
                            createNew query |> ignore

                        return ret.Command
                    }


                let mutable needMigration = false

                let commit () =
                    task {
                        cancellationToken <- commitCancellationToken

                        try
                            if needMigration then
                                do! migrate ()
                                return Ok()
                            else
                                do! runCommitCallbacks oldState.Context.Layout
                                let! _ = transaction.Commit(cancellationToken)
                                return Ok()
                        with
                        | :? ContextException as e ->
                            logger.LogError(e, "Error during commit")
                            return Error <| GECommit(e.Details)
                        | :? DbUpdateException as e ->
                            logger.LogError(e, "Error during commit")
                            return Error <| GECommit(GEOther(fullUserMessage e))
                    }

                let checkIntegrity () : Task =
                    task {
                        let assertions =
                            buildAssertions oldState.Context.Layout (filterUserLayout oldState.Context.Layout)

                        try
                            do!
                                checkAssertions
                                    transaction.Connection.Query
                                    oldState.Context.Layout
                                    assertions
                                    cancellationToken
                        with :? LayoutIntegrityException as e ->
                            raise
                            <| ContextException(GEOther("Failed to perform integrity checks: " + fullUserMessage e))
                    }

                let mutable maybeApi = None

                let setAPI api =
                    match maybeApi with
                    | Some oldApi -> failwith "Cannot set API more than once"
                    | None -> maybeApi <- Some api

                let jsEngine =
                    lazy
                        (let jsEngine = oldState.Context.JSEngine.GetValue runtime.Value
                         jsEngine.SetAPI(Option.get maybeApi)
                         jsEngine)

                let actionScripts =
                    lazy
                        (
                         // To set the API.
                         ignore <| jsEngine.Force()
                         oldState.Context.ActionScripts.GetValue runtime.Value)

                let triggerScripts =
                    lazy
                        (
                         // To set the API.
                         ignore <| jsEngine.Force()
                         oldState.Context.TriggerScripts.GetValue runtime.Value)

                let! systemInfo =
                    transaction.Connection.Query.ExecuteRowValuesQuery
                        "SELECT transaction_timestamp(), txid_current()"
                        Map.empty
                        cancellationToken

                let (transactionTime, transactionId) =
                    match systemInfo with
                    | Some [| (tsName, tsTyp, SQL.VDateTime ts); (idName, idTyp, SQL.VBigInt txid) |] -> (ts, int txid)
                    | _ -> failwith "Impossible"

                return
                    { new IContext with
                        member this.Transaction = transaction
                        member this.TransactionId = transactionId
                        member this.TransactionTime = transactionTime
                        member this.LoggerFactory = cacheParams.LoggerFactory
                        member this.Preload = preload
                        member this.Engine = jsEngine.Value

                        member this.CancellationToken
                            with get () = cancellationToken
                            and set value = cancellationToken <- value

                        member this.CommitCancellationToken
                            with get () = commitCancellationToken
                            and set value = commitCancellationToken <- value

                        member this.Layout = oldState.Context.Layout
                        member this.UserViews = oldState.Context.UserViews
                        member this.Permissions = oldState.Context.Permissions
                        member this.DefaultAttrs = oldState.Context.DefaultAttrs
                        member this.Triggers = oldState.Context.Triggers
                        member this.Domains = oldState.Context.Domains

                        member this.Commit() = commit ()
                        member this.ScheduleMigration() = needMigration <- true

                        member this.ScheduleBeforeCommit name cb =
                            if not <| OrderedMap.containsKey name scheduledBeforeCommit then
                                scheduledBeforeCommit <- OrderedMap.add name cb scheduledBeforeCommit

                        member this.SetForceAllowBroken() = forceAllowBroken <- true

                        member this.CheckIntegrity() = checkIntegrity ()
                        member this.GetAnonymousView isPrivileged query = getAnonymousView isPrivileged query
                        member this.GetAnonymousCommand isPrivileged query = getAnonymousCommand isPrivileged query

                        member this.ResolveAnonymousView isPrivileged homeSchema query =
                            resolveAnonymousView isPrivileged homeSchema query

                        member this.WriteEvent event =
                            cacheParams.EventLogger.WriteEvent(cacheParams.ConnectionString, event)

                        member this.SetAPI api = setAPI api

                        member this.FindAction ref =
                            actionScripts.Value.FindAction ref
                            |> Option.map (Result.mapError (fun err -> err.Error))

                        member this.FindTrigger ref = triggerScripts.Value.FindTrigger ref

                        member this.Dispose() =
                            if not isDisposed then
                                if jsEngine.IsValueCreated then
                                    jsEngine.Value.ResetAPI()

                                if runtime.IsValueCreated then
                                    jsRuntimes.Return runtime.Value

                                (transaction :> IDisposable).Dispose()
                                (transaction.Connection :> IDisposable).Dispose()
                                isDisposed <- true

                        member this.DisposeAsync() =
                            task {
                                if not isDisposed then
                                    if jsEngine.IsValueCreated then
                                        jsEngine.Value.ResetAPI()

                                    if runtime.IsValueCreated then
                                        jsRuntimes.Return runtime.Value

                                    do! (transaction :> IAsyncDisposable).DisposeAsync()
                                    do! (transaction.Connection :> IAsyncDisposable).DisposeAsync()
                                    isDisposed <- true
                            }
                            |> ValueTask }
            with e ->
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                do! (transaction.Connection :> IAsyncDisposable).DisposeAsync()
                return reraise' e
        }
