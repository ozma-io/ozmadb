module FunWithFlags.FunDB.API.ContextCache

open System
open System.Security.Cryptography
open System.Reflection
open System.Data
open System.IO
open System.Linq
open Z.EntityFramework.Plus
open Microsoft.EntityFrameworkCore
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.ObjectPool
open FluidCaching
open Npgsql
open FSharp.Control.Tasks.Affine
open NetJs

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Parsing
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.EventLogger
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Attributes.Schema
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Schema
open FunWithFlags.FunDB.Triggers.Resolve
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Modules.Types
open FunWithFlags.FunDB.Modules.Schema
open FunWithFlags.FunDB.Modules.Resolve
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.Actions.Schema
open FunWithFlags.FunDB.Actions.Resolve
open FunWithFlags.FunDB.Actions.Run
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.UserViews.Update
open FunWithFlags.FunDB.UserViews.Schema
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.UserViews.Generate
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.Layout.Integrity
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.Command
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.JavaScript
open FunWithFlags.FunDB.API.Triggers

type ContextException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ContextException (message, innerException, isUserException innerException)

    new (message : string) = ContextException (message, null, true)

[<NoEquality; NoComparison>]
type private AnonymousUserView =
    { Query : string
      UserView : PrefetchedUserView
    }

[<NoEquality; NoComparison>]
type private AnonymousCommand =
    { Query : string
      Privileged : bool
      Command : CompiledCommandExpr
    }

let private databaseField = "DatabaseVersion"
let private versionField = "StateVersion"
let private fallbackVersion = 0
let private migrationLockNumber = 0
let private migrationLockParams = Map.singleton 0 (SQL.VInt migrationLockNumber)

let private assemblyHash =
    lazy
        use hasher = SHA1.Create()
        use assembly = File.OpenRead(Assembly.GetExecutingAssembly().Location)
        hasher.ComputeHash(assembly) |> Hash.sha1OfBytes |> String.hexBytes

[<NoEquality; NoComparison>]
type private CachedContext =
    { Layout : Layout
      UserViews : PrefetchedUserViews
      Permissions : Permissions
      DefaultAttrs : MergedDefaultAttributes
      Domains : LayoutDomains
      Actions : ResolvedActions
      JSRuntime : IsolateLocal<JSRuntime<APITemplate>>
      ActionScripts : IsolateLocal<ActionScripts>
      Triggers : MergedTriggers
      TriggerScripts : IsolateLocal<TriggerScripts>
      SystemViews : SourceUserViews
      UserMeta : SQL.DatabaseMeta
    }

[<NoEquality; NoComparison>]
type private CachedState =
    { Version : int
      Context : CachedContext
    }

let instanceIsInitialized (conn : DatabaseTransaction) =
    task {
        use pg = createPgCatalogContext conn.Transaction
        let! stateCount = pg.Classes.CountAsync(fun cl -> cl.RelName = "state" && cl.RelKind = 'r' && cl.Namespace.NspName = string funSchema)
        return stateCount > 0
    }

[<NoEquality; NoComparison>]
type ContextCacheParams =
    { LoggerFactory : ILoggerFactory
      // Whether to allow automatically marking objects as broken.
      AllowAutoMark : bool
      Preload : HashedPreload
      ConnectionString : string
      EventLogger : EventLogger
    }

type ContextCacheStore (cacheParams : ContextCacheParams) =
    let preload = cacheParams.Preload.Preload
    let logger = cacheParams.LoggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values. Also we want to move these somewhere else, too much logic in this class.
    let anonymousViewsCache = FluidCache<AnonymousUserView>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousViewsIndex = anonymousViewsCache.AddIndex("byQuery", fun uv -> uv.Query)

    let anonymousCommandsCache = FluidCache<AnonymousCommand>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousCommandsIndex = anonymousCommandsCache.AddIndex("byQuery", fun uv -> uv.Query)

    let clearCaches () =
        anonymousViewsCache.Clear()
        anonymousCommandsCache.Clear()

    let currentDatabaseVersion = sprintf "%s %s" (assemblyHash.Force()) cacheParams.Preload.Hash

    let jsIsolates =
        let policy =
            { new IPooledObjectPolicy<Isolate> with
                  member this.Create () =
                      let isolate = Isolate.NewWithHeapSize(1UL * 1024UL * 1024UL, 32UL * 1024UL * 1024UL)
                      isolate.TerminateOnException <- true
                      isolate
                  member this.Return isolate =
                      assert (isNull isolate.CurrentContext)
                      not isolate.WasNearHeapLimit
            }
        DefaultObjectPool(policy, Environment.ProcessorCount)

    let filterSystemViews (views : SourceUserViews) : SourceUserViews =
        { Schemas = filterPreloadedSchemas preload views.Schemas }

    let filterUserLayout (layout : Layout) : Layout =
        { Schemas = filterUserSchemas preload layout.Schemas }

    // If `None` cold rebuild is needed.
    let openAndGetCurrentVersion (cancellationToken : CancellationToken) : Task<DatabaseTransaction * int option> =
        task {
            let! (transaction, versionEntry) =
                openAndCheckTransaction cacheParams.LoggerFactory cacheParams.ConnectionString IsolationLevel.Serializable cancellationToken <| fun transaction ->
                    task {
                        try
                            let! versionEntry = transaction.System.State.FirstOrDefaultAsync((fun x -> x.Name = versionField), cancellationToken)
                            return (transaction, versionEntry)
                        with
                        | :? PostgresException when transaction.Connection.Connection.State = ConnectionState.Open ->
                            // Most likely we couldn't execute the statement itself because there is no "state" table yet.
                            do! (transaction :> IAsyncDisposable).DisposeAsync ()
                            let transaction = new DatabaseTransaction (transaction.Connection)
                            return (transaction, null)
                    }
            match versionEntry with
            | null -> return (transaction, None)
            | entry -> return (transaction, tryIntInvariant entry.Value)
        }

    let ensureCurrentVersion (conn : DatabaseTransaction) (bump : bool) (cancellationToken : CancellationToken) : Task<int> =
        task {
                let! versionEntry = conn.System.State.AsTracking().FirstOrDefaultAsync((fun x -> x.Name = versionField), cancellationToken)
                match versionEntry with
                | null ->
                    let newEntry = StateValue (Name = versionField, Value = string fallbackVersion)
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

    let getDatabaseVersion (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<string option> =
        task {
            let! databaseEntry = transaction.System.State.FirstOrDefaultAsync((fun x -> x.Name = databaseField), cancellationToken)
            match databaseEntry with
            | null -> return None
            | entry -> return Some entry.Value
        }

    let ensureDatabaseVersion (conn : DatabaseTransaction) (cancellationToken : CancellationToken) : Task =
        unitTask {
            let! databaseEntry = conn.System.State.AsTracking().FirstOrDefaultAsync((fun x -> x.Name = databaseField), cancellationToken)
            match databaseEntry with
            | null ->
                let newEntry = StateValue (Name = databaseField, Value = currentDatabaseVersion)
                ignore <| conn.System.State.Add(newEntry)
                let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                ()
            | entry when entry.Value <> currentDatabaseVersion ->
                entry.Value <- currentDatabaseVersion
                let! _ = conn.SystemSaveChangesAsync(cancellationToken)
                ()
            | _ -> ()
        }

    let generateViews runtime layout userViews cancellationToken forceAllowBroken =
        let generator = UserViewsGenerator(runtime, userViews, forceAllowBroken)
        generator.GenerateUserViews layout cancellationToken forceAllowBroken

    let makeRuntime files = IsolateLocal(fun isolate ->
        let env =
            { Files = files
              SearchPath = seq { moduleDirectory }
            }
        JSRuntime(isolate, APITemplate, env)
    )

    let rec finishColdRebuild (transaction : DatabaseTransaction) (layout : Layout) (userMeta : SQL.DatabaseMeta) (isChanged : bool) (cancellationToken : CancellationToken) : Task<CachedState> = task {
        let isolate = jsIsolates.Get()
        try
            let! (transaction, currentVersion, jsRuntime, mergedAttrs, brokenAttrs, brokenViews, prefetchedViews, sourceViews) =
                task {
                    try
                        let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                        let (brokenAttrs, defaultAttrs) = resolveAttributes layout true sourceAttrs
                        let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                        let systemViews = preloadUserViews preload
                        let! sourceViews = buildSchemaUserViews transaction.System cancellationToken
                        let sourceViews = { Schemas = Map.union sourceViews.Schemas systemViews.Schemas } : SourceUserViews

                        let! sourceModules = buildSchemaModules transaction.System cancellationToken
                        let modules = resolveModules layout sourceModules

                        let jsRuntime = makeRuntime (moduleFiles modules)
                        let jsApi = jsRuntime.GetValue isolate
                        let (brokenViews, sourceViews) = generateViews jsApi layout sourceViews cancellationToken true
                        let! userViewsUpdate = updateUserViews transaction.System sourceViews cancellationToken
                        do! deleteDeferredFromUpdate layout transaction userViewsUpdate cancellationToken

                        let (newBrokenViews, userViews) = resolveUserViews layout mergedAttrs true sourceViews
                        let brokenViews = unionErroredUserViews brokenViews newBrokenViews

                        let! currentVersion = ensureCurrentVersion transaction (isChanged || not (updateResultIsEmpty userViewsUpdate)) cancellationToken

                        // To dry-run user views we need to stop the transaction.
                        let! _ = transaction.Commit (cancellationToken)
                        let! (newBrokenViews, prefetchedViews) = dryRunUserViews transaction.Connection.Query layout true None sourceViews userViews cancellationToken
                        let brokenViews = unionErroredUserViews brokenViews newBrokenViews
                        let transaction = new DatabaseTransaction (transaction.Connection)
                        return (transaction, currentVersion, jsRuntime, mergedAttrs, brokenAttrs, brokenViews, prefetchedViews, sourceViews)
                    with
                    | ex ->
                        do! transaction.Rollback ()
                        return reraise' ex
                }
            let! currentVersion2 =
                task {
                    try
                        return! ensureCurrentVersion transaction false cancellationToken
                    with
                    | ex ->
                        do! transaction.Rollback ()
                        return reraise' ex
                }
            if currentVersion2 <> currentVersion then
                let! sourceLayout = buildFullSchemaLayout transaction.System preload cancellationToken
                let (_, layout) = resolveLayout sourceLayout false
                let! userMeta = buildUserDatabaseMeta transaction.Transaction preload cancellationToken
                return! finishColdRebuild transaction layout userMeta false cancellationToken
            else
                try
                    do! checkBrokenAttributes logger cacheParams.AllowAutoMark preload transaction brokenAttrs cancellationToken
                    let jsApi = jsRuntime.GetValue isolate

                    let! sourceActions = buildSchemaActions transaction.System cancellationToken
                    let (brokenActions, actions) = resolveActions layout true sourceActions
                    let (newBrokenActions, actions) = testEvalActions jsApi true actions
                    let brokenActions = unionErroredActions brokenActions newBrokenActions
                    do! checkBrokenActions logger cacheParams.AllowAutoMark preload transaction brokenActions cancellationToken

                    let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                    let (brokenTriggers, triggers) = resolveTriggers layout true sourceTriggers
                    let (newBrokenTriggers, triggers) = testEvalTriggers jsApi true triggers
                    let brokenTriggers = unionErroredTriggers brokenTriggers newBrokenTriggers
                    let mergedTriggers = mergeTriggers layout triggers
                    do! checkBrokenTriggers logger cacheParams.AllowAutoMark preload transaction brokenTriggers cancellationToken

                    do! checkBrokenUserViews logger cacheParams.AllowAutoMark preload transaction brokenViews cancellationToken

                    let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                    let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions
                    do! checkBrokenPermissions logger cacheParams.AllowAutoMark preload transaction brokenPerms cancellationToken

                    let! _ = transaction.Commit (cancellationToken)

                    let systemViews = filterSystemViews sourceViews
                    let domains = buildLayoutDomains layout

                    let state =
                        { Layout = layout
                          Permissions = permissions
                          DefaultAttrs = mergedAttrs
                          JSRuntime = jsRuntime
                          Actions = actions
                          ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                          Triggers = mergedTriggers
                          TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                          UserViews = prefetchedViews
                          Domains = domains
                          SystemViews = systemViews
                          UserMeta = userMeta
                        }

                    return { Version = currentVersion; Context = state }
                with
                | ex ->
                    do! transaction.Rollback ()
                    return reraise' ex
        finally
            jsIsolates.Return isolate
    }

    let rec getMigrationLock (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<bool> =
        task {
            // Try to get a lock. If we fail, wait till someone else releases it and then _restart the transaction and try again_.
            // This is because otherwise transaction gets to see older state of the database.
            let! (name, typ, ret) =
                task {
                    try
                        return! transaction.Connection.Query.ExecuteValueQuery "SELECT pg_try_advisory_xact_lock(@0)" migrationLockParams cancellationToken
                    with
                    | ex ->
                        do! transaction.Rollback ()
                        return reraise' ex
                }
            match ret with
            | SQL.VBool true -> return true
            | _ ->
                try
                    let! _ = transaction.Connection.Query.ExecuteNonQuery "SELECT pg_advisory_xact_lock(@0)" migrationLockParams cancellationToken
                    ()
                with
                | ex ->
                    do! transaction.Rollback ()
                    return reraise' ex
                do! transaction.Rollback ()
                return false
        }

    let coldRebuildFromDatabase (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<CachedState option> =
        task {
            match! getMigrationLock transaction cancellationToken with
            | false -> return None
            | true ->
                let! (userMeta, layout, isChanged) =
                    task {
                        try
                            let! (isChanged, layout, userMeta) = initialMigratePreload logger cacheParams.AllowAutoMark preload transaction cancellationToken
                            do! ensureDatabaseVersion transaction cancellationToken
                            return (userMeta, layout, isChanged)
                        with
                        | ex ->
                            do! transaction.Rollback ()
                            return reraise' ex
                    }
                let! ret = finishColdRebuild transaction layout userMeta isChanged cancellationToken
                return Some ret
        }

    // Called when state update by another instance is detected. More lightweight than full `coldRebuildFromDatabase`.
    let rec rebuildFromDatabase (transaction : DatabaseTransaction) (currentVersion : int) (cancellationToken : CancellationToken) : Task<CachedState> =
        task {
            let! (transaction, layout, mergedAttrs, sourceUvs, userViews, prefetchedBadViews) =
                task {
                    try
                        let! sourceLayout = buildFullSchemaLayout transaction.System preload cancellationToken
                        let (_, layout) = resolveLayout sourceLayout false

                        let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                        let (_, defaultAttrs) = resolveAttributes layout false sourceAttrs
                        let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                        let! sourceUvs = buildSchemaUserViews transaction.System cancellationToken
                        let (_, userViews) = resolveUserViews layout mergedAttrs false sourceUvs

                        // To dry-run user views we need to stop the transaction.
                        let! _ = transaction.Rollback ()
                        // We dry-run those views that _can_ be failed here, outside of a transaction.
                        let! (_, prefetchedBadViews) = dryRunUserViews transaction.Connection.Query layout false (Some true) sourceUvs userViews cancellationToken
                        let transaction = new DatabaseTransaction(transaction.Connection)
                        return (transaction, layout, mergedAttrs, sourceUvs, userViews, prefetchedBadViews)
                    with
                    | ex ->
                        do! transaction.Rollback ()
                        return reraise' ex
                }
            let! currentVersion2 =
                task {
                    try
                        return! ensureCurrentVersion transaction false cancellationToken
                    with
                    | ex ->
                        do! transaction.Rollback ()
                        return reraise' ex
                }
            if currentVersion2 <> currentVersion then
                return! rebuildFromDatabase transaction currentVersion2 cancellationToken
            else
                try
                    // Now dry-run those views that _cannot_ fail - we can get an exception here and stop, which is the point -
                    // views with `allowBroken = true` fail on first error and so can be dry-run inside a transaction.
                    let! (_, prefetchedGoodViews) = dryRunUserViews transaction.Connection.Query layout false (Some false) sourceUvs userViews cancellationToken

                    let! sourceModules = buildSchemaModules transaction.System cancellationToken
                    let! sourceActions = buildSchemaActions transaction.System cancellationToken
                    let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                    let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                    let! userMeta = buildUserDatabaseMeta transaction.Transaction preload cancellationToken

                    do! transaction.Rollback ()

                    let (_, actions) = resolveActions layout false sourceActions
                    let (_, triggers) = resolveTriggers layout false sourceTriggers

                    let modules = resolveModules layout sourceModules
                    let jsRuntime = makeRuntime (moduleFiles modules)
                    let (actions, triggers) =
                        let myIsolate = jsIsolates.Get ()
                        try
                            let jsApi = jsRuntime.GetValue myIsolate
                            let (_, actions) = testEvalActions jsApi false actions
                            let (_, triggers) = testEvalTriggers jsApi false triggers
                            (actions, triggers)
                        finally
                            jsIsolates.Return myIsolate

                    let (_, permissions) = resolvePermissions layout false sourcePermissions

                    let prefetchedViews = mergePrefetchedUserViews prefetchedBadViews prefetchedGoodViews
                    // Another instance has already rebuilt them, so just load them from the database.
                    let systemViews = filterSystemViews sourceUvs
                    let mergedTriggers = mergeTriggers layout triggers
                    let domains = buildLayoutDomains layout

                    let newState =
                        { Layout = layout
                          Permissions = permissions
                          DefaultAttrs = mergedAttrs
                          JSRuntime = jsRuntime
                          Actions = actions
                          ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                          Triggers = mergedTriggers
                          TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                          UserViews = prefetchedViews
                          Domains = domains
                          SystemViews = systemViews
                          UserMeta = userMeta
                        }

                    return { Version = currentVersion; Context = newState }
                with
                | ex ->
                    do! transaction.Rollback ()
                    return reraise' ex
        }

    let mutable cachedState : CachedState option = None
    // Used to detect simultaneous migrations.
    let cachedStateLock = new SemaphoreSlim 1

    // Run a function that atomically updates database state and release a connection.
    let updateStateAndRelease (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) (next : unit -> Task) : Task<bool> =
        task {
            let! success = cachedStateLock.WaitAsync(0)
            if not success then
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                do! (transaction.Connection :> IAsyncDisposable).DisposeAsync()
                do! cachedStateLock.WaitAsync(cancellationToken)
                ignore <| cachedStateLock.Release()
                return false
            else
                try
                    do! next ()
                    return true
                finally
                    ignore <| cachedStateLock.Release()
                    (transaction :> IDisposable).Dispose()
                    (transaction.Connection :> IDisposable).Dispose()
        }

    let rec getCachedState (cancellationToken : CancellationToken) : Task<DatabaseTransaction * CachedState> =
        task {
            let! (transaction, ret) = openAndGetCurrentVersion cancellationToken
            match (cachedState, ret) with
            | (None, Some ver) ->
                let! _ = updateStateAndRelease transaction cancellationToken <| fun () ->
                    unitTask {
                        let! databaseVersion = getDatabaseVersion transaction cancellationToken
                        if databaseVersion = Some currentDatabaseVersion then
                            let! newState = rebuildFromDatabase transaction ver cancellationToken
                            cachedState <- Some newState
                        else
                            match! coldRebuildFromDatabase transaction cancellationToken with
                            | None -> ()
                            | Some newState ->
                                cachedState <- Some newState
                    }
                return! getCachedState cancellationToken
            | (Some oldState, Some ver) ->
                if oldState.Version <> ver then
                    let! _ = updateStateAndRelease transaction cancellationToken <| fun () ->
                        unitTask {
                            transaction.Connection.Connection.UnprepareAll()
                            let! newState = rebuildFromDatabase transaction ver cancellationToken
                            clearCaches ()
                            cachedState <- Some newState
                        }
                    return! getCachedState cancellationToken
                else
                    return (transaction, oldState)
            | (_, None)
            | (None, _) ->
                let! _ = updateStateAndRelease transaction cancellationToken <| fun () ->
                    unitTask {
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

    member this.GetCache (cancellationToken : CancellationToken) =
        task {
            let! (transaction, oldState) = getCachedState cancellationToken
            try
                let mutable isDisposed = false

                let isolate = lazy ( jsIsolates.Get() )

                let migrate () =
                    task {
                        let! localSuccess = cachedStateLock.WaitAsync(0)
                        if not localSuccess then
                            raisef ContextException "Another migration is in progress"

                        try
                            match! transaction.Connection.Query.ExecuteValueQuery "SELECT pg_try_advisory_xact_lock(@0)" migrationLockParams cancellationToken with
                            | (lockName, lockRet, SQL.VBool true) -> ()
                            | _ -> raisef ContextException "Another migration is in progress"

                            logger.LogInformation("Starting migration")
                            // Careful here not to evaluate user views before we do migration.

                            let! sourceLayout = buildSchemaLayout transaction.System Seq.empty cancellationToken
                            let sourceLayout = applyHiddenLayoutData sourceLayout (preloadLayout preload)
                            if not <| preloadLayoutIsUnchanged sourceLayout preload then
                                raisef ContextException "Cannot modify system layout"
                            let (_, layout) =
                                try
                                    resolveLayout sourceLayout false
                                with
                                | :? ResolveLayoutException as e -> raisefWithInner ContextException e "Failed to resolve layout"

                            let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                            if not <| preloadPermissionsAreUnchanged sourcePermissions preload then
                                raisef ContextException "Cannot modify system permissions"
                            let (_, permissions) =
                                try
                                    resolvePermissions layout false sourcePermissions
                                with
                                | :? ResolvePermissionsException as e -> raisefWithInner ContextException e "Failed to resolve permissions"

                            let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                            if not <| preloadAttributesAreUnchanged sourceAttrs preload then
                                raisef ContextException "Cannot modify system default attributes"
                            let (_, defaultAttrs) =
                                try
                                    resolveAttributes layout false sourceAttrs
                                with
                                | :? ResolveAttributesException as e -> raisefWithInner ContextException e "Failed to resolve default attributes"
                            let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                            let! sourceModules = buildSchemaModules transaction.System cancellationToken
                            if not <| preloadModulesAreUnchanged sourceModules preload then
                                raisef ContextException "Cannot modify system modules"
                            let modules =
                                try
                                    resolveModules layout sourceModules
                                with
                                | :? ResolveModulesException as e -> raisefWithInner ContextException e "Failed to resolve modules"

                            let jsRuntime = makeRuntime (moduleFiles modules)
                            let jsApi = jsRuntime.GetValue isolate.Value

                            let! sourceActions = buildSchemaActions transaction.System cancellationToken
                            if not <| preloadActionsAreUnchanged sourceActions preload then
                                raisef ContextException "Cannot modify system actions"
                            let (_, actions) =
                                try
                                    resolveActions layout false sourceActions
                                with
                                | :? ResolveActionsException as e -> raisefWithInner ContextException e "Failed to resolve actions"
                            let (_, actions) =
                                try
                                    testEvalActions jsApi false actions
                                with
                                | :? ActionRunException as e -> raisefWithInner ContextException e "Failed to resolve actions"

                            let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                            if not <| preloadTriggersAreUnchanged sourceTriggers preload then
                                raisef ContextException "Cannot modify system triggers"
                            let (_, triggers) =
                                try
                                    resolveTriggers layout false sourceTriggers
                                with
                                | :? ResolveTriggersException as err -> raisefWithInner ContextException err "Failed to resolve triggers"
                            let (_, triggers) =
                                try
                                    testEvalTriggers jsApi false triggers
                                with
                                | :? TriggerRunException as err -> raisefWithInner ContextException err "Failed to resolve triggers"
                            let mergedTriggers = mergeTriggers layout triggers

                            let! sourceUserViews = buildSchemaUserViews transaction.System cancellationToken
                            if filterSystemViews sourceUserViews <> oldState.Context.SystemViews then
                                raisef ContextException "Cannot modify system user views"

                            // Actually migrate.
                            let (newAssertions, wantedUserMeta) = buildFullLayoutMeta layout (filterUserLayout layout)
                            let migration = planDatabaseMigration oldState.Context.UserMeta wantedUserMeta
                            try
                                do! migrateDatabase transaction.Connection.Query migration cancellationToken
                            with
                            | :? QueryException as e -> return raisefUserWithInner ContextException e "Migration error"

                            let oldAssertions = buildAssertions oldState.Context.Layout (filterUserLayout oldState.Context.Layout)
                            let addedAssertions = differenceLayoutAssertions newAssertions oldAssertions
                            try
                                do! checkAssertions transaction.Connection.Query layout addedAssertions cancellationToken
                            with
                            | :? LayoutIntegrityException as e -> return raisefWithInner ContextException e "Failed to perform integrity checks"

                            logger.LogInformation("Updating generated user views")
                            let (_, generatedUserViews) =
                                try
                                    generateViews jsApi layout sourceUserViews cancellationToken false
                                with
                                | :? UserViewGenerateException as err -> raisefWithInner ContextException err "Failed to generate user views"
                            let! userViewsUpdate = updateUserViews transaction.System generatedUserViews cancellationToken
                            do! deleteDeferredFromUpdate layout transaction userViewsUpdate cancellationToken
                            let sourceUserViews = { Schemas = Map.union sourceUserViews.Schemas generatedUserViews.Schemas } : SourceUserViews

                            let (_, userViews) =
                                try
                                    resolveUserViews layout mergedAttrs false sourceUserViews
                                with
                                | :? UserViewResolveException as err -> raisefWithInner ContextException err "Failed to resolve user views"
                            let! (_, badUserViews) = task {
                                try
                                    return! dryRunUserViews transaction.Connection.Query layout false (Some false) sourceUserViews userViews cancellationToken
                                with
                                | :? UserViewDryRunException as err -> return raisefWithInner ContextException err "Failed to resolve user views"
                            }

                            // We update state now and check user views _after_ that.
                            // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                            let newVersion = oldState.Version + 1
                            let! _ =
                                transaction.System.State.AsQueryable()
                                    .Where(fun x -> x.Name = versionField)
                                    .UpdateAsync(Expr.toMemberInit <@ fun x -> StateValue(Value = string newVersion) @>, cancellationToken)
                            try
                                let! _ = transaction.Commit(cancellationToken)
                                ()
                            with
                            | :? DbUpdateException as e -> raisefUserWithInner ContextException e "State update error"

                            let! (_, goodUserViews) = dryRunUserViews transaction.Connection.Query layout false (Some true) sourceUserViews userViews cancellationToken

                            let domains = buildLayoutDomains layout

                            let newState =
                                { Layout = layout
                                  Permissions = permissions
                                  DefaultAttrs = mergedAttrs
                                  JSRuntime = jsRuntime
                                  Actions = actions
                                  ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                                  Triggers = mergedTriggers
                                  TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                                  UserViews = mergePrefetchedUserViews badUserViews goodUserViews
                                  Domains = domains
                                  SystemViews = filterSystemViews sourceUserViews
                                  UserMeta = wantedUserMeta
                                }

                            clearCaches ()
                            if not <| Array.isEmpty migration then
                                // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                                NpgsqlConnection.ClearPool(transaction.Connection.Connection)
                            cachedState <- Some { Version = newVersion; Context = newState }
                        finally
                            ignore <| cachedStateLock.Release()
                }

                let resolveAnonymousView (isPrivileged : bool) (homeSchema : SchemaName option) (query : string) =
                    task {
                        let findExistingView =
                            oldState.Context.UserViews.Find >> Option.map (Result.map (fun pref -> pref.UserView))
                        let uv = resolveAnonymousUserView oldState.Context.Layout isPrivileged oldState.Context.DefaultAttrs findExistingView homeSchema query
                        return! dryRunAnonymousUserView transaction.Connection.Query oldState.Context.Layout uv cancellationToken
                    }

                let getAnonymousView (isPrivileged : bool) (query : string) : Task<PrefetchedUserView> =
                    task {
                        let createNew query =
                            task {
                                let! uv = resolveAnonymousView isPrivileged None query
                                let ret =
                                    { UserView = uv
                                      Query = query
                                    }
                                return ret
                            }
                        let! ret = anonymousViewsIndex.GetItem(query, ItemCreator(createNew))
                        if not isPrivileged && ret.UserView.UserView.Resolved.Privileged then
                            // Just to throw the exception properly.
                            createNew query |> ignore
                        return ret.UserView
                    }

                let getAnonymousCommand (isPrivileged : bool) (query : string) : Task<CompiledCommandExpr> =
                    task {
                        let createNew query =
                            task {
                                let resolved = resolveCommand oldState.Context.Layout isPrivileged query
                                let compiled = compileCommandExpr oldState.Context.Layout resolved
                                let ret =
                                    { Command = compiled
                                      Privileged = resolved.Privileged
                                      Query = query
                                    }
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
                    unitTask {
                        if needMigration then
                            do! migrate ()
                        else
                            try
                                let! _ = transaction.Commit (cancellationToken)
                                ()
                            with
                            | :? DbUpdateException as e -> return raisefUserWithInner ContextException e "Failed to commit"
                    }

                let checkIntegrity () =
                    unitTask {
                        let assertions = buildAssertions oldState.Context.Layout (filterUserLayout oldState.Context.Layout)
                        try
                            do! checkAssertions transaction.Connection.Query oldState.Context.Layout assertions cancellationToken
                        with
                        | :? LayoutIntegrityException as e -> return raisefWithInner ContextException e "Failed to perform integrity checks"
                    }

                let mutable maybeApi = None
                let setAPI api =
                    match maybeApi with
                    | Some oldApi -> failwith "Cannot set API more than once"
                    | None ->
                        maybeApi <- Some api

                let jsApi =
                    lazy (
                        let jsApi = oldState.Context.JSRuntime.GetValue isolate.Value
                        jsApi.API.SetAPI (Option.get maybeApi)
                        jsApi
                    )

                let actionScripts =
                    lazy (
                        let actionScripts = oldState.Context.ActionScripts.GetValue isolate.Value
                        // Initialize JS API.
                        ignore <| jsApi.Force()
                        actionScripts
                    )

                let triggerScripts =
                    lazy (
                        let triggerScripts = oldState.Context.TriggerScripts.GetValue isolate.Value
                        // Initialize JS API.
                        ignore <| jsApi.Force()
                        triggerScripts
                    )

                let! systemInfo = transaction.Connection.Query.ExecuteRowValuesQuery "SELECT transaction_timestamp(), txid_current()" Map.empty cancellationToken
                let (transactionTime, transactionId) =
                    match systemInfo with
                    | [|(tsName, tsTyp, SQL.VDateTime ts); (idName, idTyp, SQL.VBigInt txid)|] -> (ts, int txid)
                    | _ -> failwith "Impossible"

                return
                    { new IContext with
                          member this.Transaction = transaction
                          member this.TransactionId = transactionId
                          member this.TransactionTime = transactionTime
                          member this.LoggerFactory = cacheParams.LoggerFactory
                          member this.CancellationToken = cancellationToken
                          member this.Preload = preload
                          member this.Runtime = jsApi.Value :> IJSRuntime

                          member this.Layout = oldState.Context.Layout
                          member this.UserViews = oldState.Context.UserViews
                          member this.Permissions = oldState.Context.Permissions
                          member this.DefaultAttrs = oldState.Context.DefaultAttrs
                          member this.Triggers = oldState.Context.Triggers
                          member this.Domains = oldState.Context.Domains

                          member this.Commit () = commit ()
                          member this.ScheduleMigration () =
                            needMigration <- true
                          member this.CheckIntegrity () = checkIntegrity ()
                          member this.GetAnonymousView isPrivileged query = getAnonymousView isPrivileged query
                          member this.GetAnonymousCommand isPrivileged query = getAnonymousCommand isPrivileged query
                          member this.ResolveAnonymousView isPrivileged homeSchema query = resolveAnonymousView isPrivileged homeSchema query
                          member this.WriteEvent event = cacheParams.EventLogger.WriteEvent(cacheParams.ConnectionString, event)
                          member this.SetAPI api = setAPI api
                          member this.FindAction ref =
                            match actionScripts.Value.FindAction ref with
                            | Some script -> Some (Ok script)
                            | None ->
                                match oldState.Context.Actions.FindAction ref with
                                | Some (Ok script) -> failwith "Impossible"
                                | Some (Error e) -> Some (Error e)
                                | None -> None
                          member this.FindTrigger ref = triggerScripts.Value.FindTrigger ref

                          member this.Dispose () =
                              if not isDisposed then
                                  if jsApi.IsValueCreated then
                                      jsApi.Value.API.ResetAPI ()
                                  if isolate.IsValueCreated then
                                      jsIsolates.Return isolate.Value
                                  (transaction :> IDisposable).Dispose ()
                                  (transaction.Connection :> IDisposable).Dispose ()
                                  isDisposed <- true
                          member this.DisposeAsync () =
                              unitVtask {
                                  if not isDisposed then
                                      if jsApi.IsValueCreated then
                                          jsApi.Value.API.ResetAPI ()
                                      if isolate.IsValueCreated then
                                          jsIsolates.Return isolate.Value
                                      do! (transaction :> IAsyncDisposable).DisposeAsync ()
                                      do! (transaction.Connection :> IAsyncDisposable).DisposeAsync ()
                                      isDisposed <- true
                              }
                    }
            with
            | e ->
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                do! (transaction.Connection :> IAsyncDisposable).DisposeAsync ()
                return reraise' e
        }
