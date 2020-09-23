module FunWithFlags.FunDB.API.ContextCache

open System
open System.Reflection
open System.IO
open System.Linq
open Z.EntityFramework.Plus
open Microsoft.EntityFrameworkCore
open System.Security.Cryptography
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
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.EventLogger
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
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
open FunWithFlags.FunDB.Layout.Integrity
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.JavaScript
open FunWithFlags.FunDB.API.Triggers

type ContextException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ContextException (message, null)

[<NoEquality; NoComparison>]
type private AnonymousUserView =
    { Query : string
      UserView : PrefetchedUserView
    }

let private databaseField = "DatabaseVersion"
let private versionField = "StateVersion"
let private fallbackVersion = 0
let private migrationLockNumber = 0
let private migrationLockParams = Map.singleton 0 (SQL.VInt migrationLockNumber)
let private assemblyHash =
    use hasher = SHA256.Create()
    use assembly = File.OpenRead(Assembly.GetCallingAssembly().Location)
    hasher.ComputeHash(assembly) |> Array.map (fun x -> x.ToString("x2")) |> String.concat ""

[<NoEquality; NoComparison>]
type private CachedContext =
    { Layout : Layout
      UserViews : PrefetchedUserViews
      Permissions : Permissions
      DefaultAttrs : MergedDefaultAttributes
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

type ContextCacheStore (loggerFactory : ILoggerFactory, hashedPreload : HashedPreload, connectionString : string, eventLogger : EventLogger) =
    let preload = hashedPreload.Preload
    let logger = loggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values
    let anonymousViewsCache = FluidCache<AnonymousUserView>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousViewsIndex = anonymousViewsCache.AddIndex("byQuery", fun uv -> uv.Query)

    let currentDatabaseVersion = sprintf "%s %s" assemblyHash hashedPreload.Hash

    let jsIsolates =
        DefaultObjectPool
            { new IPooledObjectPolicy<Isolate> with
                  member this.Create () =
                    Isolate.NewWithHeapSize(1UL * 1024UL * 1024UL, 32UL * 1024UL * 1024UL)
                  member this.Return _ = true
            }

    let filterSystemViews (views : SourceUserViews) : SourceUserViews =
        { Schemas = filterPreloadedSchemas preload views.Schemas }

    let filterUserLayout (layout : Layout) : Layout =
        { schemas = filterUserSchemas preload layout.schemas }

    // If `None` cold rebuild is needed.
    let getCurrentVersion (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<DatabaseTransaction * int option> =
        task {
            let! (transaction, versionEntry) = task {
                try
                    let! versionEntry = transaction.System.State.FirstOrDefaultAsync((fun x -> x.Name = versionField), cancellationToken)
                    return (transaction, versionEntry)
                with
                | :? PostgresException ->
                    do! transaction.Rollback ()
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
                    let! _ = conn.System.SaveChangesAsync(cancellationToken)
                    return fallbackVersion
                | entry ->
                    match tryIntInvariant entry.Value with
                    | Some v ->
                        if bump then
                            let newVersion = v + 1
                            entry.Value <- string newVersion
                            let! _ = conn.System.SaveChangesAsync(cancellationToken)
                            return newVersion
                        else
                            return v
                    | None ->
                        entry.Value <- string fallbackVersion
                        let! _ = conn.System.SaveChangesAsync(cancellationToken)
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
                let! _ = conn.System.SaveChangesAsync(cancellationToken)
                ()
            | entry when entry.Value <> currentDatabaseVersion ->
                entry.Value <- currentDatabaseVersion
                let! _ = conn.System.SaveChangesAsync(cancellationToken)
                ()
            | _ -> ()
        }

    let generateViews runtime layout userViews cancellationToken forceAllowBroken =
        let generator = UserViewsGenerator(runtime, userViews, forceAllowBroken)
        generator.GenerateUserViews layout cancellationToken forceAllowBroken

    let runWithRuntime (jsRuntime : IsolateLocal<JSRuntime<APITemplate>>) (func : JSRuntime<APITemplate> -> 'a) : 'a =
        let isolate = jsIsolates.Get()
        try
            let runtime = jsRuntime.GetValue isolate
            func runtime
        finally
            jsIsolates.Return(isolate)

    let makeRuntime files = IsolateLocal(fun isolate ->
        let env =
            { Files = files
              SearchPath = seq { moduleDirectory }
            }
        JSRuntime(isolate, APITemplate, env)
    )

    let rec finishColdRebuild (transaction : DatabaseTransaction) (layout : Layout) (userMeta : SQL.DatabaseMeta) (isChanged : bool) (cancellationToken : CancellationToken) : Task<CachedState> = task {
        let! (transaction, currentVersion, jsRuntime, mergedAttrs, brokenAttrs, brokenViews, prefetchedViews, sourceViews) = task {
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
                let (brokenViews, sourceViews) = runWithRuntime jsRuntime <| fun api -> generateViews api layout sourceViews cancellationToken true
                let! isChanged2 = updateUserViews transaction.System sourceViews cancellationToken

                let (newBrokenViews, userViews) = resolveUserViews layout mergedAttrs true sourceViews
                let brokenViews = unionErroredUserViews brokenViews newBrokenViews

                let! currentVersion = ensureCurrentVersion transaction (isChanged || isChanged2) cancellationToken

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
                do! checkBrokenAttributes logger preload transaction brokenAttrs cancellationToken

                let! sourceActions = buildSchemaActions transaction.System cancellationToken
                let (brokenActions, actions) = resolveActions layout true sourceActions
                let (newBrokenActions, actions) = runWithRuntime jsRuntime <| fun api -> testEvalActions api true actions
                let brokenActions = unionErroredActions brokenActions newBrokenActions
                do! checkBrokenActions logger preload transaction brokenActions cancellationToken

                let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                let (brokenTriggers, triggers) = resolveTriggers layout true sourceTriggers
                let (newBrokenTriggers, triggers) = runWithRuntime jsRuntime <| fun api -> testEvalTriggers api true triggers
                let brokenTriggers = unionErroredTriggers brokenTriggers newBrokenTriggers
                let mergedTriggers = mergeTriggers layout triggers
                do! checkBrokenTriggers logger preload transaction brokenTriggers cancellationToken

                do! checkBrokenUserViews logger preload transaction brokenViews cancellationToken

                let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions
                do! checkBrokenPermissions logger preload transaction brokenPerms cancellationToken

                let! _ = transaction.Commit (cancellationToken)

                let systemViews = filterSystemViews sourceViews

                let state =
                    { Layout = layout
                      Permissions = permissions
                      DefaultAttrs = mergedAttrs
                      JSRuntime = jsRuntime
                      ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                      Triggers = mergedTriggers
                      TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                      UserViews = prefetchedViews
                      SystemViews = systemViews
                      UserMeta = userMeta
                    }

                return { Version = currentVersion; Context = state }
            with
            | ex ->
                do! transaction.Rollback ()
                return reraise' ex
    }

    let rec getMigrationLock (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<bool> =
        task {
            // Try to get a lock. If we fail, wait till someone else releases it and then _restart the transaction and try again_.
            // This is because otherwise transaction gets to see older state of the database.
            let! ret = task {
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
                let! (userMeta, layout, isChanged) = task {
                    try
                        let! (isChanged, layout, userMeta) = initialMigratePreload logger preload transaction cancellationToken
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
    let rec rebuildFromDatabase (transaction : DatabaseTransaction) (currentVersion : int) (cancellationToken : CancellationToken) : Task<CachedState> = task {
        let! (transaction, layout, mergedAttrs, sourceUvs, userViews, prefetchedBadViews) = task {
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
                let prefetchedViews = mergePrefetchedUserViews prefetchedBadViews prefetchedGoodViews

                let! sourceModules = buildSchemaModules transaction.System cancellationToken
                let modules = resolveModules layout sourceModules

                let jsRuntime = makeRuntime (moduleFiles modules)

                let! sourceActions = buildSchemaActions transaction.System cancellationToken
                let (_, actions) = resolveActions layout false sourceActions
                let (_, actions) = runWithRuntime jsRuntime <| fun api -> testEvalActions api false actions

                let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                let (_, triggers) = resolveTriggers layout false sourceTriggers
                let (_, triggers) = runWithRuntime jsRuntime <| fun api -> testEvalTriggers api false triggers
                let mergedTriggers = mergeTriggers layout triggers

                // Another instance has already rebuilt them, so just load them from the database.
                let systemViews = filterSystemViews sourceUvs

                let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                let (_, permissions) = resolvePermissions layout false sourcePermissions

                let! userMeta = buildUserDatabaseMeta transaction.Transaction preload cancellationToken

                do! transaction.Rollback ()

                let newState =
                    { Layout = layout
                      Permissions = permissions
                      DefaultAttrs = mergedAttrs
                      JSRuntime = jsRuntime
                      ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                      Triggers = mergedTriggers
                      TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                      UserViews = prefetchedViews
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

    let tryWithStateLock (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) (next : unit -> Task) : Task<bool> =
        task {
            let! success = cachedStateLock.WaitAsync(0)
            if not success then
                do! transaction.Rollback ()
                do! cachedStateLock.WaitAsync (cancellationToken)
                ignore <| cachedStateLock.Release ()
                return false
            else
                try
                    do! next ()
                    return true
                finally
                    ignore <| cachedStateLock.Release ()
        }

    let rec getCachedState (conn : DatabaseConnection) (cancellationToken : CancellationToken) : Task<DatabaseTransaction * CachedState> =
        task {
            let transaction = new DatabaseTransaction(conn)
            let! (transaction, ret) = getCurrentVersion transaction cancellationToken
            match (cachedState, ret) with
            | (None, Some ver) ->
                let! _ = tryWithStateLock transaction cancellationToken <| fun () ->
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
                return! getCachedState conn cancellationToken
            | (Some oldState, Some ver) ->
                if oldState.Version <> ver then
                    let! _ = tryWithStateLock transaction cancellationToken <| fun () ->
                        unitTask {
                            transaction.Connection.Connection.UnprepareAll()
                            let! newState = rebuildFromDatabase transaction ver cancellationToken
                            anonymousViewsCache.Clear()
                            cachedState <- Some newState
                        }
                    return! getCachedState conn cancellationToken
                else
                    return (transaction, oldState)
            | (_, None)
            | (None, _) ->
                let! _ = tryWithStateLock transaction cancellationToken <| fun () ->
                    unitTask {
                        transaction.Connection.Connection.UnprepareAll()
                        match! coldRebuildFromDatabase transaction cancellationToken with
                        | None -> ()
                        | Some newState ->
                            anonymousViewsCache.Clear()
                            cachedState <- Some newState
                    }
                return! getCachedState conn cancellationToken
        }

    member this.LoggerFactory = loggerFactory
    member this.Preload = preload
    member this.EventLogger = eventLogger
    member this.ConnectionString = connectionString

    member this.GetCache (cancellationToken : CancellationToken) = task {
        let conn = new DatabaseConnection(loggerFactory, connectionString)
        try
            let! (transaction, oldState) = getCachedState conn cancellationToken

            let mutable isDisposed = false

            let mutable maybeIsolate = None
            let getIsolate () =
                match maybeIsolate with
                | Some isolate -> isolate
                | None ->
                    let isolate = jsIsolates.Get()
                    maybeIsolate <- Some isolate
                    isolate

            let migrate () =
                task {
                    let! localSuccess = cachedStateLock.WaitAsync(0)
                    if not localSuccess then
                        raisef ContextException "Another migration is in progress"

                    try
                        match! transaction.Connection.Query.ExecuteValueQuery "SELECT pg_try_advisory_xact_lock(@0)" migrationLockParams cancellationToken with
                        | SQL.VBool true -> ()
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
                            | :? ResolveLayoutException as err -> raisefWithInner ContextException err "Failed to resolve layout"

                        let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                        if not <| preloadPermissionsAreUnchanged sourcePermissions preload then
                            raisef ContextException "Cannot modify system permissions"
                        let (_, permissions) =
                            try
                                resolvePermissions layout false sourcePermissions
                            with
                            | :? ResolvePermissionsException as err -> raisefWithInner ContextException err "Failed to resolve permissions"

                        let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                        if not <| preloadAttributesAreUnchanged sourceAttrs preload then
                            raisef ContextException "Cannot modify system default attributes"
                        let (_, defaultAttrs) =
                            try
                                resolveAttributes layout false sourceAttrs
                            with
                            | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve default attributes"
                        let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                        let! sourceModules = buildSchemaModules transaction.System cancellationToken
                        if not <| preloadModulesAreUnchanged sourceModules preload then
                            raisef ContextException "Cannot modify system modules"
                        let modules =
                            try
                                resolveModules layout sourceModules
                            with
                            | :? ResolveModulesException as err -> raisefWithInner ContextException err "Failed to resolve modules"

                        let jsRuntime = makeRuntime (moduleFiles modules)

                        let! sourceActions = buildSchemaActions transaction.System cancellationToken
                        if not <| preloadActionsAreUnchanged sourceActions preload then
                            raisef ContextException "Cannot modify system actions"
                        let (_, actions) =
                            try
                                resolveActions layout false sourceActions
                            with
                            | :? ResolveActionsException as err -> raisefWithInner ContextException err "Failed to resolve actions"
                        let (_, actions) = runWithRuntime jsRuntime <| fun api ->
                            try
                                testEvalActions api false actions
                            with
                            | :? ActionRunException as err -> raisefWithInner ContextException err "Failed to resolve actions"

                        let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                        if not <| preloadTriggersAreUnchanged sourceTriggers preload then
                            raisef ContextException "Cannot modify system triggers"
                        let (_, triggers) =
                            try
                                resolveTriggers layout false sourceTriggers
                            with
                            | :? ResolveTriggersException as err -> raisefWithInner ContextException err "Failed to resolve triggers"
                        let (_, triggers) = runWithRuntime jsRuntime <| fun api ->
                            try
                                testEvalTriggers api false triggers
                            with
                            | :? TriggerRunException as err -> raisefWithInner ContextException err "Failed to resolve triggers"
                        let mergedTriggers = mergeTriggers layout triggers

                        let! sourceUserViews = buildSchemaUserViews transaction.System cancellationToken
                        if filterSystemViews sourceUserViews <> oldState.Context.SystemViews then
                            raisef ContextException "Cannot modify system user views"

                        // Actually migrate.
                        let (newAssertions, wantedUserMeta) = buildFullLayoutMeta layout (filterUserLayout layout)
                        let migration = planDatabaseMigration oldState.Context.UserMeta wantedUserMeta
                        let! migrated = task {
                            try
                                return! migrateDatabase transaction.Connection.Query migration cancellationToken
                            with
                            | :? QueryException as err -> return raisefWithInner ContextException err "Migration error"
                        }

                        logger.LogInformation("Updating generated user views")
                        let (_, userViewsSource) = runWithRuntime jsRuntime <| fun api ->
                            try
                                generateViews api layout sourceUserViews cancellationToken false
                            with
                            | :? UserViewGenerateException as err -> raisefWithInner ContextException err "Failed to generate user views"
                        let! _ = updateUserViews transaction.System userViewsSource cancellationToken

                        let! newUserViewsSource = buildSchemaUserViews transaction.System cancellationToken
                        let (_, userViews) =
                            try
                                resolveUserViews layout mergedAttrs false newUserViewsSource
                            with
                            | :? UserViewResolveException as err -> raisefWithInner ContextException err "Failed to resolve user views"
                        let! (_, badUserViews) = task {
                            try
                                return! dryRunUserViews conn.Query layout false (Some false) newUserViewsSource userViews cancellationToken
                            with
                            | :? UserViewDryRunException as err -> return raisefWithInner ContextException err "Failed to resolve user views"
                        }

                        let oldAssertions = buildAssertions oldState.Context.Layout (filterUserLayout oldState.Context.Layout)
                        for check in Set.difference newAssertions oldAssertions do
                            logger.LogInformation("Running integrity check {check}", check)
                            try
                                do! checkAssertion transaction.Connection.Query layout check cancellationToken
                            with
                            | :? LayoutIntegrityException as err -> return raisefWithInner ContextException err "Failed to perform integrity check"

                        // We update state now and check user views _after_ that.
                        // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                        let newVersion = oldState.Version + 1
                        // Serialized access error: 40001, may need to process it differently later (retry with fallback?)
                        try
                            let! _ = transaction.System.State.AsQueryable().Where(fun x -> x.Name = versionField).UpdateAsync(Expr.toMemberInit <@ fun x -> StateValue(Value = string newVersion) @>, cancellationToken)
                            let! _ = transaction.Commit (cancellationToken)
                            ()
                        with
                        | :? DbUpdateException as err -> raisefWithInner ContextException err "State update error"

                        let! (_, goodUserViews) = dryRunUserViews conn.Query layout false (Some true) newUserViewsSource userViews cancellationToken

                        let newState =
                            { Layout = layout
                              Permissions = permissions
                              DefaultAttrs = mergedAttrs
                              JSRuntime = jsRuntime
                              ActionScripts = IsolateLocal(fun isolate -> prepareActionScripts (jsRuntime.GetValue isolate) actions)
                              Triggers = mergedTriggers
                              TriggerScripts = IsolateLocal(fun isolate -> prepareTriggerScripts (jsRuntime.GetValue isolate) triggers)
                              UserViews = mergePrefetchedUserViews badUserViews goodUserViews
                              SystemViews = filterSystemViews userViewsSource
                              UserMeta = wantedUserMeta
                            }

                        anonymousViewsCache.Clear()
                        if migrated then
                            // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                            NpgsqlConnection.ClearPool(transaction.Connection.Connection)
                        cachedState <- Some { Version = newVersion; Context = newState }
                    finally
                        ignore <| cachedStateLock.Release()
            }

            let resolveAnonymousView homeSchema query = task {
                let findExistingView =
                    oldState.Context.UserViews.Find >> Option.map (Result.map (fun pref -> pref.UserView))
                let uv = resolveAnonymousUserView oldState.Context.Layout oldState.Context.DefaultAttrs findExistingView homeSchema query
                return! dryRunAnonymousUserView conn.Query oldState.Context.Layout uv cancellationToken
            }

            let createNewAnonymousView query = task {
                let! uv = resolveAnonymousView None query
                let ret =
                    { UserView = uv
                      Query = query
                    }
                return ret
            }
            let newAnonymousViewCreator = ItemCreator(createNewAnonymousView)

            let getAnonymousView (query : string) : Task<PrefetchedUserView> = task {
                let! ret = anonymousViewsIndex.GetItem(query, newAnonymousViewCreator)
                return ret.UserView
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
                        | :? DbUpdateException as ex -> return raisefWithInner ContextException ex "Failed to commit"
                }

            let mutable maybeApi = None
            let setAPI api =
                match maybeApi with
                | Some oldApi -> failwith "Cannot set API more than once"
                | None ->
                    maybeApi <- Some api

            let mutable maybeJSAPI = None
            let getJSAPI () =
                match maybeJSAPI with
                | Some jsApi -> jsApi
                | None ->
                    let jsApi = oldState.Context.JSRuntime.GetValue (getIsolate ())
                    jsApi.API.SetAPI (Option.get maybeApi)
                    maybeJSAPI <- Some jsApi
                    jsApi

            let mutable maybeActionScripts = None
            let getActionScripts () =
                match maybeActionScripts with
                | Some actionScripts -> actionScripts
                | None ->
                    let actionScripts = oldState.Context.ActionScripts.GetValue (getIsolate ())
                    // Initialize JS API.
                    ignore <| getJSAPI ()
                    maybeActionScripts <- Some actionScripts
                    actionScripts

            let mutable maybeTriggerScripts = None
            let getTriggerScripts () =
                match maybeTriggerScripts with
                | Some triggerScripts -> triggerScripts
                | None ->
                    let triggerScripts = oldState.Context.TriggerScripts.GetValue (getIsolate ())
                    // Initialize JS API.
                    ignore <| getJSAPI ()
                    maybeTriggerScripts <- Some triggerScripts
                    triggerScripts

            let! systemInfo = transaction.Connection.Query.ExecuteValuesQuery "SELECT transaction_timestamp(), txid_current()" Map.empty cancellationToken
            let (transactionTime, transactionId) =
                match systemInfo with
                | [|SQL.VDateTime ts; SQL.VBigInt txid|] -> (ts, int txid)
                | _ -> failwith "Impossible"

            return
                { new IContext with
                      member this.Transaction = transaction
                      member this.TransactionId = transactionId
                      member this.TransactionTime = transactionTime
                      member this.LoggerFactory = loggerFactory
                      member this.CancellationToken = cancellationToken
                      member this.Preload = preload
                      member this.Runtime = getJSAPI () :> IJSRuntime

                      member this.Layout = oldState.Context.Layout
                      member this.UserViews = oldState.Context.UserViews
                      member this.Permissions = oldState.Context.Permissions
                      member this.DefaultAttrs = oldState.Context.DefaultAttrs
                      member this.Triggers = oldState.Context.Triggers

                      member this.Commit () = commit ()
                      member this.ScheduleMigration () =
                        needMigration <- true
                      member this.GetAnonymousView query = getAnonymousView query
                      member this.ResolveAnonymousView homeSchema query = resolveAnonymousView homeSchema query
                      member this.WriteEvent event = eventLogger.WriteEvent(connectionString, event)
                      member this.SetAPI api = setAPI api
                      member this.FindAction ref = (getActionScripts ()).FindAction ref
                      member this.FindTrigger ref = (getTriggerScripts ()).FindTrigger ref

                      member this.Dispose () =
                          if not isDisposed then
                              isDisposed <- true
                              match maybeJSAPI with
                              | Some jsApi -> jsApi.API.ResetAPI ()
                              | None -> ()
                              match maybeIsolate with
                              | Some isolate -> jsIsolates.Return isolate
                              | None -> ()
                              (transaction :> IDisposable).Dispose ()
                              (conn :> IDisposable).Dispose ()
                      member this.DisposeAsync () =
                          unitVtask {
                              if not isDisposed then
                                  isDisposed <- true
                                  match maybeJSAPI with
                                  | Some jsApi -> jsApi.API.ResetAPI ()
                                  | None -> ()
                                  match maybeIsolate with
                                  | Some isolate -> jsIsolates.Return isolate
                                  | None -> ()
                                  do! (transaction :> IAsyncDisposable).DisposeAsync ()
                                  do! (conn :> IAsyncDisposable).DisposeAsync ()
                          }
                }
        with
        | e ->
            (conn :> IDisposable).Dispose()
            return reraise' e
    }
