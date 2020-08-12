module FunWithFlags.FunDB.Operations.ContextCache

open System
open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.ObjectPool
open Microsoft.EntityFrameworkCore
open FluidCaching
open Npgsql
open FSharp.Control.Tasks.V2.ContextInsensitive
open NetJs

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Attributes.Schema
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Schema
open FunWithFlags.FunDB.Triggers.Resolve
open FunWithFlags.FunDB.Triggers.Update
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
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.EventLogger

type ContextException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ContextException (message, null)

[<NoEquality; NoComparison>]
type CachedRequestContext =
    { Layout : Layout
      UserViews : PrefetchedUserViews
      Permissions : Permissions
      DefaultAttrs : MergedDefaultAttributes
      Triggers : ResolvedTriggers
      SystemViews : SourceUserViews
      UserMeta : SQL.DatabaseMeta
    }

type IContext =
    inherit IDisposable

    abstract member Migrate : unit -> Task<unit>
    abstract member GetAnonymousView : string -> Task<PrefetchedUserView>
    abstract member ResolveAnonymousView : SchemaName option -> string -> Task<PrefetchedUserView>
    abstract member WriteEvent : EventEntry -> ValueTask
    abstract Connection : DatabaseConnection with get
    abstract Transaction : DatabaseTransaction with get
    abstract State : CachedRequestContext with get

[<NoEquality; NoComparison>]
type private AnonymousUserView =
    { Query : string
      UserView : PrefetchedUserView
    }

let private versionField = "StateVersion"
let private fallbackVersion = 0
let private migrationLockNumber = 0

[<NoEquality; NoComparison>]
type CachedState =
    { Version : int
      Context : CachedRequestContext
    }

type ContextCacheStore (loggerFactory : ILoggerFactory, preload : Preload, connectionString : string, eventLogger : EventLogger, warmStartup : bool) =
    let logger = loggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values
    let anonymousViewsCache = FluidCache<AnonymousUserView>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousViewsIndex = anonymousViewsCache.AddIndex("byQuery", fun uv -> uv.Query)

    let jsIsolates =
        DefaultObjectPool
            { new IPooledObjectPolicy<CachingIsolate> with
                  member this.Create () =
                    { isolate = Isolate.NewWithHeapSize(1UL * 1024UL * 1024UL, 32UL * 1024UL * 1024UL)
                      cache = ObjectMap()
                    }
                  member this.Return _ = true
            }

    let filterSystemViews (views : SourceUserViews) : SourceUserViews =
        { Schemas = filterPreloadedSchemas preload views.Schemas }

    let filterUserLayout (layout : Layout) : Layout =
        { schemas = filterUserSchemas preload layout.schemas }

    // If `None` cold rebuild if needed.
    let getCurrentVersion (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<DatabaseTransaction * int option> = task {
        let! (transaction, versionEntry) = task {
            try
                let! versionEntry = transaction.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefaultAsync(cancellationToken)
                return (transaction, versionEntry)
            with
            | :? PostgresException ->
                transaction.Rollback ()
                let transaction = new DatabaseTransaction (transaction.Connection)
                return (transaction, null)
        }
        match versionEntry with
        | null -> return (transaction, None)
        | entry -> return (transaction, tryIntInvariant entry.Value)
    }

    let ensureCurrentVersion (conn : DatabaseTransaction) (bump : bool) (cancellationToken : CancellationToken) : Task<int> = task {
        let! versionEntry = conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefaultAsync(cancellationToken)
        match versionEntry with
        | null ->
            let newEntry = StateValue (Name = versionField, Value = string fallbackVersion)
            ignore <| conn.System.State.Add(newEntry)
            let! _ = conn.System.SaveChangesAsync()
            return fallbackVersion
        | entry ->
            match tryIntInvariant entry.Value with
            | Some v ->
                if bump then
                    let newVersion = v + 1
                    entry.Value <- string newVersion
                    let! _ = conn.System.SaveChangesAsync()
                    return newVersion
                else
                    return v
            | None ->
                entry.Value <- string fallbackVersion
                let! _ = conn.System.SaveChangesAsync()
                return fallbackVersion
    }

    let checkBrokenAttributes (conn : DatabaseTransaction) (brokenAttrs : ErroredDefaultAttributes) (cancellationToken : CancellationToken) =
        task {
            if not (Map.isEmpty brokenAttrs) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenAttrs do
                    let isSystem = Map.containsKey schemaName preload.Schemas
                    if isSystem then
                        critical <- true
                    for KeyValue(attrsSchemaName, attrsSchema) in schema do
                        for KeyValue(attrsEntityName, attrsEntity) in attrsSchema do
                            for KeyValue(attrsFieldName, err) in attrsEntity do
                                let schemaStr = schemaName.ToString()
                                let defFieldName = ({ entity = { schema = attrsSchemaName; name = attrsEntityName }; name = attrsFieldName } : ResolvedFieldRef).ToString()
                                if isSystem then
                                    logger.LogError(err, "System default attributes from {schema} are broken for field {field}", schemaStr, defFieldName)
                                else
                                    logger.LogWarning(err, "Marking default attributes from {schema} for {field} as broken", schemaStr, defFieldName)
                if critical then
                    failwith "Broken system default attributes"
                do! markBrokenAttributes conn.System brokenAttrs cancellationToken
        }

    let checkBrokenTriggers (conn : DatabaseTransaction) (brokenTriggers : ErroredTriggers) (cancellationToken : CancellationToken) =
        task {
            if not (Map.isEmpty brokenTriggers) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenTriggers do
                    let isSystem = Map.containsKey schemaName preload.Schemas
                    if isSystem then
                        critical <- true
                    for KeyValue(triggerSchemaName, triggersSchema) in schema do
                        for KeyValue(triggerEntityName, triggersEntity) in triggersSchema do
                            for KeyValue(triggerName, err) in triggersEntity do
                                let schemaStr = schemaName.ToString()
                                let triggerName = ({ entity = { schema = triggerSchemaName; name = triggerEntityName }; name = triggerName } : ResolvedFieldRef).ToString()
                                if isSystem then
                                    logger.LogError(err, "System trigger {name} from {schema} is broken", triggerName, schemaStr)
                                else
                                    logger.LogWarning(err, "Marking trigger {name} from {schema} as broken", triggerName, schemaStr)
                if critical then
                    failwith "Broken system triggers"
                do! markBrokenTriggers conn.System brokenTriggers cancellationToken
        }

    let checkBrokenUserViews (conn : DatabaseTransaction) (brokenViews : ErroredUserViews) (cancellationToken : CancellationToken) =
        task {
            if not (Map.isEmpty brokenViews) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenViews do
                    let isSystem = Map.containsKey schemaName preload.Schemas
                    if isSystem then
                        critical <- true
                    for KeyValue(uvName, err) in schema do
                        let uvName = ({ schema = schemaName; name = uvName } : ResolvedUserViewRef).ToString()
                        if isSystem then
                            logger.LogError(err, "System view {uv} is broken", uvName)
                        else
                            logger.LogWarning(err, "Marking {uv} as broken", uvName)
                if critical then
                    failwith "Broken system user views"
                do! markBrokenUserViews conn.System brokenViews cancellationToken
        }

    let checkBrokenPermissions (conn : DatabaseTransaction) (brokenPerms : ErroredPermissions) (cancellationToken : CancellationToken) =
        task {
            if not (Map.isEmpty brokenPerms) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenPerms do
                    let isSystem = Map.containsKey schemaName preload.Schemas
                    if isSystem then
                        critical <- true
                    for KeyValue(roleName, role) in schema do
                        match role with
                        | EFatal err ->
                                if isSystem then
                                    logger.LogError(err, "System role {role} is broken", roleName)
                                else
                                    logger.LogWarning(err, "Marking {role} as broken", roleName)
                        | EDatabase errs ->
                            for KeyValue(allowedSchemaName, allowedSchema) in errs do
                                for KeyValue(allowedEntityName, err) in allowedSchema do
                                    let roleName = ({ schema = schemaName; name = roleName } : ResolvedRoleRef).ToString()
                                    let allowedName = ({ schema = allowedSchemaName; name = allowedEntityName } : ResolvedEntityRef).ToString()
                                    if isSystem then
                                        logger.LogError(err, "System role {role} is broken for entity {entity}", roleName, allowedName)
                                    else
                                        logger.LogWarning(err, "Marking {role} as broken for entity {entity}", roleName, allowedName)
                if critical then
                    failwith "Broken system roles"
                do! markBrokenPermissions conn.System brokenPerms cancellationToken
        }

    let generateViews layout userViews cancellationToken =
        let isolate = jsIsolates.Get()
        try
            let template = isolate.cache.GetOrCreate (fun () -> UserViewGeneratorTemplate isolate.isolate)
            generateUserViews template layout userViews cancellationToken
        finally
            jsIsolates.Return(isolate)

    let rec finishGetCachedState (transaction : DatabaseTransaction) (layout : Layout) (userMeta : SQL.DatabaseMeta) (isChanged : bool) (cancellationToken : CancellationToken) : Task<CachedState> = task {
        let! (transaction, currentVersion, brokenViews, mergedAttrs, brokenAttrs, triggers, brokenTriggers, prefetchedViews, sourceViews) = task {
            try
                let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                let (brokenAttrs, defaultAttrs) = resolveAttributes layout true sourceAttrs
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                let (brokenTriggers, triggers) = resolveTriggers layout true sourceTriggers

                let systemViews = preloadUserViews preload
                let! sourceViews = buildSchemaUserViews transaction.System cancellationToken
                let sourceViews = { Schemas = Map.union sourceViews.Schemas systemViews.Schemas } : SourceUserViews
                let sourceViews = generateViews layout sourceViews cancellationToken
                let! isChanged2 = updateUserViews transaction.System sourceViews cancellationToken
                let (brokenViews1, userViews) = resolveUserViews layout mergedAttrs true sourceViews

                let! currentVersion = ensureCurrentVersion transaction (isChanged || isChanged2) cancellationToken

                // To dry-run user views we need to stop the transaction.
                do! transaction.Commit ()
                let! (brokenViews2, prefetchedViews) = dryRunUserViews transaction.Connection.Query layout true None sourceViews userViews cancellationToken
                let transaction = new DatabaseTransaction (transaction.Connection)
                let brokenViews = unionErroredUserViews brokenViews1 brokenViews2
                return (transaction, currentVersion, brokenViews, mergedAttrs, brokenAttrs, triggers, brokenTriggers, prefetchedViews, sourceViews)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        let! currentVersion2 = task {
            try
                return! ensureCurrentVersion transaction false cancellationToken
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        if currentVersion2 <> currentVersion then
            let! sourceLayout = buildSchemaLayout transaction.System cancellationToken
            let (_, layout) = resolveLayout sourceLayout false
            let! state = finishGetCachedState transaction layout userMeta false cancellationToken
            let! meta = buildDatabaseMeta transaction.Transaction cancellationToken
            let userMeta = filterUserMeta preload meta
            return { state with Context = { state.Context with UserMeta = userMeta } }
        else
            try
                do! checkBrokenAttributes transaction brokenAttrs cancellationToken
                do! checkBrokenTriggers transaction brokenTriggers cancellationToken
                do! checkBrokenUserViews transaction brokenViews cancellationToken

                let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions
                do! checkBrokenPermissions transaction brokenPerms cancellationToken

                let systemViews = filterSystemViews sourceViews

                do! transaction.Commit ()

                let state =
                    { Layout = layout
                      Permissions = permissions
                      DefaultAttrs = mergedAttrs
                      Triggers = triggers
                      UserViews = prefetchedViews
                      SystemViews = systemViews
                      UserMeta = userMeta
                    }

                return { Version = currentVersion; Context = state }
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
    }

    let rec getMigrationLock (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<DatabaseTransaction> = task {
        // Try to get a lock. If we fail, wait till someone else releases it and then _restart the transaction and try again_.
        // This is because otherwise transaction gets to see older state of the database.
        let migrationLockParams = Map.singleton 0 (SQL.VInt migrationLockNumber)
        let! ret = task {
            try
                return! transaction.Connection.Query.ExecuteValueQuery "SELECT pg_try_advisory_xact_lock(@0)" migrationLockParams cancellationToken
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        match ret with
        | SQL.VBool true -> return transaction
        | _ ->
            try
                let! _ = transaction.Connection.Query.ExecuteNonQuery "SELECT pg_advisory_xact_lock(@0)" migrationLockParams cancellationToken
                ()
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
            transaction.Rollback ()
            let transaction = new DatabaseTransaction (transaction.Connection)
            return! getMigrationLock transaction cancellationToken
    }

    let getCachedState (transaction : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<CachedState> = task {
        let! transaction = getMigrationLock transaction cancellationToken
        let! (userMeta, layout, isChanged) = task {
            try
                let! (isChanged, layout, userMeta) = initialMigratePreload logger transaction preload cancellationToken
                return (userMeta, layout, isChanged)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        return! finishGetCachedState transaction layout userMeta isChanged cancellationToken
    }

    // Called when state update by another instance is detected.
    let rec rebuildFromDatabase (transaction : DatabaseTransaction) (currentVersion : int) (cancellationToken : CancellationToken) : Task<DatabaseTransaction * CachedState> = task {
        let! (transaction, layout, mergedAttrs, triggers, sourceUvs, userViews, prefetchedBadViews) = task {
            try
                let! sourceLayout = buildSchemaLayout transaction.System cancellationToken
                let (_, layout) = resolveLayout sourceLayout false

                let! sourceAttrs = buildSchemaAttributes transaction.System cancellationToken
                let (_, defaultAttrs) = resolveAttributes layout false sourceAttrs
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! sourceTriggers = buildSchemaTriggers transaction.System cancellationToken
                let (_, triggers) = resolveTriggers layout false sourceTriggers

                let! sourceUvs = buildSchemaUserViews transaction.System cancellationToken
                let (_, userViews) = resolveUserViews layout mergedAttrs false sourceUvs

                // To dry-run user views we need to stop the transaction.
                do! transaction.Commit ()
                // We dry-run those views that _can_ be failed here, outside of a transaction.
                let! (_, prefetchedBadViews) = dryRunUserViews transaction.Connection.Query layout false (Some true) sourceUvs userViews cancellationToken
                let transaction = new DatabaseTransaction(transaction.Connection)
                return (transaction, layout, mergedAttrs, triggers, sourceUvs, userViews, prefetchedBadViews)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        let! currentVersion2 = task {
            try
                return! ensureCurrentVersion transaction false cancellationToken
            with
            | ex ->
                transaction.Rollback ()
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

                // Another instance has already rebuilt them, so just load them from the database.
                let systemViews = filterSystemViews sourceUvs

                let! sourcePermissions = buildSchemaPermissions transaction.System cancellationToken
                let (_, permissions) = resolvePermissions layout false sourcePermissions

                let! meta = buildDatabaseMeta transaction.Transaction cancellationToken
                let userMeta = filterUserMeta preload meta

                let newState =
                    { Layout = layout
                      Permissions = permissions
                      DefaultAttrs = mergedAttrs
                      Triggers = triggers
                      UserViews = prefetchedViews
                      SystemViews = systemViews
                      UserMeta = userMeta
                    }

                return (transaction, { Version = currentVersion; Context = newState })
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
    }

    let mutable cachedState : CachedState option = None

    let getInitialCachedState (cancellationToken : CancellationToken) : Task<CachedState> = task {
        use conn = new DatabaseConnection(loggerFactory, connectionString)
        let transaction = new DatabaseTransaction(conn)
        if warmStartup then
                let! (transaction, ret) = getCurrentVersion transaction cancellationToken
                match ret with
                | None ->
                    return! getCachedState transaction cancellationToken
                | Some ver ->
                    let! (transaction, newState) = rebuildFromDatabase transaction ver cancellationToken
                    transaction.Rollback ()
                    return newState
        else
            return! getCachedState transaction cancellationToken
    }

    member this.LoggerFactory = loggerFactory
    member this.Preload = preload
    member this.EventLogger = eventLogger
    member this.ConnectionString = connectionString

    member this.GetCache (cancellationToken : CancellationToken) = task {
        let conn = new DatabaseConnection(loggerFactory, connectionString)
        try
            let transaction = new DatabaseTransaction(conn)
            let! (transaction, oldState) =
                task {
                    let! (transaction, ret) = getCurrentVersion transaction cancellationToken
                    match (cachedState, ret) with
                    | (_, None)
                    | (None, _) ->
                        transaction.Connection.Connection.UnprepareAll ()
                        let! newState = getCachedState transaction cancellationToken
                        anonymousViewsCache.Clear()
                        cachedState <- Some newState
                        let transaction = new DatabaseTransaction (transaction.Connection)
                        return (transaction, newState)
                    | (Some oldState, Some ver) ->
                        if oldState.Version <> ver then
                            transaction.Connection.Connection.UnprepareAll ()
                            let! (transaction, newState) = rebuildFromDatabase transaction ver cancellationToken
                            anonymousViewsCache.Clear()
                            cachedState <- Some newState
                            return (transaction, newState)
                        else
                            return (transaction, oldState)
                }

            let migrate () = task {
                logger.LogInformation("Starting migration")
                // Careful here not to evaluate user views before we do migration

                let! layoutSource = buildSchemaLayout transaction.System cancellationToken
                if not <| preloadLayoutIsUnchanged layoutSource preload then
                    raisef ContextException "Cannot modify system layout"
                let (_, layout) =
                    try
                        resolveLayout layoutSource false
                    with
                    | :? ResolveLayoutException as err -> raisefWithInner ContextException err "Failed to resolve layout"

                let! permissionsSource = buildSchemaPermissions transaction.System cancellationToken
                if not <| preloadPermissionsAreUnchanged permissionsSource preload then
                    raisef ResolveLayoutException "Cannot modify system permissions"
                let (_, permissions) =
                    try
                        resolvePermissions layout false permissionsSource
                    with
                    | :? ResolvePermissionsException as err -> raisefWithInner ContextException err "Failed to resolve permissions"

                let! attrsSource = buildSchemaAttributes transaction.System cancellationToken
                if not <| preloadAttributesAreUnchanged attrsSource preload then
                    raisef ResolveLayoutException "Cannot modify system default attributes"
                let (_, defaultAttrs) =
                    try
                        resolveAttributes layout false attrsSource
                    with
                    | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve default attributes"
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! triggersSource = buildSchemaTriggers transaction.System cancellationToken
                if not <| preloadTriggersAreUnchanged triggersSource preload then
                    raisef ResolveLayoutException "Cannot modify system triggers"
                let (_, triggers) =
                    try
                        resolveTriggers layout false triggersSource
                    with
                    | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve triggers"
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! userViewsSource = buildSchemaUserViews transaction.System cancellationToken
                if filterSystemViews userViewsSource <> oldState.Context.SystemViews then
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
                let userViewsSource = generateViews layout userViewsSource cancellationToken
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
                let! versionEntry = transaction.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstAsync()
                versionEntry.Value <- string newVersion
                // Serialized access error: 40001, may need to process it differently later (retry with fallback?)
                try
                    let! _ = transaction.System.SaveChangesAsync()
                    ()
                with
                | :? DbUpdateException as err -> raisefWithInner ContextException err "State update error"

                do! transaction.Commit()

                let! (_, goodUserViews) = dryRunUserViews conn.Query layout false (Some true) newUserViewsSource userViews cancellationToken

                (conn :> IDisposable).Dispose()

                let newState =
                    { Layout = layout
                      Permissions = permissions
                      DefaultAttrs = mergedAttrs
                      Triggers = triggers
                      UserViews = mergePrefetchedUserViews badUserViews goodUserViews
                      SystemViews = filterSystemViews userViewsSource
                      UserMeta = wantedUserMeta
                    }

                anonymousViewsCache.Clear()
                if migrated then
                    // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                    NpgsqlConnection.ClearPool(transaction.Connection.Connection)
                cachedState <- Some { Version = newVersion; Context = newState }
                return ()
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

            return
                { new IContext with
                      member this.Transaction = transaction
                      member this.Connection = conn
                      member this.State = oldState.Context
                      member this.Migrate () = migrate ()
                      member this.GetAnonymousView query = getAnonymousView query
                      member this.ResolveAnonymousView homeSchema query = resolveAnonymousView homeSchema query
                      member this.WriteEvent event = eventLogger.WriteEvent(connectionString, event, cancellationToken)
                      member this.Dispose () =
                          (transaction :> IDisposable).Dispose()
                          (conn :> IDisposable).Dispose()
                }
        with
        | e ->
            (conn :> IDisposable).Dispose()
            return reraise' e
    }
