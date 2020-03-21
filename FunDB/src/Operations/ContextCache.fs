module FunWithFlags.FunDB.Operations.ContextCache

open System
open System.Linq
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open FluidCaching
open Npgsql
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Utils
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
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.UserViews.Update
open FunWithFlags.FunDB.UserViews.Schema
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Layout.Integrity
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.EventLogger

type ContextException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ContextException (message, null)

[<NoEquality; NoComparison>]
type CachedRequestContext =
    { layout : Layout
      userViews : PrefetchedUserViews
      permissions : Permissions
      defaultAttrs : MergedDefaultAttributes
      systemViews : SourceUserViews
      userMeta : SQL.DatabaseMeta
    }

type IContext =
    inherit IDisposable

    abstract member Migrate : unit -> Task<unit>
    abstract member GetAnonymousView : string -> Task<PrefetchedUserView>
    abstract Connection : DatabaseConnection with get
    abstract Transaction : DatabaseTransaction with get
    abstract State : CachedRequestContext with get

[<NoEquality; NoComparison>]
type private AnonymousUserView =
    { query : string
      uv : PrefetchedUserView
    }

let private versionField = "StateVersion"
let private fallbackVersion = 0
let private migrationLockNumber = 0

[<NoEquality; NoComparison>]
type CachedState =
    { version : int
      context : CachedRequestContext
    }

type ContextCacheStore (loggerFactory : ILoggerFactory, preload : Preload, connectionString : string, eventLogger : EventLogger, warmStartup : bool) =
    let logger = loggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values
    let anonymousViewsCache = FluidCache<AnonymousUserView>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousViewsIndex = anonymousViewsCache.AddIndex("byQuery", fun uv -> uv.query)

    let filterSystemViews (views : SourceUserViews) : SourceUserViews =
        { schemas = filterPreloadedSchemas preload views.schemas }

    let filterUserLayout (layout : Layout) : Layout =
        { schemas = filterUserSchemas preload layout.schemas }

    // If `None` cold rebuild if needed.
    let getCurrentVersion (transaction : DatabaseTransaction) : Task<DatabaseTransaction * int option> = task {
        let! (transaction, versionEntry) = task {
            try
                let! versionEntry = transaction.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefaultAsync()
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

    let ensureCurrentVersion (conn : DatabaseTransaction) (bump : bool) : Task<int> = task {
        let! versionEntry = conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefaultAsync()
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

    let checkBrokenAttributes (conn : DatabaseTransaction) (brokenAttrs : ErroredDefaultAttributes) =
        task {
            if not (Map.isEmpty brokenAttrs) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenAttrs do
                    let isSystem = Map.containsKey schemaName preload.schemas
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
                do! markBrokenAttributes conn.System brokenAttrs
        }

    let checkBrokenUserViews (conn : DatabaseTransaction) (brokenViews : ErroredUserViews) =
        task {
            if not (Map.isEmpty brokenViews) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenViews do
                    let isSystem = Map.containsKey schemaName preload.schemas
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
                do! markBrokenUserViews conn.System brokenViews
        }

    let checkBrokenPermissions (conn : DatabaseTransaction) (brokenPerms : ErroredPermissions) =
        task {
            if not (Map.isEmpty brokenPerms) then
                let mutable critical = false
                for KeyValue(schemaName, schema) in brokenPerms do
                    let isSystem = Map.containsKey schemaName preload.schemas
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
                do! markBrokenPermissions conn.System brokenPerms
        }
    
    let rec finishGetCachedState (transaction : DatabaseTransaction) (layout : Layout) (userMeta : SQL.DatabaseMeta) (isChanged : bool) : Task<CachedState> = task {
        let! (transaction, currentVersion, brokenViews, mergedAttrs, brokenAttrs, prefetchedViews, sourceViews) = task {
            try
                let! currentVersion = ensureCurrentVersion transaction isChanged
                // We don't do commit here and instead resolve as much as possible first to fail early.

                let! sourceAttrs = buildSchemaAttributes transaction.System
                let (brokenAttrs, defaultAttrs) = resolveAttributes layout true sourceAttrs

                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs
                let! sourceViews = buildSchemaUserViews transaction.System
                let (brokenViews1, userViews) = resolveUserViews layout mergedAttrs true sourceViews

                // To dry-run user views we need to stop the transaction.
                do! transaction.Commit ()
                let! (brokenViews2, prefetchedViews) = dryRunUserViews transaction.Connection.Query layout true None sourceViews userViews
                let transaction = new DatabaseTransaction (transaction.Connection)
                let brokenViews = unionErroredUserViews brokenViews1 brokenViews2
                return (transaction, currentVersion, brokenViews, mergedAttrs, brokenAttrs, prefetchedViews, sourceViews)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex            
        }
        let! currentVersion2 = task {
            try
                return! ensureCurrentVersion transaction false
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }    
        if currentVersion2 <> currentVersion then
            let! sourceLayout = buildSchemaLayout transaction.System
            let (_, layout) = resolveLayout sourceLayout false
            let! state = finishGetCachedState transaction layout userMeta false
            let! meta = buildDatabaseMeta transaction.Transaction
            let userMeta = filterUserMeta preload meta
            return { state with context = { state.context with userMeta = userMeta } }
        else
            try
                do! checkBrokenAttributes transaction brokenAttrs
                do! checkBrokenUserViews transaction brokenViews

                let! sourcePermissions = buildSchemaPermissions transaction.System
                let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions
                do! checkBrokenPermissions transaction brokenPerms

                let systemViews = filterSystemViews sourceViews

                do! transaction.Commit ()

                let state =
                    { layout = layout
                      permissions = permissions
                      defaultAttrs = mergedAttrs
                      userViews = prefetchedViews
                      systemViews = systemViews
                      userMeta = userMeta
                    }

                return { version = currentVersion; context = state }
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex    
    }

    let rec getMigrationLock (transaction : DatabaseTransaction) : Task<DatabaseTransaction> = task {
        // Try to get a lock. If we fail, wait till someone else releases it and then _restart the transaction and try again_.
        // This is because otherwise transaction gets to see older state of the database.
        let migrationLockParams = Map.singleton 0 (SQL.VInt migrationLockNumber)
        let! ret = task {
            try
                return! transaction.Connection.Query.ExecuteValueQuery "SELECT pg_try_advisory_xact_lock(@0)" migrationLockParams
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        match ret with
        | SQL.VBool true -> return transaction
        | _ ->
            try
                let! _ = transaction.Connection.Query.ExecuteNonQuery "SELECT pg_advisory_xact_lock(@0)" migrationLockParams
                ()
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
            transaction.Rollback ()
            let transaction = new DatabaseTransaction (transaction.Connection)
            return! getMigrationLock transaction            
    }

    let getCachedState (transaction : DatabaseTransaction) : Task<CachedState> = task {       
        let! transaction = getMigrationLock transaction 
        let! (userMeta, layout, isChanged) = task {
            try
                let! (isChanged1, layout, userMeta) = initialMigratePreload logger transaction preload
                let systemViews = preloadUserViews layout preload
                let! isChanged2 = updateUserViews transaction.System systemViews
                let isChanged = isChanged1 || isChanged2
                return (userMeta, layout, isChanged)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        return! finishGetCachedState transaction layout userMeta isChanged
    }

    // Called when state update by another instance is detected.
    let rec rebuildFromDatabase (transaction : DatabaseTransaction) (currentVersion : int) : Task<DatabaseTransaction * CachedState> = task {
        let! (transaction, layout, mergedAttrs, sourceUvs, userViews, prefetchedBadViews) = task {
            try
                let! sourceLayout = buildSchemaLayout transaction.System
                let (_, layout) = resolveLayout sourceLayout false

                let! sourceAttrs = buildSchemaAttributes transaction.System
                let (_, defaultAttrs) = resolveAttributes layout false sourceAttrs
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! sourceUvs = buildSchemaUserViews transaction.System
                let (_, userViews) = resolveUserViews layout mergedAttrs false sourceUvs

                // To dry-run user views we need to stop the transaction.
                do! transaction.Commit ()
                // We dry-run those views that _can_ be failed here, outside of a transaction.
                let! (_, prefetchedBadViews) = dryRunUserViews transaction.Connection.Query layout false (Some true) sourceUvs userViews
                let transaction = new DatabaseTransaction(transaction.Connection)
                return (transaction, layout, mergedAttrs, sourceUvs, userViews, prefetchedBadViews)
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        let! currentVersion2 = task {
            try
                return! ensureCurrentVersion transaction false
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
        }
        if currentVersion2 <> currentVersion then
            return! rebuildFromDatabase transaction currentVersion2
        else
            try
                // Now dry-run those views that _cannot_ fail - we can get an exception here and stop, which is the point -
                // views with `allowBroken = true` fail on first error and so can be dry-run inside a transaction.
                let! (_, prefetchedGoodViews) = dryRunUserViews transaction.Connection.Query layout false (Some false) sourceUvs userViews
                let prefetchedViews = mergePrefetchedUserViews prefetchedBadViews prefetchedGoodViews

                // Another instance has already rebuilt them, so just load them from the database.
                let systemViews = filterSystemViews sourceUvs

                let! sourcePermissions = buildSchemaPermissions transaction.System
                let (_, permissions) = resolvePermissions layout false sourcePermissions

                let! meta = buildDatabaseMeta transaction.Transaction
                let userMeta = filterUserMeta preload meta

                let newState =
                    { layout = layout
                      permissions = permissions
                      defaultAttrs = mergedAttrs
                      userViews = prefetchedViews
                      systemViews = systemViews
                      userMeta = userMeta
                    }

                return (transaction, { version = currentVersion; context = newState })
            with
            | ex ->
                transaction.Rollback ()
                return reraise' ex
    }

    let mutable cachedState =
        use conn = new DatabaseConnection(loggerFactory, connectionString)
        let transaction = new DatabaseTransaction(conn)
        if warmStartup then
            Task.awaitSync <| task {
                let! (transaction, ret) = getCurrentVersion transaction
                match ret with
                | None ->
                    return! getCachedState transaction
                | Some ver ->
                    let! (transaction, newState) = rebuildFromDatabase transaction ver
                    transaction.Rollback ()
                    return newState
            }
        else
            Task.awaitSync <| getCachedState transaction

    member this.LoggerFactory = loggerFactory
    member this.Preload = preload
    member this.EventLogger = eventLogger
    member this.ConnectionString = connectionString

    member this.WriteEvent (entry : EventEntry) =
        eventLogger.WriteEvent ((connectionString, entry))

    member this.GetCache () = task {
        let conn = new DatabaseConnection(loggerFactory, connectionString)
        try
            let oldState = cachedState
            let transaction = new DatabaseTransaction(conn)
            let! (transaction, oldState) =
                task {
                    let! (transaction, ret) = getCurrentVersion transaction
                    match ret with
                    | None ->
                        transaction.Connection.Connection.UnprepareAll ()
                        let! newState = getCachedState transaction
                        anonymousViewsCache.Clear()
                        cachedState <- newState
                        let transaction = new DatabaseTransaction (transaction.Connection)
                        return (transaction, newState)
                    | Some ver ->
                        if oldState.version <> ver then
                            transaction.Connection.Connection.UnprepareAll ()
                            let! (transaction, newState) = rebuildFromDatabase transaction ver
                            anonymousViewsCache.Clear()
                            cachedState <- newState
                            return (transaction, newState)
                        else
                            return (transaction, oldState)
                }

            let migrate () = task {
                logger.LogInformation("Starting migration")
                // Careful here not to evaluate user views before we do migration

                let! layoutSource = buildSchemaLayout transaction.System
                if not <| preloadLayoutIsUnchanged layoutSource preload then
                    raisef ContextException "Cannot modify system layout"
                let (_, layout) =
                    try
                        resolveLayout layoutSource false
                    with
                    | :? ResolveLayoutException as err -> raisefWithInner ContextException err "Failed to resolve layout"

                let! permissionsSource = buildSchemaPermissions transaction.System
                if not <| preloadPermissionsAreUnchanged permissionsSource preload then
                    raisef ResolveLayoutException "Cannot modify system permissions"
                let (_, permissions) =
                    try
                        resolvePermissions layout false permissionsSource
                    with
                    | :? ResolvePermissionsException as err -> raisefWithInner ContextException err "Failed to resolve permissions"

                let! attrsSource = buildSchemaAttributes transaction.System
                if not <| preloadAttributesAreUnchanged attrsSource preload then
                    raisef ResolveLayoutException "Cannot modify system default attributes"
                let (_, defaultAttrs) =
                    try
                        resolveAttributes layout false attrsSource
                    with
                    | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve default attributes"
                let mergedAttrs = mergeDefaultAttributes layout defaultAttrs

                let! userViewsSource = buildSchemaUserViews transaction.System
                if filterSystemViews userViewsSource <> oldState.context.systemViews then
                    raisef ContextException "Cannot modify system user views"

                // Actually migrate.
                let (newAssertions, wantedUserMeta) = buildFullLayoutMeta layout (filterUserLayout layout)
                let migration = planDatabaseMigration oldState.context.userMeta wantedUserMeta
                let! migrated = task {
                    try
                        return! migrateDatabase transaction.Connection.Query migration
                    with
                    | :? QueryException as err -> return raisefWithInner ContextException err "Migration error"
                }

                logger.LogInformation("Updating system user views")
                let newSystemViews = preloadUserViews layout preload
                let! _ = updateUserViews transaction.System newSystemViews

                let! newUserViewsSource = buildSchemaUserViews transaction.System
                let (_, userViews) =
                    try
                        resolveUserViews layout mergedAttrs false newUserViewsSource
                    with
                    | :? UserViewResolveException as err -> raisefWithInner ContextException err "Failed to resolve user views"
                let! (_, badUserViews) = task {
                    try
                        return! dryRunUserViews conn.Query layout false (Some false) newUserViewsSource userViews
                    with
                    | :? UserViewDryRunException as err -> return raisefWithInner ContextException err "Failed to resolve user views"
                }

                let oldAssertions = buildAssertions oldState.context.layout (filterUserLayout oldState.context.layout)
                for check in Set.difference newAssertions oldAssertions do
                    logger.LogInformation("Running integrity check {check}", check)
                    try
                        do! checkAssertion transaction.Connection.Query layout check
                    with
                    | :? LayoutIntegrityException as err -> return raisefWithInner ContextException err "Failed to perform integrity check"

                // We update state now and check user views _after_ that.
                // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                let newVersion = oldState.version + 1
                let! versionEntry = transaction.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstAsync()
                versionEntry.Value <- string newVersion
                // Serialized access error: 40001, may need to process it differently later (retry with fallback?)
                try
                    let! _ = transaction.System.SaveChangesAsync()
                    ()
                with
                | :? DbUpdateException as err -> raisefWithInner ContextException err "State update error"

                do! transaction.Commit()

                let! (_, goodUserViews) = dryRunUserViews conn.Query layout false (Some true) newUserViewsSource userViews

                (conn :> IDisposable).Dispose()

                let newState =
                    { layout = layout
                      permissions = permissions
                      defaultAttrs = mergedAttrs
                      userViews = mergePrefetchedUserViews badUserViews goodUserViews
                      systemViews = newSystemViews
                      userMeta = wantedUserMeta
                    }

                anonymousViewsCache.Clear()
                if migrated then
                    // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                    NpgsqlConnection.ClearPool(transaction.Connection.Connection)
                cachedState <- { version = newVersion; context = newState }
                return ()
            }

            let createNewAnonymousView query = task {
                let findExistingView = oldState.context.userViews.Find >> Option.map (Result.map (fun pref -> pref.uv))
                let uv = resolveAnonymousUserView oldState.context.layout oldState.context.defaultAttrs findExistingView query
                let! prefetched = dryRunAnonymousUserView conn.Query oldState.context.layout uv
                let ret =
                    { uv = prefetched
                      query = query
                    }
                return ret
            }
            let newAnonymousViewCreator = ItemCreator(createNewAnonymousView)

            let getAnonymousView (query : string) : Task<PrefetchedUserView> = task {
                let! ret = anonymousViewsIndex.GetItem(query, newAnonymousViewCreator)
                return ret.uv
            }

            return
                { new IContext with
                      member this.Transaction = transaction
                      member this.Connection = conn
                      member this.State = oldState.context
                      member this.Migrate () = migrate ()
                      member this.GetAnonymousView query = getAnonymousView query
                      member this.Dispose () =
                          (transaction :> IDisposable).Dispose()
                          (conn :> IDisposable).Dispose()
                }
        with
        | e ->
            (conn :> IDisposable).Dispose()
            return reraise' e
    }
