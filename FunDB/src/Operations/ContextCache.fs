module FunWithFlags.FunDB.Operations.ContextCache

open System
open System.Linq
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open FluidCaching
open Npgsql
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Permissions.Flatten
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
open FunWithFlags.FunDB.Layout.Meta
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Operations.Preload

type ContextException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ContextException (message, null)

[<NoComparison>]
type CachedRequestContext =
    { layout : Layout
      userViews : PrefetchedUserViews
      permissions : FlatPermissions
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

[<NoComparison>]
type private AnonymousUserView =
    { query : string
      uv : PrefetchedUserView
    }

type ContextCacheStore (loggerFactory : ILoggerFactory, connectionString : string, preload : Preload) =
    let versionField = "StateVersion"
    let fallbackVersion = 0

    let logger = loggerFactory.CreateLogger<ContextCacheStore>()
    // FIXME: random values
    let anonymousViewsCache = FluidCache<AnonymousUserView>(64, TimeSpan.FromSeconds(0.0), TimeSpan.FromSeconds(600.0), fun () -> DateTime.Now)
    let anonymousViewsIndex = anonymousViewsCache.AddIndex("byQuery", fun uv -> uv.query)

    let filterSystemViews (views : SourceUserViews) : SourceUserViews =
        { schemas = filterPreloadedSchemas preload views.schemas }

    let filterUserLayout (layout : Layout) : Layout =
        { schemas = filterUserSchemas preload layout.schemas }


    // Returns a version and whether a rebuild should be force-performed.
    let getCurrentVersion (conn : DatabaseTransaction) (bump : bool) : Task<bool * int> = task {
        let! state = conn.System.State.AsTracking().ToDictionaryAsync(fun x -> x.Name)
        match tryGetValue state versionField with
        | None ->
            let newEntry = StateValue (Name = versionField, Value = string fallbackVersion)
            ignore <| conn.System.State.Add(newEntry)
            let! _ = conn.System.SaveChangesAsync()
            return (true, fallbackVersion)
        | Some entry ->
            match tryIntInvariant entry.Value with
            | Some v ->
                if bump then
                    let newVersion = v + 1
                    entry.Value <- string newVersion
                    let! _ = conn.System.SaveChangesAsync()
                    return (true, newVersion)
                else
                    return (false, v)
            | None ->
                entry.Value <- string fallbackVersion
                let! _ = conn.System.SaveChangesAsync()
                return (true, fallbackVersion)
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
                        for KeyValue(allowedSchemaName, allowedSchema) in role do
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

    let rec getCachedState (transaction1 : DatabaseTransaction) = task {
        let! (isChanged1, layout, userMeta) = initialMigratePreload logger transaction1 preload
        let systemViews = preloadUserViews layout preload
        let! isChanged2 = updateUserViews transaction1.System systemViews
        let isChanged = isChanged1 || isChanged2
        let! (_, currentVersion) = getCurrentVersion transaction1 isChanged
        // We don't do commit here and instead resolve as much as possible first to fail early.

        let! sourceAttrs = buildSchemaAttributes transaction1.System
        let (brokenAttrs, defaultAttrs) = resolveAttributes layout true sourceAttrs

        let mergedAttrs = mergeDefaultAttributes defaultAttrs
        let! sourceViews = buildSchemaUserViews transaction1.System
        let (brokenViews1, userViews) = resolveUserViews layout mergedAttrs true sourceViews

        // To dry-run user views we need to stop the transaction.
        do! transaction1.Commit ()
        let! (brokenViews2, prefetchedViews) = dryRunUserViews transaction1.Connection.Query layout true None sourceViews userViews
        let transaction2 = new DatabaseTransaction (transaction1.Connection)
        let! (_, currentVersion2) = getCurrentVersion transaction2 false
        if currentVersion2 <> currentVersion then
            return! getCachedState transaction2
        else
            do! checkBrokenAttributes transaction2 brokenAttrs
            do! checkBrokenUserViews transaction2 brokenViews1
            do! checkBrokenUserViews transaction2 brokenViews2

            let! sourcePermissions = buildSchemaPermissions transaction2.System
            let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions
            do! checkBrokenPermissions transaction2 brokenPerms

            do! transaction2.Commit ()

            let state =
                { layout = layout
                  permissions = flattenPermissions layout permissions
                  defaultAttrs = mergedAttrs
                  userViews = prefetchedViews
                  systemViews = systemViews
                  userMeta = userMeta
                }

            return (currentVersion, state)
    }

    let mutable cachedState =
        use conn = new DatabaseConnection(loggerFactory, connectionString)
        let transaction = new DatabaseTransaction(conn)
        Task.awaitSync <| getCachedState transaction

    // Called when state update by another instance is detected.
    let rec rebuildFromDatabase (transaction1 : DatabaseTransaction) (currentVersion : int) = task {
        // Clear prepared statements so that things don't break if e.g. database types have changed.
        transaction1.Connection.Connection.UnprepareAll ()
        let! sourceLayout = buildSchemaLayout transaction1.System
        let layout = resolveLayout sourceLayout

        let! sourceAttrs = buildSchemaAttributes transaction1.System
        let (_, defaultAttrs) = resolveAttributes layout false sourceAttrs
        let mergedAttrs = mergeDefaultAttributes defaultAttrs

        let! sourceUvs = buildSchemaUserViews transaction1.System
        let (_, userViews) = resolveUserViews layout mergedAttrs false sourceUvs

        // To dry-run user views we need to stop the transaction.
        do! transaction1.Commit ()
        // We dry-run those views that _can_ be failed here, outside of a transaction.
        let! (_, prefetchedBadViews) = dryRunUserViews transaction1.Connection.Query layout false (Some true) sourceUvs userViews
        let transaction2 = new DatabaseTransaction(transaction1.Connection)
        let! (_, currentVersion2) = getCurrentVersion transaction2 false
        if currentVersion2 <> currentVersion then
            return! rebuildFromDatabase transaction2 currentVersion2
        else
            // Now dry-run those views that _cannot_ fail - we can get an exception here and stop, which is the point -
            // views with `allowBroken = true` fail on first error and so can be dry-run inside a transaction.
            let! (_, prefetchedGoodViews) = dryRunUserViews transaction2.Connection.Query layout false (Some false) sourceUvs userViews
            let prefetchedViews = mergePrefetchedUserViews prefetchedBadViews prefetchedGoodViews

            // Another instance has already rebuilt them, so just load them from the database.
            let systemViews = filterSystemViews sourceUvs

            let! sourcePermissions = buildSchemaPermissions transaction2.System
            let (_, permissions) = resolvePermissions layout false sourcePermissions

            let! meta = buildDatabaseMeta transaction2.Transaction
            let userMeta = filterUserMeta preload meta

            let newState =
                { layout = layout
                  permissions = flattenPermissions layout permissions
                  defaultAttrs = mergedAttrs
                  userViews = prefetchedViews
                  systemViews = systemViews
                  userMeta = userMeta
                }

            anonymousViewsCache.Clear()
            cachedState <- (currentVersion2, newState)

            return (currentVersion2, transaction2, newState)
    }

    member this.LoggerFactory = loggerFactory
    member this.Preload = preload

    member this.GetCache () = task {
        let conn = new DatabaseConnection(loggerFactory, connectionString)
        try
            let (veryOldVersion, veryOldState) = cachedState
            let oldTransaction = new DatabaseTransaction(conn)
            let! (newTransaction, oldVersion, oldState) =
                task {
                    let! (forceRebuild, currentVersion) = getCurrentVersion oldTransaction false
                    if forceRebuild || veryOldVersion <> currentVersion then
                        let! (newVersion, newTransaction, newState) = rebuildFromDatabase oldTransaction currentVersion
                        return (newTransaction, newVersion, newState)
                    else
                        return (oldTransaction, currentVersion, veryOldState)
                }

            let migrate () = task {
                logger.LogInformation("Starting migration")
                // Careful here not to evaluate user views before we do migration

                let! layoutSource = buildSchemaLayout newTransaction.System
                if not <| preloadLayoutIsUnchanged layoutSource preload then
                    raisef ContextException "Cannot modify system layout"
                let layout =
                    try
                        resolveLayout layoutSource
                    with
                    | :? ResolveLayoutException as err -> raisefWithInner ContextException err "Failed to resolve layout"

                let! permissionsSource = buildSchemaPermissions newTransaction.System
                if not <| preloadPermissionsAreUnchanged permissionsSource preload then
                    raisef ResolveLayoutException "Cannot modify system permissions"
                let (_, permissions) =
                    try
                        resolvePermissions layout false permissionsSource
                    with
                    | :? ResolvePermissionsException as err -> raisefWithInner ContextException err "Failed to resolve permissions"

                let! attrsSource = buildSchemaAttributes newTransaction.System
                if not <| preloadAttributesAreUnchanged attrsSource preload then
                    raisef ResolveLayoutException "Cannot modify system default attributes"
                let (_, defaultAttrs) =
                    try
                        resolveAttributes layout false attrsSource
                    with
                    | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve default attributes"
                let mergedAttrs = mergeDefaultAttributes defaultAttrs

                let! userViewsSource = buildSchemaUserViews newTransaction.System
                if filterSystemViews userViewsSource <> oldState.systemViews then
                    raisef ContextException "Cannot modify system user views"

                // Actually migrate.
                let wantedUserMeta = buildLayoutMeta (filterUserLayout layout)
                let migration = planDatabaseMigration oldState.userMeta wantedUserMeta
                let! migrared = task {
                    try
                        return! migrateDatabase newTransaction.Connection.Query migration
                    with
                    | :? QueryException as err -> return raisefWithInner ContextException err "Migration error"
                }

                logger.LogInformation("Updating system user views")
                let newSystemViews = preloadUserViews layout preload
                let! _ = updateUserViews newTransaction.System newSystemViews

                let! newUserViewsSource = buildSchemaUserViews newTransaction.System
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

                // We update state now and check user views _after_ that.
                // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                let newVersion = oldVersion + 1
                let! versionEntry = newTransaction.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstAsync()
                versionEntry.Value <- string newVersion
                // Serialized access error: 40001, may need to process it differently later (retry with fallback?)
                try
                    let! _ = newTransaction.System.SaveChangesAsync()
                    ()
                with
                | :? DbUpdateException as err -> raisefWithInner ContextException err "State update error"

                do! newTransaction.Commit()

                let! (_, goodUserViews) = dryRunUserViews conn.Query layout false (Some true) newUserViewsSource userViews

                (conn :> IDisposable).Dispose()

                let newState =
                    { layout = layout
                      permissions = flattenPermissions layout permissions
                      defaultAttrs = mergedAttrs
                      userViews = mergePrefetchedUserViews badUserViews goodUserViews
                      systemViews = newSystemViews
                      userMeta = wantedUserMeta
                    }

                anonymousViewsCache.Clear()
                if migrared then
                    // There is no way to force-clear prepared statements for all connections in the pool, so we clear the pool itself instead.
                    NpgsqlConnection.ClearPool(newTransaction.Connection.Connection)
                cachedState <- (newVersion, newState)
                return ()
            }

            let getAnonymousView (query : string) : Task<PrefetchedUserView> = task {
                let createNew query = task {
                    let findExistingView = oldState.userViews.Find >> Option.map (Result.map (fun pref -> pref.uv))
                    let uv = resolveAnonymousUserView oldState.layout oldState.defaultAttrs findExistingView query
                    let! prefetched = dryRunAnonymousUserView conn.Query oldState.layout uv
                    let ret =
                        { uv = prefetched
                          query = query
                        }
                    return ret
                }
                let! r = anonymousViewsIndex.GetItem(query, ItemCreator(createNew))
                return r.uv
            }

            return
                { new IContext with
                      member this.Transaction = newTransaction
                      member this.Connection = conn
                      member this.State = oldState
                      member this.Migrate () = migrate ()
                      member this.GetAnonymousView query = getAnonymousView query
                      member this.Dispose () =
                          (newTransaction :> IDisposable).Dispose()
                          (conn :> IDisposable).Dispose()
                }
        with
        | e ->
            (conn :> IDisposable).Dispose()
            return reraise' e
    }