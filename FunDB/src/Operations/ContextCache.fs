module FunWithFlags.FunDB.Operations.ContextCache

open System
open System.Linq
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open FluidCaching
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
open FunWithFlags.FunDB.Attributes.Schema
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.UserViews.Update
open FunWithFlags.FunDB.UserViews.Schema
open FunWithFlags.FunDB.Layout.Meta
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
module SQL = FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.Operations.Connection
open FunWithFlags.FunDB.Operations.Preload

type ContextException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ContextException (message, null)

[<NoComparison>]
type CachedRequestContext =
    { layout : Layout
      userViews : UserViews
      permissions : FlatPermissions
      defaultAttrs : MergedDefaultAttributes
      systemViews : SourceUserViews
      userMeta : SQL.DatabaseMeta
    }

type IContext =
    inherit IDisposable

    abstract member Migrate : unit -> Task<unit>
    abstract member GetAnonymousView : string -> Task<ResolvedUserView>
    abstract Connection : DatabaseConnection with get
    abstract State : CachedRequestContext with get

[<NoComparison>]
type private AnonymousUserView =
    { query : string
      uv : ResolvedUserView
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

    // Called when state update by another instance is detected.
    let rebuildFromDatabase (conn : DatabaseConnection) = task {
        let! meta = buildDatabaseMeta conn.Transaction
        let userMeta = filterUserMeta preload meta

        let! sourceLayout = buildSchemaLayout conn.System
        let layout = resolveLayout sourceLayout

        let! sourcePermissions = buildSchemaPermissions conn.System
        let (_, permissions) = resolvePermissions layout false sourcePermissions

        let! sourceAttrs = buildSchemaAttributes conn.System
        let (_, defaultAttrs) = resolveAttributes layout false sourceAttrs
        let mergedAttrs = mergeDefaultAttributes defaultAttrs

        let! sourceUvs = buildSchemaUserViews conn.System
        let! (_, userViews) = resolveUserViews conn.Query layout mergedAttrs false sourceUvs

        // Another instance has already rebuilt them, so just load them from the database.
        let systemViews = filterSystemViews sourceUvs

        return
            { layout = layout
              permissions = flattenPermissions layout permissions
              defaultAttrs = mergedAttrs
              userViews = userViews
              systemViews = systemViews
              userMeta = userMeta
            }
    }

    // Returns a version and whether a rebuild should be force-performed.
    let getCurrentVersion (conn : DatabaseConnection) (bump : bool) : Task<bool * int> = task {
        match! conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstOrDefaultAsync() with
        | null ->
            let newEntry = StateValue (Name = versionField, Value = string fallbackVersion)
            ignore <| conn.System.State.Add(newEntry)
            let! _ = conn.System.SaveChangesAsync()
            return (true, fallbackVersion)
        | entry ->
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

    let getCachedState () =
        task {
            use conn = new DatabaseConnection(loggerFactory, connectionString)
            let! (isChanged, layout, userMeta) = initialMigratePreload logger conn preload
            let! (_, currentVersion) = getCurrentVersion conn isChanged

            logger.LogInformation("Updating system user views")
            let systemViews = preloadUserViews layout preload
            let! _ = updateUserViews conn.System systemViews

            let! sourceAttrs = buildSchemaAttributes conn.System
            let (brokenAttrs, defaultAttrs) = resolveAttributes layout true sourceAttrs

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

            let mergedAttrs = mergeDefaultAttributes defaultAttrs

            let! sourceViews = buildSchemaUserViews conn.System
            let! (brokenViews, userViews) = resolveUserViews conn.Query layout mergedAttrs true sourceViews

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

            let! sourcePermissions = buildSchemaPermissions conn.System
            let (brokenPerms, permissions) = resolvePermissions layout true sourcePermissions

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

            let state =
                { layout = layout
                  permissions = flattenPermissions layout permissions
                  defaultAttrs = mergedAttrs
                  userViews = userViews
                  systemViews = systemViews
                  userMeta = userMeta
                }

            do! conn.Commit()

            return (currentVersion, state)
        }
    let mutable cachedState = Task.awaitSync <| getCachedState ()

    member this.LoggerFactory = loggerFactory
    member this.Preload = preload

    member this.GetCache () = task {
        let conn = new DatabaseConnection(loggerFactory, connectionString)
        try
            let (oldVersion, veryOldState) = cachedState
            let! (forceRebuild, currentVersion) = getCurrentVersion conn false
            let! oldState = task {
                if forceRebuild || oldVersion <> currentVersion then
                    let! newState = rebuildFromDatabase conn
                    cachedState <- (currentVersion, newState)
                    anonymousViewsCache.Clear()
                    return newState
                else
                    return veryOldState
            }

            let migrate () = task {
                logger.LogInformation("Starting migration")
                // Careful here not to evaluate user views before we do migration

                let! layoutSource = buildSchemaLayout conn.System
                if not <| preloadLayoutIsUnchanged layoutSource preload then
                    raisef ContextException "Cannot modify system layout"
                let layout =
                    try
                        resolveLayout layoutSource
                    with
                    | :? ResolveLayoutException as err -> raisefWithInner ContextException err "Failed to resolve layout"

                let! permissionsSource = buildSchemaPermissions conn.System
                if not <| preloadPermissionsAreUnchanged permissionsSource preload then
                    raisef ResolveLayoutException "Cannot modify system permissions"
                let (_, permissions) =
                    try
                        resolvePermissions layout false permissionsSource
                    with
                    | :? ResolvePermissionsException as err -> raisefWithInner ContextException err "Failed to resolve permissions"

                let! attrsSource = buildSchemaAttributes conn.System
                if not <| preloadAttributesAreUnchanged attrsSource preload then
                    raisef ResolveLayoutException "Cannot modify system default attributes"
                let (_, defaultAttrs) =
                    try
                        resolveAttributes layout false attrsSource
                    with
                    | :? ResolveAttributesException as err -> raisefWithInner ContextException err "Failed to resolve default attributes"
                let mergedAttrs = mergeDefaultAttributes defaultAttrs                

                let! userViewsSource = buildSchemaUserViews conn.System
                if filterSystemViews userViewsSource <> oldState.systemViews then
                    raisef ContextException "Cannot modify system user views"

                // Actually migrate.
                let wantedUserMeta = buildLayoutMeta (filterUserLayout layout)
                let migration = migrateDatabase oldState.userMeta wantedUserMeta
                try
                    for action in migration do
                        logger.LogInformation("Migration step: {}", action)
                        let! _ = conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
                        ()
                with
                | :? QueryException as err -> raisefWithInner ContextException err "Migration error"

                logger.LogInformation("Updating system user views")
                let newSystemViews = preloadUserViews layout preload
                let! _ = updateUserViews conn.System newSystemViews

                let! newUserViewsSource = buildSchemaUserViews conn.System
                let! (_, userViews) = task {
                    try
                        return! resolveUserViews conn.Query layout mergedAttrs false newUserViewsSource
                    with
                    | :? UserViewResolveException as err -> return raisefWithInner ContextException err "Failed to resolve user views"
                }

                let newState =
                    { layout = layout
                      permissions = flattenPermissions layout permissions
                      defaultAttrs = mergedAttrs
                      userViews = userViews
                      systemViews = newSystemViews
                      userMeta = wantedUserMeta
                    }

                // At this point we are sure there is a valid versionEntry because GetCache should have been called.
                let newVersion = oldVersion + 1
                let! versionEntry = conn.System.State.AsTracking().Where(fun x -> x.Name = versionField).FirstAsync()
                versionEntry.Value <- string newVersion
                // Serialized access error: 40001, may need to process it differently later (retry with fallback?)
                try
                    let! _ = conn.System.SaveChangesAsync()
                    ()
                with
                | :? DbUpdateException as err -> raisefWithInner ContextException err "State update error"
                do! conn.Commit()
                (conn :> IDisposable).Dispose()
                anonymousViewsCache.Clear()
                cachedState <- (newVersion, newState)
                return ()
            }

            let getAnonymousView (query : string) : Task<ResolvedUserView> = task {
                let createNew query = task {
                    let! uv = resolveAnonymousUserView conn.Query oldState.layout oldState.defaultAttrs oldState.userViews query
                    let ret =
                        { uv = uv
                          query = query
                        }
                    return ret
                }
                let! r = anonymousViewsIndex.GetItem(query, ItemCreator(createNew))
                return r.uv
            }

            return
                { new IContext with
                      member this.Connection = conn
                      member this.State = oldState
                      member this.Migrate () = migrate ()
                      member this.GetAnonymousView query = getAnonymousView query
                      member this.Dispose () =
                          (conn :> IDisposable).Dispose()
                }
        with
        | e ->
            (conn :> IDisposable).Dispose()
            return reraise' e
    }