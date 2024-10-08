module OzmaDB.Operations.Preload

open System.IO
open System.Linq
open System.Text
open System.Threading
open System.Threading.Tasks
open System.ComponentModel
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharpPlus
open System.Security.Cryptography

open OzmaDB.OzmaUtils
open OzmaDB.SQL.Meta
open OzmaDB.SQL.Migration
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.Layout.System
open OzmaDB.Layout.Types
open OzmaDB.Layout.Schema
open OzmaDB.Layout.Source
open OzmaDB.Layout.Resolve
open OzmaDB.Layout.Update
open OzmaDB.Layout.Integrity
open OzmaDB.Layout.Correlate
open OzmaDB.Layout.Meta
open OzmaDB.UserViews.Source
open OzmaDB.Permissions.Resolve
open OzmaDB.Permissions.Types
open OzmaDB.Permissions.Source
open OzmaDB.Permissions.Update
open OzmaDB.Attributes.Parse
open OzmaDB.Attributes.Resolve
open OzmaDB.Attributes.Types
open OzmaDB.Attributes.Source
open OzmaDB.Attributes.Update
open OzmaDB.Actions.Resolve
open OzmaDB.Actions.Types
open OzmaDB.Actions.Run
open OzmaDB.Actions.Source
open OzmaDB.Actions.Update
open OzmaDB.Modules.Types
open OzmaDB.Modules.Resolve
open OzmaDB.Modules.Source
open OzmaDB.Modules.Update
open OzmaDB.Triggers.Resolve
open OzmaDB.Triggers.Types
open OzmaDB.Triggers.Source
open OzmaDB.Triggers.Update
open OzmaDB.Triggers.Run
open OzmaDB.UserViews.Types
open OzmaDB.UserViews.DryRun
open OzmaDB.UserViews.Update
open OzmaDB.Connection
open OzmaDB.Operations.Update

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.DDL
open OzmaDBSchema.System

type SourcePreloadedSchema =
    { Entities: Map<EntityName, SourceEntity>
      Roles: Map<RoleName, SourceRole>
      DefaultAttributes: Map<SchemaName, SourceAttributesSchema>
      Modules: Map<ModulePath, SourceModule>
      Actions: Map<ActionName, SourceAction>
      Triggers: Map<SchemaName, SourceTriggersSchema>
      UserViewGenerator: string option // Path to .js file
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

type SourcePreload =
    { [<JsonProperty(Required = Required.DisallowNull)>]
      Schemas: Map<SchemaName, SourcePreloadedSchema> }

type SourcePreloadFile =
    { Preload: SourcePreload
      DirName: string }

let emptySourcePreloadFile: SourcePreloadFile =
    { Preload = { Schemas = Map.empty }
      DirName = "" }

type PreloadedSchema =
    { Schema: SourceSchema
      Permissions: SourcePermissionsSchema
      DefaultAttributes: SourceAttributesDatabase
      Modules: SourceModulesSchema
      Actions: SourceActionsSchema
      Triggers: SourceTriggersDatabase
      UserViews: SourceUserViewsSchema }

type Preload =
    { Schemas: Map<SchemaName, PreloadedSchema> }

let readSourcePreload (path: string) : SourcePreloadFile =
    use stream = File.OpenText path
    use jsonStream = new JsonTextReader(stream)
    let serializer = JsonSerializer.CreateDefault()
    let preload = serializer.Deserialize<SourcePreload>(jsonStream)

    if isRefNull preload then
        invalidArg "path" "Preload cannot be null"

    { Preload = preload
      DirName = Path.GetDirectoryName path }

type HashedPreload(preload: Preload) =
    let preloadStr = JsonConvert.SerializeObject preload
    let preloadBytes = Encoding.UTF8.GetBytes preloadStr

    let preloadHash =
        use hasher = SHA1.Create()
        hasher.ComputeHash(preloadBytes) |> String.hexBytes

    member this.Preload = preload
    member this.Hash = preloadHash

// Empty schemas in Roles aren't reflected in the database so we need to remove them -- otherwise a "change" will always be detected.
let private normalizeRole (role: SourceRole) =
    { role with
        Permissions =
            { role.Permissions with
                Schemas =
                    role.Permissions.Schemas
                    |> Map.filter (fun name schema -> not (Map.isEmpty schema.Entities)) } }

let private resolvePreloadedSchema (dirname: string) (preload: SourcePreloadedSchema) : PreloadedSchema =
    let schema =
        { Entities = preload.Entities
          Description = preload.Description
          Metadata = preload.Metadata }
        : SourceSchema

    let permissions = { Roles = Map.map (fun name -> normalizeRole) preload.Roles }

    let defaultAttributes =
        { Schemas = preload.DefaultAttributes }: SourceAttributesDatabase

    let modules = { Modules = preload.Modules }: SourceModulesSchema
    let actions = { Actions = preload.Actions }: SourceActionsSchema
    let triggers = { Schemas = preload.Triggers }: SourceTriggersDatabase

    let readUserViewScript (path: string) =
        let realPath =
            if Path.IsPathRooted(path) then
                path
            else
                Path.Join(dirname, path)

        let script = File.ReadAllText realPath
        { AllowBroken = false; Script = script }

    let userViews =
        { UserViews = Map.empty
          GeneratorScript = Option.map readUserViewScript preload.UserViewGenerator }
        : SourceUserViewsSchema

    { Schema = schema
      Permissions = permissions
      DefaultAttributes = defaultAttributes
      Modules = modules
      Actions = actions
      Triggers = triggers
      UserViews = userViews }

let preloadLayout (preload: Preload) : SourceLayout =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Schema) }

let preloadPermissions (preload: Preload) : SourcePermissions =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Permissions) }

let preloadDefaultAttributes (preload: Preload) : SourceDefaultAttributes =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.DefaultAttributes) }

let preloadModules (preload: Preload) : SourceModules =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Modules) }

let preloadActions (preload: Preload) : SourceActions =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Actions) }

let preloadTriggers (preload: Preload) : SourceTriggers =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Triggers) }

let preloadUserViews (preload: Preload) : SourceUserViews =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.UserViews) }

let resolvePreload (source: SourcePreloadFile) : Preload =
    let preloadedSchemas =
        source.Preload.Schemas
        |> Map.map (fun name -> resolvePreloadedSchema source.DirName)

    if Map.containsKey funSchema preloadedSchemas then
        failwith "Preload cannot contain public schema"

    let systemPreload =
        { Schema = buildSystemSchema typeof<SystemContext>
          Permissions = emptySourcePermissionsSchema
          DefaultAttributes = emptySourceAttributesDatabase
          Modules = emptySourceModulesSchema
          Actions = emptySourceActionsSchema
          Triggers = emptySourceTriggersDatabase
          UserViews = emptySourceUserViewsSchema }

    { Schemas = Map.add funSchema systemPreload preloadedSchemas }

let preloadLayoutIsUnchanged (sourceLayout: SourceLayout) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourceSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Schema
        | None -> true

    sourceLayout.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadPermissionsAreUnchanged (sourcePerms: SourcePermissions) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourcePermissionsSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Permissions
        | None -> true

    sourcePerms.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadAttributesAreUnchanged (sourceAttrs: SourceDefaultAttributes) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourceAttributesDatabase) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.DefaultAttributes
        | None -> true

    sourceAttrs.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadModulesAreUnchanged (sourceModules: SourceModules) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourceModulesSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Modules
        | None -> true

    sourceModules.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadActionsAreUnchanged (sourceActions: SourceActions) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourceActionsSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Actions
        | None -> true

    sourceActions.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadTriggersAreUnchanged (sourceTriggers: SourceTriggers) (preload: Preload) =
    let notChanged (name: SchemaName, schema: SourceTriggersDatabase) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Triggers
        | None -> true

    sourceTriggers.Schemas |> Map.toSeq |> Seq.forall notChanged

let filterPreloadedSchemas (preload: Preload) =
    Map.filter (fun name _ -> Map.containsKey name preload.Schemas)

let filterUserSchemas (preload: Preload) =
    Map.filter (fun name _ -> not <| Map.containsKey name preload.Schemas)

let filterPreloadedMeta (preload: Preload) (meta: SQL.DatabaseMeta) =
    { Schemas = Map.filter (fun name _ -> Map.containsKey (decompileName name) preload.Schemas) meta.Schemas
      Extensions = meta.Extensions }
    : SQL.DatabaseMeta

let filterUserMeta (preload: Preload) (meta: SQL.DatabaseMeta) =
    { Schemas = Map.filter (fun name _ -> not <| Map.containsKey (decompileName name) preload.Schemas) meta.Schemas
      Extensions = meta.Extensions }
    : SQL.DatabaseMeta

let buildFullLayoutMeta (layout: Layout) (subLayout: Layout) : LayoutAssertions * SQL.DatabaseMeta =
    let meta1 = buildLayoutMeta layout subLayout
    let assertions = buildAssertions layout subLayout
    let meta2 = buildAssertionsMeta layout assertions
    let meta = SQL.unionDatabaseMeta meta1 meta2
    (assertions, meta)

let buildSchemaLayoutExceptPreload
    (systemContext: SystemContext)
    (preload: Preload)
    (cancellationToken: CancellationToken)
    : Task<SourceLayout> =
    let neededSchemas = preload.Schemas |> Map.keys |> Seq.map string |> Seq.toArray

    let schemaCheck =
        Expr.toExpressionFunc <@ fun (schema: Schema) -> not (neededSchemas.Contains(schema.Name)) @>

    buildSchemaLayout systemContext (Some schemaCheck) cancellationToken

let buildFullSchemaLayout
    (systemContext: SystemContext)
    (preload: Preload)
    (cancellationToken: CancellationToken)
    : Task<SourceLayout> =
    task {
        let! sourceUserLayout = buildSchemaLayoutExceptPreload systemContext preload cancellationToken
        return unionSourceLayout (preloadLayout preload) sourceUserLayout
    }

let buildUserDatabaseMeta
    (transaction: DatabaseTransaction)
    (preload: Preload)
    (cancellationToken: CancellationToken)
    : Task<SQL.DatabaseMeta> =
    task {
        let! meta =
            buildDatabaseMeta transaction cancellationToken
            |> Task.map correlateDatabaseMeta

        return
            SQL.filterDatabaseMeta
                (fun (SQL.SQLName name) -> not <| Map.containsKey (OzmaQLName name) preload.Schemas)
                meta
    }

let checkBrokenLayout
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (layout: Layout)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in layout.Schemas do
            for KeyValue(entityName, entity) in schema.Entities do
                for KeyValue(compName, maybeComputedField) in entity.ComputedFields do
                    match maybeComputedField with
                    | Error err when not err.AllowBroken ->
                        let isSystem = Map.containsKey schemaName preload.Schemas

                        if isSystem || not allowAutoMark then
                            critical <- true

                            logger.LogWarning(
                                err.Error,
                                "Computed field {ref} as broken",
                                { Entity =
                                    { Schema = schemaName
                                      Name = entityName }
                                  Name = compName }
                            )
                        else
                            logger.LogWarning(
                                err.Error,
                                "Marking computed field {ref} as broken",
                                { Entity =
                                    { Schema = schemaName
                                      Name = entityName }
                                  Name = compName }
                            )
                    | _ -> ()

            if critical then
                failwith "Cannot mark some layout as broken"

            do! markBrokenLayout conn.System layout cancellationToken
    }

let checkBrokenAttributes
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (attrs: DefaultAttributes)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in attrs.Schemas do
            for KeyValue(attrsSchemaName, attrsSchema) in schema.Schemas do
                for KeyValue(attrsEntityName, attrsEntity) in attrsSchema.Entities do
                    for KeyValue(attrsFieldName, maybeAttr) in attrsEntity.Fields do
                        match maybeAttr with
                        | Error err when not err.AllowBroken ->
                            let isSystem = Map.containsKey schemaName preload.Schemas

                            let defFieldName =
                                string (
                                    { Entity =
                                        { Schema = attrsSchemaName
                                          Name = attrsEntityName }
                                      Name = attrsFieldName }
                                    : ResolvedFieldRef
                                )

                            if isSystem || not allowAutoMark then
                                critical <- true

                                logger.LogError(
                                    err.Error,
                                    "Default attributes from {schema} are broken for field {field}",
                                    schemaName,
                                    defFieldName
                                )
                            else
                                logger.LogWarning(
                                    err.Error,
                                    "Marking default attributes from {schema} for {field} as broken",
                                    schemaName,
                                    defFieldName
                                )
                        | _ -> ()

        if critical then
            failwith "Cannot mark some default attributes as broken"

        do! markBrokenAttributes conn.System attrs cancellationToken
    }

let checkBrokenActions
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (actions: PreparedActions)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in actions.Schemas do
            for KeyValue(actionName, maybeAction) in schema.Actions do
                match maybeAction with
                | Error err when not err.AllowBroken ->
                    let isSystem = Map.containsKey schemaName preload.Schemas

                    let actionNameStr =
                        string (
                            { Schema = schemaName
                              Name = actionName }
                            : ActionRef
                        )

                    if isSystem || not allowAutoMark then
                        critical <- true
                        logger.LogError(err.Error, "Action {name} from {schema} is broken", actionNameStr, schemaName)
                    else
                        logger.LogWarning(
                            err.Error,
                            "Marking action {name} from {schema} as broken",
                            actionNameStr,
                            schemaName
                        )
                | _ -> ()

        if critical then
            failwith "Cannot mark some system actions as broken"

        do! markBrokenActions conn.System actions cancellationToken
    }

let checkBrokenModules
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (modules: ResolvedModules)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in modules.Schemas do
            for KeyValue(modulePath, maybeModule) in schema.Modules do
                match maybeModule with
                | Error err when not err.AllowBroken ->
                    let isSystem = Map.containsKey schemaName preload.Schemas

                    let moduleNameStr =
                        string (
                            { Schema = schemaName
                              Path = modulePath }
                            : ModuleRef
                        )

                    if isSystem || not allowAutoMark then
                        critical <- true
                        logger.LogError(err.Error, "Module {name} from {schema} is broken", moduleNameStr, schemaName)
                    else
                        logger.LogWarning(
                            err.Error,
                            "Marking module {name} from {schema} as broken",
                            moduleNameStr,
                            schemaName
                        )
                | _ -> ()

        if critical then
            failwith "Cannot mark some system modules as broken"

        do! markBrokenModules conn.System modules cancellationToken
    }

let checkBrokenTriggers
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (triggers: PreparedTriggers)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in triggers.Schemas do
            for KeyValue(triggerSchemaName, triggersSchema) in schema.Schemas do
                for KeyValue(triggerEntityName, triggersEntity) in triggersSchema.Entities do
                    for KeyValue(triggerName, maybeTrigger) in triggersEntity.Triggers do
                        match maybeTrigger with
                        | Error err when not err.AllowBroken ->
                            let isSystem = Map.containsKey schemaName preload.Schemas

                            let triggerNameStr =
                                string (
                                    { Schema = schemaName
                                      Entity =
                                        { Schema = triggerSchemaName
                                          Name = triggerEntityName }
                                      Name = triggerName }
                                    : TriggerRef
                                )

                            if isSystem || not allowAutoMark then
                                critical <- true

                                logger.LogError(
                                    err.Error,
                                    "Trigger {name} from {schema} is broken",
                                    triggerNameStr,
                                    schemaName
                                )
                            else
                                logger.LogWarning(
                                    err.Error,
                                    "Marking trigger {name} from {schema} as broken",
                                    triggerNameStr,
                                    schemaName
                                )
                        | _ -> ()

        if critical then
            failwith "Cannot mark some triggers as broken"

        do! markBrokenTriggers conn.System triggers cancellationToken
    }

let checkBrokenUserViews
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (userViews: PrefetchedUserViews)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, maybeSchema) in userViews.Schemas do
            let isSystem = Map.containsKey schemaName preload.Schemas

            match maybeSchema with
            | Ok schema ->
                for KeyValue(uvName, maybeUv) in schema.UserViews do
                    match maybeUv with
                    | Error err when not err.AllowBroken ->
                        let uvName = string ({ Schema = schemaName; Name = uvName }: ResolvedUserViewRef)

                        if isSystem || not allowAutoMark then
                            critical <- true
                            logger.LogError(err.Error, "View {uv} is broken", uvName)
                        else
                            logger.LogWarning(err.Error, "Marking {uv} as broken", uvName)
                    | _ -> ()
            | Error err when not err.AllowBroken ->
                if isSystem || not allowAutoMark then
                    critical <- true
                    logger.LogError(err.Error, "View generator for schema {schema} is broken", schemaName)
                else
                    logger.LogWarning(err.Error, "Marking view generator for schema {schema} as broken", schemaName)
            | _ -> ()

        if critical then
            failwith "Cannot mark some user views as broken"

        do! markBrokenUserViews conn.System userViews cancellationToken
    }

let checkBrokenPermissions
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (perms: Permissions)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let mutable critical = false

        for KeyValue(schemaName, schema) in perms.Schemas do
            let isSystem = Map.containsKey schemaName preload.Schemas

            for KeyValue(roleName, maybeRole) in schema.Roles do
                match maybeRole with
                | Ok role ->
                    for KeyValue(allowedSchemaName, allowedSchema) in role.Permissions.Schemas do
                        for KeyValue(allowedEntityName, maybeEntity) in allowedSchema.Entities do
                            let markRole (e: exn) =
                                let roleName = string ({ Schema = schemaName; Name = roleName }: ResolvedRoleRef)

                                let allowedName =
                                    string (
                                        { Schema = allowedSchemaName
                                          Name = allowedEntityName }
                                        : ResolvedEntityRef
                                    )

                                if isSystem || not allowAutoMark then
                                    critical <- true

                                    logger.LogError(
                                        e,
                                        "Role {role} is broken for entity {entity}",
                                        roleName,
                                        allowedName
                                    )
                                else
                                    logger.LogWarning(
                                        e,
                                        "Marking {role} as broken for entity {entity}",
                                        roleName,
                                        allowedName
                                    )

                            match maybeEntity with
                            | Ok entity ->
                                match entity.Insert with
                                | Error e when not entity.AllowBroken -> markRole e
                                | _ -> ()

                                match entity.Delete with
                                | Error e when not entity.AllowBroken -> markRole e
                                | _ -> ()
                            | Error err when not err.AllowBroken -> markRole err.Error
                            | _ -> ()
                | Error err when not err.AllowBroken ->
                    if isSystem || not allowAutoMark then
                        critical <- true
                        logger.LogError(err.Error, "Role {role} is broken", roleName)
                    else
                        logger.LogWarning(err.Error, "Marking {role} as broken", roleName)
                | _ -> ()

        if critical then
            failwith "Cannot mark some roles as broken"

        do! markBrokenPermissions conn.System perms cancellationToken
    }

// Returns only user meta
let initialMigratePreload
    (logger: ILogger)
    (allowAutoMark: bool)
    (preload: Preload)
    (conn: DatabaseTransaction)
    (cancellationToken: CancellationToken)
    : Task<bool * Layout * SQL.DatabaseMeta> =
    task {
        logger.LogInformation("Migrating system entities to the current version")
        let sourcePreloadLayout = preloadLayout preload
        let preloadLayout = resolveLayout false sourcePreloadLayout
        let (_, newSystemMeta) = buildFullLayoutMeta preloadLayout preloadLayout
        let! currentMeta = buildDatabaseMeta conn cancellationToken |> Task.map correlateDatabaseMeta
        let currentSystemMeta = filterPreloadedMeta preload currentMeta

        let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
        let! _ = migrateDatabase conn.Connection.Query systemMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () =
            task {
                let! currentMeta = buildDatabaseMeta conn cancellationToken |> Task.map correlateDatabaseMeta
                let currentSystemMeta = filterPreloadedMeta preload currentMeta
                let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta

                if Array.isEmpty systemMigration then
                    return true
                else
                    return
                        failwithf
                            "Non-indempotent migration detected: %s"
                            (systemMigration |> Array.map string |> String.concat ", ")
            }

        assert (Task.awaitSync <| sanityCheck ())

        let! sourceUserLayout = buildSchemaLayoutExceptPreload conn.System preload cancellationToken
        let sourceLayout = unionSourceLayout sourcePreloadLayout sourceUserLayout
        let layout = resolveLayout true sourceLayout
        do! checkBrokenLayout logger allowAutoMark preload conn layout cancellationToken

        // We migrate layout first so that permissions and attributes have schemas in the table.
        let! layoutUpdate = updateLayout conn.System sourcePreloadLayout cancellationToken

        let! permissionsUpdate =
            task {
                let permissions = preloadPermissions preload

                try
                    return! updatePermissions conn.System permissions cancellationToken
                with e ->
                    // Maybe we'll get a better error
                    let perms = resolvePermissions preloadLayout emptyHasUserView false permissions
                    return reraise' e
            }

        let! attributesUpdate =
            task {
                let defaultAttributes = preloadDefaultAttributes preload

                try
                    return! updateAttributes conn.System defaultAttributes cancellationToken
                with e ->
                    // Maybe we'll get a better error
                    let parsedAttrs = parseAttributes false defaultAttributes
                    let errors = resolveAttributes preloadLayout emptyHasUserView false parsedAttrs
                    return reraise' e
            }

        let! actionsUpdate =
            task {
                let actions = preloadActions preload

                try
                    return! updateActions conn.System actions cancellationToken
                with e ->
                    // Maybe we'll get a better error
                    let actions = resolveActions preloadLayout false actions
                    return reraise' e
            }

        let! triggersUpdate =
            task {
                let triggers = preloadTriggers preload

                try
                    return! updateTriggers conn.System triggers cancellationToken
                with e ->
                    // Maybe we'll get a better error
                    let triggers = resolveTriggers preloadLayout false triggers
                    return reraise' e
            }

        let! modulesUpdate =
            task {
                let modules = preloadModules preload

                try
                    return! updateModules conn.System modules cancellationToken
                with e ->
                    // Maybe we'll get a better error
                    let modules = resolveModules preloadLayout modules
                    return reraise' e
            }

        let fullUpdate =
            seq {
                layoutUpdate
                permissionsUpdate
                attributesUpdate
                actionsUpdate
                triggersUpdate
                modulesUpdate
            }
            |> Seq.fold1 unionUpdateResult

        do! deleteDeferredFromUpdate layout conn fullUpdate cancellationToken

        logger.LogInformation("Phase 2: Migrating all remaining entities")

        let userLayout =
            filterLayout (fun name -> not <| Map.containsKey name preloadLayout.Schemas) layout

        let (_, newUserMeta) = buildFullLayoutMeta layout userLayout

        let currentUserMeta =
            { filterUserMeta preload currentMeta with
                Extensions = newSystemMeta.Extensions }

        let userMigration = planDatabaseMigration currentUserMeta newUserMeta
        let! _ = migrateDatabase conn.Connection.Query userMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () =
            task {
                let! currentMeta = buildDatabaseMeta conn cancellationToken |> Task.map correlateDatabaseMeta
                let currentUserMeta = filterUserMeta preload currentMeta
                let systemMigration = planDatabaseMigration currentUserMeta newUserMeta

                if Array.isEmpty systemMigration then
                    return true
                else
                    return
                        failwithf
                            "Non-indempotent migration detected: %s"
                            (systemMigration |> Array.map string |> String.concat ", ")
            }

        assert (Task.awaitSync <| sanityCheck ())

        return (not <| updateResultIsEmpty fullUpdate, layout, newUserMeta)
    }
