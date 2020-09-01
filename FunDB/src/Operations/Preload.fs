module FunWithFlags.FunDB.Operations.Preload

open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.Affine
open Npgsql
open System.Data.HashFunction.CityHash

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.System
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Layout.Integrity
open FunWithFlags.FunDB.Layout.Meta
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.Triggers.Resolve
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Update
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Update
open FunWithFlags.FunDB.Connection
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDBSchema.System

type SourcePreloadedSchema =
    { Entities : Map<EntityName, SourceEntity>
      Roles : Map<RoleName, SourceRole>
      DefaultAttributes : Map<SchemaName, SourceAttributesSchema>
      Triggers : Map<SchemaName, SourceTriggersSchema>
      UserViewGenerator : string option // Path to .js file
    }

type SourcePreload =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      Schemas : Map<SchemaName, SourcePreloadedSchema>
    }

type SourcePreloadFile =
    { Preload : SourcePreload
      DirName : string
    }

let emptySourcePreloadFile : SourcePreloadFile =
    { Preload = { Schemas = Map.empty }
      DirName = ""
    }

type PreloadedSchema =
    { Schema : SourceSchema
      Permissions : SourcePermissionsSchema
      DefaultAttributes : SourceAttributesDatabase
      Triggers : SourceTriggersDatabase
      UserViews : SourceUserViewsSchema
    }

type Preload =
    { Schemas : Map<SchemaName, PreloadedSchema>
    }

let readSourcePreload (path : string) : SourcePreloadFile =
    use stream = File.OpenText path
    use jsonStream = new JsonTextReader(stream)
    let serializer = JsonSerializer.CreateDefault()
    let preload = serializer.Deserialize<SourcePreload>(jsonStream)
    { Preload = preload
      DirName = Path.GetDirectoryName path
    }

let preloadHasher = CityHashFactory.Instance.Create()

type HashedPreload (preload : Preload) =
    let preloadStr = JsonConvert.SerializeObject preload
    let preloadBytes = Encoding.UTF8.GetBytes preloadStr
    let preloadHash = preloadHasher.ComputeHash(preloadBytes).AsHexString()

    member this.Preload = preload
    member this.Hash = preloadHash

// Empty schemas in Roles aren't reflected in the database so we need to remove them -- otherwise a "change" will always be detected.
let private normalizeRole (role : SourceRole) =
    { role with
          Permissions =
              { role.Permissions with
                    Schemas = role.Permissions.Schemas |> Map.filter (fun name schema -> not (Map.isEmpty schema.Entities))
              }
    }

let private resolvePreloadedSchema (dirname : string) (preload : SourcePreloadedSchema) : PreloadedSchema =
    let schema =
        { Entities = preload.Entities
        } : SourceSchema
    let permissions = { Roles = Map.map (fun name -> normalizeRole) preload.Roles }
    let defaultAttributes = { Schemas = preload.DefaultAttributes } : SourceAttributesDatabase
    let triggers = { Schemas = preload.Triggers } : SourceTriggersDatabase
    let readUserViewScript (path : string) =
        let realPath =
            if Path.IsPathRooted(path) then
                path
            else
                Path.Join(dirname, path)
        let script = File.ReadAllText realPath
        { AllowBroken = false; Script = script }
    let userViews =
        { UserViews = Map.empty
          GeneratorScript = Option.map readUserViewScript preload.UserViewGenerator
        } : SourceUserViewsSchema

    { Schema = schema
      Permissions = permissions
      DefaultAttributes = defaultAttributes
      Triggers = triggers
      UserViews = userViews
    }

let preloadLayout (preload : Preload) : SourceLayout =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Schema) }

let preloadPermissions (preload : Preload) : SourcePermissions =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Permissions) }

let preloadDefaultAttributes (preload : Preload) : SourceDefaultAttributes =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.DefaultAttributes) }

let preloadTriggers (preload : Preload) : SourceTriggers =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.Triggers) }

let preloadUserViews (preload : Preload) : SourceUserViews =
    { Schemas = preload.Schemas |> Map.map (fun name schema -> schema.UserViews) }

let resolvePreload (source : SourcePreloadFile) : Preload =
    let preloadedSchemas = source.Preload.Schemas |> Map.map (fun name -> resolvePreloadedSchema source.DirName)
    if Map.containsKey funSchema preloadedSchemas then
        failwith "Preload cannot contain public schema"
    let systemPreload =
        { Schema = buildSystemSchema typeof<SystemContext>
          Permissions = emptySourcePermissionsSchema
          DefaultAttributes = emptySourceAttributesDatabase
          Triggers = emptySourceTriggersDatabase
          UserViews = emptySourceUserViewsSchema
        }
    { Schemas = Map.add funSchema systemPreload preloadedSchemas
    }

let preloadLayoutIsUnchanged (sourceLayout : SourceLayout) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourceSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Schema
        | None -> true
    sourceLayout.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadPermissionsAreUnchanged (sourcePerms : SourcePermissions) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourcePermissionsSchema) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Permissions
        | None -> true
    sourcePerms.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadAttributesAreUnchanged (sourceAttrs : SourceDefaultAttributes) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourceAttributesDatabase) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.DefaultAttributes
        | None -> true
    sourceAttrs.Schemas |> Map.toSeq |> Seq.forall notChanged

let preloadTriggersAreUnchanged (sourceTriggers : SourceTriggers) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourceTriggersDatabase) =
        match Map.tryFind name preload.Schemas with
        | Some existing -> schema = existing.Triggers
        | None -> true
    sourceTriggers.Schemas |> Map.toSeq |> Seq.forall notChanged

let filterPreloadedSchemas (preload : Preload) = Map.filter (fun name _ -> Map.containsKey name preload.Schemas)

let filterUserSchemas (preload : Preload) = Map.filter (fun name _ -> not <| Map.containsKey name preload.Schemas)

let filterPreloadedMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { Schemas = Map.filter (fun name _ -> Map.containsKey (FunQLName name) preload.Schemas) meta.Schemas
    } : SQL.DatabaseMeta

let filterUserMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { Schemas = Map.filter (fun name _ -> not <| Map.containsKey (FunQLName name) preload.Schemas) meta.Schemas
    } : SQL.DatabaseMeta

let buildFullLayoutMeta (layout : Layout) (subLayout : Layout) : Set<LayoutAssertion> * SQL.DatabaseMeta =
    let meta1 = buildLayoutMeta layout subLayout
    let assertions = buildAssertions layout subLayout
    let meta2 = buildAssertionsMeta layout assertions
    let meta = SQL.unionDatabaseMeta meta1 meta2
    (assertions, meta)

let buildFullSchemaLayout (systemContext : SystemContext) (preload : Preload) (cancellationToken : CancellationToken) : Task<SourceLayout> =
    task {
        let! sourceUserLayout = buildSchemaLayout systemContext (Map.keys preload.Schemas) cancellationToken
        return unionSourceLayout (preloadLayout preload) sourceUserLayout
    }

let buildUserDatabaseMeta (transaction : NpgsqlTransaction) (preload : Preload) (cancellationToken : CancellationToken) : Task<SQL.DatabaseMeta> =
    buildDatabaseMeta transaction (Map.keys preload.Schemas |> Seq.map compileName) cancellationToken

let private checkBrokenLayout (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (brokenLayout : ErroredLayout) (cancellationToken : CancellationToken) =
    unitTask {
        let mutable critical = false
        for KeyValue(schemaName, schema) in brokenLayout do
            for KeyValue(entityName, entity) in schema do
                let isSystem = Map.containsKey schemaName preload.Schemas
                for KeyValue(compName, err) in entity.ComputedFields do
                        if isSystem then
                            logger.LogWarning(err, "System computed field {ref} as broken", { entity = { schema = schemaName; name = entityName }; name = compName })
                        else
                            logger.LogWarning(err, "Marking computed field {ref} as broken", { entity = { schema = schemaName; name = entityName }; name = compName })

            if critical then
                failwith "Broken system layout"
            do! markBrokenLayout conn.System brokenLayout cancellationToken
    }


let checkBrokenAttributes (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (brokenAttrs : ErroredDefaultAttributes) (cancellationToken : CancellationToken) =
    unitTask {
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

let checkBrokenTriggers (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (brokenTriggers : ErroredTriggers) (cancellationToken : CancellationToken) =
    unitTask {
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

let checkBrokenUserViews (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (brokenViews : ErroredUserViews) (cancellationToken : CancellationToken) =
    unitTask {
        let mutable critical = false
        for KeyValue(schemaName, mschema) in brokenViews do
            let isSystem = Map.containsKey schemaName preload.Schemas
            if isSystem then
                critical <- true
            match mschema with
            | UEUserViews schema ->
                for KeyValue(uvName, err) in schema do
                    let uvName = ({ schema = schemaName; name = uvName } : ResolvedUserViewRef).ToString()
                    if isSystem then
                        logger.LogError(err, "System view {uv} is broken", uvName)
                    else
                        logger.LogWarning(err, "Marking {uv} as broken", uvName)
            | UEGenerator err ->
                if isSystem then
                    logger.LogError(err, "System view generator for schema {schema} is broken", schemaName)
                else
                    logger.LogWarning(err, "Marking view generator for schema {schema} as broken", schemaName)
        if critical then
            failwith "Broken system user views"
        do! markBrokenUserViews conn.System brokenViews cancellationToken
    }

let checkBrokenPermissions (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (brokenPerms : ErroredPermissions) (cancellationToken : CancellationToken) =
    unitTask {
        let mutable critical = false
        for KeyValue(schemaName, schema) in brokenPerms do
            let isSystem = Map.containsKey schemaName preload.Schemas
            if isSystem then
                critical <- true
            for KeyValue(roleName, role) in schema do
                match role with
                | ERFatal err ->
                        if isSystem then
                            logger.LogError(err, "System role {role} is broken", roleName)
                        else
                            logger.LogWarning(err, "Marking {role} as broken", roleName)
                | ERDatabase errs ->
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

// Returns only user meta
let initialMigratePreload (logger :ILogger) (preload : Preload) (conn : DatabaseTransaction) (cancellationToken : CancellationToken) : Task<bool * Layout * SQL.DatabaseMeta> =
    task {
        logger.LogInformation("Migrating system entities to the current version")
        let sourcePreloadLayout = preloadLayout preload
        let (_, preloadLayout) = resolveLayout sourcePreloadLayout false
        let (_, newSystemMeta) = buildFullLayoutMeta preloadLayout preloadLayout
        let! currentMeta = buildDatabaseMeta conn.Transaction Seq.empty cancellationToken
        let currentSystemMeta = filterPreloadedMeta preload currentMeta

        let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
        let! _ = migrateDatabase conn.Connection.Query systemMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () = task {
            let! currentMeta = buildDatabaseMeta conn.Transaction Seq.empty cancellationToken
            let currentSystemMeta = filterPreloadedMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
            if Array.isEmpty systemMigration then
                return true
            else
                return failwithf "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
        }
        assert (Task.awaitSync <| sanityCheck ())

        // We migrate layout first so that permissions and attributes have schemas in the table.
        let! changed1 = updateLayout conn.System sourcePreloadLayout cancellationToken
        let permissions = preloadPermissions preload
        let defaultAttributes = preloadDefaultAttributes preload
        let triggers = preloadTriggers preload
        let! changed2 =
            try
                updatePermissions conn.System permissions cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, perms) = resolvePermissions preloadLayout false permissions
                reraise ()
        let! changed3 =
            try
                updateAttributes conn.System defaultAttributes cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, attrs) = resolveAttributes preloadLayout false defaultAttributes
                reraise ()
        let! changed4 =
            try
                updateTriggers conn.System triggers cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, triggers) = resolveTriggers preloadLayout false triggers
                reraise ()

        let! sourceUserLayout = buildSchemaLayout conn.System (Map.keys preload.Schemas) cancellationToken
        let sourceLayout = unionSourceLayout sourcePreloadLayout sourceUserLayout
        let (brokenLayout, layout) = resolveLayout sourceLayout true

        do! checkBrokenLayout logger preload conn brokenLayout cancellationToken

        logger.LogInformation("Phase 2: Migrating all remaining entities")
        let userLayout = filterLayout (fun name -> not <| Map.containsKey name preloadLayout.schemas) layout
        let (_, newUserMeta) = buildFullLayoutMeta layout userLayout
        let currentUserMeta = filterUserMeta preload currentMeta

        let userMigration = planDatabaseMigration currentUserMeta newUserMeta
        let! _ = migrateDatabase conn.Connection.Query userMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () = task {
            let! currentMeta = buildDatabaseMeta conn.Transaction Seq.empty cancellationToken
            let currentUserMeta = filterUserMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentUserMeta newUserMeta
            if Array.isEmpty systemMigration then
                return true
            else
                return failwithf "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
        }
        assert (Task.awaitSync <| sanityCheck ())

        return (changed1 || changed2 || changed3 || changed4, layout, newUserMeta)
    }