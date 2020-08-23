module FunWithFlags.FunDB.Operations.Preload

open System.IO
open System.Linq
open Microsoft.EntityFrameworkCore
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
open FunWithFlags.FunDB.FunQL.AST
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
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.Triggers.Resolve
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Update
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

[<NoEquality; NoComparison>]
type PreloadedSchema =
    { Schema : SourceSchema
      Permissions : SourcePermissionsSchema
      DefaultAttributes : SourceAttributesDatabase
      Triggers : SourceTriggersDatabase
      UserViews : SourceUserViewsSchema
    }

[<NoEquality; NoComparison>]
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
        }

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

let buildFullOldLayoutMeta (layout : Layout) (subLayout : Layout) : Set<LayoutAssertion> * SQL.DatabaseMeta =
    let meta1 = buildOldLayoutMeta layout subLayout
    let assertions = buildAssertions layout subLayout
    let meta2 = buildAssertionsMeta layout assertions
    let meta = SQL.unionDatabaseMeta meta1 meta2
    (assertions, meta)

// Returns only user meta
let initialMigratePreload (logger :ILogger) (conn : DatabaseTransaction) (preload : Preload) (cancellationToken : CancellationToken) : Task<bool * Layout * SQL.DatabaseMeta> =
    task {
        logger.LogInformation("Migrating system entities to the current version")
        let sourceLayout = preloadLayout preload
        let (_, layout) = resolveLayout sourceLayout false
        let! layoutVersion = conn.System.State.FirstOrDefaultAsync((fun x -> x.Name = "LayoutVersion"), cancellationToken)
        logger.LogInformation("Not renaming old tables: {}", isNull layoutVersion)
        if isNull layoutVersion then
            logger.LogInformation("Renaming old tables")
            let! currentMeta = buildDatabaseMeta conn.Transaction cancellationToken
            let currentSystemMeta = filterPreloadedMeta preload currentMeta
            let (_, newSystemMeta) = buildFullOldLayoutMeta layout layout
            let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta

            let! _ = migrateDatabase conn.Connection.Query systemMigration cancellationToken
            logger.LogInformation("Finished renaming old tables")
            ()
        let (_, newSystemMeta) = buildFullLayoutMeta layout layout
        let! currentMeta = buildDatabaseMeta conn.Transaction cancellationToken
        let currentSystemMeta = filterPreloadedMeta preload currentMeta

        let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
        let! _ = migrateDatabase conn.Connection.Query systemMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () = task {
            let! currentMeta = buildDatabaseMeta conn.Transaction cancellationToken
            let currentSystemMeta = filterPreloadedMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
            if Array.isEmpty systemMigration then
                return true
            else
                return failwithf "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
        }
        assert (Task.awaitSync <| sanityCheck ())

        // We migrate layout first so that permissions and attributes have schemas in the table.
        let! changed1 = updateLayout conn.System sourceLayout cancellationToken
        let permissions = preloadPermissions preload
        let defaultAttributes = preloadDefaultAttributes preload
        let triggers = preloadTriggers preload
        let! changed2 =
            try
                updatePermissions conn.System permissions cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, perms) = resolvePermissions layout false permissions
                reraise ()
        let! changed3 =
            try
                updateAttributes conn.System defaultAttributes cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, attrs) = resolveAttributes layout false defaultAttributes
                reraise ()
        let! changed4 =
            try
                updateTriggers conn.System triggers cancellationToken
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, triggers) = resolveTriggers layout false triggers
                reraise ()

        let! newLayoutSource = buildSchemaLayout conn.System cancellationToken
        let newLayoutSource = applyHiddenLayoutData newLayoutSource sourceLayout
        let (brokenLayout, newLayout) = resolveLayout newLayoutSource true

        for KeyValue(schemaName, schema) in brokenLayout do
            for KeyValue(entityName, entity) in schema do
                for KeyValue(compName, err) in entity.computedFields do
                    logger.LogWarning(err, "Marking computed field {ref} as broken", { entity = { schema = schemaName; name = entityName }; name = compName })

        do! markBrokenLayout conn.System brokenLayout cancellationToken

        logger.LogInformation("Phase 2: Migrating all remaining entities")

        if isNull layoutVersion then
            logger.LogInformation("Renaming old tables")
            let! currentMeta = buildDatabaseMeta conn.Transaction cancellationToken
            let currentUserMeta = filterUserMeta preload currentMeta
            let (_, newMeta) = buildFullOldLayoutMeta newLayout newLayout
            let newUserMeta = filterUserMeta preload newMeta
            let userMigration = planDatabaseMigration currentUserMeta newUserMeta

            let! _ = migrateDatabase conn.Connection.Query userMigration cancellationToken

            let newEntry = StateValue (Name = "LayoutVersion", Value = "1")
            ignore <| conn.System.State.Add(newEntry)
            let! _ = conn.System.SaveChangesAsync(cancellationToken)
            logger.LogInformation("Finished renaming old tables")
            ()
        let (_, newMeta) = buildFullLayoutMeta newLayout newLayout
        let newUserMeta = filterUserMeta preload newMeta
        let! currentMeta =
            (*if Array.isEmpty systemMigration then
                Task.result currentMeta
            else*)
                buildDatabaseMeta conn.Transaction cancellationToken
        let currentUserMeta = filterUserMeta preload currentMeta

        let userMigration = planDatabaseMigration currentUserMeta newUserMeta
        let! _ = migrateDatabase conn.Connection.Query userMigration cancellationToken

        // Second migration shouldn't produce any changes.
        let sanityCheck () = task {
            let! currentMeta = buildDatabaseMeta conn.Transaction cancellationToken
            let currentUserMeta = filterUserMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentUserMeta newUserMeta
            if Array.isEmpty systemMigration then
                return true
            else
                return failwithf "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
        }
        assert (Task.awaitSync <| sanityCheck ())

        return (changed1 || changed2 || changed3 || changed4, newLayout, newUserMeta)
    }