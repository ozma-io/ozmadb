module FunWithFlags.FunDB.Operations.Preload

open System.IO
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
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
open FunWithFlags.FunDB.UserViews.Generate
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Attributes.Resolve
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.Connection
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDBSchema.System

type SourcePreloadedSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceEntity>
      [<JsonProperty(Required=Required.DisallowNull)>]
      roles : Map<RoleName, SourceRole>
      [<JsonProperty(Required=Required.DisallowNull)>]
      defaultAttributes : Map<SchemaName, SourceAttributesSchema>
      userViewGenerator : string option // Path to .js file
    }

type SourcePreload =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourcePreloadedSchema>
    }

type SourcePreloadFile =
    { preload : SourcePreload
      dirname : string
    }

let emptySourcePreloadFile : SourcePreloadFile =
    { preload = { schemas = Map.empty }
      dirname = ""
    }

[<NoEquality; NoComparison>]
type PreloadedSchema =
    { schema : SourceSchema
      permissions : SourcePermissionsSchema
      defaultAttributes : SourceAttributesDatabase
      userViewGenerator : UserViewGenerator option
    }

[<NoEquality; NoComparison>]
type Preload =
    { schemas : Map<SchemaName, PreloadedSchema>
    }

let readSourcePreload (path : string) : SourcePreloadFile =
    use stream = File.OpenText path
    use jsonStream = new JsonTextReader(stream)
    let serializer = JsonSerializer.CreateDefault()
    let preload = serializer.Deserialize<SourcePreload>(jsonStream)
    { preload = preload
      dirname = Path.GetDirectoryName path
    }

// Empty schemas in Roles aren't reflected in the database so we need to remove them -- otherwise a "change" will always be detected.
let private normalizeRole (role : SourceRole) =
    { role with
          permissions =
              { role.permissions with
                    schemas = role.permissions.schemas |> Map.filter (fun name schema -> not (Map.isEmpty schema.entities))
              }
    }

let private resolvePreloadedSchema (dirname : string) (preload : SourcePreloadedSchema) : PreloadedSchema =
    let schema =
        { entities = preload.entities
        } : SourceSchema
    let permissions = { roles = Map.map (fun name -> normalizeRole) preload.roles }
    let defaultAttributes = { schemas = preload.defaultAttributes } : SourceAttributesDatabase

    let readUserViewGenerator (path : string) =
        let realPath =
            if Path.IsPathRooted(path) then
                path
            else
                Path.Join(dirname, path)
        let program = File.ReadAllText realPath
        UserViewGenerator program
    let userViewGen = Option.map readUserViewGenerator preload.userViewGenerator

    { schema = schema
      permissions = permissions
      defaultAttributes = defaultAttributes
      userViewGenerator = userViewGen
    }

let preloadLayout (preload : Preload) : SourceLayout =
    { schemas = preload.schemas |> Map.map (fun name schema -> schema.schema) }

let preloadPermissions (preload : Preload) : SourcePermissions =
    { schemas = preload.schemas |> Map.map (fun name schema -> schema.permissions) }

let preloadDefaultAttributes (preload : Preload) : SourceDefaultAttributes =
    { schemas = preload.schemas |> Map.map (fun name schema -> schema.defaultAttributes) }

let preloadUserViews (fullLayout : Layout) (preload : Preload) : SourceUserViews =
    let generateUvs : UserViewGenerator option -> SourceUserViewsSchema = function
    | None -> emptySourceUserViewsSchema
    | Some uvGen -> uvGen.Generate fullLayout

    { schemas = preload.schemas |> Map.map (fun name schema -> generateUvs schema.userViewGenerator)
    }

let resolvePreload (source : SourcePreloadFile) : Preload =
    let preloadedSchemas = source.preload.schemas |> Map.map (fun name -> resolvePreloadedSchema source.dirname)
    if Map.containsKey funSchema preloadedSchemas then
        failwith "Preload cannot contain public schema"
    let systemPreload =
        { schema = buildSystemSchema typeof<SystemContext>
          permissions = emptySourcePermissionsSchema
          defaultAttributes = emptySourceAttributesSchema
          userViewGenerator = None
        }
    { schemas = Map.add funSchema systemPreload preloadedSchemas
    }

let preloadLayoutIsUnchanged (sourceLayout : SourceLayout) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourceSchema) =
        match Map.tryFind name preload.schemas with
        | Some existing -> schema = existing.schema
        | None -> true
    sourceLayout.schemas |> Map.toSeq |> Seq.forall notChanged

let preloadPermissionsAreUnchanged (sourcePerms : SourcePermissions) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourcePermissionsSchema) =
        match Map.tryFind name preload.schemas with
        | Some existing -> schema = existing.permissions
        | None -> true
    sourcePerms.schemas |> Map.toSeq |> Seq.forall notChanged

let preloadAttributesAreUnchanged (sourceAttrs : SourceDefaultAttributes) (preload : Preload) =
    let notChanged (name : SchemaName, schema : SourceAttributesDatabase) =
        match Map.tryFind name preload.schemas with
        | Some existing -> schema = existing.defaultAttributes
        | None -> true
    sourceAttrs.schemas |> Map.toSeq |> Seq.forall notChanged

let filterPreloadedSchemas (preload : Preload) = Map.filter (fun name _ -> Map.containsKey name preload.schemas)

let filterUserSchemas (preload : Preload) = Map.filter (fun name _ -> not <| Map.containsKey name preload.schemas)

let filterPreloadedMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { schemas = Map.filter (fun (SQL.SQLName name) _ -> Map.containsKey (FunQLName name) preload.schemas) meta.schemas
    } : SQL.DatabaseMeta

let filterUserMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { schemas = Map.filter (fun (SQL.SQLName name) _ -> not <| Map.containsKey (FunQLName name) preload.schemas) meta.schemas
    } : SQL.DatabaseMeta

let buildFullLayoutMeta (layout : Layout) (subLayout : Layout) : Set<LayoutAssertion> * SQL.DatabaseMeta =
    let meta1 = buildLayoutMeta layout subLayout
    let assertions = buildAssertions layout subLayout
    let meta2 = buildAssertionsMeta layout assertions
    let meta = SQL.unionDatabaseMeta meta1 meta2
    (assertions, meta)

// Returns only user meta
let initialMigratePreload (logger :ILogger) (conn : DatabaseTransaction) (preload : Preload) : Task<bool * Layout * SQL.DatabaseMeta> =
    task {
        logger.LogInformation("Migrating system entities to the current version")
        let sourceLayout = preloadLayout preload
        let (_, layout) = resolveLayout sourceLayout false
        let (_, newSystemMeta) = buildFullLayoutMeta layout layout
        let! currentMeta = buildDatabaseMeta conn.Transaction
        let currentSystemMeta = filterPreloadedMeta preload currentMeta

        let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta
        let! _ = migrateDatabase conn.Connection.Query systemMigration

        // Second migration shouldn't produce any changes.
        let sanityCheck () =
            let currentMeta = Task.awaitSync <| buildDatabaseMeta conn.Transaction
            let currentSystemMeta = filterPreloadedMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentSystemMeta newSystemMeta |> Seq.toArray
            if Array.isEmpty systemMigration then
                true
            else
                eprintfn "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
                false
        assert sanityCheck ()

        // We migrate layout first so that permissions and attributes have schemas in the table.
        let! changed1 = updateLayout conn.System sourceLayout
        let permissions = preloadPermissions preload
        let defaultAttributes = preloadDefaultAttributes preload
        let! changed2 =
            try
                updatePermissions conn.System permissions
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, perms) = resolvePermissions layout false permissions
                reraise ()
        let! changed3 =
            try
                updateAttributes conn.System defaultAttributes
            with
            | _ ->
                // Maybe we'll get a better error
                let (errors, attrs) = resolveAttributes layout false defaultAttributes
                reraise ()

        let! newLayoutSource = buildSchemaLayout conn.System
        let (brokenLayout, newLayout) = resolveLayout newLayoutSource true

        for KeyValue(schemaName, schema) in brokenLayout do
            for KeyValue(entityName, entity) in schema do
                for KeyValue(compName, err) in entity.computedFields do
                    logger.LogWarning(err, "Marking computed field {ref} as broken", { entity = { schema = schemaName; name = entityName }; name = compName })

        do! markBrokenLayout conn.System brokenLayout

        logger.LogInformation("Phase 2: Migrating all remaining entities")

        let (_, newMeta) = buildFullLayoutMeta newLayout newLayout
        let newUserMeta = filterUserMeta preload newMeta
        let! currentMeta =
            if Array.isEmpty systemMigration then
                Task.result currentMeta
            else
                buildDatabaseMeta conn.Transaction
        let currentUserMeta = filterUserMeta preload currentMeta

        let userMigration = planDatabaseMigration currentUserMeta newUserMeta
        let! _ = migrateDatabase conn.Connection.Query userMigration

        // Second migration shouldn't produce any changes.
        let sanityCheck () =
            let currentMeta = Task.awaitSync <| buildDatabaseMeta conn.Transaction
            let currentUserMeta = filterUserMeta preload currentMeta
            let systemMigration = planDatabaseMigration currentUserMeta newUserMeta |> Seq.toArray
            if Array.isEmpty systemMigration then
                true
            else
                eprintfn "Non-indempotent migration detected: %s" (systemMigration |> Array.map string |> String.concat ", ")
                false
        assert sanityCheck ()

        return (changed1 || changed2 || changed3, newLayout, newUserMeta)
    }