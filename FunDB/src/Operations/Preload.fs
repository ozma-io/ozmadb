module FunWithFlags.FunDB.Operations.Preload

open System.IO
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.System
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Layout.Meta
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Generate
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.Operations.Connection
module SQL = FunWithFlags.FunDB.SQL.AST

type SourcePreloadedSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceEntity>
      [<JsonProperty(Required=Required.DisallowNull)>]
      roles : Map<RoleName, SourceRole>
      userViewGenerator : string option // Path to .js file
    }

type SourcePreload =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourcePreloadedSchema>
    }

let emptySourcePreload : SourcePreload =
    { schemas = Map.empty
    }

[<NoComparison>]
type PreloadedSchema =
    { schema : SourceSchema
      permissions : SourcePermissionsSchema
      userViewGenerator : UserViewGenerator option
    }

[<NoComparison>]
type Preload =
    { schemas : Map<SchemaName, PreloadedSchema>
    }

let readSourcePreload (path : string) : SourcePreload =
    use stream = File.OpenText path
    use jsonStream = new JsonTextReader(stream)
    let serializer = JsonSerializer.CreateDefault()
    serializer.Deserialize<SourcePreload>(jsonStream)

// Empty schemas in Roles aren't reflected in the database so we need to remove them -- otherwise a "change" will always be detected.
let private normalizeRole (role : SourceRole) =
    { role with
          permissions =
              { role.permissions with
                    schemas = role.permissions.schemas |> Map.filter (fun name schema -> not (Map.isEmpty schema.entities))
              }
    }

let private resolvePreloadedSchema (preload : SourcePreloadedSchema) : PreloadedSchema =
    let schema = { entities = preload.entities } : SourceSchema
    let permissions = { roles = Map.map (fun name -> normalizeRole) preload.roles }

    let readUserViewGenerator path =
        let program = File.ReadAllText path
        UserViewGenerator program
    let userViewGen = Option.map readUserViewGenerator preload.userViewGenerator

    { schema = schema
      permissions = permissions
      userViewGenerator = userViewGen
    }

let preloadLayout (preload : Preload) : SourceLayout =
    { schemas = preload.schemas |> Map.map (fun name schema -> schema.schema) }

let preloadPermissions (preload : Preload) : SourcePermissions =
    { schemas = preload.schemas |> Map.map (fun name schema -> schema.permissions) }

let preloadUserViews (fullLayout : Layout) (preload : Preload) : SourceUserViews =
    let generateUvs : UserViewGenerator option -> SourceUserViewsSchema = function
    | None -> emptySourceUserViewsSchema
    | Some uvGen -> uvGen.Generate fullLayout

    { schemas = preload.schemas |> Map.map (fun name schema -> generateUvs schema.userViewGenerator)
    }

let resolvePreload (source : SourcePreload) : Preload =
    let preloadedSchemas = source.schemas |> Map.map (fun name -> resolvePreloadedSchema)
    if Map.containsKey funSchema preloadedSchemas then
        failwith "Preload cannot contain public schema"
    let systemPreload =
        { schema = buildSystemSchema typeof<SystemContext>
          permissions = emptySourcePermissionsSchema
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

let filterPreloadedSchemas (preload : Preload) = Map.filter (fun name _ -> Map.containsKey name preload.schemas)

let filterUserSchemas (preload : Preload) = Map.filter (fun name _ -> not <| Map.containsKey name preload.schemas)

let filterPreloadedMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { schemas = Map.filter (fun (SQL.SQLName name) _ -> Map.containsKey (FunQLName name) preload.schemas) meta.schemas
    } : SQL.DatabaseMeta

let filterUserMeta (preload : Preload) (meta : SQL.DatabaseMeta) =
    { schemas = Map.filter (fun (SQL.SQLName name) _ -> not <| Map.containsKey (FunQLName name) preload.schemas) meta.schemas
    } : SQL.DatabaseMeta

// Returns only user meta
let initialMigratePreload (logger :ILogger) (conn : DatabaseConnection) (preload : Preload) : Task<bool * Layout * SQL.DatabaseMeta> =
    task {
        logger.LogInformation("Migrating system entities to the current version")
        let sourceLayout = preloadLayout preload
        let layout = resolveLayout sourceLayout
        let newSystemMeta = buildLayoutMeta layout
        let! currentMeta = buildDatabaseMeta conn.Transaction
        let currentSystemMeta = filterPreloadedMeta preload currentMeta

        let systemMigration = migrateDatabase currentSystemMeta newSystemMeta
        for action in systemMigration do
            logger.LogInformation("System migration step: {}", action)
            let! _ = conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
            ()

        let! changed1 = updateLayout conn.System sourceLayout
        let permissions = preloadPermissions preload
        let! changed2 = updatePermissions conn.System permissions
        let! newLayoutSource = buildSchemaLayout conn.System
        let newLayout = resolveLayout newLayoutSource

        logger.LogInformation("Phase 2: Migrating all remaining entities")

        let newMeta = buildLayoutMeta newLayout
        let newUserMeta = filterUserMeta preload newMeta
        let! currentMeta2 = buildDatabaseMeta conn.Transaction
        let currentUserMeta2 = filterUserMeta preload currentMeta2

        let userMigration = migrateDatabase currentUserMeta2 newUserMeta
        for action in userMigration do
            logger.LogInformation("User migration step: {}", action)
            let! _ = conn.Query.ExecuteNonQuery (action.ToSQLString()) Map.empty
            ()

        return (changed1 || changed2, newLayout, newUserMeta)
    }