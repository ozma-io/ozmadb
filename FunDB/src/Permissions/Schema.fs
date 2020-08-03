module FunWithFlags.FunDB.Permissions.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

type SchemaRolesException (message : string) =
    inherit Exception(message)

let private makeSourceAllowedField (field : RoleColumnField) : SourceAllowedField =
    { change = field.Change
      select = Option.ofNull field.Select
    }

let private makeSourceAllowedEntity (entity : RoleEntity) : SourceAllowedEntity =
    let fields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.ColumnName, makeSourceAllowedField col)) |> Map.ofSeqUnique

    { fields = fields
      allowBroken = entity.AllowBroken
      check = Option.ofNull entity.Check
      insert = entity.Insert
      select = Option.ofNull entity.Select
      update = Option.ofNull entity.Update
      delete = Option.ofNull entity.Delete
    }

let private makeSourceAllowedEntities (entity : RoleEntity) : Map<EntityName, SourceAllowedEntity> =
    Map.singleton (FunQLName entity.Entity.Name) (makeSourceAllowedEntity entity)

let private makeSourceAllowedDatabase (role : Role) : SourceAllowedDatabase =
    let schemas =
        role.Entities |>
        Seq.map (fun entity -> (FunQLName entity.Entity.Schema.Name, makeSourceAllowedEntities entity)) |>
        Map.ofSeqWith (fun name -> Map.unionUnique) |>
        Map.map (fun name entities -> { entities = entities })
    { schemas = schemas
    }

let private makeSourceRole (role : Role) : SourceRole =
    { parents = role.Parents |> Seq.map (fun role -> { schema = FunQLName role.Parent.Schema.Name; name = FunQLName role.Parent.Name }) |> Set.ofSeqUnique
      permissions = makeSourceAllowedDatabase role
      allowBroken = role.AllowBroken
    }

let private makeSourcePermissionsSchema (schema : Schema) : SourcePermissionsSchema =
    { roles = schema.Roles |> Seq.map (fun role -> (FunQLName role.Name, makeSourceRole role)) |> Map.ofSeqUnique
    }

let buildSchemaPermissions (db : SystemContext) : Task<SourcePermissions> =
    task {
        let currentSchemas = db.GetRolesObjects ()
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourcePermissionsSchema schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }