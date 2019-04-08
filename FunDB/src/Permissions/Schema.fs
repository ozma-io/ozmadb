module FunWithFlags.FunDB.Permissions.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.FunQL.AST

type SchemaRolesException (message : string) =
    inherit Exception(message)

let private makeSourceAllowedEntity (entity : RoleEntity) : SourceAllowedEntity =
    let checkColumnField (col : RoleColumnField) =
        if col.ColumnField.EntityId <> entity.EntityId then
            raisef SchemaRolesException "Role column field \"%s\" doesn't correspond to entity \"%s\"" col.ColumnField.Name entity.Entity.Name
    entity.ColumnFields |> Seq.iter checkColumnField

    { fields = entity.ColumnFields |> Seq.map (fun col -> FunQLName col.ColumnField.Name) |> Set.ofSeqUnique
      where =
        if isNull entity.Where then
            None
        else
            Some entity.Where
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
    }

let private makeSourcePermissionsSchema (schema : Schema) : SourcePermissionsSchema =
    { roles = schema.Roles |> Seq.map (fun role -> (FunQLName role.Name, makeSourceRole role)) |> Map.ofSeqUnique
    }

let buildSchemaPermissions (db : SystemContext) : Task<SourcePermissions> =
    task {
        let currentSchemas = getRolesObjects db.Schemas
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourcePermissionsSchema schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }