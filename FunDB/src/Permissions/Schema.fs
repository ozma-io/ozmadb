module FunWithFlags.FunDB.Permissions.Schema

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

type SchemaRolesException (message : string) =
    inherit Exception(message)

let private makeSourceAllowedField (field : RoleColumnField) : SourceAllowedField =
    { Change = field.Change
      Select = Option.ofObj field.Select
    }

let private makeSourceAllowedEntity (entity : RoleEntity) : SourceAllowedEntity =
    let fields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.ColumnName, makeSourceAllowedField col)) |> Map.ofSeqUnique

    { Fields = fields
      AllowBroken = entity.AllowBroken
      Check = Option.ofObj entity.Check
      Insert = entity.Insert
      Select = Option.ofObj entity.Select
      Update = Option.ofObj entity.Update
      Delete = Option.ofObj entity.Delete
    }

let private makeSourceAllowedEntities (entity : RoleEntity) : Map<EntityName, SourceAllowedEntity> =
    Map.singleton (FunQLName entity.Entity.Name) (makeSourceAllowedEntity entity)

let private makeSourceAllowedDatabase (role : Role) : SourceAllowedDatabase =
    let schemas =
        role.Entities |>
        Seq.map (fun entity -> (FunQLName entity.Entity.Schema.Name, makeSourceAllowedEntities entity)) |>
        Map.ofSeqWith (fun name -> Map.unionUnique) |>
        Map.map (fun name entities -> { Entities = entities })
    { Schemas = schemas
    }

let private makeSourceRole (role : Role) : SourceRole =
    { Parents = role.Parents |> Seq.map (fun role -> { Schema = FunQLName role.Parent.Schema.Name; Name = FunQLName role.Parent.Name }) |> Set.ofSeqUnique
      Permissions = makeSourceAllowedDatabase role
      AllowBroken = role.AllowBroken
      AllowAnonymousQueries = role.AllowAnonymousQueries
    }

let private makeSourcePermissionsSchema (schema : Schema) : SourcePermissionsSchema =
    { Roles = schema.Roles |> Seq.map (fun role -> (FunQLName role.Name, makeSourceRole role)) |> Map.ofSeqUnique
    }

let buildSchemaPermissions (db : SystemContext) (cancellationToken : CancellationToken) : Task<SourcePermissions> =
    task {
        let currentSchemas = db.GetRolesObjects ()
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourcePermissionsSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }