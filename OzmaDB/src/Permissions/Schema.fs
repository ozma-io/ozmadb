module OzmaDB.Permissions.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore

open OzmaDB.OzmaUtils
open OzmaDB.Permissions.Source
open OzmaDB.OzmaQL.AST
open OzmaDBSchema.System

type SchemaRolesException(message: string) =
    inherit Exception(message)

let private makeSourceAllowedField (field: RoleColumnField) : SourceAllowedField =
    { Insert = field.Insert
      Select = Option.ofObj field.Select
      Update = Option.ofObj field.Update
      Check = Option.ofObj field.Check }

let private makeSourceAllowedEntity (entity: RoleEntity) : SourceAllowedEntity =
    let fields =
        entity.ColumnFields
        |> Seq.map (fun col -> (OzmaQLName col.ColumnName, makeSourceAllowedField col))
        |> Map.ofSeqUnique

    { Fields = fields
      AllowBroken = entity.AllowBroken
      Check = Option.ofObj entity.Check
      Insert = entity.Insert
      Select = Option.ofObj entity.Select
      Update = Option.ofObj entity.Update
      Delete = Option.ofObj entity.Delete }

let private makeSourceAllowedEntities (entity: RoleEntity) : Map<EntityName, SourceAllowedEntity> =
    Map.singleton (OzmaQLName entity.Entity.Name) (makeSourceAllowedEntity entity)

let private makeSourceAllowedDatabase (role: Role) : SourceAllowedDatabase =
    let schemas =
        role.Entities
        |> Seq.map (fun entity -> (OzmaQLName entity.Entity.Schema.Name, makeSourceAllowedEntities entity))
        |> Map.ofSeqWith (fun name -> Map.unionUnique)
        |> Map.map (fun name entities -> { Entities = entities }: SourceAllowedSchema)

    { Schemas = schemas }

let private makeSourceRole (role: Role) : SourceRole =
    { Parents =
        role.Parents
        |> Seq.map (fun role ->
            { Schema = OzmaQLName role.Parent.Schema.Name
              Name = OzmaQLName role.Parent.Name })
        |> Set.ofSeqUnique
      Permissions = makeSourceAllowedDatabase role
      AllowBroken = role.AllowBroken }

let private makeSourcePermissionsSchema (schema: Schema) : SourcePermissionsSchema =
    { Roles =
        schema.Roles
        |> Seq.map (fun role -> (OzmaQLName role.Name, makeSourceRole role))
        |> Map.ofSeqUnique }

let buildSchemaPermissions
    (db: SystemContext)
    (filter: Expression<Func<Schema, bool>> option)
    (cancellationToken: CancellationToken)
    : Task<SourcePermissions> =
    task {
        let currentSchemas = db.GetRolesObjects()

        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)

        let! schemas = currentSchemas.ToListAsync(cancellationToken)

        let sourceSchemas =
            schemas
            |> Seq.map (fun schema -> (OzmaQLName schema.Name, makeSourcePermissionsSchema schema))
            |> Map.ofSeqUnique

        return ({ Schemas = sourceSchemas }: SourcePermissions)
    }
