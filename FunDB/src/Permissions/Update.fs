module FunWithFlags.FunDB.Permissions.Update

open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

type private EntityKey = SchemaName * EntityName

type private PermissionsUpdater (db : SystemContext, allSchemas : Schema seq) =
    let entityToInfo (entity : Entity) = entity.Id
    let makeEntity schemaName (entity : Entity) = ((schemaName, FunQLName entity.Name), entityToInfo entity)
    let makeSchema (schema : Schema) = Seq.map (makeEntity (FunQLName schema.Name)) schema.Entities

    let allEntitiesMap = allSchemas |> Seq.collect makeSchema |> Map.ofSeq

    let updateAllowedField (field : SourceAllowedField) (existingField : RoleColumnField) : unit =
        existingField.Change <- field.change
        existingField.Select <- Option.toNull field.select

    let updateAllowedFields (entityKey : EntityKey) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) =
        let oldFieldsMap =
            existingEntity.ColumnFields
            |> Seq.map (fun roleField -> (FunQLName roleField.ColumnName, roleField))
            |> Map.ofSeq

        let updateFunc _ = updateAllowedField
        let createFunc (FunQLName fieldName) =
            let newField =
                RoleColumnField (
                    ColumnName = fieldName
                )
            existingEntity.ColumnFields.Add(newField)
            newField
        updateDifference db updateFunc createFunc entity.fields oldFieldsMap

    let updateAllowedEntity (entityKey : EntityKey) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) : unit =
        updateAllowedFields entityKey entity existingEntity

        existingEntity.AllowBroken <- entity.allowBroken
        existingEntity.Insert <- entity.insert
        existingEntity.Check <- Option.toNull entity.check
        existingEntity.Select <- Option.toNull entity.select
        existingEntity.Update <- Option.toNull entity.update
        existingEntity.Delete <- Option.toNull entity.delete

    let updateAllowedDatabase (role : SourceRole) (existingRole : Role) : unit =
        let oldEntitiesMap =
            existingRole.Entities
            |> Seq.map (fun roleEntity -> ((FunQLName roleEntity.Entity.Schema.Name, FunQLName roleEntity.Entity.Name), roleEntity))
            |> Map.ofSeq

        let entitiesMap =
            role.permissions.schemas |> Map.toSeq
            |> Seq.collect (fun (schemaName, entities) -> entities.entities |> Map.toSeq |> Seq.map (fun (entityName, entity) -> ((schemaName, entityName), entity)))
            |> Map.ofSeq

        let updateFunc = updateAllowedEntity
        let createFunc entityKey =
            let entityId = Map.find entityKey allEntitiesMap
            let newEntity =
                RoleEntity (
                    EntityId = entityId
                )
            existingRole.Entities.Add(newEntity)
            newEntity
        updateDifference db updateFunc createFunc entitiesMap oldEntitiesMap

    let updatePermissionsSchema (schema : SourcePermissionsSchema) (existingSchema : Schema) : unit =
        let oldRolesMap =
            existingSchema.Roles |> Seq.map (fun role -> (FunQLName role.Name, role)) |> Map.ofSeq

        let updateFunc _ = updateAllowedDatabase
        let createFunc (FunQLName name) =
            let newRole =
                Role (
                    Name = name
                )
            existingSchema.Roles.Add(newRole)
            newRole
        updateDifference db updateFunc createFunc schema.roles oldRolesMap

    let updateSchemas : Map<SchemaName, SourcePermissionsSchema> -> Map<SchemaName, Schema> -> unit =
        let updateFunc _ = updatePermissionsSchema
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        updateDifference db updateFunc createFunc

    member this.UpdateSchemas = updateSchemas

let updatePermissions (db : SystemContext) (roles : SourcePermissions) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync()

        let currentSchemas = db.Schemas |> getFieldsObjects |> getRolesObjects

        // We don't touch in any way schemas not in layout.
        let wantedSchemas = roles.schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
        let! schemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync()

        let schemasMap = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, schema)) |> Map.ofSeq

        let updater = PermissionsUpdater(db, schemas)
        updater.UpdateSchemas roles.schemas schemasMap
        let! changedEntries = db.SaveChangesAsync()
        return changedEntries > 0
    }

let markBrokenPermissions (db : SystemContext) (perms : ErroredPermissions) : Task<unit> =
    task {
        let currentSchemas = db.Schemas |> getFieldsObjects |> getRolesObjects

        let! schemas = currentSchemas.AsTracking().ToListAsync()

        for schema in schemas do
            match Map.tryFind (FunQLName schema.Name) perms with
            | None -> ()
            | Some schemaErrors ->
                for role in schema.Roles do
                    match Map.tryFind (FunQLName role.Name) schemaErrors with
                    | None -> ()
                    | Some roleErrors ->
                        for entity in role.Entities do
                            match Map.tryFind (FunQLName entity.Entity.Schema.Name) roleErrors with
                            | None -> ()
                            | Some entityErrors ->
                                if Map.containsKey (FunQLName entity.Entity.Name) entityErrors then
                                    entity.AllowBroken <- true

        let! _ = db.SaveChangesAsync()
        return ()
    }