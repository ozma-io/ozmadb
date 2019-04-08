module FunWithFlags.FunDB.Permissions.Update

open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source

type private EntityKey = SchemaName * EntityName

type private PermissionsUpdater (db : SystemContext, allSchemas : Schema seq) =
    let entityToInfo (entity : Entity) =
        (entity.Id, entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, col.Id)) |> Map.ofSeq)
    let allEntitiesMap =
        allSchemas
        |> Seq.collect (fun schema -> schema.Entities |> Seq.map (fun entity -> ((FunQLName schema.Name, FunQLName entity.Name), entityToInfo entity)))
        |> Map.ofSeqUnique

    let updateAllowedFields (entityKey : EntityKey) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) =
        let oldFieldsMap =
            existingEntity.ColumnFields
            |> Seq.map (fun roleField -> (FunQLName roleField.ColumnField.Name, roleField))
            |> Map.ofSeqUnique

        let fieldsMap =
            entity.fields |> Set.toSeq |> Seq.map (fun name -> (name, ())) |> Map.ofSeq

        let (_, allFieldsMap) = Map.find entityKey allEntitiesMap

        let updateFunc _ a b = ()
        let createFunc fieldName =
            let fieldId = Map.find fieldName allFieldsMap
            let newField =
                RoleColumnField (
                    ColumnFieldId = fieldId
                )
            existingEntity.ColumnFields.Add(newField)
            newField
        updateDifference db updateFunc createFunc fieldsMap oldFieldsMap

    let updateAllowedEntity (entityKey : EntityKey) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) : unit =
        updateAllowedFields entityKey entity existingEntity
        match entity.where with
        | None ->
            existingEntity.Where <- null
        | Some whereStr ->
            existingEntity.Where <- whereStr

    let updateAllowedDatabase (role : SourceRole) (existingRole : Role) : unit =
        let oldEntitiesMap =
            existingRole.Entities
            |> Seq.map (fun roleEntity -> ((FunQLName roleEntity.Entity.Schema.Name, FunQLName roleEntity.Entity.Name), roleEntity))
            |> Map.ofSeqUnique

        let entitiesMap =
            role.permissions.schemas |> Map.toSeq
            |> Seq.collect (fun (schemaName, entities) -> entities.entities |> Map.toSeq |> Seq.map (fun (entityName, entity) -> ((schemaName, entityName), entity)))
            |> Map.ofSeqUnique

        let updateFunc = updateAllowedEntity
        let createFunc entityKey =
            let (entityId, _) = Map.find entityKey allEntitiesMap
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