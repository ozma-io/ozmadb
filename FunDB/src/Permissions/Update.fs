module FunWithFlags.FunDB.Permissions.Update

open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore;
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private PermissionsUpdater (db : SystemContext, allSchemas : Schema seq) =
    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAllowedField (field : SourceAllowedField) (existingField : RoleColumnField) : unit =
        existingField.Change <- field.change
        existingField.Select <- Option.toNull field.select

    let updateAllowedFields (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) =
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
        ignore <| updateDifference db updateFunc createFunc entity.fields oldFieldsMap

    let updateAllowedEntity (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) : unit =
        updateAllowedFields entityRef entity existingEntity

        existingEntity.AllowBroken <- entity.allowBroken
        existingEntity.Insert <- entity.insert
        existingEntity.Check <- Option.toNull entity.check
        existingEntity.Select <- Option.toNull entity.select
        existingEntity.Update <- Option.toNull entity.update
        existingEntity.Delete <- Option.toNull entity.delete

    let updateAllowedDatabase (role : SourceRole) (existingRole : Role) : unit =
        let oldEntitiesMap =
            existingRole.Entities
            |> Seq.map (fun roleEntity -> ({ schema = FunQLName roleEntity.Entity.Schema.Name; name = FunQLName roleEntity.Entity.Name }, roleEntity))
            |> Map.ofSeq

        let entitiesMap =
            role.permissions.schemas |> Map.toSeq
            |> Seq.collect (fun (schemaName, entities) -> entities.entities |> Map.toSeq |> Seq.map (fun (entityName, entity) -> ({ schema = schemaName; name = entityName }, entity)))
            |> Map.ofSeq

        existingRole.AllowBroken <- role.allowBroken

        let updateFunc = updateAllowedEntity
        let createFunc entityRef =
            let entityId = Map.find entityRef allEntitiesMap
            let newEntity =
                RoleEntity (
                    EntityId = entityId,
                    ColumnFields = List()
                )
            existingRole.Entities.Add(newEntity)
            newEntity
        ignore <| updateDifference db updateFunc createFunc entitiesMap oldEntitiesMap

    let updatePermissionsSchema (schema : SourcePermissionsSchema) (existingSchema : Schema) : unit =
        let oldRolesMap =
            existingSchema.Roles |> Seq.map (fun role -> (FunQLName role.Name, role)) |> Map.ofSeq

        let updateFunc _ = updateAllowedDatabase
        let createFunc (FunQLName name) =
            let newRole =
                Role (
                    Name = name,
                    Entities = List()
                )
            existingSchema.Roles.Add(newRole)
            newRole
        ignore <| updateDifference db updateFunc createFunc schema.roles oldRolesMap

    let updateSchemas (schemas : Map<SchemaName, SourcePermissionsSchema>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updatePermissionsSchema
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updatePermissions (db : SystemContext) (roles : SourcePermissions) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync()

        let currentSchemas = db.GetRolesObjects ()

        let! allSchemas = currentSchemas.AsTracking().ToListAsync()
        // We don't touch in any way schemas not in layout.
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name roles.schemas)
            |> Map.ofSeq

        let updater = PermissionsUpdater(db, allSchemas)
        updater.UpdateSchemas roles.schemas schemasMap
        let! changedEntries = db.SaveChangesAsync()
        return changedEntries > 0
    }

let markBrokenPermissions (db : SystemContext) (perms : ErroredPermissions) : Task<unit> =
    task {
        let currentSchemas = db.GetRolesObjects ()

        let! schemas = currentSchemas.AsTracking().ToListAsync()

        for schema in schemas do
            match Map.tryFind (FunQLName schema.Name) perms with
            | None -> ()
            | Some schemaErrors ->
                for role in schema.Roles do
                    match Map.tryFind (FunQLName role.Name) schemaErrors with
                    | None -> ()
                    | Some (EFatal err) ->
                        role.AllowBroken <- true
                    | Some (EDatabase roleErrors) ->
                        for entity in role.Entities do
                            match Map.tryFind (FunQLName entity.Entity.Schema.Name) roleErrors with
                            | None -> ()
                            | Some entityErrors ->
                                if Map.containsKey (FunQLName entity.Entity.Name) entityErrors then
                                    entity.AllowBroken <- true

        let! _ = db.SaveChangesAsync()
        return ()
    }