module FunWithFlags.FunDB.Permissions.Update

open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore;
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private PermissionsUpdater (db : SystemContext, allSchemas : Schema seq) as this =
    inherit SystemUpdater(db)

    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAllowedField (field : SourceAllowedField) (existingField : RoleColumnField) : unit =
        existingField.Change <- field.Change
        existingField.Select <- Option.toNull field.Select

    let updateAllowedFields (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) =
        let oldFieldsMap =
            existingEntity.ColumnFields
            |> Seq.map (fun roleField -> (FunQLName roleField.ColumnName, roleField))
            |> Map.ofSeq

        let updateFunc _ = updateAllowedField
        let createFunc (FunQLName fieldName) =
            RoleColumnField (
                ColumnName = fieldName
            )
        ignore <| this.UpdateDifference updateFunc createFunc entity.Fields oldFieldsMap

    let updateAllowedEntity (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) : unit =
        updateAllowedFields entityRef entity existingEntity

        existingEntity.AllowBroken <- entity.AllowBroken
        existingEntity.Insert <- entity.Insert
        existingEntity.Check <- Option.toNull entity.Check
        existingEntity.Select <- Option.toNull entity.Select
        existingEntity.Update <- Option.toNull entity.Update
        existingEntity.Delete <- Option.toNull entity.Delete

    let updateAllowedDatabase (role : SourceRole) (existingRole : Role) : unit =
        let oldEntitiesMap =
            existingRole.Entities
            |> Seq.map (fun roleEntity -> ({ schema = FunQLName roleEntity.Entity.Schema.Name; name = FunQLName roleEntity.Entity.Name }, roleEntity))
            |> Map.ofSeq

        let entitiesMap =
            role.Permissions.Schemas |> Map.toSeq
            |> Seq.collect (fun (schemaName, entities) -> entities.Entities |> Map.toSeq |> Seq.map (fun (entityName, entity) -> ({ schema = schemaName; name = entityName }, entity)))
            |> Map.ofSeq

        existingRole.AllowBroken <- role.AllowBroken

        let updateFunc = updateAllowedEntity
        let createFunc entityRef =
            let entity =
                match Map.tryFind entityRef allEntitiesMap with
                | Some id -> id
                | None -> raisef SystemUpdaterException "Unknown entity %O" entityRef
            RoleEntity (
                Entity = entity
            )
        ignore <| this.UpdateDifference updateFunc createFunc entitiesMap oldEntitiesMap

    let updatePermissionsSchema (schema : SourcePermissionsSchema) (existingSchema : Schema) : unit =
        let oldRolesMap =
            existingSchema.Roles |> Seq.map (fun role -> (FunQLName role.Name, role)) |> Map.ofSeq

        let updateFunc name schema existingSchema =
            try
                updateAllowedDatabase schema existingSchema
            with
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e.InnerException "In allowed schema %O: %s" name e.Message
        let createFunc (FunQLName name) =
            let newRole =
                Role (
                    Name = name,
                    Entities = List()
                )
            existingSchema.Roles.Add(newRole)
            newRole
        ignore <| this.UpdateDifference updateFunc createFunc schema.Roles oldRolesMap

    let updateSchemas (schemas : Map<SchemaName, SourcePermissionsSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updatePermissionsSchema schema existingSchema
            with
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e.InnerException "In schema %O: %s" name e.Message
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updatePermissions (db : SystemContext) (roles : SourcePermissions) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetRolesObjects ()

            let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
            // We don't touch in any way schemas not in layout.
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name roles.Schemas)
                |> Map.ofSeq

            let updater = PermissionsUpdater(db, allSchemas)
            ignore <| updater.UpdateSchemas roles.Schemas schemasMap
            return updater
        }

type private RoleErrorRef = ERRole of RoleRef
                          | EREntity of AllowedEntityRef

let private findBrokenAllowedSchema (roleRef : RoleRef) (allowedSchemaName : SchemaName) (schema : ErroredAllowedSchema) : RoleErrorRef seq =
    seq {
        for KeyValue(allowedEntityName, entity) in schema do
            yield EREntity { Role = roleRef; Entity = { schema = allowedSchemaName; name = allowedEntityName } }
    }

let private findBrokenRole (roleRef : RoleRef) (role : ErroredRole) : RoleErrorRef seq =
    seq {
        match role with
        | ERFatal e -> yield ERRole roleRef
        | ERDatabase db ->
            for KeyValue(allowedSchemaName, schema) in db do
                yield! findBrokenAllowedSchema roleRef allowedSchemaName schema
    }

let private findBrokenRolesSchema (schemaName : SchemaName) (roles : ErroredRoles) : RoleErrorRef seq =
    seq {
        for KeyValue(roleName, role) in roles do
            yield! findBrokenRole { schema = schemaName; name = roleName } role
    }

let private findBrokenPermissions (roles : ErroredPermissions) : RoleErrorRef seq =
    seq {
        for KeyValue(schemaName, schema) in roles do
            yield! findBrokenRolesSchema schemaName schema
    }

let private checkRoleName (ref : RoleRef) : Expr<Role -> bool> =
    let checkSchema = checkSchemaName ref.schema
    let roleName = string ref.name
    <@ fun role -> (%checkSchema) role.Schema && role.Name = roleName @>

let private checkAllowedEntityName (ref : AllowedEntityRef) : Expr<RoleEntity -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let checkRole = checkRoleName ref.Role
    <@ fun allowed -> (%checkRole) allowed.Role && (%checkEntity) allowed.Entity @>

let markBrokenPermissions (db : SystemContext) (roles : ErroredPermissions) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let broken = findBrokenPermissions roles
        let roleChecks = broken |> Seq.mapMaybe (function ERRole ref -> Some ref | _ -> None) |> Seq.map checkRoleName
        do! genericMarkBroken db.Roles roleChecks <@ fun x -> Role(AllowBroken = true) @> cancellationToken
        let allowedEntityChecks = broken |> Seq.mapMaybe (function EREntity ref -> Some ref | _ -> None) |> Seq.map checkAllowedEntityName
        do! genericMarkBroken db.RoleEntities allowedEntityChecks <@ fun x -> RoleEntity(AllowBroken = true) @> cancellationToken
    }
