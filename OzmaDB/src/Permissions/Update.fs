module OzmaDB.Permissions.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore;
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils
open OzmaDBSchema.System
open OzmaDB.Operations.Update
open OzmaDB.OzmaQL.AST
open OzmaDB.Permissions.Source
open OzmaDB.Permissions.Types

type private PermissionsUpdater (db : SystemContext, allSchemas : Schema seq) as this =
    inherit SystemUpdater(db)

    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAllowedField (field : SourceAllowedField) (existingField : RoleColumnField) : unit =
        existingField.Insert <- field.Insert
        existingField.Select <- Option.toObj field.Select
        existingField.Update <- Option.toObj field.Update
        existingField.Check <- Option.toObj field.Check

    let updateAllowedFields (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) =
        let oldFieldsMap =
            existingEntity.ColumnFields
            |> Seq.ofObj
            |> Seq.map (fun roleField -> (OzmaQLName roleField.ColumnName, roleField))
            |> Map.ofSeq

        let updateFunc _ = updateAllowedField
        let createFunc (OzmaQLName fieldName) =
            RoleColumnField (
                ColumnName = fieldName,
                RoleEntity = existingEntity
            )
        ignore <| this.UpdateDifference updateFunc createFunc entity.Fields oldFieldsMap

    let updateAllowedEntity (entityRef : ResolvedEntityRef) (entity : SourceAllowedEntity) (existingEntity : RoleEntity) : unit =
        updateAllowedFields entityRef entity existingEntity

        existingEntity.AllowBroken <- entity.AllowBroken
        existingEntity.Insert <- entity.Insert
        existingEntity.Check <- Option.toObj entity.Check
        existingEntity.Select <- Option.toObj entity.Select
        existingEntity.Update <- Option.toObj entity.Update
        existingEntity.Delete <- Option.toObj entity.Delete

    let updateAllowedDatabase (role : SourceRole) (existingRole : Role) : unit =
        let oldEntitiesMap =
            existingRole.Entities
            |> Seq.ofObj
            |> Seq.map (fun roleEntity -> ({ Schema = OzmaQLName roleEntity.Entity.Schema.Name; Name = OzmaQLName roleEntity.Entity.Name }, roleEntity))
            |> Map.ofSeq

        let entitiesMap =
            role.Permissions.Schemas
            |> Map.toSeq
            |> Seq.collect (fun (schemaName, entities) -> entities.Entities |> Map.toSeq |> Seq.map (fun (entityName, entity) -> ({ Schema = schemaName; Name = entityName }, entity)))
            |> Map.ofSeq

        existingRole.AllowBroken <- role.AllowBroken

        let updateFunc = updateAllowedEntity
        let createFunc entityRef =
            let entity =
                match Map.tryFind entityRef allEntitiesMap with
                | Some id -> id
                | None -> raisef SystemUpdaterException "Unknown entity %O" entityRef
            RoleEntity (
                Entity = entity,
                Role = existingRole
            )
        ignore <| this.UpdateDifference updateFunc createFunc entitiesMap oldEntitiesMap

    let updatePermissionsSchema (schema : SourcePermissionsSchema) (existingSchema : Schema) : unit =
        let oldRolesMap =
            existingSchema.Roles |> Seq.map (fun role -> (OzmaQLName role.Name, role)) |> Map.ofSeq

        let updateFunc name schema existingSchema =
            try
                updateAllowedDatabase schema existingSchema
            with
            | e -> raisefWithInner SystemUpdaterException e "In allowed schema %O" name
        let createFunc (OzmaQLName name) =
            Role (
                Name = name,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc schema.Roles oldRolesMap

    let updateSchemas (schemas : Map<SchemaName, SourcePermissionsSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updatePermissionsSchema schema existingSchema
            with
            | e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updatePermissions (db : SystemContext) (roles : SourcePermissions) (cancellationToken : CancellationToken) : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetRolesObjects ()

            let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
            // We don't touch in any way schemas not in layout.
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (OzmaQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name roles.Schemas)
                |> Map.ofSeq

            let updater = PermissionsUpdater(db, allSchemas)
            ignore <| updater.UpdateSchemas roles.Schemas schemasMap
            return updater
        }

type private RoleErrorRef = ERRole of RoleRef
                          | EREntity of AllowedEntityRef

let private findBrokenAllowedSchema (roleRef : RoleRef) (allowedSchemaName : SchemaName) (schema : AllowedSchema) : RoleErrorRef seq =
    seq {
        for KeyValue(allowedEntityName, maybeEntity) in schema.Entities do
            let markBroken =
                match maybeEntity with
                | Ok entity -> not entity.AllowBroken && allowedEntityIsHalfBroken entity
                | Error e -> not e.AllowBroken
            if markBroken then
                yield EREntity { Role = roleRef; Entity = { Schema = allowedSchemaName; Name = allowedEntityName } }
    }

let private findBrokenRole (roleRef : RoleRef) (role : ResolvedRole) : RoleErrorRef seq =
    seq {
        for KeyValue(allowedSchemaName, schema) in role.Permissions.Schemas do
            yield! findBrokenAllowedSchema roleRef allowedSchemaName schema
    }

let private findBrokenRolesSchema (schemaName : SchemaName) (schema : PermissionsSchema) : RoleErrorRef seq =
    seq {
        for KeyValue(roleName, maybeRole) in schema.Roles do
            let ref = { Schema = schemaName; Name = roleName }
            match maybeRole with
            | Ok role ->
                yield! findBrokenRole ref role
            | Error e ->
                if not e.AllowBroken then
                    yield ERRole ref
    }

let private findBrokenPermissions (roles : Permissions) : RoleErrorRef seq =
    seq {
        for KeyValue(schemaName, schema) in roles.Schemas do
            yield! findBrokenRolesSchema schemaName schema
    }

let private checkRoleName (ref : RoleRef) : Expr<Role -> bool> =
    let checkSchema = checkSchemaName ref.Schema
    let roleName = string ref.Name
    <@ fun role -> (%checkSchema) role.Schema && role.Name = roleName @>

let private checkAllowedEntityName (ref : AllowedEntityRef) : Expr<RoleEntity -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let checkRole = checkRoleName ref.Role
    <@ fun allowed -> (%checkRole) allowed.Role && (%checkEntity) allowed.Entity @>

let markBrokenPermissions (db : SystemContext) (roles : Permissions) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let broken = findBrokenPermissions roles
        let roleChecks = broken |> Seq.mapMaybe (function ERRole ref -> Some ref | _ -> None) |> Seq.map checkRoleName
        do! genericMarkBroken db.Roles roleChecks cancellationToken
        let allowedEntityChecks = broken |> Seq.mapMaybe (function EREntity ref -> Some ref | _ -> None) |> Seq.map checkAllowedEntityName
        do! genericMarkBroken db.RoleEntities allowedEntityChecks cancellationToken
    }
