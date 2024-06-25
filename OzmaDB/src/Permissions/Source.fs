module OzmaDB.Permissions.Source

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST

type ResolvedRoleRef = ResolvedEntityRef

type SourceAllowedField =
    { Insert: bool
      Select: string option
      Update: string option
      Check: string option }

type SourceAllowedEntity =
    { AllowBroken: bool
      Check: string option
      Insert: bool
      Select: string option
      Update: string option
      Delete: string option
      Fields: Map<FieldName, SourceAllowedField> }

type SourceAllowedSchema =
    { Entities: Map<EntityName, SourceAllowedEntity> }

type SourceAllowedDatabase =
    { Schemas: Map<SchemaName, SourceAllowedSchema> }

    member this.FindEntity(entity: ResolvedEntityRef) =
        match Map.tryFind entity.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind entity.Name schema.Entities

type SourceRole =
    { Parents: Set<ResolvedRoleRef>
      Permissions: SourceAllowedDatabase
      AllowBroken: bool }

type SourcePermissionsSchema = { Roles: Map<RoleName, SourceRole> }

let emptySourcePermissionsSchema: SourcePermissionsSchema = { Roles = Map.empty }

let mergeSourcePermissionsSchema (a: SourcePermissionsSchema) (b: SourcePermissionsSchema) : SourcePermissionsSchema =
    { Roles = Map.unionUnique a.Roles b.Roles }

type SourcePermissions =
    { Schemas: Map<SchemaName, SourcePermissionsSchema> }

let emptySourcePermissions: SourcePermissions = { Schemas = Map.empty }
