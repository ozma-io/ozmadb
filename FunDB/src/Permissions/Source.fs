module FunWithFlags.FunDB.Permissions.Source

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST

type ResolvedRoleRef = ResolvedEntityRef

type SourceAllowedField =
    { Change : bool
      Select : string option
    }

type SourceAllowedEntity =
    { AllowBroken : bool
      Check : string option
      Insert : bool
      Select : string option
      Update : string option
      Delete : string option
      Fields : Map<FieldName, SourceAllowedField>
    }

type SourceAllowedSchema =
    { Entities : Map<EntityName, SourceAllowedEntity>
    }

type SourceAllowedDatabase =
    { Schemas : Map<SchemaName, SourceAllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.Entities

type SourceRole =
    { Parents : Set<ResolvedRoleRef>
      Permissions : SourceAllowedDatabase
      AllowBroken : bool
    }

type SourcePermissionsSchema =
    { Roles : Map<RoleName, SourceRole>
    }

let emptySourcePermissionsSchema : SourcePermissionsSchema =
    { Roles = Map.empty }

let mergeSourcePermissionsSchema (a : SourcePermissionsSchema) (b : SourcePermissionsSchema) : SourcePermissionsSchema =
    { Roles = Map.unionUnique a.Roles b.Roles
    }

type SourcePermissions =
    { Schemas : Map<SchemaName, SourcePermissionsSchema>
    }

let emptySourcePermissions : SourcePermissions =
    { Schemas = Map.empty }