module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.AST

type RoleName = FunQLName
type UserName = string

 // Actually stringified expression, used for deduplication.
type RestrictionKey = string

[<NoComparison>]
type AllowedEntity =
    { fields : Set<FieldName>
      where : LocalFieldExpr option
    }

[<NoComparison>]
type AllowedSchema =
    { entities : Map<EntityName, AllowedEntity>
    }

[<NoComparison>]
type AllowedDatabase =
    { schemas : Map<SchemaName, AllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities

type Restrictions = Map<RestrictionKey, LocalFieldExpr>
type RoleRef = ResolvedEntityRef

[<NoComparison>]
type FlatAllowedEntity =
    { fields : Map<FieldName, Restrictions option>
    }

[<NoComparison>]
type FlatAllowedSchema =
    { entities : Map<EntityName, FlatAllowedEntity>
    }

[<NoComparison>]
type FlatAllowedDatabase =
    { schemas : Map<SchemaName, FlatAllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities

[<NoComparison>]
type ResolvedRole =
    { parents : Set<RoleRef>
      permissions : AllowedDatabase
      flatPermissions : FlatAllowedDatabase
    }

[<NoComparison>]
type PermissionsSchema =
    { roles : Map<RoleName, ResolvedRole>
    }

[<NoComparison>]
type Permissions =
    { schemas : Map<SchemaName, PermissionsSchema>
    } with
        member this.Find (ref : RoleRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.roles
