module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Permissions.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type UserName = string

type ResolvedRoleRef = Source.ResolvedRoleRef

type RoleRef = ResolvedEntityRef

type AllowedEntityRef =
  { Role : RoleRef
    Entity : ResolvedEntityRef
  }

[<NoEquality; NoComparison>]
type AllowedField =
    { // Are you allowed to change (UPDATE/INSERT) this field?
      Change : bool
      // Are you allowed to select this entry? If yes, what _additional_ restrictions are in place if this field is used, on top of entity-wide?
      Select : ResolvedOptimizedFieldExpr
    }

[<NoEquality; NoComparison>]
type AllowedOperationError =
    { Source : string
      Error : exn
    }

[<NoEquality; NoComparison>]
type AllowedEntity =
    { AllowBroken : bool
      // Post-UPDATE/INSERT check expression.
      Check : ResolvedOptimizedFieldExpr
      // Are you allowed to INSERT?
      Insert : bool
      // Which entries are you allowed to SELECT?
      Select : ResolvedOptimizedFieldExpr
      // Which entries are you allowed to UPDATE (on top of SELECT)?
      Update : ResolvedOptimizedFieldExpr
      // Which entries are you allowed to DELETE (on top of SELECT)?
      Delete : ResolvedOptimizedFieldExpr
      Fields : Map<FieldName, AllowedField>
    }

[<NoEquality; NoComparison>]
type AllowedEntityError =
    { Source : SourceAllowedEntity
      Error : exn
    }

[<NoEquality; NoComparison>]
type AllowedSchema =
    { Entities : Map<EntityName, Result<AllowedEntity, AllowedEntityError>>
    }

[<NoEquality; NoComparison>]
type AllowedDatabase =
    { Schemas : Map<SchemaName, AllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.Schema this.Schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.Name schema.Entities

let emptyAllowedDatabase : AllowedDatabase =
    { Schemas = Map.empty
    }

[<NoEquality; NoComparison>]
type FlatAllowedDerivedEntity =
    { Insert : bool
      Check : ResolvedOptimizedFieldExpr
      Select : ResolvedOptimizedFieldExpr
      Update : ResolvedOptimizedFieldExpr
      Delete : ResolvedOptimizedFieldExpr
    }

[<NoEquality; NoComparison>]
type FlatAllowedEntity =
    { Children : Map<ResolvedEntityRef, FlatAllowedDerivedEntity>
      Fields : Map<ResolvedFieldRef, AllowedField>
    }

type FlatAllowedDatabase = Map<ResolvedEntityRef, FlatAllowedEntity>

[<NoEquality; NoComparison>]
type ResolvedRole =
    { Parents : Set<ResolvedRoleRef>
      Permissions : AllowedDatabase
      Flattened : FlatAllowedDatabase
      AllowBroken : bool
    }

let emptyResolvedRole =
    { Parents = Set.empty
      Permissions = emptyAllowedDatabase
      Flattened = Map.empty
      AllowBroken = false
    }

[<NoEquality; NoComparison>]
type PermissionsSchema =
    { Roles : Map<RoleName, Result<ResolvedRole, exn>>
    }

[<NoEquality; NoComparison>]
type Permissions =
    { Schemas : Map<SchemaName, PermissionsSchema>
    } with
        member this.Find (ref : ResolvedRoleRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.Name schema.Roles

type ErroredAllowedSchema = Map<EntityName, exn>
type ErroredAllowedDatabase = Map<SchemaName, ErroredAllowedSchema>

type ErroredRole =
    | ERFatal of exn
    | ERDatabase of ErroredAllowedDatabase

type ErroredRoles = Map<RoleName, ErroredRole>
type ErroredPermissions = Map<SchemaName, ErroredRoles>