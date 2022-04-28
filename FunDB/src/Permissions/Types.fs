module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunUtils

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Objects.Types
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
    { // Are you allowed to INSERT this field?
      Insert : bool
      // Are you allowed to UPDATE this field? If yes, what _additional_ restrictions are in place, added to this entity UPDATE filter?
      Update : ResolvedOptimizedFieldExpr
      // Are you allowed to select this field? If yes, what _additional_ restrictions are in place, added to this entity SELECT filter?
      Select : ResolvedOptimizedFieldExpr
      // Post-UPDATE/INSERT check expression, in addition to entity-wise check.
      Check : ResolvedOptimizedFieldExpr
    }

// Each filter and check expression here is later multiplied (ANDed) by corresponding parent entity expressions (or empty allowed entity if parent entity is not in allowed, effectively rendering all filters FALSE).
// Role may work even when broken, just with less access rights. Hence we split AllowBroken and exceptions in several fields.
[<NoEquality; NoComparison>]
type AllowedEntity =
    { AllowBroken : bool
      // Post-UPDATE/INSERT check expression.
      Check : ResolvedOptimizedFieldExpr
      // Are you allowed to INSERT?
      Insert : Result<bool, exn>
      // Which entries are you allowed to SELECT?
      Select : ResolvedOptimizedFieldExpr
      // Which entries are you allowed to UPDATE? This is multiplied with SELECT later.
      Update : ResolvedOptimizedFieldExpr
      // Which entries are you allowed to DELETE? This is multiplied with SELECT later.
      Delete : Result<ResolvedOptimizedFieldExpr, exn>
      Fields : Map<FieldName, AllowedField>
    }

let allowedEntityIsHalfBroken (entity : AllowedEntity) = (Result.isError entity.Insert || Result.isError entity.Delete)

let emptyAllowedEntity : AllowedEntity =
    { AllowBroken = false
      Check = OFEFalse
      Insert = Ok false
      Select = OFEFalse
      Update = OFEFalse
      Delete = Ok OFEFalse
      Fields = Map.empty
    }

[<NoEquality; NoComparison>]
type AllowedSchema =
    { Entities : Map<EntityName, PossiblyBroken<AllowedEntity>>
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
      // Needed for entity info.
      CombinedSelect : bool
      CombinedInsert : bool
      CombinedDelete : bool
    }

let emptyFlatAllowedDerivedEntity : FlatAllowedDerivedEntity =
    { Insert = false
      Check = OFEFalse
      Select = OFEFalse
      Update = OFEFalse
      Delete = OFEFalse
      CombinedSelect = false
      CombinedInsert = false
      CombinedDelete = false
    }

let fullFlatAllowedDerivedEntity : FlatAllowedDerivedEntity =
    { Insert = true
      Check = OFETrue
      Select = OFETrue
      Update = OFETrue
      Delete = OFETrue
      CombinedSelect = true
      CombinedInsert = true
      CombinedDelete = true
    }

[<NoEquality; NoComparison>]
type FlatAllowedRoleEntity =
    { Children : Map<ResolvedEntityRef, FlatAllowedDerivedEntity>
      Fields : Map<ResolvedFieldRef, AllowedField>
    }

[<NoEquality; NoComparison>]
type FlatAllowedEntity =
  { Roles : Map<ResolvedRoleRef, FlatAllowedRoleEntity>
  }

type FlatAllowedDatabase = Map<ResolvedEntityRef, FlatAllowedEntity>

type FlatRole =
    { Entities : FlatAllowedDatabase
    }

[<NoEquality; NoComparison>]
type ResolvedRole =
    { Parents : Set<ResolvedRoleRef>
      Permissions : AllowedDatabase
      Flattened : FlatRole
    }

let emptyFlatRole =
    { Entities = Map.empty
    }

let emptyResolvedRole =
    { Parents = Set.empty
      Permissions = emptyAllowedDatabase
      Flattened = emptyFlatRole
    }

[<NoEquality; NoComparison>]
type PermissionsSchema =
    { Roles : Map<RoleName, PossiblyBroken<ResolvedRole>>
    }

[<NoEquality; NoComparison>]
type Permissions =
    { Schemas : Map<SchemaName, PermissionsSchema>
    } with
        member this.Find (ref : ResolvedRoleRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.Name schema.Roles
