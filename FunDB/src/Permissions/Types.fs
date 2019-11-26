module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Permissions.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type UserName = string

type ResolvedRoleRef = Source.ResolvedRoleRef

[<NoComparison>]
type Restriction =
    { expression : ResolvedOptimizedFieldExpr
      globalArguments : Set<ArgumentName>
    } with
    override this.ToString () = this.ToFunQLString ()

    member this.ToFunQLString () = this.expression.ToFunQLString ()

    interface IFunQLString with
        member this.ToFunQLString () = this.ToFunQLString()

[<NoComparison>]
type AllowedField =
    { // Are you allowed to change (UPDATE/INSERT) this field?
      change : bool
      // Are you allowed to select this entry? If yes, what _additional_ restrictions are in place if this field is used, on top of entity-wide?
      select : Restriction
    }

[<NoComparison>]
type AllowedOperationError =
    { source : string
      error : exn
    }

[<NoComparison>]
type AllowedEntity =
    { allowBroken : bool
      // Post-UPDATE/INSERT check expression.
      check : Restriction
      // Are you allowed to INSERT?
      insert : bool
      // Which entries are you allowed to SELECT?
      select : Restriction
      // Which entries are you allowed to UPDATE (on top of SELECT)?
      update : Restriction
      // Which entries are you allowed to DELETE (on top of SELECT)?
      delete : Restriction
      fields : Map<FieldName, AllowedField>
    }

[<NoComparison>]
type AllowedEntityError =
    { source : SourceAllowedEntity
      error : exn
    }

[<NoComparison>]
type AllowedSchema =
    { entities : Map<EntityName, Result<AllowedEntity, AllowedEntityError>>
    }

[<NoComparison>]
type AllowedDatabase =
    { schemas : Map<SchemaName, AllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities

[<NoComparison>]
type FlatAllowedDerivedEntity =
    { insert : bool
      check : Restriction
      select : Restriction
      update : Restriction
      delete : Restriction
    }

[<NoComparison>]
type FlatAllowedEntity =
    { children : Map<ResolvedEntityRef, FlatAllowedDerivedEntity>
      fields : Map<ResolvedFieldRef, AllowedField>
    }

type FlatAllowedDatabase = Map<ResolvedEntityRef, FlatAllowedEntity>

[<NoComparison>]
type ResolvedRole =
    { parents : Set<ResolvedRoleRef>
      permissions : AllowedDatabase
      flattened : FlatAllowedDatabase
      allowBroken : bool
    }

[<NoComparison>]
type RoleError =
    { source : SourceRole
      error : exn
    }

[<NoComparison>]
type PermissionsSchema =
    { roles : Map<RoleName, Result<ResolvedRole, RoleError>>
    }

[<NoComparison>]
type Permissions =
    { schemas : Map<SchemaName, PermissionsSchema>
    } with
        member this.Find (ref : ResolvedRoleRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.roles

type ErroredAllowedSchema = Map<EntityName, exn>
type ErroredAllowedDatabase = Map<SchemaName, ErroredAllowedSchema>

type ErroredRole =
    | EFatal of exn
    | EDatabase of ErroredAllowedDatabase

type ErroredRoles = Map<RoleName, ErroredRole>
type ErroredPermissions = Map<SchemaName, ErroredRoles>