module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source

type UserName = string

type ResolvedRoleRef = ResolvedEntityRef

[<NoComparison>]
type Restriction =
    { expression : LocalFieldExpr
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
      select : Restriction option
    }

[<NoComparison>]
type AllowedOperationError =
    { source : string
      error : exn
    }

[<NoComparison>]
type AllowedEntity =
    { allowBroken : bool
      // Post-UPDATE/INSERT check expression. If None you cannot UPDATE nor INSERT.
      check : Restriction option
      // Are you allowed to INSERT?
      insert : Result<bool, exn>
      // Which entries are you allowed to SELECT?
      select : Restriction option
      // Which entries are you allowed to UPDATE (on top of SELECT)?
      update : Restriction option
      // Which entries are you allowed to DELETE (on top of SELECT)?
      delete : Result<Restriction, AllowedOperationError> option
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
type ResolvedRole =
    { parents : Set<ResolvedRoleRef>
      permissions : AllowedDatabase
    }

[<NoComparison>]
type PermissionsSchema =
    { roles : Map<RoleName, ResolvedRole>
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
type ErroredRoles = Map<RoleName, ErroredAllowedDatabase>
type ErroredPermissions = Map<SchemaName, ErroredRoles>