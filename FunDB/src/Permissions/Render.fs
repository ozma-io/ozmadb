module FunWithFlags.FunDB.Permissions.Render

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

let private namesToRefs =
    let resolveValueRef = function
        | VRColumn (boundRef : BoundRef<FieldRef>) -> VRColumn { boundRef with ref = { boundRef.ref with entity = None } }
        | VRPlaceholder p -> VRPlaceholder p
    let resolveReference (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
        { ref = resolveValueRef ref.ref; path = ref.path }
    let mapper = idFieldExprMapper resolveReference id
    mapFieldExpr mapper

let private renderRestriction (e : Restriction) =
    match e.expression with
    | OFEFalse -> None
    | _ -> (e.expression.ToFieldExpr() |> namesToRefs).ToFunQLString() |> Some

let private renderAllowedField (allowedField : AllowedField) : SourceAllowedField =
    { change = allowedField.change
      select = renderRestriction allowedField.select
    }

let private renderAllowedEntity : Result<AllowedEntity, AllowedEntityError> -> SourceAllowedEntity = function
    | Ok allowedEntity ->
        { allowBroken = allowedEntity.allowBroken
          fields = allowedEntity.fields |> Map.map (fun name -> renderAllowedField)
          check = renderRestriction allowedEntity.check
          insert = allowedEntity.insert
          select = renderRestriction allowedEntity.select
          update = renderRestriction allowedEntity.update
          delete = renderRestriction allowedEntity.delete
        } : SourceAllowedEntity
    | Error e -> e.source

let private renderAllowedSchema (allowedSchema : AllowedSchema) : SourceAllowedSchema =
    { entities = Map.map (fun name -> renderAllowedEntity) allowedSchema.entities
    }

let private renderAllowedDatabase (allowedDb : AllowedDatabase) : SourceAllowedDatabase =
    { schemas = Map.map (fun name -> renderAllowedSchema) allowedDb.schemas
    }

let private renderRole : Result<ResolvedRole, RoleError> -> SourceRole = function
    | Ok role ->
        { parents = role.parents
          permissions = renderAllowedDatabase role.permissions
          allowBroken = role.allowBroken
        }
    | Error e -> e.source

let renderPermissionsSchema (schema : PermissionsSchema) : SourcePermissionsSchema =
    { roles = Map.map (fun name -> renderRole) schema.roles
    }

let renderPermissions (perms : Permissions) : SourcePermissions =
    { schemas = Map.map (fun name -> renderPermissionsSchema) perms.schemas
    }