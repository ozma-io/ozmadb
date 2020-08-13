module FunWithFlags.FunDB.Permissions.Render

open FunWithFlags.FunUtils
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
    { Change = allowedField.change
      Select = renderRestriction allowedField.select
    }

let private renderAllowedEntity : Result<AllowedEntity, AllowedEntityError> -> SourceAllowedEntity = function
    | Ok allowedEntity ->
        { AllowBroken = allowedEntity.allowBroken
          Fields = allowedEntity.fields |> Map.map (fun name -> renderAllowedField)
          Check = renderRestriction allowedEntity.check
          Insert = allowedEntity.insert
          Select = renderRestriction allowedEntity.select
          Update = renderRestriction allowedEntity.update
          Delete = renderRestriction allowedEntity.delete
        } : SourceAllowedEntity
    | Error e -> e.source

let private renderAllowedSchema (allowedSchema : AllowedSchema) : SourceAllowedSchema =
    { Entities = Map.map (fun name -> renderAllowedEntity) allowedSchema.entities
    }

let private renderAllowedDatabase (allowedDb : AllowedDatabase) : SourceAllowedDatabase =
    { Schemas = Map.map (fun name -> renderAllowedSchema) allowedDb.schemas
    }

let private renderRole : Result<ResolvedRole, RoleError> -> SourceRole = function
    | Ok role ->
        { Parents = role.parents
          Permissions = renderAllowedDatabase role.permissions
          AllowBroken = role.allowBroken
        }
    | Error e -> e.source

let renderPermissionsSchema (schema : PermissionsSchema) : SourcePermissionsSchema =
    { Roles = Map.map (fun name -> renderRole) schema.roles
    }

let renderPermissions (perms : Permissions) : SourcePermissions =
    { Schemas = Map.map (fun name -> renderPermissionsSchema) perms.schemas
    }