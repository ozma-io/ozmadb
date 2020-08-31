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
    match e.Expression with
    | OFEFalse -> None
    | _ -> (e.Expression.ToFieldExpr() |> namesToRefs).ToFunQLString() |> Some

let private renderAllowedField (allowedField : AllowedField) : SourceAllowedField =
    { Change = allowedField.Change
      Select = renderRestriction allowedField.Select
    }

let private renderAllowedEntity : Result<AllowedEntity, AllowedEntityError> -> SourceAllowedEntity = function
    | Ok allowedEntity ->
        { AllowBroken = allowedEntity.AllowBroken
          Fields = allowedEntity.Fields |> Map.map (fun name -> renderAllowedField)
          Check = renderRestriction allowedEntity.Check
          Insert = allowedEntity.Insert
          Select = renderRestriction allowedEntity.Select
          Update = renderRestriction allowedEntity.Update
          Delete = renderRestriction allowedEntity.Delete
        } : SourceAllowedEntity
    | Error e -> e.Source

let private renderAllowedSchema (allowedSchema : AllowedSchema) : SourceAllowedSchema =
    { Entities = Map.map (fun name -> renderAllowedEntity) allowedSchema.Entities
    }

let private renderAllowedDatabase (allowedDb : AllowedDatabase) : SourceAllowedDatabase =
    { Schemas = Map.map (fun name -> renderAllowedSchema) allowedDb.Schemas
    }

let private renderRole : Result<ResolvedRole, RoleError> -> SourceRole = function
    | Ok role ->
        { Parents = role.Parents
          Permissions = renderAllowedDatabase role.Permissions
          AllowBroken = role.AllowBroken
        }
    | Error e -> e.Source

let renderPermissionsSchema (schema : PermissionsSchema) : SourcePermissionsSchema =
    { Roles = Map.map (fun name -> renderRole) schema.Roles
    }

let renderPermissions (perms : Permissions) : SourcePermissions =
    { Schemas = Map.map (fun name -> renderPermissionsSchema) perms.Schemas
    }