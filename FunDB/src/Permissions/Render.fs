module FunWithFlags.FunDB.Permissions.Render

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

let private renderLocalExpr (e : LocalFieldExpr) = e.ToFunQLString()

let private renderOperation = function
  | Ok e -> renderLocalExpr e
  | Error (e : AllowedOperationError) -> e.source

let private renderAllowedField (allowedField : AllowedField) : SourceAllowedField =
    { change = allowedField.change
      select = Option.map renderLocalExpr allowedField.select
    }

let private renderAllowedEntity = function
    | Ok allowedEntity ->
        { allowBroken = allowedEntity.allowBroken
          fields = Map.map (fun name -> renderAllowedField) allowedEntity.fields
          check = Option.map renderLocalExpr allowedEntity.check
          insert =
            match allowedEntity.insert with
            | Ok i -> i
            | Error _ -> true
          select = Option.map renderLocalExpr allowedEntity.select
          update = Option.map renderLocalExpr allowedEntity.update
          delete = Option.map renderOperation allowedEntity.delete
        } : SourceAllowedEntity
    | Error e -> e.source

let private renderAllowedSchema (allowedSchema : AllowedSchema) : SourceAllowedSchema =
    { entities = Map.map (fun name -> renderAllowedEntity) allowedSchema.entities
    }

let private renderAllowedDatabase (allowedDb : AllowedDatabase) : SourceAllowedDatabase =
    { schemas = Map.map (fun name -> renderAllowedSchema) allowedDb.schemas
    }

let private renderRole (role : ResolvedRole) : SourceRole =
    { parents = role.parents
      permissions = renderAllowedDatabase role.permissions
    }

let renderPermissionsSchema (schema : PermissionsSchema) : SourcePermissionsSchema =
    { roles = Map.map (fun name -> renderRole) schema.roles
    }

let renderPermissions (perms : Permissions) : SourcePermissions =
    { schemas = Map.map (fun name -> renderPermissionsSchema) perms.schemas
    }