module FunWithFlags.FunDB.Permissions.Render

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

let private renderAllowedEntity (allowedEntity : AllowedEntity) : SourceAllowedEntity =
    { fields = allowedEntity.fields
      where = Option.map (fun (expr : LocalFieldExpr) -> expr.ToFunQLString()) allowedEntity.where
    }

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