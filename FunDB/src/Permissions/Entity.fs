module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsEntityException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        PermissionsEntityException (message, innerException, isUserException innerException)

    new (message : string) = PermissionsEntityException (message, null, true)

let serializeEntityRestricted (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) : SerializedEntity =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind entity.Root role.Flattened.Entities with
        | None -> raisef PermissionsEntityException "Access denied"
        | Some access -> access

    let checkRoles (f : FlatAllowedDerivedEntity -> bool) : bool =
        let checkRole roleRef (flatAllowedEntity : FlatAllowedRoleEntity) =
            match Map.tryFind entityRef flatAllowedEntity.Children with
            | None -> false
            | Some child -> f child
        Map.exists checkRole flattened.Roles

    if not (checkRoles (fun _ -> true)) then
        raisef PermissionsEntityException "Access denied to get info"

    let serialized = serializeEntity layout entity

    let entityAccess : SerializedEntityAccess =
        { Select = checkRoles (fun entity -> entity.CombinedSelect)
          Insert = checkRoles (fun entity -> entity.CombinedInsert)
          Delete = checkRoles (fun entity -> entity.CombinedDelete)
        }

    let applyToField name (field : SerializedColumnField) : SerializedColumnField option =
        let parentEntity = Option.defaultValue entityRef field.InheritedFrom
        let fieldRef = { Entity = parentEntity; Name = name }

        let checkRoleFields (f : AllowedField -> bool) : bool =
            let checkOne roleRef (flatAllowedEntity : FlatAllowedRoleEntity) =
                match Map.tryFind fieldRef flatAllowedEntity.Fields with
                | None -> false
                | Some child -> f child
            Map.exists checkOne flattened.Roles

        if not (checkRoleFields (fun _ -> true)) then
            None
        else
            let access : SerializedFieldAccess =
                let select = entityAccess.Select && checkRoleFields (fun field -> not (optimizedIsFalse field.Select))
                let change = checkRoleFields (fun field -> field.Change)
                { Select = select
                  Insert = entityAccess.Insert && change
                  Update = select && change
                }
            let field =
                { field with
                      Access = access
                }
            Some field

    let columnFields = serialized.ColumnFields |> Map.mapMaybe applyToField

    { serialized with
          ColumnFields = columnFields
          Access = entityAccess
    }
