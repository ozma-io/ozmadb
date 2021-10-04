module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
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
    if not (flattened.Roles |> Map.toSeq |> Seq.exists (fun (roleRef, allowedEntity) -> Map.containsKey entityRef allowedEntity.Children)) then
        raisef PermissionsEntityException "Access denied to get info"

    let serialized = serializeEntity layout entity

    let filterField name (field : SerializedColumnField) =
        let parentEntity = Option.defaultValue entityRef field.InheritedFrom
        let fieldRef = { Entity = parentEntity; Name = name }
        flattened.Roles
            |> Map.toSeq
            |> Seq.exists (fun (roleRef, allowedEntity) -> Map.containsKey fieldRef allowedEntity.Fields)

    let columnFields = serialized.ColumnFields |> Map.filter filterField

    { serialized with
          ColumnFields = columnFields
          // FIXME!
          ComputedFields = Map.empty
          UniqueConstraints = Map.empty
          CheckConstraints = Map.empty
    }
