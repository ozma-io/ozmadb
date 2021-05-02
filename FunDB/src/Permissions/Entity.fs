module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Compile
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

type PermissionsEntityException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsEntityException (message, null)

type RestrictedTableInfo =
    { Ref : ResolvedEntityRef
    }

type RestrictedColumnInfo =
    { Name : FieldName
    }

let applyRoleInsert (layout : Layout)  (role : ResolvedRole) (query : Query<SQL.InsertExpr>) : Query<SQL.InsertExpr> =
    let tableInfo = query.Expression.Extra :?> RestrictedTableInfo
    let entity = layout.FindEntity tableInfo.Ref |> Option.get
    let flattened =
        match Map.tryFind entity.Root role.Flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to insert"
    match Map.tryFind tableInfo.Ref flattened.Children with
    | Some { Insert = true } -> ()
    | _ -> raisef PermissionsEntityException" Access denied to insert"

    for (extra, col) in query.Expression.Columns do
        match extra with
        | :? RestrictedColumnInfo as colInfo ->
            let field = Map.find colInfo.Name entity.ColumnFields
            let parentEntity = Option.defaultValue tableInfo.Ref field.InheritedFrom
            match Map.tryFind { Entity = parentEntity; Name = colInfo.Name } flattened.Fields with
            | Some { Change = true } -> ()
            | _ -> raisef PermissionsEntityException "Access denied to insert field %O" colInfo.Name
        | _ -> ()
    query

let applyRoleUpdate (layout : Layout) (role : ResolvedRole) (query : Query<SQL.UpdateExpr>) : Query<SQL.UpdateExpr> =
    let tableInfo = query.Expression.Extra :?> RestrictedTableInfo
    let entity = layout.FindEntity tableInfo.Ref |> Option.get
    let flattened =
        match Map.tryFind entity.Root role.Flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to update"

    let accessor (derived : FlatAllowedDerivedEntity) =
        andFieldExpr derived.Select derived.Update
    let updateRestr = applyRestrictionExpression accessor layout flattened tableInfo.Ref

    let addRestriction restriction (extra : obj, col) =
        match extra with
        | :? RestrictedColumnInfo as colInfo ->
            let field = Map.find colInfo.Name entity.ColumnFields
            let parentEntity = Option.defaultValue tableInfo.Ref field.InheritedFrom
            match Map.tryFind { Entity = parentEntity; Name = colInfo.Name } flattened.Fields with
            | Some { Change = true; Select = select } -> andFieldExpr restriction select
            | _ -> raisef PermissionsEntityException "Access denied to update field %O" colInfo.Name
        | _ -> restriction
    let fieldsRestriction = query.Expression.Columns |> Map.values |> Seq.fold addRestriction updateRestr

    match fieldsRestriction with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to update"
    | OFETrue -> query
    | _ ->
        let (arguments, newExpr) = compileValueRestriction layout tableInfo.Ref query.Arguments fieldsRestriction
        let expr =
            { query.Expression with
                  Where = Option.unionWith (curry SQL.VEAnd) query.Expression.Where (Some newExpr)
            }
        { Arguments = arguments
          Expression = expr
        }

// We don't allow to delete other entities in the inheritance hierarchy, so we don't need to apply restrictions from parents and children.
let applyRoleDelete (layout : Layout) (role : ResolvedRole) (query : Query<SQL.DeleteExpr>) : Query<SQL.DeleteExpr> =
    let tableInfo = query.Expression.Extra :?> RestrictedTableInfo
    let entity = layout.FindEntity tableInfo.Ref |> Option.get
    let flattened =
        match Map.tryFind  entity.Root role.Flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to delete"
    let deleteRestr =
        match Map.tryFind tableInfo.Ref flattened.Children with
        | Some r -> andFieldExpr r.Select r.Delete
        | _ -> raisef PermissionsEntityException" Access denied to delete"

    let addRestriction restriction (name, field : ResolvedColumnField) =
        let parentEntity = Option.defaultValue tableInfo.Ref field.InheritedFrom
        match Map.tryFind { Entity = parentEntity; Name = name } flattened.Fields with
        | Some allowedField -> andFieldExpr restriction allowedField.Select
        | _ -> raisef PermissionsEntityException "Access denied to select field %O" name
    let fieldsRestriction = entity.ColumnFields |> Map.toSeq |> Seq.fold addRestriction deleteRestr

    match fieldsRestriction with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to delete"
    | OFETrue -> query
    | _ ->
        let (arguments, newExpr) = compileValueRestriction layout tableInfo.Ref query.Arguments fieldsRestriction
        let expr =
            { query.Expression with
                  Where = Option.unionWith (curry SQL.VEAnd) query.Expression.Where (Some newExpr)
            }
        { Arguments = arguments
          Expression = expr
        }

let serializeEntityRestricted (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) : SerializedEntity =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind entity.Root role.Flattened with
        | None -> raisef PermissionsEntityException "Access denied"
        | Some access -> access
    match Map.tryFind entityRef flattened.Children with
    | Some _ -> ()
    | _ -> raisef PermissionsEntityException "Access denied to get info"

    let serialized = serializeEntity layout entity

    let filterField name (field : SerializedColumnField) =
        let parentEntity = Option.defaultValue entityRef field.InheritedFrom
        Map.containsKey { Entity = parentEntity; Name = name } flattened.Fields

    let columnFields = serialized.ColumnFields |> Map.filter filterField

    { serialized with
          ColumnFields = columnFields
          // FIXME!
          ComputedFields = Map.empty
          UniqueConstraints = Map.empty
          CheckConstraints = Map.empty
    }
