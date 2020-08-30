module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Compile
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

type PermissionsEntityException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsEntityException (message, null)

type RestrictedTableInfo =
    { ref : ResolvedEntityRef
    }

type RestrictedColumnInfo =
    { name : FieldName
    }

let applyRoleInsert (layout : Layout)  (role : ResolvedRole) (query : Query<SQL.InsertExpr>) : Query<SQL.InsertExpr> =
    let tableInfo = query.Expression.Extra :?> RestrictedTableInfo
    let entity = layout.FindEntity tableInfo.ref |> Option.get
    let flattened =
        match Map.tryFind entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to insert"
    match Map.tryFind tableInfo.ref flattened.children with
    | Some { insert = true } -> ()
    | _ -> raisef PermissionsEntityException" Access denied to insert"

    for (extra, col) in query.Expression.Columns do
        match extra with
        | :? RestrictedColumnInfo as colInfo ->
            let field = Map.find colInfo.name entity.columnFields
            let parentEntity = Option.defaultValue tableInfo.ref field.inheritedFrom
            match Map.tryFind { entity = parentEntity; name = colInfo.name } flattened.fields with
            | Some { change = true } -> ()
            | _ -> raisef PermissionsEntityException "Access denied to insert field %O" colInfo.name
        | _ -> ()
    query

let applyRoleUpdate (layout : Layout) (role : ResolvedRole) (query : Query<SQL.UpdateExpr>) : Query<SQL.UpdateExpr> =
    let tableInfo = query.Expression.Extra :?> RestrictedTableInfo
    let entity = layout.FindEntity tableInfo.ref |> Option.get
    let flattened =
        match Map.tryFind entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to update"

    let accessor (derived : FlatAllowedDerivedEntity) =
        andRestriction derived.select derived.update
    let updateRestr = applyRestrictionExpression accessor layout flattened tableInfo.ref

    let addRestriction restriction (extra : obj, col) =
        match extra with
        | :? RestrictedColumnInfo as colInfo ->
            let field = Map.find colInfo.name entity.columnFields
            let parentEntity = Option.defaultValue tableInfo.ref field.inheritedFrom
            match Map.tryFind { entity = parentEntity; name = colInfo.name } flattened.fields with
            | Some { change = true; select = select } -> andRestriction restriction select
            | _ -> raisef PermissionsEntityException "Access denied to update field %O" colInfo.name
        | _ -> restriction
    let fieldsRestriction = query.Expression.Columns |> Map.values |> Seq.fold addRestriction updateRestr

    match fieldsRestriction.expression with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to update"
    | OFETrue -> query
    | _ ->
        let findOrAddOne args name typ =
            if Set.contains name fieldsRestriction.globalArguments then
                let (newArg, args) = addArgument (PGlobal name) typ args
                args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOrAddOne query.Arguments
        let newExpr = compileValueRestriction layout tableInfo.ref arguments.Types fieldsRestriction
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
    let entity = layout.FindEntity tableInfo.ref |> Option.get
    let flattened =
        match Map.tryFind  entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to delete"
    let deleteRestr =
        match Map.tryFind tableInfo.ref flattened.children with
        | Some r -> andRestriction r.select r.delete
        | _ -> raisef PermissionsEntityException" Access denied to delete"

    let addRestriction restriction (name, field : ResolvedColumnField) =
        let parentEntity = Option.defaultValue tableInfo.ref field.inheritedFrom
        match Map.tryFind { entity = parentEntity; name = name } flattened.fields with
        | Some allowedField -> andRestriction restriction allowedField.select
        | _ -> raisef PermissionsEntityException "Access denied to select field %O" name
    let fieldsRestriction = entity.columnFields |> Map.toSeq |> Seq.fold addRestriction deleteRestr

    match fieldsRestriction.expression with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to delete"
    | OFETrue -> query
    | _ ->
        let findOrAddOne args name typ =
            if Set.contains name fieldsRestriction.globalArguments then
                let (newArg, args) = addArgument (PGlobal name) typ args
                args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOrAddOne query.Arguments
        let newExpr = compileValueRestriction layout tableInfo.ref arguments.Types fieldsRestriction
        let expr =
            { query.Expression with
                  Where = Option.unionWith (curry SQL.VEAnd) query.Expression.Where (Some newExpr)
            }
        { Arguments = arguments
          Expression = expr
        }

let applyRoleInfo (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) : SerializedEntity =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind entity.root role.flattened with
        | None -> raisef PermissionsEntityException "Access denied"
        | Some access -> access
    match Map.tryFind entityRef flattened.children with
    | Some _ -> ()
    | _ -> raisef PermissionsEntityException" Access denied to get info"

    let getField name (field : ResolvedColumnField) =
        let parentEntity = Option.defaultValue entityRef field.inheritedFrom
        match Map.tryFind { entity = parentEntity; name = name } flattened.fields with
        | Some allowedField -> serializeColumnField field |> Some
        | _ -> None

    let columnFields = entity.columnFields |> Map.mapMaybe getField

    { ColumnFields = columnFields
      // FIXME!
      ComputedFields = Map.empty
      UniqueConstraints = Map.empty
      CheckConstraints = Map.empty
      MainField = entity.mainField
      ForbidExternalReferences = entity.forbidExternalReferences
      Parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      Children = entity.children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.direct })
      IsAbstract = entity.isAbstract
      IsFrozen = entity.isFrozen
      Root = entity.root
    }
