module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Compile
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsEntityException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsEntityException (message, null)

let applyRoleInsert (layout : Layout)  (role : ResolvedRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.InsertExpr>) : Query<SQL.InsertExpr> =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to insert"
    match Map.tryFind entityRef flattened.children with
    | Some { insert = true } -> ()
    | _ -> raisef PermissionsEntityException" Access denied to insert"

    for col in query.expression.columns do
        if col = sqlFunId || col = sqlFunSubEntity then
            ()
        else
            let name = decompileName col
            let field = Map.find name entity.columnFields
            let parentEntity = Option.defaultValue entityRef field.inheritedFrom
            match Map.tryFind { entity = parentEntity; name = name } flattened.fields with
            | Some { change = true } -> ()
            | _ -> raisef PermissionsEntityException "Access denied to insert field %O" name
    query

let applyRoleUpdate (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.UpdateExpr>) : Query<SQL.UpdateExpr> =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to update"

    let accessor (derived : FlatAllowedDerivedEntity) =
        andRestriction derived.select derived.update
    let updateRestr = applyRestrictionExpression accessor layout flattened entityRef

    let addRestriction restriction col =
        let name = decompileName col
        let field = Map.find name entity.columnFields
        let parentEntity = Option.defaultValue entityRef field.inheritedFrom
        match Map.tryFind { entity = parentEntity; name = name } flattened.fields with
        | Some { change = true; select = select } -> andRestriction restriction select
        | _ -> raisef PermissionsEntityException "Access denied to update field %O" name
    let fieldsRestriction = query.expression.columns |> Map.keys |> Seq.fold addRestriction updateRestr

    match fieldsRestriction.expression with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to update"
    | OFETrue -> query
    | _ ->
        let findOne args name typ =
            if Set.contains name fieldsRestriction.globalArguments then
                addArgument (PGlobal name) typ args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOne query.arguments
        let newExpr = compileValueRestriction layout entityRef arguments.types fieldsRestriction
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }

// We don't allow to delete other entities in the inheritance hierarchy, so we don't need to apply restrictions from parents and children.
let applyRoleDelete (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.DeleteExpr>) : Query<SQL.DeleteExpr> =
    let entity = layout.FindEntity entityRef |> Option.get
    let flattened =
        match Map.tryFind  entity.root role.flattened with
        | Some flat -> flat
        | _ -> raisef PermissionsEntityException "Access denied to delete"
    let deleteRestr =
        match Map.tryFind entityRef flattened.children with
        | Some r -> andRestriction r.select r.delete
        | _ -> raisef PermissionsEntityException" Access denied to delete"

    let addRestriction restriction (name, field : ResolvedColumnField) =
        let parentEntity = Option.defaultValue entityRef field.inheritedFrom
        match Map.tryFind { entity = parentEntity; name = name } flattened.fields with
        | Some allowedField -> andRestriction restriction allowedField.select
        | _ -> raisef PermissionsEntityException "Access denied to select field %O" name
    let fieldsRestriction = entity.columnFields |> Map.toSeq |> Seq.fold addRestriction deleteRestr

    match fieldsRestriction.expression with
    | OFEFalse -> raisef PermissionsEntityException "Access denied to delete"
    | OFETrue -> query
    | _ ->
        let findOne args name typ =
            if Set.contains name fieldsRestriction.globalArguments then
                addArgument (PGlobal name) typ args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOne query.arguments
        let newExpr = compileValueRestriction layout entityRef arguments.types fieldsRestriction
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }

let applyRoleInfo (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SerializedEntity =
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

    { columnFields = columnFields
      // FIXME!
      computedFields = Map.empty
      uniqueConstraints = Map.empty
      checkConstraints = Map.empty
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      hidden = entity.hidden
      parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      children = entity.children
      isAbstract = entity.isAbstract
      root = entity.root
    }