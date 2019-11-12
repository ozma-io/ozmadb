module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Layout.Render
open FunWithFlags.FunDB.Permissions.Flatten
open FunWithFlags.FunDB.Permissions.Compile
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsEntityException (message : string) =
    inherit Exception(message)

let applyRoleInsert (layout : Layout)  (role : FlatRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.InsertExpr>) : Query<SQL.InsertExpr> =
    let allowedFields =
        match role.FindEntity entityRef with
        | Some { insert = Some insert } -> insert
        | _ -> raisef PermissionsEntityException "Access denied to insert"

    for col in query.expression.columns do
        if col <> sqlFunId && not <| Set.contains (decompileName col) allowedFields then
            raisef PermissionsEntityException "Access denied to insert field %O" col
    query

let applyRoleUpdate (layout : Layout) (role : FlatRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.UpdateExpr>) : Query<SQL.UpdateExpr> =
    let allowedFields =
        match role.FindEntity entityRef with
        | Some entity when not <| Map.isEmpty entity.update -> entity.update
        | _ -> raisef PermissionsEntityException "Access denied to update"

    let addRestriction restrictions col =
        match Map.tryFind (decompileName col) allowedFields with
            | Some restr -> mergeAllowedOperation restrictions restr
            | None -> raisef PermissionsEntityException "Access denied to insert field %O" col
    let maybeRestriction = query.expression.columns |> Map.keys |> Seq.fold addRestriction OAllowed |> allowedOperationRestriction

    match maybeRestriction with
    | None -> query
    | Some restriction ->
        let findOne args name typ =
            if Set.contains name restriction.globalArguments then
                addArgument (PGlobal name) typ args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOne query.arguments
        let newExpr = compileValueRestriction layout entityRef arguments.types restriction
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }

let applyRoleDelete (layout : Layout) (role : FlatRole) (entityRef : ResolvedEntityRef) (query : Query<SQL.DeleteExpr>) : Query<SQL.DeleteExpr> =
    let maybeRestriction =
        match role.FindEntity entityRef with
        | Some { delete = Some delete } -> allowedOperationRestriction delete
        | _ -> raisef PermissionsEntityException "Access denied to delete"

    match maybeRestriction with
    | None -> query
    | Some restriction ->
        let findOne args name typ =
            if Set.contains name restriction.globalArguments then
                addArgument (PGlobal name) typ args
            else
                args
        let arguments = globalArgumentTypes |> Map.fold findOne query.arguments
        let newExpr = compileValueRestriction layout entityRef arguments.types restriction
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }

let applyRoleInfo (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SerializedEntity =
    let entityAccess =
        match role.FindEntity entityRef with
        | None -> raisef PermissionsEntityException "Access denied"
        | Some access -> access
    let fields1 =
        match entityAccess.insert with
        | None -> Set.empty
        | Some fields -> fields
    let fields2 =
        match entityAccess.select with
        | None -> Set.empty
        | Some fields -> Map.keysSet fields
    let allowedFields = Set.union fields1 fields2

    let columnFields = entity.columnFields |> Map.filter (fun name field -> Set.contains name allowedFields) |> Map.map (fun name field -> serializeColumnField field)

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