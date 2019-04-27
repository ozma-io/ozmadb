module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Flatten
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsEntityException (message : string) =
    inherit Exception(message)

let applyRoleInsert (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (query : Query<SQL.InsertExpr>) : Query<SQL.InsertExpr> =
    let allowedFields =
        match role.FindEntity entityRef with
        | Some { insert = Some insert } -> insert
        | _ -> raisef PermissionsEntityException "Access denied to insert"
    
    for col in query.expression.columns do
        if col <> sqlFunId && not <| Set.contains (decompileName col) allowedFields then
            raisef PermissionsEntityException "Access denied to insert field %O" col
    query

let applyRoleUpdate (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (query : Query<SQL.UpdateExpr>) : Query<SQL.UpdateExpr> =
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
        let newExpr = compileLocalFieldExpr arguments.types query.expression.name entity restriction.expression
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }

let applyRoleDelete (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (query : Query<SQL.DeleteExpr>) : Query<SQL.DeleteExpr> =
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
        let newExpr = compileLocalFieldExpr arguments.types query.expression.name entity restriction.expression
        let expr =
            { query.expression with
                  where = Option.unionWith (curry SQL.VEAnd) query.expression.where (Some newExpr)
            }
        { arguments = arguments
          expression = expr
        }