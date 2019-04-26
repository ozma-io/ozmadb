module FunWithFlags.FunDB.Permissions.Entity

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Flatten
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsEntityException (message : string) =
    inherit Exception(message)

let applyRoleInsert (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (expr : SQL.InsertExpr) : SQL.InsertExpr =
    let allowedFields =
        match role.FindEntity entityRef with
        | Some { insert = Some insert } -> insert
        | _ -> raisef PermissionsEntityException "Access denied to insert"
    
    for col in expr.columns do
        if col <> sqlFunId && not <| Set.contains (decompileName col) allowedFields then
            raisef PermissionsEntityException "Access denied to insert field %O" col
    expr

let applyRoleUpdate (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (expr : SQL.UpdateExpr) : SQL.UpdateExpr =
    let allowedFields =
        match role.FindEntity entityRef with
        | Some entity when not <| Map.isEmpty entity.update -> entity.update
        | _ -> raisef PermissionsEntityException "Access denied to update"
            
    let addRestriction restrictions col =
        match Map.tryFind (decompileName col) allowedFields with
            | Some restr -> mergeAllowedOperation restrictions restr
            | None -> raisef PermissionsEntityException "Access denied to insert field %O" col   
    let restrictions = expr.columns |> Map.keys |> Seq.fold addRestriction OAllowed
    
    let filter = Option.map (compileLocalFieldExpr expr.name entity) <| allowedOperationExpression restrictions
    let where = Option.unionWith (curry SQL.VEAnd) expr.where filter

    { expr with
          where = where
    }

let applyRoleDelete (role : FlatRole) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (expr : SQL.DeleteExpr) : SQL.DeleteExpr =
    let restrictions =
        match role.FindEntity entityRef with
        | Some { delete = Some delete } -> delete
        | _ -> raisef PermissionsEntityException "Access denied to delete"
        
    let filter = Option.map (compileLocalFieldExpr expr.name entity) <| allowedOperationExpression restrictions
    let where = Option.unionWith (curry SQL.VEAnd) expr.where filter

    { expr with
          where = where
    }