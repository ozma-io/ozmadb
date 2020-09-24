module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let compileRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.SingleSelectExpr =
    let entity = layout.FindEntity ref |> Option.get
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, ref)) (Some <| restr.Expression.ToFieldExpr())
    { Columns = [| compileNoSchemaResolvedEntityRef ref |> Some |> SQL.SCAll |]
      From = Some from
      Where = where
      GroupBy = [||]
      OrderLimit = SQL.emptyOrderLimitClause
      Extra = null
    }

let compileValueRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.ValueExpr =
    let entity = layout.FindEntity ref |> Option.get
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, ref)) (Some <| restr.Expression.ToFieldExpr())
    match from with
    | SQL.FTable _ -> where |> Option.get
    | _ ->
        let select =
            { Columns = [| SQL.SCExpr (None, SQL.VEColumn { table = Some <| compileNoSchemaResolvedEntityRef ref; name = sqlFunId }) |]
              From = Some from
              Where = where
              GroupBy = [||]
              OrderLimit = SQL.emptyOrderLimitClause
              Extra = null
            } : SQL.SingleSelectExpr
        let subexpr = { CTEs = None; Tree = SQL.SSelect select } : SQL.SelectExpr
        let idColumn = SQL.VEColumn { table = None; name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)