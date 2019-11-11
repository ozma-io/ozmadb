module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let compileRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.SingleSelectExpr =
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, ref)) (Some restr.expression)
    { columns = [| compileResolvedEntityRef ref |> Some |> SQL.SCAll |]
      from = Some from
      where = where
      groupBy = [||]
      orderLimit = SQL.emptyOrderLimitClause
    }

let compileValueRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.ValueExpr =
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, ref)) (Some restr.expression)
    match from with
    | SQL.FTable _ -> where |> Option.get
    | _ ->
        let select =
            { columns = [| SQL.SCExpr (None, SQL.VEColumn { table = Some <| compileResolvedEntityRef ref; name = sqlFunId }) |]
              from = Some from
              where = where
              groupBy = [||]
              orderLimit = SQL.emptyOrderLimitClause
            } : SQL.SingleSelectExpr
        let subexpr = SQL.SSelect select
        let idColumn = SQL.VEColumn { table = None; name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)