module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let compileRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.SingleSelectExpr =
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, relaxEntityRef ref)) (Some <| restr.Expression.ToFieldExpr())
    { Columns = [| compileRenamedResolvedEntityRef ref |> Some |> SQL.SCAll |]
      From = Some from
      Where = where
      GroupBy = [||]
      OrderLimit = SQL.emptyOrderLimitClause
      Extra = null
    }

let compileValueRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.ValueExpr =
    let (from, where) = compileSingleFromClause layout arguments (FEntity (None, relaxEntityRef ref)) (Some <| restr.Expression.ToFieldExpr())
    match from with
    | SQL.FTable _ ->
        // We can make expression simpler in this case, just using `WHERE`.
        // Drop the table names beforehand, as we are in an `UPDATE` or `DELETE` with only one table name bound.
        // For example, "schema__table"."foo" becomes just "foo", because we already do an update on "schema"."table".
        let expr = Option.get where
        let mapper =
            { SQL.idValueExprMapper with
                  ColumnReference = fun col -> { col with table = None }
            }
        SQL.mapValueExpr mapper expr
    | _ ->
        let select =
            { Columns = [| SQL.SCExpr (None, SQL.VEColumn { table = Some <| compileRenamedResolvedEntityRef ref; name = sqlFunId }) |]
              From = Some from
              Where = where
              GroupBy = [||]
              OrderLimit = SQL.emptyOrderLimitClause
              Extra = null
            } : SQL.SingleSelectExpr
        let subexpr = { CTEs = None; Extra = null; Tree = SQL.SSelect select } : SQL.SelectExpr
        let idColumn = SQL.VEColumn { table = None; name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)