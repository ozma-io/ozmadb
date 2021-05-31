module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type CompiledRestriction =
    { Joins : JoinPaths
      Where : SQL.ValueExpr
    }

let compileRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : QueryArguments) (restr : ResolvedOptimizedFieldExpr) : QueryArguments * CompiledRestriction =
    let (info, from) = compileSingleFromExpr layout arguments (FEntity (None, relaxEntityRef ref)) (Some <| restr.ToFieldExpr())
    let ret =
        { Joins = from.Joins
          Where = Option.get from.Where
        }
    (info.Arguments, ret)

let compileValueRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : QueryArguments) (restr : ResolvedOptimizedFieldExpr) : QueryArguments * SQL.ValueExpr =
    let (info, from) = compileSingleFromExpr layout arguments (FEntity (None, relaxEntityRef ref)) (Some <| restr.ToFieldExpr())
    let ret =
        match from.From with
        | SQL.FTable _ ->
            // We can make expression simpler in this case, just using `WHERE`.
            // Drop the table names beforehand, as we are in an `UPDATE` or `DELETE` with only one table name bound.
            // For example, "schema__table"."foo" becomes just "foo", because we already do an update on "schema"."table".
            let expr = Option.get from.Where
            let mapper =
                { SQL.idValueExprMapper with
                      ColumnReference = fun col -> { col with Table = None }
                }
            SQL.mapValueExpr mapper expr
        | _ ->
            let select =
                { Columns = [| SQL.SCExpr (None, SQL.VEColumn { Table = Some <| compileRenamedResolvedEntityRef ref; Name = sqlFunId }) |]
                  From = Some from.From
                  Where = from.Where
                  GroupBy = [||]
                  OrderLimit = SQL.emptyOrderLimitClause
                  Extra = null
                } : SQL.SingleSelectExpr
            let subexpr = { CTEs = None; Extra = null; Tree = SQL.SSelect select } : SQL.SelectExpr
            let idColumn = SQL.VEColumn { Table = None; Name = sqlFunId }
            SQL.VEInQuery (idColumn, subexpr)
    (info.Arguments, ret)