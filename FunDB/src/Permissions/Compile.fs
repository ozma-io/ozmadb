module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type CompiledRestriction =
    { From : SQL.FromExpr
      Joins : JoinPaths
      Where : SQL.ValueExpr
    }

let private defaultWhere : SQL.ValueExpr = SQL.VEValue (SQL.VBool true)

let compileRestriction (layout : Layout) (entityRef : ResolvedEntityRef) (arguments : QueryArguments) (restr : ResolvedFieldExpr) : QueryArguments * CompiledRestriction =
    let entity = layout.FindEntity entityRef |> Option.get
    // We don't want compiler to add type check to the result, because our own typecheck is built into the restriction.
    // Hence, a hack: we pretend to use root entity instead, but add an alias so that expression properly binds.
    let fEntity =
        { fromEntity (relaxEntityRef entity.Root) with
              Alias = renameResolvedEntityRef entityRef |> decompileName |> Some
        }
    let (info, from) = compileSingleFromExpr layout arguments (FEntity fEntity) (Some restr)
    let ret =
        { From = from.From
          Joins = from.Joins
          Where = Option.defaultValue defaultWhere from.Where
        }
    (info.Arguments, ret)

let restrictionToSelect (ref : ResolvedEntityRef) (restr : CompiledRestriction) : SQL.SelectExpr =
    let select =
        { SQL.emptySingleSelectExpr with
              Columns = [| SQL.SCAll (Some <| compileRenamedResolvedEntityRef ref) |]
              From = Some restr.From
              Where = Some restr.Where
        }
    { CTEs = None
      Tree = SQL.SSelect select
      Extra = null
    }

// TODO: This can be improved: instead of sub-SELECT we could merge the restriction and add FROM entries to UPDATE or DELETE,
// just like we already do with `IsInner` SELECTs.
let restrictionToValueExpr (entityRef : ResolvedEntityRef) (newTableName : SQL.TableName) (restr : CompiledRestriction) : SQL.ValueExpr =
    match restr.From with
    | SQL.FTable table ->
        // We can make expression simpler in this case, just using `WHERE`.
        // `(table.Extra :?> RealEntityAnnotation).RealEntity` is always `rootRef` here, see above.
        let renamesMap = Map.singleton (fromTableName table) newTableName
        renameAllValueExprTables renamesMap restr.Where
    | _ ->
        let select =
            { SQL.emptySingleSelectExpr with
                  Columns = [| SQL.SCExpr (None, SQL.VEColumn { Table = Some <| compileRenamedResolvedEntityRef entityRef; Name = sqlFunId }) |]
                  From = Some restr.From
                  Where = Some restr.Where
            }
        let subexpr = { CTEs = None; Extra = null; Tree = SQL.SSelect select } : SQL.SelectExpr
        let idColumn = SQL.VEColumn { Table = Some { Schema = None; Name = newTableName }; Name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)
