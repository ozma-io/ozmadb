module OzmaDB.Permissions.Compile

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.Permissions.Resolve
module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Rename

type CompiledRestriction =
    { From : SQL.FromExpr
      Joins : JoinPaths
      Where : SQL.ValueExpr
    }

let private defaultWhere : SQL.ValueExpr = SQL.VEValue (SQL.VBool true)

let private restrictionJoinNamespace = OzmaQLName "restr"

let private restrictionCompilationFlags = { defaultCompilationFlags with SubExprJoinNamespace = restrictionJoinNamespace }

let compileRestriction (layout : Layout) (entityRef : ResolvedEntityRef) (arguments : QueryArguments) (restr : ResolvedFieldExpr) : QueryArguments * CompiledRestriction =
    let entity = layout.FindEntity entityRef |> Option.get
    // We don't want compiler to add type check to the result, because our own typecheck is built into the restriction.
    // Hence, a hack: we pretend to use root entity instead, but add an alias so that expression properly binds.
    let fEntity =
        { fromEntity (relaxEntityRef entity.Root) with
              Alias = Some restrictedEntityRef.Name
        }
    let (info, from) = compileSingleFromExpr restrictionCompilationFlags layout arguments (FEntity fEntity) (Some restr)
    let ret =
        { From = from.From
          Joins = from.Joins
          Where = Option.defaultValue defaultWhere from.Where
        }
    (info.Arguments, ret)

let restrictionToSelect (ref : ResolvedEntityRef) (restr : CompiledRestriction) : SQL.SelectExpr =
    let select =
        { SQL.emptySingleSelectExpr with
              Columns = [| SQL.SCAll (Some restrictedTableRef) |]
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
        SQL.naiveRenameTablesExpr renamesMap restr.Where
    | _ ->
        let select =
            { SQL.emptySingleSelectExpr with
                  Columns = [| SQL.SCExpr (None, SQL.VEColumn { Table = Some restrictedTableRef; Name = sqlFunId }) |]
                  From = Some restr.From
                  Where = Some restr.Where
            }
        let subexpr = { CTEs = None; Extra = null; Tree = SQL.SSelect select } : SQL.SelectExpr
        let idColumn = SQL.VEColumn { Table = Some { Schema = None; Name = newTableName }; Name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)
