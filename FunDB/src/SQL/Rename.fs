module FunWithFlags.FunDB.SQL.Rename

open FunWithFlags.FunDB.SQL.AST

type RenamesMap = Map<TableName, TableName>

type private NaiveRenamer (renamesMap : RenamesMap) =
    let rec renameSelectTreeExpr : SelectTreeExpr -> SelectTreeExpr = function
        | SSelect query -> SSelect <| renameSingleSelectExpr query
        | SSetOp setOp ->
            SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = renameSelectExpr setOp.A
                  B = renameSelectExpr setOp.B
                  OrderLimit = renameOrderLimitClause setOp.OrderLimit
                }
        | SValues values -> SValues values

    and renameCommonTableExpr (cte : CommonTableExpr) =
        { Fields = cte.Fields
          Materialized = cte.Materialized
          Expr = renameDataExpr cte.Expr
        }

    and renameCommonTableExprs (ctes : CommonTableExprs) =
        { Recursive = ctes.Recursive
          Exprs = Array.map (fun (name, expr) -> (name, renameCommonTableExpr expr)) ctes.Exprs
        }

    and renameSelectExpr (select : SelectExpr) : SelectExpr =
        { CTEs = Option.map renameCommonTableExprs select.CTEs
          Tree = renameSelectTreeExpr select.Tree
          Extra = select.Extra
        }

    and renameSingleSelectExpr (query : SingleSelectExpr) : SingleSelectExpr =
        { Columns = Array.map renameSelectedColumn query.Columns
          From = Option.map renameFromExpr query.From
          Where = Option.map renameValueExpr query.Where
          GroupBy = Array.map renameValueExpr query.GroupBy
          OrderLimit = renameOrderLimitClause query.OrderLimit
          Locking = query.Locking
          Extra = query.Extra
        }

    and renameOrderColumn (col : OrderColumn) : OrderColumn =
        { Expr = renameValueExpr col.Expr
          Order = col.Order
          Nulls = col.Nulls
        }

    and renameOrderLimitClause (clause : OrderLimitClause) : OrderLimitClause =
        { Limit = Option.map renameValueExpr clause.Limit
          Offset = Option.map renameValueExpr clause.Offset
          OrderBy = Array.map renameOrderColumn clause.OrderBy
        }

    and renameSelectedColumn : SelectedColumn -> SelectedColumn = function
        | SCAll _ -> failwith "Unexpected SELECT *"
        | SCExpr (name, expr) -> SCExpr (name, renameValueExpr expr)

    and renameValueExpr =
        let mapColumn : ColumnRef -> ColumnRef = function
            | { Table = Some { Schema = None; Name = tableName }; Name = colName } as ref ->
                match Map.tryFind tableName renamesMap with
                | None -> ref
                | Some newName -> { Table = Some { Schema = None; Name = newName }; Name = colName }
            | ref -> ref
        mapValueExpr
            { idValueExprMapper with
                ColumnReference = mapColumn
                Query = renameSelectExpr
            }

    and renameTableExpr : TableExpr -> TableExpr = function
        | TESelect subsel -> TESelect <| renameSelectExpr subsel
        | TEFunc (name, args) -> TEFunc (name, Array.map renameValueExpr args)

    and renameFromTableExpr (expr : FromTableExpr) : FromTableExpr =
        { Expression = renameTableExpr expr.Expression
          Alias = expr.Alias
          Lateral = expr.Lateral
        }

    and renameFromExpr : FromExpr -> FromExpr = function
        | FTable fTable -> FTable fTable
        | FTableExpr expr -> FTableExpr <| renameFromTableExpr expr
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = renameFromExpr join.A
                  B = renameFromExpr join.B
                  Condition = renameValueExpr join.Condition
                }

    and renameInsertValue = function
        | IVDefault -> IVDefault
        | IVExpr expr -> IVExpr <| renameValueExpr expr

    and renameUpdateAssignExpr = function
        | UAESet (name, expr) -> UAESet (name, renameInsertValue expr)
        | UAESelect (cols, select) -> UAESelect (cols, renameSelectExpr select)

    and renameUpdateConflictAction (update : UpdateConflictAction) : UpdateConflictAction =
        { Assignments = Array.map renameUpdateAssignExpr update.Assignments
          Where = Option.map renameValueExpr update.Where
        }

    and renameConflictExpr (conflict : OnConflictExpr) : OnConflictExpr =
        let action =
            match conflict.Action with
            | CANothing -> CANothing
            | CAUpdate update -> CAUpdate <| renameUpdateConflictAction update
        { Target = conflict.Target
          Action = action
        }

    and renameInsertExpr (query : InsertExpr) : InsertExpr =
        let source =
            match query.Source with
            | ISDefaultValues -> ISDefaultValues
            | ISSelect expr -> ISSelect <| renameSelectExpr expr
            | ISValues vals -> ISValues <| Array.map (Array.map renameInsertValue) vals
        // INSERT permissions are checked when permissions are applied; no need to add any checks here.
        { CTEs = Option.map renameCommonTableExprs query.CTEs
          Table = query.Table
          Columns = query.Columns
          Source = source
          Returning = query.Returning
          OnConflict = Option.map renameConflictExpr query.OnConflict
          Extra = query.Extra
        }

    and renameUpdateExpr (query : UpdateExpr) : UpdateExpr =
        { CTEs = Option.map renameCommonTableExprs query.CTEs
          Table = query.Table
          Assignments = Array.map renameUpdateAssignExpr query.Assignments
          From = Option.map renameFromExpr query.From
          Where = Option.map renameValueExpr query.Where
          Returning = query.Returning
          Extra = query.Extra
        }

    and renameDeleteExpr (query : DeleteExpr) : DeleteExpr =
        { CTEs = Option.map renameCommonTableExprs query.CTEs
          Table = query.Table
          Using = Option.map renameFromExpr query.Using
          Where = Option.map renameValueExpr query.Where
          Returning = query.Returning
          Extra = query.Extra
        }

    and renameDataExpr = function
        | DESelect expr -> DESelect <| renameSelectExpr expr
        | DEInsert expr -> DEInsert <| renameInsertExpr expr
        | DEUpdate expr -> DEUpdate <| renameUpdateExpr expr
        | DEDelete expr -> DEDelete <| renameDeleteExpr expr

    member this.RenameValueExpr expr = renameValueExpr expr

// This is called "naive" because it doesn't take aliasing into account. So if a table
// has been bound under the renamed name in an inner SELECT, it will still be renamed.
// Use with caution!
let naiveRenameTablesExpr (renamesMap : RenamesMap) (expr : ValueExpr) =
    let renamer = NaiveRenamer (renamesMap)
    renamer.RenameValueExpr expr
