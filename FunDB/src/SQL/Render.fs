module internal FunWithFlags.FunDB.SQL.Render

open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Utils

let renderTable (table : Table) =
    match table.schema with
        | None -> renderSqlName table.name
        | Some(schema) -> sprintf "%s.%s" (renderSqlName schema) (renderSqlName table.name)

let renderSortOrder = function
    | Asc -> "ASC"
    | Desc -> "DESC"

let renderJoinType = function
    | Inner -> "INNER"
    | Left -> "LEFT"
    | Right -> "RIGHT"
    | Full -> "FULL"

let renderColumn (column : Column) = sprintf "%s.%s" (renderTable column.table) (renderSqlName column.name)

let renderBool = function
    | true -> "TRUE"
    | false -> "FALSE"

let rec renderSelect (expr : SelectExpr) : string =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderWhere c
            | None -> ""
    let orderExpr =
        if Array.isEmpty expr.orderBy
        then ""
        else sprintf "ORDER BY %s" (Array.map (fun (fexpr, sord) -> sprintf "%s %s" (renderColumn fexpr) (renderSortOrder sord)) expr.orderBy |> String.concat ", ")
    let limitExpr =
        match expr.limit with
            | Some(n) -> sprintf "LIMIT %i" n
            | None -> ""
    let offsetExpr =
        match expr.offset with
            | Some(n) -> sprintf "OFFSET %i" n
            | None -> ""
    sprintf "SELECT %s FROM %s %s %s %s %s"
        (Array.map renderSelectedColumn expr.columns |> String.concat ", ")
        (renderFrom expr.from)
        condExpr
        orderExpr
        limitExpr
        offsetExpr

and renderSelectedColumn = function
    | SCColumn(col) -> renderColumn col
    | SCExpr(expr, name) -> sprintf "%s AS %s" (renderSelectExpr expr) (renderSqlName name)

and renderSelectExpr = function
    | CColumn(col) -> renderColumn col

and renderFrom = function
    | FTable(table) -> renderTable table
    | FJoin(typ, a, b, on) -> sprintf "(%s %s JOIN %s ON %s)" (renderFrom a) (renderJoinType typ) (renderFrom b) (renderWhere on)
    | FSubExpr(sel, name) -> sprintf "(%s) AS %s" (renderSelect sel) (renderSqlName name)

and renderWhere = function
    | WColumn(col) -> renderColumn col
    | WInt(i) -> i.ToString()
    // FIXME
    | WFloat(f) -> invalidOp "Not supported"
    | WString(str) -> renderSqlString str
    | WBool(b) -> renderBool b
    | WEq(a, b) -> sprintf "(%s) = (%s)" (renderWhere a) (renderWhere b)
    | WAnd(a, b) -> sprintf "(%s) AND (%s)" (renderWhere a) (renderWhere b)
    | WNull -> "NULL"

let renderInsertValue values = 
    values |> Array.map (fun v -> v.ToString ()) |> String.concat ", " |> sprintf "(%s)"

let renderInsert (expr : InsertExpr) = 
    sprintf "INSERT INTO %s (%s) VALUES %s"
        (renderTable expr.name)
        (expr.columns |> Array.map renderSqlName |> String.concat ", ")
        (expr.values |> Array.map renderInsertValue |> String.concat ", ")
