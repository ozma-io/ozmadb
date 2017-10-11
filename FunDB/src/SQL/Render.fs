module internal FunWithFlags.FunDB.SQL.Render

open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.Utils

let renderSortOrder = function
    | Asc -> "ASC"
    | Desc -> "DESC"

let renderJoinType = function
    | Inner -> "INNER"
    | Left -> "LEFT"
    | Right -> "RIGHT"
    | Full -> "FULL"

let renderBool = function
    | true -> "TRUE"
    | false -> "FALSE"

let rec renderValueExpr = function
    | WValue(v) -> v.ToString ()
    | WColumn(col) -> col.ToString ()
    | WEq(a, b) -> sprintf "(%s) = (%s)" (renderValueExpr a) (renderValueExpr b)
    | WAnd(a, b) -> sprintf "(%s) AND (%s)" (renderValueExpr a) (renderValueExpr b)

let rec renderSelect (expr : SelectExpr) : string =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    let orderExpr =
        if Array.isEmpty expr.orderBy
        then ""
        else sprintf "ORDER BY %s" (Array.map (fun (fexpr, sord) -> sprintf "%s %s" (fexpr.ToString ()) (renderSortOrder sord)) expr.orderBy |> String.concat ", ")
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
    | SCColumn(col) -> col.ToString ()
    | SCExpr(expr, name) -> sprintf "%s AS %s" (renderValueExpr expr) (renderSqlName name)

and renderFrom = function
    | FTable(table) -> table.ToString ()
    | FJoin(typ, a, b, on) -> sprintf "(%s %s JOIN %s ON %s)" (renderFrom a) (renderJoinType typ) (renderFrom b) (renderValueExpr on)
    | FSubExpr(sel, name) -> sprintf "(%s) AS %s" (renderSelect sel) (renderSqlName name)

let renderInsertValue values = 
    values |> Array.map renderValueExpr |> String.concat ", " |> sprintf "(%s)"

let renderInsert (expr : InsertExpr) = 
    sprintf "INSERT INTO %s (%s) VALUES %s"
        (expr.name.ToString ())
        (expr.columns |> Array.map renderSqlName |> String.concat ", ")
        (expr.values |> Array.map renderInsertValue |> String.concat ", ")

let renderUpdate (expr : UpdateExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "UPDATE %s SET %s %s"
        (expr.name.ToString ())
        (Array.map (fun (name, expr) -> sprintf "%s = %s" (renderSqlName name) (renderValueExpr expr)) expr.columns |> String.concat ", ")
        condExpr

let renderDelete (expr : DeleteExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "DELETE FROM %s %s"
        (expr.name.ToString ())
        condExpr
