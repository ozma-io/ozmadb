module internal FunWithFlags.FunDB.SQL.Render

open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST

let renderBool = function
    | true -> "TRUE"
    | false -> "FALSE"

let rec renderValueExpr = function
    | VEValue(v) -> v.ToString ()
    | VEColumn(col) -> col.ToString ()
    | VENot(a) -> sprintf "NOT (%s)" (renderValueExpr a)
    | VEConcat(a, b) -> sprintf "(%s) || (%s)" (renderValueExpr a) (renderValueExpr b)
    | VEEq(a, b) -> sprintf "(%s) = (%s)" (renderValueExpr a) (renderValueExpr b)
    | VEIn(a, arr) ->
        if arr.Length = 0
        then "FALSE" // Note: empty IN sets are forbidden, but it's the same
        else sprintf "(%s) IN (%s)" (renderValueExpr a) (arr |> Seq.map renderValueExpr |> String.concat ", ")
    | VEAnd(a, b) -> sprintf "(%s) AND (%s)" (renderValueExpr a) (renderValueExpr b)
    | VEFunc(name, args) -> sprintf "%s(%s)" (renderSqlName name) (args |> Seq.map renderValueExpr |> String.concat ", ")
    | VECast(a, typ) -> sprintf "(%s) :: %O" (renderValueExpr a) typ

let rec renderSelect (expr : SelectExpr) : string =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    let orderExpr =
        if Array.isEmpty expr.orderBy
        then ""
        else sprintf "ORDER BY %s" (expr.orderBy |> Seq.map (fun (sord, fexpr) -> sprintf "%s %O" (renderValueExpr fexpr) sord) |> String.concat ", ")
    let limitExpr =
        match expr.limit with
            | Some(n) -> sprintf "LIMIT %i" n
            | None -> ""
    let offsetExpr =
        match expr.offset with
            | Some(n) -> sprintf "OFFSET %i" n
            | None -> ""
    sprintf "SELECT %s FROM %s %s %s %s %s"
        (expr.columns |> Seq.map renderSelectedColumn |> String.concat ", ")
        (renderFrom expr.from)
        condExpr
        orderExpr
        limitExpr
        offsetExpr

and renderSelectedColumn = function
    | SCColumn(col) -> col.ToString ()
    | SCExpr(name, expr) -> sprintf "%s AS %s" (renderValueExpr expr) (renderSqlName name)

and renderFrom = function
    | FTable(table) -> table.ToString ()
    | FJoin(typ, a, b, on) -> sprintf "(%s %O JOIN %s ON %s)" (renderFrom a) typ (renderFrom b) (renderValueExpr on)
    | FSubExpr(name, sel) -> sprintf "(%s) AS %s" (renderSelect sel) (renderSqlName name)

let renderInsertValue values = 
    values |> Seq.map renderValueExpr |> String.concat ", " |> sprintf "(%s)"

let renderInsert (expr : InsertExpr) = 
    sprintf "INSERT INTO %O (%s) VALUES %s"
        expr.name
        (expr.columns |> Seq.map renderSqlName |> String.concat ", ")
        (expr.values |> Seq.map renderInsertValue |> String.concat ", ")

let renderUpdate (expr : UpdateExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "UPDATE %O SET %s %s"
        expr.name
        (expr.columns |> Seq.map (fun (name, expr) -> sprintf "%s = %s" (renderSqlName name) (renderValueExpr expr)) |> String.concat ", ")
        condExpr

let renderDelete (expr : DeleteExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "DELETE FROM %O %s"
        expr.name
        condExpr

let renderSchemaOperation = function
    | SOCreateSchema(schema) -> sprintf "CREATE SCHEMA %s" (renderSqlName schema)
    | SODeleteSchema(schema) -> sprintf "DROP SCHEMA %s" (renderSqlName schema)
    | SOCreateTable(table) -> sprintf "CREATE TABLE %O ()" table
    | SODeleteTable(table) -> sprintf "DROP TABLE %O" table
    | SOCreateSequence(seq) -> sprintf "CREATE SEQUENCE %O" seq
    | SODeleteSequence(seq) -> sprintf "DROP SEQUENCE %O" seq
    | SOCreateConstraint(constr, table, pars) ->
        let constraintStr =
            match pars with
                | CMUnique(cols) -> cols |> Seq.map (fun x -> x.ToString ()) |> String.concat ", " |> sprintf "UNIQUE (%s)"
                | CMPrimaryKey(cols) -> cols |> Seq.map (fun x -> x.ToString ()) |> String.concat ", " |> sprintf "PRIMARY KEY (%s)"
                | CMForeignKey(col, rcol) -> sprintf "FOREIGN KEY (%O) REFERENCES %O (%s)" col rcol.table (renderSqlName rcol.name)
        sprintf "ALTER TABLE %O ADD CONSTRAINT %s %s" { constr with name = table } (renderSqlName constr.name) constraintStr
    | SODeleteConstraint(constr, table) -> sprintf "ALTER TABLE %O DROP CONSTRAINT %s" { constr with name = table } (renderSqlName constr.name)
    | SOCreateColumn(col, pars) ->
        let notNullStr = if pars.nullable then "NULL" else "NOT NULL"
        let defaultStr =
            match pars.defaultValue with
                | None -> ""
                | Some(def) -> sprintf "DEFAULT %s" (renderValueExpr def)
        sprintf "ALTER TABLE %O ADD COLUMN %s %O %s %s" col.table (renderSqlName col.name) pars.colType notNullStr defaultStr
    | SODeleteColumn(col) -> sprintf "ALTER TABLE %O DROP COLUMN %s" col.table (renderSqlName col.name)

let renderEvaluate (expr : EvaluateExpr) =
    sprintf "SELECT %s"
        (expr.values |> Seq.map (fun (name, expr) -> sprintf "%s AS %s" (renderValueExpr expr) (renderSqlName name))  |> String.concat ", ")
