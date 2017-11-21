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
    | VECast(a, typ) -> sprintf "(%s) :: %s" (renderValueExpr a) (typ.ToString ())

let rec renderSelect (expr : SelectExpr) : string =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    let orderExpr =
        if Array.isEmpty expr.orderBy
        then ""
        else sprintf "ORDER BY %s" (expr.orderBy |> Seq.map (fun (fexpr, sord) -> sprintf "%s %s" (fexpr.ToString ()) (sord.ToString ())) |> String.concat ", ")
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
    | SCExpr(expr, name) -> sprintf "%s AS %s" (renderValueExpr expr) (renderSqlName name)

and renderFrom = function
    | FTable(table) -> table.ToString ()
    | FJoin(typ, a, b, on) -> sprintf "(%s %s JOIN %s ON %s)" (renderFrom a) (typ.ToString ()) (renderFrom b) (renderValueExpr on)
    | FSubExpr(sel, name) -> sprintf "(%s) AS %s" (renderSelect sel) (renderSqlName name)

let renderInsertValue values = 
    values |> Seq.map renderValueExpr |> String.concat ", " |> sprintf "(%s)"

let renderInsert (expr : InsertExpr) = 
    sprintf "INSERT INTO %s (%s) VALUES %s"
        (expr.name.ToString ())
        (expr.columns |> Seq.map renderSqlName |> String.concat ", ")
        (expr.values |> Seq.map renderInsertValue |> String.concat ", ")

let renderUpdate (expr : UpdateExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "UPDATE %s SET %s %s"
        (expr.name.ToString ())
        (expr.columns |> Seq.map (fun (name, expr) -> sprintf "%s = %s" (renderSqlName name) (renderValueExpr expr)) |> String.concat ", ")
        condExpr

let renderDelete (expr : DeleteExpr) =
    let condExpr =
        match expr.where with
            | Some(c) -> sprintf "WHERE %s" <| renderValueExpr c
            | None -> ""
    sprintf "DELETE FROM %s %s"
        (expr.name.ToString ())
        condExpr

let renderSchemaOperation = function
    | SOCreateSchema(schema) -> sprintf "CREATE SCHEMA %s" (renderSqlName schema)
    | SODeleteSchema(schema) -> sprintf "DROP SCHEMA %s" (renderSqlName schema)
    | SOCreateTable(table) -> sprintf "CREATE TABLE %s ()" (table.ToString ())
    | SODeleteTable(table) -> sprintf "DROP TABLE %s" (table.ToString ())
    | SOCreateSequence(seq) -> sprintf "CREATE SEQUENCE %s" (seq.ToString ())
    | SODeleteSequence(seq) -> sprintf "DROP SEQUENCE %s" (seq.ToString ())
    | SOCreateConstraint(constr, table, pars) ->
        let constraintStr =
            match pars with
                | CMUnique(cols) -> cols |> Seq.map (fun x -> x.ToString ()) |> String.concat ", " |> sprintf "UNIQUE (%s)"
                | CMPrimaryKey(cols) -> cols |> Seq.map (fun x -> x.ToString ()) |> String.concat ", " |> sprintf "PRIMARY KEY (%s)"
                | CMForeignKey(col, rcol) -> sprintf "FOREIGN KEY (%s) REFERENCES %s (%s)" (col.ToString ()) (rcol.table.ToString ()) (renderSqlName rcol.name)
        sprintf "ALTER TABLE %s ADD CONSTRAINT %s %s" ({ constr with name = table }.ToString ()) (renderSqlName constr.name) constraintStr
    | SODeleteConstraint(constr, table) -> sprintf "ALTER TABLE %s DROP CONSTRAINT %s" ({ constr with name = table }.ToString ()) (renderSqlName constr.name)
    | SOCreateColumn(col, pars) ->
        let notNullStr = if pars.nullable then "NULL" else "NOT NULL"
        let defaultStr =
            match pars.defaultValue with
                | None -> ""
                | Some(def) -> sprintf "DEFAULT %s" (renderValueExpr def)
        sprintf "ALTER TABLE %s ADD COLUMN %s %s %s %s" (col.table.ToString ()) (renderSqlName col.name) (pars.colType.ToString ()) notNullStr defaultStr
    | SODeleteColumn(col) -> sprintf "ALTER TABLE %s DROP COLUMN %s" (col.table.ToString ()) (renderSqlName col.name)
