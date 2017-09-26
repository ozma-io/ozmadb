module internal FunWithFlags.FunDB.Query

open System
open Npgsql

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Escape

module AST =
    type ColumnName = string
    type TableName = string

    type Table =
        { schema: string option;
          name: TableName;
        }

    type Column =
        { table: Table;
          name: ColumnName;
        }

    type SortOrder = Asc | Desc

    type JoinType = Inner | Left | Right | Full

    type WhereExpr =
        | WColumn of Column
        | WInt of int
        | WString of string
        | WBool of bool
        | WEq of WhereExpr * WhereExpr
        | WAnd of WhereExpr * WhereExpr

    and FromExpr =
        | FTable of Table
        | FJoin of JoinType * FromExpr * FromExpr * WhereExpr
        | FSubExpr of SelectExpr * TableName

    and SelectedColumn =
        | SCColumn of Column
        | SCExpr of ColumnExpr * TableName
    
    and ColumnExpr =
        | CColumn of Column

    // FIXME: convert to arrays
    and SelectExpr =
        { columns: SelectedColumn array;
          from: FromExpr;
          where: WhereExpr option;
          orderBy: (Column * SortOrder) array;
          limit: int option;
          offset: int option;
          }


module Render =
    open AST

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
                | Some(c) -> renderWhere c
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
        | WString(str) -> renderSqlString str
        | WBool(b) -> renderBool b
        | WEq(a, b) -> sprintf "(%s) = (%s)" (renderWhere a) (renderWhere b)
        | WAnd(a, b) -> sprintf "(%s) AND (%s)" (renderWhere a) (renderWhere b)

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()

    member this.Query (expr: AST.SelectExpr) : string array array =
        use command = new NpgsqlCommand(Render.renderSelect expr, connection)
        connection.Open()
        try
            use reader = command.ExecuteReader()
            seq { while reader.Read() do
                      yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i].ToString()) |> Seq.toArray
                } |> Seq.toArray
        finally
            connection.Close()
