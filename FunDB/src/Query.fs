namespace FunWithFlags.FunDB

open System
open System.Runtime.InteropServices
open System.Linq.Expressions
open Npgsql
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging

open FunWithFlags.FunCore

type ColumnName = string
type TableName = string

type Table =
    { schema: string option;
      name: TableName;
    } with
        static member System(name: TableName) =
            { schema = None; name = name; }
        static member User(schema: string, name: TableName) =
            { schema = Some schema; name = name; }
        static member FromEntity(entity: Entity) =
            { schema = if entity.SchemaId.HasValue then Some entity.Schema.Name else None;
              name = entity.Name; }


type Column =
    { table: Table;
      name: ColumnName;
    }

type SortOrder = Asc | Desc

type JoinType = Inner | Left | Right | Full

type CondExpr =
    | CColumn of Column
    | CInt of int
    | CString of string
    | CBool of bool
    | CEq of CondExpr * CondExpr

and FromExpr =
    | FTable of Table
    | FJoin of JoinType * FromExpr * FromExpr * CondExpr
    | FSubExpr of SelectExpr * TableName

and FieldExpr = EColumn of Column

and SelectExpr =
    { fields: (FieldExpr * TableName) list;
      from: FromExpr;
      where: CondExpr;
      orderBy: (FieldExpr * SortOrder) list;
      limit: int option;
      offset: int option;
    } with
        static member Single
            (
                entity : Table,
                columns : seq<ColumnName>,
                [<Optional; DefaultParameterValue(null)>] ?where : CondExpr,
                [<Optional; DefaultParameterValue(null)>] ?orderBy : seq<string * SortOrder>,
                [<Optional; DefaultParameterValue(null)>] ?limit : int,
                [<Optional; DefaultParameterValue(null)>] ?offset : int
            ) : SelectExpr =
            { fields = columns |> Seq.map (fun name -> (EColumn { table = entity; name = name; }, name)) |> Seq.toList;
              from = FTable entity;
              where = defaultArg where (CBool true);
              orderBy =
                  match orderBy with
                      | Some(ords) -> ords |> Seq.map (fun (name, ord) -> (EColumn { table = entity; name = name; }, ord)) |> Seq.toList
                      | None -> [];
              limit = limit;
              offset = offset;
            }

module private Render =
    let renderSqlName (str : string) = sprintf "\"%s\"" (str.Replace("\"", "\\\""))
    let renderSqlString (str : string) = sprintf "'%s'" (str.Replace("'", "''"))

    let renderTable (table : Table) =
        match table.schema with
            | None -> renderSqlName table.name
            | Some(schema) -> sprintf "%s.%s" (renderSqlName schema) (renderSqlName table.name)

    let renderSortOrder (sord : SortOrder) =
        match sord with
            | Asc -> "ASC"
            | Desc -> "DESC"

    let renderJoinType jtype =
        match jtype with
            | Inner -> "INNER"
            | Left -> "LEFT"
            | Right -> "RIGHT"
            | Full -> "FULL"

    let renderColumn (column : Column) = sprintf "%s.%s" (renderTable column.table) (renderSqlName column.name)

    let renderBool b =
        match b with
            | true -> "TRUE"
            | false -> "FALSE"

    let rec renderSelect (expr : SelectExpr) : string =
        let orderExpr =
            if List.isEmpty expr.orderBy
            then ""
            else sprintf "ORDER BY %s" (List.map (fun (fexpr, sord) -> sprintf "%s %s" (renderField fexpr) (renderSortOrder sord)) expr.orderBy |> String.concat ", ")
        let limitExpr =
            match expr.limit with
                | Some(n) -> sprintf "LIMIT %i" n
                | None -> ""
        let offsetExpr =
            match expr.offset with
                | Some(n) -> sprintf "OFFSET %i" n
                | None -> ""
        sprintf "SELECT %s FROM %s WHERE %s %s %s %s"
            (List.map (fun (fexpr, name) -> sprintf "%s AS %s" (renderField fexpr) (renderSqlName name)) expr.fields |> String.concat ", ")
            (renderFrom expr.from)
            (renderCond expr.where)
            orderExpr
            limitExpr
            offsetExpr
    and renderField expr =
        match expr with
            | EColumn(col) -> renderColumn col
    and renderFrom expr =
        match expr with
            | FTable(table) -> renderTable table
            | FJoin(typ, a, b, on) -> sprintf "(%s %s JOIN %s ON %s)" (renderFrom a) (renderJoinType typ) (renderFrom b) (renderCond on)
            | FSubExpr(sel, name) -> sprintf "(%s) AS %s" (renderSelect sel) (renderSqlName name)
    and renderCond expr =
        match expr with
            | CColumn(col) -> renderColumn col
            | CInt(i) -> i.ToString()
            | CString(str) -> renderSqlString str
            | CBool(b) -> renderBool b
            | CEq(a, b) -> sprintf "(%s = %s)" (renderCond a) (renderCond b)

type DBQuery(connectionString : string, loggerFactory : ILoggerFactory) =
    let connection = new NpgsqlConnection(connectionString)
    let dbOptions = (new DbContextOptionsBuilder<DatabaseContext>())
                        .UseNpgsql(connectionString)
                        .UseLoggerFactory(loggerFactory)
    let db = new DatabaseContext(dbOptions.Options)

    interface System.IDisposable with
        member this.Dispose() =
            connection.Dispose()
            db.Dispose()

    member this.Database = db

    member this.Query(expr: SelectExpr) : seq<string[]> =
        use command = new NpgsqlCommand(Render.renderSelect expr, connection)
        connection.Open()
        use reader = command.ExecuteReader()
        seq { while reader.Read() do
                  yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i].ToString()) |> Seq.toArray
            } |> Seq.toList |> List.toSeq
