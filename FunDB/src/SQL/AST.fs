module internal FunWithFlags.FunDB.SQL.AST

open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.Utils

type ColumnName = string
type TableName = string

type Table =
    { schema: string option;
      name: TableName;
    }
    with
        override this.ToString () =
            match this.schema with
                | None -> renderSqlName this.name
                | Some(schema) -> sprintf "%s.%s" (renderSqlName schema) (renderSqlName this.name)

type Column =
    { table: Table;
      name: ColumnName;
    }
    with
        override this.ToString () = sprintf "%s.%s" (this.table.ToString ()) (renderSqlName this.name)

type SortOrder = Asc | Desc

type JoinType = Inner | Left | Right | Full

and FromExpr =
    | FTable of Table
    | FJoin of JoinType * FromExpr * FromExpr * ValueExpr<Column>
    | FSubExpr of SelectExpr * TableName

and SelectedColumn =
    | SCColumn of Column
    | SCExpr of ValueExpr<Column> * TableName

and SelectExpr =
    { columns: SelectedColumn array;
      from: FromExpr;
      where: ValueExpr<Column> option;
      orderBy: (Column * SortOrder) array;
      limit: int option;
      offset: int option;
      }

type InsertExpr =
    { name: Table;
      columns: string array;
      values: Value array array;
    }
