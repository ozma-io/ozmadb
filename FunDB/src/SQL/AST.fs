module internal FunWithFlags.FunDB.SQL.AST

open FunWithFlags.FunDB.SQL.Value

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

// FIXME: Split values away
type WhereExpr =
    | WColumn of Column
    | WInt of int
    | WString of string
    | WBool of bool
    | WFloat of float
    | WNull
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

type InsertExpr =
    { name: Table;
      columns: string array;
      values: Value array array;
    }
