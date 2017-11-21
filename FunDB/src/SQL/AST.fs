module internal FunWithFlags.FunDB.SQL.AST

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.Utils

type ColumnName = string
type TableName = string
type SequenceName = string
type ConstraintName = string
type SchemaName = string

type LocalColumn = LocalColumn of string
    with
        override this.ToString () =
            match this with
                | LocalColumn(c) -> renderSqlName c

// Values

type DBObject =
    { schema: string option;
      name: TableName;
    }
    with
        override this.ToString () =
            match this.schema with
                | None -> renderSqlName this.name
                | Some(schema) -> sprintf "%s.%s" (renderSqlName schema) (renderSqlName this.name)

type Table = DBObject

type Column =
    { table: Table;
      name: ColumnName;
    }
    with
        override this.ToString () = sprintf "%s.%s" (this.table.ToString ()) (renderSqlName this.name)

let columnFromLocal (table : Table) (column : LocalColumn) =
    match column with
        | LocalColumn(name) -> { table = table; name = name; }

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

let simpleSelect (columns : string seq) (table : Table) : SelectExpr =
    { columns = columns |> Seq.map (fun x -> SCColumn({ table = table; name = x; })) |> Seq.toArray;
      from = FTable(table);
      where = None;
      orderBy = [||];
      limit = None;
      offset = None;
    }

type InsertExpr =
    { name: Table;
      columns: string array;
      values: ValueExpr<LocalColumn> array array;
    }

type UpdateExpr =
    { name: Table;
      columns: (string * ValueExpr<LocalColumn>) array;
      where: ValueExpr<LocalColumn> option;
    }

type DeleteExpr =
    { name: Table;
      where: ValueExpr<LocalColumn> option;
    }

// Meta

type ColumnMeta =
    { colType: ValueType;
      nullable: bool;
      defaultValue: ValueExpr<LocalColumn> option;
    }

type ConstraintType =
    | CTUnique
    | CTPrimaryKey
    | CTForeignKey

// XXX: Simplified model: no arbitrary expressions in constraints
type ConstraintMeta =
    | CMUnique of Set<LocalColumn>
    | CMPrimaryKey of Set<LocalColumn>
    // XXX: We don't support multi-column foreign keys
    | CMForeignKey of LocalColumn * Column

type TableMeta =
    { columns: Map<LocalColumn, ColumnMeta>;
    }

let emptyTableMeta =
    { columns = Map.empty;
    }

type SchemaMeta =
    { tables: Map<TableName, TableMeta>;
      sequences: Set<SequenceName>;
      constraints: Map<ConstraintName, TableName * ConstraintMeta>;
    }

let emptySchemaMeta =
    { tables = Map.empty;
      sequences = Set.empty;
      constraints = Map.empty;
    }

let mergeSchemaMeta (a : SchemaMeta) (b : SchemaMeta) : SchemaMeta =
    { tables = mapUnionUnique a.tables b.tables;
      sequences = Set.union a.sequences b.sequences;
      constraints = mapUnionUnique a.constraints b.constraints;
    }

type DatabaseMeta = Map<SchemaName, SchemaMeta>

type SchemaOperation =
    | SOCreateSchema of SchemaName
    | SODeleteSchema of SchemaName
    | SOCreateTable of Table
    | SODeleteTable of Table
    | SOCreateSequence of DBObject
    | SODeleteSequence of DBObject
    | SOCreateConstraint of DBObject * TableName * ConstraintMeta
    | SODeleteConstraint of DBObject * TableName
    | SOCreateColumn of Column * ColumnMeta
    | SODeleteColumn of Column
