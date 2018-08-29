module FunWithFlags.FunDB.SQL.AST

open System
open System.Globalization
open System.Runtime.InteropServices

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils

type ColumnName = string
type TableName = string
type SequenceName = string
type ConstraintName = string
type SchemaName = string

type LocalColumn = LocalColumn of ColumnName
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
        override this.ToString () = sprintf "%O.%s" this.table (renderSqlName this.name)

let internal columnFromLocal (table : Table) (column : LocalColumn) =
    match column with
        | LocalColumn(name) -> { table = table; name = name; }

type ValueType =
    | VTInt
    | VTFloat
    | VTString
    | VTBool
    | VTDateTime
    | VTDate
    | VTObject
    with
        override this.ToString () =
            match this with
                | VTInt -> "bigint"
                | VTFloat -> "double precision"
                | VTString -> "text"
                | VTBool -> "bool"
                | VTDateTime -> "timestamp"
                | VTDate -> "date"
                | VTObject -> "regclass"

        member this.IsInt =
            match this with
                | VTInt -> true
                | _ -> false

        member this.IsFloat =
            match this with
                | VTFloat -> true
                | _ -> false

        member this.IsString =
            match this with
                | VTString -> true
                | _ -> false

        member this.IsBool =
            match this with
                | VTBool -> true
                | _ -> false

        member this.IsDateTime =
            match this with
                | VTDateTime -> true
                | _ -> false

        member this.IsDate =
            match this with
                | VTDate -> true
                | _ -> false

        member this.IsObject =
            match this with
                | VTObject -> true
                | _ -> false

let internal parseValueType (str : string) =
    match str.ToLower() with
        | "bigint" -> Some(VTInt)
        | "double precision" -> Some(VTFloat)
        | "text" -> Some(VTString)
        | "boolean" -> Some(VTBool)
        | "timestamp" -> Some(VTDateTime)
        | "timestamp without timezone" -> Some(VTDateTime)
        | "date" -> Some(VTDate)
        | "regclass" -> Some(VTObject)
        | _ -> None

let internal parseCoerceValueType (str : string) =
    match parseValueType str with
        | Some(x) -> Some(x)
        | None ->
            let name = str.ToLower()
            match name with
                | "varchar" -> Some(VTString)
                | "character varying" -> Some(VTString)
                | "bool" -> Some(VTBool)
                | "int2" -> Some(VTInt)
                | "int4" -> Some(VTInt)
                | "int8" -> Some(VTInt)
                | Regex @"character varying\((\d+)\)" [size] -> Some(VTString)
                | _ ->
                    eprintfn "Unknown coerce value type: %s" str
                    None

type ObjectRef = ObjectRef of string array
    with
        override this.ToString () =
            match this with
                | ObjectRef (arr) -> arr |> Array.toSeq |> Seq.map renderSqlName |> String.concat "."

type ColumnRef =
    { tableRef: Table option;
      name: ColumnName;
    } with
        override this.ToString () =
            match this.tableRef with
                | None -> renderSqlName this.name
                | Some(entity) -> sprintf "%O.%s" entity (renderSqlName this.name)

let internal columnFromObjectRef = function
    | ObjectRef [| schema; table; col |] -> Some({ tableRef = Some({ schema = Some(schema); name = table; }); name = col; })
    | ObjectRef [| table; col |] -> Some({ tableRef = Some({ schema = None; name = table; }); name = col; })
    | ObjectRef [| col |] -> Some({ tableRef = None; name = col; })
    | _ -> None

type Value<'o> =
    // XXX: Convert to int64! Possibly add VUInt.
    | VInt of int
    | VFloat of double
    | VString of string
    | VBool of bool
    | VDateTime of DateTime
    | VDate of DateTime
    | VObject of 'o
    | VNull
    with
        override this.ToString () =
            match this with
                | VInt(i) -> i.ToString(CultureInfo.InvariantCulture)
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> renderSqlString s
                | VBool(b) -> renderBool b
                | VDateTime(dt) -> sprintf "(%s :: timestamp)" (dt.ToString("O", CultureInfo.InvariantCulture) |> renderSqlString)
                | VDate(d) -> sprintf "(%s :: date)" (d.ToString("d", CultureInfo.InvariantCulture) |> renderSqlString)
                | VObject(obj) -> sprintf "(%s :: regclass)" (obj.ToString () |> renderSqlString)
                | VNull -> "NULL"

        member this.IsInt =
            match this with
                | VInt(_) -> true
                | _ -> false

        member this.GetInt () =
            match this with
                | VInt(i) -> i
                | _ -> invalidOp "GetInt"

        member this.IsFloat =
            match this with
                | VFloat(_) -> true
                | _ -> false

        member this.GetFloat () =
            match this with
                | VFloat(f) -> f
                | _ -> invalidOp "GetFloat"

        member this.IsString =
            match this with
                | VString(_) -> true
                | _ -> false

        member this.GetString () =
            match this with
                | VString(s) -> s
                | _ -> invalidOp "GetString"

        member this.IsBool =
            match this with
                | VBool(_) -> true
                | _ -> false

        member this.GetBool () =
            match this with
                | VBool(b) -> b
                | _ -> invalidOp "GetBool"

        member this.IsDateTime =
            match this with
                | VDateTime(_) -> true
                | _ -> false

        member this.GetDateTime () =
            match this with
                | VDateTime(dt) -> dt
                | _ -> invalidOp "GetDateTime"

        member this.IsDate =
            match this with
                | VDate(_) -> true
                | _ -> false

        member this.GetDate () =
            match this with
                | VDate(dt) -> dt
                | _ -> invalidOp "GetDate"

        member this.IsObject =
            match this with
                | VObject(_) -> true
                | _ -> false

        member this.GetObject () =
            match this with
                | VObject(obj) -> obj
                | _ -> invalidOp "GetObject"

        member this.IsNull =
            match this with
                | VNull -> true
                | _ -> false

type ParsedValue = Value<string>
type QualifiedValue = Value<ObjectRef>

type ValueExpr<'f, 'o> =
    | VEValue of Value<'o>
    | VEColumn of 'f
    | VENot of ValueExpr<'f, 'o>
    | VEConcat of ValueExpr<'f, 'o> * ValueExpr<'f, 'o>
    | VEEq of ValueExpr<'f, 'o> * ValueExpr<'f, 'o>
    | VEIn of ValueExpr<'f, 'o> * (ValueExpr<'f, 'o> array)
    | VEAnd of ValueExpr<'f, 'o> * ValueExpr<'f, 'o>
    | VEFunc of string * (ValueExpr<'f, 'o> array)
    | VECast of ValueExpr<'f, 'o> * ValueType

type PartialValueExpr = ValueExpr<ColumnRef, string>
type ParsedValueExpr = ValueExpr<ColumnRef, ObjectRef>
type QualifiedValueExpr = ValueExpr<Column, ObjectRef>
type LocalValueExpr = ValueExpr<LocalColumn, ObjectRef>
type PureValueExpr = ValueExpr<Void, ObjectRef>

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () =
            match this with
                | Asc -> "ASC"
                | Desc -> "DESC"

type JoinType =
    | Inner
    | Left
    | Right
    | Full
    with
        override this.ToString () =
            match this with
                | Inner -> "INNER"
                | Left -> "LEFT"
                | Right -> "RIGHT"
                | Full -> "FULL"

and FromExpr =
    | FTable of Table
    | FJoin of JoinType * FromExpr * FromExpr * QualifiedValueExpr
    | FSubExpr of TableName * SelectExpr

and SelectedColumn =
    | SCColumn of Column
    | SCExpr of TableName * QualifiedValueExpr

and SelectExpr =
    { columns: SelectedColumn array;
      from: FromExpr;
      where: QualifiedValueExpr option;
      orderBy: (SortOrder * QualifiedValueExpr) array;
      limit: int option;
      offset: int option;
    }

and EvaluateExpr =
    { values: (TableName * PureValueExpr) array;
    }

let internal simpleSelect (columns : string seq) (table : Table) : SelectExpr =
    { columns = columns |> Seq.map (fun x -> SCColumn({ table = table; name = x; })) |> Seq.toArray;
      from = FTable(table);
      where = None;
      orderBy = [||];
      limit = None;
      offset = None;
    }

type InsertExpr =
    { name: Table;
      columns: ColumnName array;
      values: LocalValueExpr array array;
    }

type UpdateExpr =
    { name: Table;
      columns: (ColumnName * LocalValueExpr) array;
      where: LocalValueExpr option;
    }

type DeleteExpr =
    { name: Table;
      where: LocalValueExpr option;
    }

// Meta

type ColumnMeta =
    { colType: ValueType;
      nullable: bool;
      defaultValue: LocalValueExpr option;
    }

type ConstraintType =
    | CTUnique
    | CTPrimaryKey
    | CTForeignKey

let internal parseConstraintType (str : string) =
    match str.ToUpper() with
        | "UNIQUE" -> Some(CTUnique)
        | "PRIMARY KEY" -> Some(CTPrimaryKey)
        | "FOREIGN KEY" -> Some(CTForeignKey)
        | _ -> None

// XXX: Simplified model: no arbitrary expressions in constraints
type ConstraintMeta =
    | CMUnique of Set<LocalColumn>
    | CMPrimaryKey of Set<LocalColumn>
    // XXX: We don't support multi-column foreign keys
    | CMForeignKey of LocalColumn * Column

type TableMeta =
    { columns: Map<LocalColumn, ColumnMeta>;
    }

let internal emptyTableMeta =
    { columns = Map.empty;
    }

type SchemaMeta =
    { tables: Map<TableName, TableMeta>;
      sequences: Set<SequenceName>;
      constraints: Map<ConstraintName, TableName * ConstraintMeta>;
    }

let internal emptySchemaMeta =
    { tables = Map.empty;
      sequences = Set.empty;
      constraints = Map.empty;
    }

let internal mergeSchemaMeta (a : SchemaMeta) (b : SchemaMeta) : SchemaMeta =
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
