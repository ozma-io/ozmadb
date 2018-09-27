module FunWithFlags.FunDB.SQL.AST

// SQL module does not attempt to restrict SQL in any way apart from type safety for values (explicit strings, integers etc.).

open System
open System.Globalization
open System.Runtime.InteropServices

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils

type SQLName = SQLName of string
    with
        override this.ToString () =
            match this with
                | SQLName name -> name

        member this.ToSQLString () =
            match this with
                | SQLName c -> renderSqlName c

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type SchemaName = SQLName
type TableName = SQLName
type ColumnName = SQLName
type ConstraintName = SQLName
type SequenceName = SQLName

// Values

type SchemaObject =
    { schema : SchemaName option
      name : SQLName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () = 
            match this.schema with
                | None -> this.name.ToSQLString()
                | Some schema -> sprintf "%s.%s" (schema.ToSQLString()) (this.name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type TableRef = SchemaObject

type ResolvedColumnRef =
    { table : TableRef
      name : ColumnName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            sprintf "%s.%s" (this.table.ToSQLString()) (this.name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ColumnRef =
    { maybeTable : TableRef option
      name : ColumnName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this.maybeTable with
                | None -> this.name.ToSQLString()
                | Some entity -> sprintf "%s.%s" (entity.ToSQLString()) (this.name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ArrayValue<'t> =
    | AVValue of 't
    | AVArray of ArrayValue<'t> array
    | AVNull

and ValueArray<'t> = ArrayValue<'t> array

let rec mapArrayValue (func : 'a -> 'b) : ArrayValue<'a> -> ArrayValue<'b> = function
    | AVValue a -> AVValue (func a)
    | AVArray vals -> AVArray (mapValueArray func vals)
    | AVNull -> AVNull

and mapValueArray (func : 'a -> 'b) (vals : ValueArray<'a>) : ValueArray<'b> =
    Array.map (mapArrayValue func) vals

type Value =
    | VInt of int
    | VString of string
    | VRegclass of SchemaObject
    | VBool of bool
    | VDateTime of DateTime
    | VDate of DateTime
    | VIntArray of ValueArray<int>
    | VStringArray of ValueArray<string>
    | VBoolArray of ValueArray<bool>
    | VDateTimeArray of ValueArray<DateTime>
    | VDateArray of ValueArray<DateTime>
    | VRegclassArray of ValueArray<SchemaObject>
    | VNull
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let renderArray func typeName arr =
                let rec renderArrayInsides arr =
                    let renderValue = function
                        | AVValue v -> func v
                        | AVArray vals -> renderArrayInsides vals
                        | AVNull -> "NULL"
                    arr |> Seq.map renderValue |> String.concat ", " |> sprintf "{%s}"
                sprintf "E%s :: %s[]" (renderArrayInsides arr |> renderSqlString) typeName

            match this with
                | VInt i -> renderSqlInt i
                | VString s -> sprintf "E%s" (renderSqlString s)
                | VRegclass rc -> sprintf "E%s :: regclass" (rc.ToSQLString() |> renderSqlString)
                | VBool b -> renderSqlBool b
                | VDateTime dt -> sprintf "%s :: timestamp" (dt |> renderSqlDateTime |> renderSqlString)
                | VDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
                | VIntArray vals -> renderArray renderSqlInt "int4" vals
                | VStringArray vals -> renderArray escapeDoubleQuotes "text" vals
                | VBoolArray vals -> renderArray renderSqlBool "bool" vals
                | VDateTimeArray vals -> renderArray (renderSqlDateTime >> escapeDoubleQuotes) "timestamp" vals
                | VDateArray vals -> renderArray (renderSqlDate >> escapeDoubleQuotes) "date" vals
                | VRegclassArray vals -> renderArray (fun (x : SchemaObject) -> x.ToSQLString()) "regclass" vals
                | VNull -> "NULL"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

// Simplified list of PostgreSQL types. Other types are casted to those.
// Used when interpreting query results and for compiling FunQL.
type SimpleType =
    | STInt
    | STString
    | STBool
    | STDateTime
    | STDate
    | STRegclass
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | STInt -> "int4"
                | STString -> "text"
                | STBool -> "bool"
                | STDateTime -> "timestamp"
                | STDate -> "date"
                | STRegclass -> "regclass"

        member this.ToSQLName () = SQLName (this.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

// Find the closest simple type to a given.
let findSimpleType (str : SQLName) : SimpleType option =
    match str.ToString() with
        | "int4" -> Some STInt
        | "integer" -> Some STInt
        | "text" -> Some STString
        | "varchar" -> Some STString
        | "character varying" -> Some STString
        | "bool" -> Some STBool
        | "boolean" -> Some STBool
        | "timestamp" -> Some STDateTime
        | "date" -> Some STDate
        | "regclass" -> Some STRegclass
        | _ -> None

type ValueType<'t> when 't :> ISQLString =
    | VTScalar of 't
    | VTArray of 't
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | VTScalar scalar -> scalar.ToSQLString()
                | VTArray scalar -> sprintf "%s[]" (scalar.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

let mapValueType (func : 'a -> 'b) : ValueType<'a> -> ValueType<'b> = function
    | VTScalar a -> VTScalar (func a)
    | VTArray a -> VTArray (func a)

type DBValueType = ValueType<SQLName>
type SimpleValueType = ValueType<SimpleType>

// Parameters go in same order they go in SQL commands (e.g. VECast (value, type) because "foo :: bar").
type ValueExpr<'f> when 'f :> ISQLString =
    | VEValue of Value
    | VEColumn of 'f
    | VEPlaceholder of int
    | VENot of ValueExpr<'f>
    | VEAnd of ValueExpr<'f> * ValueExpr<'f>
    | VEOr of ValueExpr<'f> * ValueExpr<'f>
    | VEConcat of ValueExpr<'f> * ValueExpr<'f>
    | VEEq of ValueExpr<'f> * ValueExpr<'f>
    | VENotEq of ValueExpr<'f> * ValueExpr<'f>
    | VELike of ValueExpr<'f> * ValueExpr<'f>
    | VENotLike of ValueExpr<'f> * ValueExpr<'f>
    | VELess of ValueExpr<'f> * ValueExpr<'f>
    | VELessEq of ValueExpr<'f> * ValueExpr<'f>
    | VEGreater of ValueExpr<'f> * ValueExpr<'f>
    | VEGreaterEq of ValueExpr<'f> * ValueExpr<'f>
    | VEIn of ValueExpr<'f> * (ValueExpr<'f> array)
    | VENotIn of ValueExpr<'f> * (ValueExpr<'f> array)
    | VEIsNull of ValueExpr<'f>
    | VEIsNotNull of ValueExpr<'f>
    | VEFunc of SQLName * (ValueExpr<'f> array)
    | VECast of ValueExpr<'f> * DBValueType
    with
        override this.ToString () = this.ToSQLString()
        
        member this.ToSQLString () =
            match this with
                | VEValue v -> v.ToSQLString()
                | VEColumn col -> (col :> ISQLString).ToSQLString()
                // Npgsql uses @name to denote parameters. We use integers
                // because Npgsql's parser is not robust enough to handle arbitrary
                // FunQL argument names.
                | VEPlaceholder i -> sprintf "@%s" (renderSqlInt i)
                | VENot a -> sprintf "NOT (%s)" (a.ToSQLString())
                | VEAnd (a, b) -> sprintf "(%s) AND (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEOr (a, b) -> sprintf "(%s) OR (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEConcat (a, b) -> sprintf "(%s) || (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEEq (a, b) -> sprintf "(%s) = (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VENotEq (a, b) -> sprintf "(%s) <> (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VELike (e, pat) -> sprintf "(%s) LIKE (%s)" (e.ToSQLString()) (pat.ToSQLString())
                | VENotLike (e, pat) -> sprintf "(%s) LIKE (%s)" (e.ToSQLString()) (pat.ToSQLString())
                | VELess (a, b) -> sprintf "(%s) < (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VELessEq (a, b) -> sprintf "(%s) <= (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEGreater (a, b) -> sprintf "(%s) > (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEGreaterEq (a, b) -> sprintf "(%s) >= (%s)" (a.ToSQLString()) (b.ToSQLString())
                | VEIn (e, vals) ->
                    assert (not <| Array.isEmpty vals)
                    sprintf "(%s) IN (%s)" (e.ToSQLString()) (vals |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", ")
                | VENotIn (e, vals) ->
                    assert (not <| Array.isEmpty vals)
                    sprintf "(%s) NOT IN (%s)" (e.ToSQLString()) (vals |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", ")
                | VEIsNull a -> sprintf "(%s) IS NULL" (a.ToSQLString())
                | VEIsNotNull a -> sprintf "(%s) IS NOT NULL" (a.ToSQLString())
                | VEFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map (fun arg -> arg.ToSQLString()) |> String.concat ", ")
                | VECast (e, typ) -> sprintf "(%s) :: %s" (e.ToSQLString()) (typ.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

let mapValueExpr (colFunc : 'a -> 'b) (placeholderFunc : int -> int) : ValueExpr<'a> -> ValueExpr<'b> =
    let rec traverse = function
        | VEValue value -> VEValue value
        | VEColumn c -> VEColumn (colFunc c)
        | VEPlaceholder i -> VEPlaceholder (placeholderFunc i)
        | VENot e -> VENot (traverse e)
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
        | VELike (e, pat) -> VELike (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VEFunc (name,  args) -> VEFunc (name, Array.map traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
    traverse

let iterValueExpr (colFunc : 'a -> unit) (placeholderFunc : int -> unit) : ValueExpr<'a> -> unit =
    let rec traverse = function
        | VEValue value -> ()
        | VEColumn c -> colFunc c
        | VEPlaceholder i -> placeholderFunc i
        | VENot e -> traverse e
        | VEAnd (a, b) -> traverse a; traverse b
        | VEOr (a, b) -> traverse a; traverse b
        | VEConcat (a, b) -> traverse a; traverse b
        | VEEq (a, b) -> traverse a; traverse b
        | VENotEq (a, b) -> traverse a; traverse b
        | VELike (e, pat) -> traverse e; traverse pat
        | VENotLike (e, pat) -> traverse e; traverse pat
        | VELess (a, b) -> traverse a; traverse b
        | VELessEq (a, b) -> traverse a; traverse b
        | VEGreater (a, b) -> traverse a; traverse b
        | VEGreaterEq (a, b) -> traverse a; traverse b
        | VEIn (e, vals) -> traverse e; Array.iter traverse vals
        | VENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | VEIsNull e -> traverse e
        | VEIsNotNull e -> traverse e
        | VEFunc (name,  args) -> Array.iter traverse args
        | VECast (e, typ) -> traverse e
    traverse

type SQLVoid = private SQLVoid of unit with
    interface ISQLString with
        member this.ToSQLString () = failwith "impossible"

type PureValueExpr = ValueExpr<SQLVoid>
type FullValueExpr = ValueExpr<ColumnRef>
type LocalValueExpr = ValueExpr<ColumnName>

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () = this.ToSQLString()
        
        member this.ToSQLString () =
            match this with
                | Asc -> "ASC"
                | Desc -> "DESC"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type JoinType =
    | Inner
    | Left
    | Right
    | Full
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | Inner -> "INNER"
                | Left -> "LEFT"
                | Right -> "RIGHT"
                | Full -> "FULL"
        
        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type FromExpr =
    | FTable of TableRef
    | FJoin of JoinType * FromExpr * FromExpr * FullValueExpr
    | FSubExpr of SQLName * SelectExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | FTable t -> t.ToSQLString()
                | FJoin (joinType, a, b, cond) -> sprintf "%s %s JOIN %s ON %s" (a.ToSQLString()) (joinType.ToSQLString()) (b.ToSQLString()) (cond.ToSQLString())
                | FSubExpr (name, expr) -> sprintf "(%s) AS %s" (expr.ToSQLString()) (name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and SelectedColumn =
    | SCColumn of ColumnName
    | SCExpr of ColumnName * FullValueExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | SCColumn c -> c.ToSQLString()
                | SCExpr (name, expr) -> sprintf "%s AS %s" (expr.ToSQLString()) (name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and FromClause =
    { from : FromExpr
      where : FullValueExpr option
      orderBy : (SortOrder * FullValueExpr) array
      limit : int option
      offset : int option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let whereStr =
                match this.where with
                    | None -> ""
                    | Some cond -> sprintf "WHERE %s" (cond.ToSQLString())
            let orderByStr =
                if Array.isEmpty this.orderBy
                then ""
                else sprintf "ORDER BY %s" (this.orderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (ord.ToSQLString()) (expr.ToSQLString())) |> String.concat ", ")
            let limitStr =
                match this.limit with
                    | Some n -> sprintf "LIMIT %i" n
                    | None -> ""
            let offsetStr =
                match this.offset with
                    | Some n -> sprintf "OFFSET %i" n
                    | None -> ""

            sprintf "FROM %s" (concatWithWhitespaces [this.from.ToSQLString(); whereStr; orderByStr; limitStr; offsetStr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and SelectExpr =
    { columns : SelectedColumn array
      clause : FromClause option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let resultsStr = this.columns |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
            let fromStr =
                match this.clause with
                    | None -> ""
                    | Some clause -> clause.ToSQLString()

            sprintf "SELECT %s" (concatWithWhitespaces [resultsStr; fromStr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type InsertExpr =
    { name : TableRef
      columns : ColumnName array
      values : (PureValueExpr array) array
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let renderInsertValue (values : PureValueExpr array) = 
                values |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"

            assert (not <| Array.isEmpty this.values)
            sprintf "INSERT INTO %s (%s) VALUES %s"
                (this.name.ToSQLString())
                (this.columns |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", ")
                (this.values |> Seq.map renderInsertValue |> String.concat ", ")

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type UpdateExpr =
    { name : TableRef
      columns : Map<ColumnName, LocalValueExpr>
      where : FullValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not <| Map.isEmpty this.columns)

            let valuesExpr = this.columns |> Map.toSeq |> Seq.map (fun (name, expr) -> sprintf "%s = %s" (name.ToSQLString()) (expr.ToSQLString())) |> String.concat ", "
            let condExpr =
                match this.where with
                    | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                    | None -> ""
            sprintf "UPDATE %s SET %s" (this.name.ToSQLString()) (concatWithWhitespaces [valuesExpr; condExpr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type DeleteExpr =
    { name : TableRef
      where : FullValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let condExpr =
                match this.where with
                    | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                    | None -> ""
            sprintf "DELETE FROM %s" (concatWithWhitespaces [this.name.ToSQLString(); condExpr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

// Meta

type ColumnMeta =
    { columnType : DBValueType
      isNullable : bool
      defaultExpr : PureValueExpr option
    }

type ConstraintType =
    | CTUnique
    | CTCheck
    | CTPrimaryKey
    | CTForeignKey

type ConstraintMeta =
    | CMUnique of ColumnName array
    | CMCheck of LocalValueExpr
    | CMPrimaryKey of ColumnName array
    | CMForeignKey of TableRef * ((ColumnName * ColumnName) array)

type TableMeta =
    { columns : Map<ColumnName, ColumnMeta>
    }

let emptyTableMeta =
    { columns = Map.empty
    }

type ObjectMeta =
    | OMTable of TableMeta
    | OMSequence
    | OMConstraint of TableName * ConstraintMeta

type SchemaMeta =
    { objects : Map<SQLName, ObjectMeta>
    }

let emptySchemaMeta =
    { objects = Map.empty
    }

type DatabaseMeta =
    { schemas : Map<SchemaName option, SchemaMeta>
    }

type SchemaOperation =
    | SOCreateSchema of SchemaName
    | SODeleteSchema of SchemaName
    | SOCreateTable of TableRef
    | SODeleteTable of TableRef
    | SOCreateSequence of SchemaObject
    | SODeleteSequence of SchemaObject
    | SOCreateConstraint of SchemaObject * TableName * ConstraintMeta
    | SODeleteConstraint of SchemaObject * TableName
    | SOCreateColumn of ResolvedColumnRef * ColumnMeta
    | SODeleteColumn of ResolvedColumnRef
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
                | SOCreateSchema schema -> sprintf "CREATE SCHEMA %s" (schema.ToSQLString())
                | SODeleteSchema schema -> sprintf "DROP SCHEMA %s" (schema.ToSQLString())
                | SOCreateTable table -> sprintf "CREATE TABLE %s ()" (table.ToSQLString())
                | SODeleteTable table -> sprintf "DROP TABLE %s" (table.ToSQLString())
                | SOCreateSequence seq -> sprintf "CREATE SEQUENCE %s" (seq.ToSQLString())
                | SODeleteSequence seq -> sprintf "DROP SEQUENCE %s" (seq.ToSQLString())
                | SOCreateConstraint (constr, table, pars) ->
                    let constraintStr =
                        match pars with
                            | CMUnique exprs ->
                                assert (not <| Array.isEmpty exprs)
                                exprs |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "UNIQUE (%s)"
                            | CMPrimaryKey cols ->
                                assert (not <| Array.isEmpty cols)
                                cols |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "PRIMARY KEY (%s)"
                            | CMForeignKey (ref, cols) ->
                                let myCols = cols |> Seq.map (fun (name, refName) -> name.ToSQLString()) |> String.concat ", "
                                let refCols = cols |> Seq.map (fun (name, refName) -> refName.ToSQLString()) |> String.concat ", "
                                sprintf "FOREIGN KEY (%s) REFERENCES %s (%s)" myCols (ref.ToSQLString()) refCols
                            | CMCheck expr -> sprintf "CHECK %s" (expr.ToSQLString())
                    sprintf "ALTER TABLE %s ADD CONSTRAINT %s %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString()) constraintStr
                | SODeleteConstraint (constr, table) -> sprintf "ALTER TABLE %s DROP CONSTRAINT %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString())
                | SOCreateColumn (col, pars) ->
                    let notNullStr = if pars.isNullable then "NULL" else "NOT NULL"
                    let defaultStr =
                        match pars.defaultExpr with
                            | None -> ""
                            | Some def -> sprintf "DEFAULT %s" (def.ToSQLString())
                    sprintf "ALTER TABLE %s ADD COLUMN %s %s %s %s" (col.table.ToSQLString()) (col.name.ToSQLString()) (pars.columnType.ToSQLString()) notNullStr defaultStr
                | SODeleteColumn col -> sprintf "ALTER TABLE %s DROP COLUMN %s" (col.table.ToSQLString()) (col.name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

let publicSchema = SQLName "public"
