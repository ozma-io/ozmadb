module FunWithFlags.FunDB.SQL.AST

// SQL module does not attempt to restrict SQL in any way apart from type safety for values (explicit strings, integers etc.).

open System
open Newtonsoft.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils

type SQLName = SQLName of string
    with
        override this.ToString () =
            match this with
            | SQLName name -> name

        member this.ToSQLString () =
            match this with
            | SQLName name -> renderSqlName name

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type SQLRawString = SQLRawString of string
    with
        override this.ToString () =
            match this with
            | SQLRawString s -> s

        member this.ToSQLString () =
            match this with
            | SQLRawString s -> s

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
    { table : TableRef option
      name : ColumnName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this.table with
            | None -> this.name.ToSQLString()
            | Some entity -> sprintf "%s.%s" (entity.ToSQLString()) (this.name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ArrayValue<'t> =
    | AVValue of 't
    | AVArray of ArrayValue<'t>[]
    | AVNull

and ValueArray<'t> = ArrayValue<'t> array

let rec mapArrayValue (func : 'a -> 'b) : ArrayValue<'a> -> ArrayValue<'b> = function
    | AVValue a -> AVValue (func a)
    | AVArray vals -> AVArray (mapValueArray func vals)
    | AVNull -> AVNull

and mapValueArray (func : 'a -> 'b) (vals : ValueArray<'a>) : ValueArray<'b> =
    Array.map (mapArrayValue func) vals

type [<JsonConverter(typeof<ValueConverter>)>] Value =
    | VInt of int
    | VDecimal of decimal
    | VString of string
    | VRegclass of SchemaObject
    | VBool of bool
    | VDateTime of DateTimeOffset
    | VDate of DateTimeOffset
    | VIntArray of ValueArray<int>
    | VDecimalArray of ValueArray<decimal>
    | VStringArray of ValueArray<string>
    | VBoolArray of ValueArray<bool>
    | VDateTimeArray of ValueArray<DateTimeOffset>
    | VDateArray of ValueArray<DateTimeOffset>
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
            | VDecimal d -> renderSqlDecimal d
            | VString s -> sprintf "E%s" (renderSqlString s)
            | VRegclass rc -> sprintf "E%s :: regclass" (rc.ToSQLString() |> renderSqlString)
            | VBool b -> renderSqlBool b
            | VDateTime dt -> sprintf "%s :: timestamp" (dt |> renderSqlDateTime |> renderSqlString)
            | VDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
            | VIntArray vals -> renderArray renderSqlInt "int4" vals
            | VDecimalArray vals -> renderArray renderSqlDecimal "decimal" vals
            | VStringArray vals -> renderArray escapeSqlDoubleQuotes "text" vals
            | VBoolArray vals -> renderArray renderSqlBool "bool" vals
            | VDateTimeArray vals -> renderArray (renderSqlDateTime >> escapeSqlDoubleQuotes) "timestamp" vals
            | VDateArray vals -> renderArray (renderSqlDate >> escapeSqlDoubleQuotes) "date" vals
            | VRegclassArray vals -> renderArray (fun (x : SchemaObject) -> x.ToSQLString()) "regclass" vals
            | VNull -> "NULL"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

and ValueConverter () =
    inherit JsonConverter<Value> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : Value =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : Value, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)

        let rec convertValueArray (convertFunc : 'a -> 'b) (vals : ValueArray<'a>) : obj array =
            let convertValue = function
                | AVArray vals -> convertValueArray convertFunc vals :> obj
                | AVValue v -> convertFunc v :> obj
                | AVNull -> null
            Array.map convertValue vals
        let serializeArray (convertFunc : 'a -> 'b) (vals : ValueArray<'a>) : unit =
            serialize <| convertValueArray convertFunc vals

        match value with
        | VInt i -> serialize i
        | VDecimal d -> serialize d
        | VString s -> serialize s
        | VBool b -> serialize b
        | VDateTime dt -> serialize <| dt.ToUnixTimeSeconds()
        | VDate dt -> serialize <| dt.ToUnixTimeSeconds()
        | VRegclass rc -> serialize <| string rc
        | VIntArray vals -> serializeArray id vals
        | VDecimalArray vals -> serializeArray id vals
        | VStringArray vals -> serializeArray id vals
        | VBoolArray vals -> serializeArray id vals
        | VDateTimeArray vals -> serializeArray (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | VDateArray vals -> serializeArray (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | VRegclassArray vals -> serializeArray string vals
        | VNull -> serialize null

// Simplified list of PostgreSQL types. Other types are casted to those.
// Used when interpreting query results and for compiling FunQL.
type [<JsonConverter(typeof<SimpleTypeConverter>)>] SimpleType =
    | STInt
    | STString
    | STDecimal
    | STBool
    | STDateTime
    | STDate
    | STRegclass
    with
        override this.ToString () = this.ToJSONString()

        member this.ToSQLString () =
            match this with
            | STInt -> "int4"
            | STString -> "text"
            | STDecimal -> "numeric"
            | STBool -> "bool"
            | STDateTime -> "timestamp"
            | STDate -> "date"
            | STRegclass -> "regclass"

        member this.ToJSONString () =
            match this with
            | STInt -> "int"
            | STString -> "string"
            | STDecimal -> "numeric"
            | STBool -> "bool"
            | STDateTime -> "datetime"
            | STDate -> "date"
            | STRegclass -> "regclass"

        member this.ToSQLRawString () = SQLRawString (this.ToSQLString ())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and SimpleTypeConverter () =
    inherit JsonConverter<SimpleType> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : SimpleType =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : SimpleType, serializer : JsonSerializer) : unit =
        serializer.Serialize(writer, value.ToJSONString())

// Find the closest simple type to a given.
let findSimpleType (str : SQLRawString) : SimpleType option =
    match str.ToString() with
    | "int4" -> Some STInt
    | "integer" -> Some STInt
    | "text" -> Some STString
    | "decimal" -> Some STDecimal
    | "numeric" -> Some STDecimal
    | "varchar" -> Some STString
    | "character varying" -> Some STString
    | "bool" -> Some STBool
    | "boolean" -> Some STBool
    | "timestamp with time zone" -> Some STDateTime
    | "timestamp without time zone" -> Some STDateTime
    | "timestamp" -> Some STDateTime
    | "date" -> Some STDate
    | "regclass" -> Some STRegclass
    | _ -> None

type [<JsonConverter(typeof<ValueTypeConverter>)>] ValueType<'t> when 't :> ISQLString =
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

and ValueTypeConverter () =
    inherit JsonConverter ()

    override this.CanConvert (objectType : Type) =
        objectType.GetGenericTypeDefinition() = typeof<ValueType<_>>

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, serializer : JsonSerializer) : obj =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let dict =
            match castUnion<ValueType<ISQLString>> value with
            | Some (VTScalar st) -> [("type", st :> obj)]
            | Some (VTArray st) -> [("type", "array" :> obj); ("subtype", st :> obj)]
            | None -> failwith "impossible"
        serializer.Serialize(writer, Map.ofList dict)

let mapValueType (func : 'a -> 'b) : ValueType<'a> -> ValueType<'b> = function
    | VTScalar a -> VTScalar (func a)
    | VTArray a -> VTArray (func a)

type DBValueType = ValueType<SQLRawString>
type SimpleValueType = ValueType<SimpleType>

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

type SetOperation =
    | Union
    | Intersect
    | Except
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | Union -> "UNION"
            | Intersect -> "INTERSECT"
            | Except -> "EXCEPT"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

// Parameters go in same order they go in SQL commands (e.g. VECast (value, type) because "foo :: bar").
type ValueExpr =
    | VEValue of Value
    | VEColumn of ColumnRef
    | VEPlaceholder of int
    | VENot of ValueExpr
    | VEAnd of ValueExpr * ValueExpr
    | VEOr of ValueExpr * ValueExpr
    | VEConcat of ValueExpr * ValueExpr
    | VEEq of ValueExpr * ValueExpr
    | VENotEq of ValueExpr * ValueExpr
    | VELike of ValueExpr * ValueExpr
    | VENotLike of ValueExpr * ValueExpr
    | VELess of ValueExpr * ValueExpr
    | VELessEq of ValueExpr * ValueExpr
    | VEGreater of ValueExpr * ValueExpr
    | VEGreaterEq of ValueExpr * ValueExpr
    | VEIn of ValueExpr * ValueExpr[]
    | VENotIn of ValueExpr * ValueExpr[]
    | VEInQuery of ValueExpr * SelectExpr
    | VENotInQuery of ValueExpr * SelectExpr
    | VEIsNull of ValueExpr
    | VEIsNotNull of ValueExpr
    | VEFunc of SQLName * ValueExpr[]
    | VECast of ValueExpr * DBValueType
    | VECase of (ValueExpr * ValueExpr)[] * (ValueExpr option)
    | VECoalesce of ValueExpr[]
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
            | VEInQuery (e, query) -> sprintf "(%s) IN (%s)" (e.ToSQLString()) (query.ToSQLString())
            | VENotInQuery (e, query) -> sprintf "(%s) NOT IN (%s)" (e.ToSQLString()) (query.ToSQLString())
            | VEIsNull a -> sprintf "(%s) IS NULL" (a.ToSQLString())
            | VEIsNotNull a -> sprintf "(%s) IS NOT NULL" (a.ToSQLString())
            | VEFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map (fun arg -> arg.ToSQLString()) |> String.concat ", ")
            | VECast (e, typ) -> sprintf "(%s) :: %s" (e.ToSQLString()) (typ.ToSQLString())
            | VECase (es, els) ->
                let esStr = es |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToSQLString()) (e.ToSQLString())) |> String.concat " "
                let elsStr =
                    match els with
                    | None -> ""
                    | Some e -> sprintf "ELSE %s" (e.ToSQLString())
                concatWithWhitespaces ["CASE"; esStr; elsStr; "END"]
            | VECoalesce vals ->
                assert (not <| Array.isEmpty vals)
                sprintf "COALESCE(%s)" (vals |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", ")

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

and FromExpr =
    | FTable of TableRef
    | FJoin of JoinType * FromExpr * FromExpr * ValueExpr
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
    | SCAll
    | SCColumn of ColumnRef
    | SCExpr of ColumnName * ValueExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SCAll -> "*"
            | SCColumn c -> c.ToSQLString()
            | SCExpr (name, expr) -> sprintf "%s AS %s" (expr.ToSQLString()) (name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and FromClause =
    { from : FromExpr
      where : ValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let whereStr =
                match this.where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToSQLString())

            sprintf "FROM %s" (concatWithWhitespaces [this.from.ToSQLString(); whereStr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and SingleSelectExpr =
    { columns : SelectedColumn[]
      clause : FromClause option
      orderLimit : OrderLimitClause
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let resultsStr = this.columns |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
            let fromStr =
                match this.clause with
                | None -> ""
                | Some clause -> clause.ToSQLString()

            sprintf "SELECT %s" (concatWithWhitespaces [resultsStr; fromStr; this.orderLimit.ToSQLString()])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and OrderLimitClause =
    { orderBy : (SortOrder * ValueExpr)[]
      limit : ValueExpr option
      offset : ValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
                let orderByStr =
                    if Array.isEmpty this.orderBy
                    then ""
                    else sprintf "ORDER BY %s" (this.orderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (expr.ToSQLString()) (ord.ToSQLString())) |> String.concat ", ")
                let limitStr =
                    match this.limit with
                    | Some e -> sprintf "LIMIT %s" (e.ToSQLString())
                    | None -> ""
                let offsetStr =
                    match this.offset with
                    | Some e -> sprintf "OFFSET %s" (e.ToSQLString())
                    | None -> ""
                concatWithWhitespaces [orderByStr; limitStr; offsetStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and SelectExpr =
    | SSelect of SingleSelectExpr
    | SSetOp of SetOperation * SelectExpr * SelectExpr * OrderLimitClause
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SSelect e -> e.ToSQLString()
            | SSetOp (op, a, b, order) ->
                let setStr = sprintf "(%s) %s (%s)" (a.ToSQLString()) (op.ToSQLString()) (b.ToSQLString())
                concatWithWhitespaces [setStr; order.ToSQLString()]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

let iterValueExpr (colFunc : ColumnRef -> unit) (placeholderFunc : int -> unit) (queryFunc : SelectExpr -> unit) : ValueExpr -> unit =
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
        | VEInQuery (e, query) -> traverse e; queryFunc query
        | VENotInQuery (e, query) -> traverse e; queryFunc query
        | VEIsNull e -> traverse e
        | VEIsNotNull e -> traverse e
        | VEFunc (name,  args) -> Array.iter traverse args
        | VECast (e, typ) -> traverse e
        | VECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | VECoalesce vals -> Array.iter traverse vals
    traverse

type InsertValue =
    | IVValue of ValueExpr
    | IVDefault
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IVValue e -> e.ToSQLString()
            | IVDefault -> "DEFAULT"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type InsertValues =
    | IValues of InsertValue[][]
    | IDefaults
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IValues values ->
                let renderInsertValue (values : InsertValue[]) =
                    values |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"

                assert (not <| Array.isEmpty values)
                sprintf "VALUES %s" (values |> Seq.map renderInsertValue |> String.concat ", ")
            | IDefaults -> "DEFAULT VALUES"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type InsertExpr =
    { name : TableRef
      columns : ColumnName[]
      values : InsertValues
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            sprintf "INSERT INTO %s (%s) %s"
                (this.name.ToSQLString())
                (this.columns |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", ")
                (this.values.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type UpdateExpr =
    { name : TableRef
      columns : Map<ColumnName, ValueExpr>
      where : ValueExpr option
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
      where : ValueExpr option
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
      defaultExpr : ValueExpr option
    }

type ConstraintType =
    | CTUnique
    | CTCheck
    | CTPrimaryKey
    | CTForeignKey

type ConstraintMeta =
    | CMUnique of ColumnName[]
    | CMCheck of ValueExpr
    | CMPrimaryKey of ColumnName[]
    | CMForeignKey of TableRef * (ColumnName * ColumnName)[]

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
    { schemas : Map<SchemaName, SchemaMeta>
    }

type TableOperation =
    | TOCreateColumn of ColumnName * ColumnMeta
    | TODeleteColumn of ColumnName
    | TOAlterColumnType of ColumnName * DBValueType
    | TOAlterColumnNull of ColumnName * bool
    | TOAlterColumnDefault of ColumnName * ValueExpr option
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TOCreateColumn (col, pars) ->
                let notNullStr = if pars.isNullable then "NULL" else "NOT NULL"
                let defaultStr =
                    match pars.defaultExpr with
                    | None -> ""
                    | Some def -> sprintf "DEFAULT %s" (def.ToSQLString())
                sprintf "ADD COLUMN %s %s %s %s" (col.ToSQLString()) (pars.columnType.ToSQLString()) notNullStr defaultStr
            | TODeleteColumn col -> sprintf "DROP COLUMN %s" (col.ToSQLString())
            | TOAlterColumnType (col, typ) -> sprintf "ALTER COLUMN %s SET DATA TYPE %s" (col.ToSQLString()) (typ.ToSQLString())
            | TOAlterColumnNull (col, isNullable) -> sprintf "ALTER COLUMN %s %s NOT NULL" (col.ToSQLString()) (if isNullable then "DROP" else "SET")
            | TOAlterColumnDefault (col, None) -> sprintf "ALTER COLUMN %s DROP DEFAULT" (col.ToSQLString())
            | TOAlterColumnDefault (col, Some def) -> sprintf "ALTER COLUMN %s SET DEFAULT %s" (col.ToSQLString()) (def.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type SchemaOperation =
    | SOCreateSchema of SchemaName
    | SODeleteSchema of SchemaName
    | SOCreateTable of TableRef
    | SODeleteTable of TableRef
    | SOCreateSequence of SchemaObject
    | SODeleteSequence of SchemaObject
    // Constraint operations are not plain ALTER TABLE operations because they create new objects at schema namespace.
    | SOCreateConstraint of SchemaObject * TableName * ConstraintMeta
    | SODeleteConstraint of SchemaObject * TableName
    | SOAlterTable of TableRef * TableOperation[]
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
                    | CMCheck expr -> sprintf "CHECK (%s)" (expr.ToSQLString())
                sprintf "ALTER TABLE %s ADD CONSTRAINT %s %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString()) constraintStr
            | SODeleteConstraint (constr, table) -> sprintf "ALTER TABLE %s DROP CONSTRAINT %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString())
            | SOAlterTable (table, ops) -> sprintf "ALTER TABLE %s %s" (table.ToSQLString()) (ops |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", ")

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

let emptyOrderLimitClause = { orderBy = [||]; limit = None; offset = None }
