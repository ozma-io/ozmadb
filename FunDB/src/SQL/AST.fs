module FunWithFlags.FunDB.SQL.AST

// SQL module does not attempt to restrict SQL in any way apart from type safety for values (explicit strings, integers etc.).

open System
open Newtonsoft.Json
open Newtonsoft.Json.Linq

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
type IndexName = SQLName
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

type [<JsonConverter(typeof<ValueConverter>)>] [<NoComparison>] Value =
    | VInt of int
    | VDecimal of decimal
    | VString of string
    | VRegclass of SchemaObject
    | VBool of bool
    | VDateTime of DateTimeOffset
    | VDate of DateTimeOffset
    | VJson of JToken
    | VIntArray of ValueArray<int>
    | VDecimalArray of ValueArray<decimal>
    | VStringArray of ValueArray<string>
    | VBoolArray of ValueArray<bool>
    | VDateTimeArray of ValueArray<DateTimeOffset>
    | VDateArray of ValueArray<DateTimeOffset>
    | VRegclassArray of ValueArray<SchemaObject>
    | VJsonArray of ValueArray<JToken>
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
                    arr |> Seq.map renderValue |> String.concat ", " |> sprintf "ARRAY[%s]"
                sprintf "%s :: %s[]" (renderArrayInsides arr) typeName

            match this with
            | VInt i -> renderSqlInt i
            | VDecimal d -> renderSqlDecimal d
            | VString s -> sprintf "E%s :: text" (renderSqlString s)
            | VRegclass rc -> sprintf "E%s :: regclass" (rc.ToSQLString() |> renderSqlString)
            | VBool b -> renderSqlBool b
            | VDateTime dt -> sprintf "%s :: timestamp" (dt |> renderSqlDateTime |> renderSqlString)
            | VDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
            | VJson j -> sprintf "%s :: jsonb" (j |> renderSqlJson |> renderSqlString)
            | VIntArray vals -> renderArray renderSqlInt "int4" vals
            | VDecimalArray vals -> renderArray renderSqlDecimal "decimal" vals
            | VStringArray vals -> renderArray renderSqlString "text" vals
            | VBoolArray vals -> renderArray renderSqlBool "bool" vals
            | VDateTimeArray vals -> renderArray renderSqlDateTime "timestamp" vals
            | VDateArray vals -> renderArray renderSqlDate "date" vals
            | VRegclassArray vals -> renderArray (fun (x : SchemaObject) -> x.ToSQLString()) "regclass" vals
            | VJsonArray vals -> renderArray renderSqlJson "jsonb" vals
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
        | VJson j -> j.WriteTo(writer)
        | VRegclass rc -> serialize <| string rc
        | VIntArray vals -> serializeArray id vals
        | VDecimalArray vals -> serializeArray id vals
        | VStringArray vals -> serializeArray id vals
        | VBoolArray vals -> serializeArray id vals
        | VDateTimeArray vals -> serializeArray (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | VDateArray vals -> serializeArray (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | VRegclassArray vals -> serializeArray string vals
        | VJsonArray vals -> serializeArray id vals
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
    | STJson
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
            | STJson -> "jsonb"

        member this.ToJSONString () =
            match this with
            | STInt -> "int"
            | STString -> "string"
            | STDecimal -> "numeric"
            | STBool -> "bool"
            | STDateTime -> "datetime"
            | STDate -> "date"
            | STRegclass -> "regclass"
            | STJson -> "json"

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
    | "bigint" -> Some STInt
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
    | "jsonb" -> Some STJson
    | "json" -> Some STJson
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

let valueSimpleType : Value -> SimpleValueType option = function
    | VInt i -> Some <| VTScalar STInt
    | VDecimal d -> Some <| VTScalar STDecimal
    | VString s -> Some <| VTScalar STString
    | VBool b -> Some <| VTScalar STBool
    | VDateTime dt -> Some <| VTScalar STDateTime
    | VDate dt -> Some <| VTScalar STDate
    | VRegclass rc -> Some <| VTScalar STRegclass
    | VJson j -> Some <| VTScalar STJson
    | VIntArray vals -> Some <| VTArray STInt
    | VDecimalArray vals -> Some <| VTArray STDecimal
    | VStringArray vals -> Some <| VTArray STString
    | VBoolArray vals -> Some <| VTArray STBool
    | VDateTimeArray vals -> Some <| VTArray STDateTime
    | VDateArray vals -> Some <| VTArray STDate
    | VRegclassArray vals -> Some <| VTArray STRegclass
    | VJsonArray vals -> Some <| VTArray STJson
    | VNull -> None

let findSimpleValueType : DBValueType -> SimpleValueType option = function
    | VTScalar a -> Option.map VTScalar (findSimpleType a)
    | VTArray a -> Option.map VTArray (findSimpleType a)

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
type [<NoComparison>] ValueExpr =
    | VEValue of Value
    | VEColumn of ColumnRef
    | VEPlaceholder of int
    | VENot of ValueExpr
    | VEAnd of ValueExpr * ValueExpr
    | VEOr of ValueExpr * ValueExpr
    | VEConcat of ValueExpr * ValueExpr
    | VEEq of ValueExpr * ValueExpr
    | VEEqAny of ValueExpr * ValueExpr
    | VENotEq of ValueExpr * ValueExpr
    | VENotEqAll of ValueExpr * ValueExpr
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
    | VEAggFunc of SQLName * AggExpr
    | VECast of ValueExpr * DBValueType
    | VECase of (ValueExpr * ValueExpr)[] * (ValueExpr option)
    | VECoalesce of ValueExpr[]
    | VEJsonArrow of ValueExpr * ValueExpr
    | VEJsonTextArrow of ValueExpr * ValueExpr
    | VEArray of ValueExpr[]
    | VESubquery of SelectExpr
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
            | VEEqAny (e, arr) -> sprintf "(%s) = ANY (%s)" (e.ToSQLString()) (arr.ToSQLString())
            | VENotEq (a, b) -> sprintf "(%s) <> (%s)" (a.ToSQLString()) (b.ToSQLString())
            | VENotEqAll (e, arr) -> sprintf "(%s) <> ALL (%s)" (e.ToSQLString()) (arr.ToSQLString())
            | VELike (e, pat) -> sprintf "(%s) LIKE (%s)" (e.ToSQLString()) (pat.ToSQLString())
            | VENotLike (e, pat) -> sprintf "(%s) NOT LIKE (%s)" (e.ToSQLString()) (pat.ToSQLString())
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
            | VEAggFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args.ToSQLString())
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
            | VEJsonArrow (a, b) -> sprintf "(%s)->(%s)" (a.ToSQLString()) (b.ToSQLString())
            | VEJsonTextArrow (a, b) -> sprintf "(%s)->>(%s)" (a.ToSQLString()) (b.ToSQLString())
            | VEArray vals -> sprintf "ARRAY[%s]" (vals |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", ")
            | VESubquery query -> sprintf "(%s)" (query.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

and [<NoComparison>] AggExpr =
    | AEAll of ValueExpr[]
    | AEDistinct of ValueExpr
    | AEStar
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | AEAll exprs ->
                assert (not <| Array.isEmpty exprs)
                exprs |> Array.map (fun x -> x.ToSQLString()) |> String.concat ", "
            | AEDistinct expr -> sprintf "DISTINCT %s" (expr.ToSQLString())
            | AEStar -> "*"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoComparison>] FromExpr =
    | FTable of obj * TableName option * TableRef // obj is extra meta info
    | FJoin of JoinType * FromExpr * FromExpr * ValueExpr
    | FSubExpr of TableName * ColumnName[] option * SelectExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FTable (_, name, t) ->
                match name with
                | Some n -> sprintf "%s AS %s" (t.ToSQLString()) (n.ToSQLString())
                | None -> t.ToSQLString()
            | FJoin (joinType, a, b, cond) ->
                sprintf "(%s %s JOIN %s ON %s)" (a.ToSQLString()) (joinType.ToSQLString()) (b.ToSQLString()) (cond.ToSQLString())
            | FSubExpr (name, fieldNames, expr) ->
                let fieldNamesStr =
                    match fieldNames with
                    | None -> ""
                    | Some names -> names |> Seq.map (fun n -> n.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"
                let subStr = sprintf "(%s) AS %s" (expr.ToSQLString()) (name.ToSQLString())
                concatWithWhitespaces [subStr; fieldNamesStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoComparison>] SelectedColumn =
    | SCAll of TableRef option
    | SCExpr of ColumnName option * ValueExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SCAll None -> "*"
            | SCAll (Some ref) -> sprintf "%s.*" (ref.ToSQLString())
            | SCExpr (None, expr) -> expr.ToSQLString()
            | SCExpr (Some name, expr) -> sprintf "%s AS %s" (expr.ToSQLString()) (name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoComparison>] SingleSelectExpr =
    { columns : SelectedColumn[]
      from : FromExpr option
      where : ValueExpr option
      groupBy : ValueExpr[]
      orderLimit : OrderLimitClause
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let resultsStr = this.columns |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
            let fromStr =
                match this.from with
                | None -> ""
                | Some from -> sprintf "FROM %s" (from.ToSQLString())
            let whereStr =
                match this.where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToSQLString())
            let groupByStr =
                if Array.isEmpty this.groupBy then
                    ""
                else
                    sprintf "GROUP BY %s" (this.groupBy |> Array.map (fun x -> x.ToSQLString()) |> String.concat ", ")

            sprintf "SELECT %s" (concatWithWhitespaces [resultsStr; fromStr; whereStr; groupByStr; this.orderLimit.ToSQLString()])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoComparison>] OrderLimitClause =
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

and [<NoComparison>] SelectExpr =
    | SSelect of SingleSelectExpr
    | SValues of ValueExpr[][]
    | SSetOp of SetOperation * SelectExpr * SelectExpr * OrderLimitClause
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SSelect e -> e.ToSQLString()
            | SValues values ->
                assert not (Array.isEmpty values)
                let valuesStr = values |> Seq.map (fun array -> array |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", " |> sprintf "(%s)") |> String.concat ", "
                sprintf "VALUES %s" valuesStr
            | SSetOp (op, a, b, order) ->
                let setStr = sprintf "(%s) %s (%s)" (a.ToSQLString()) (op.ToSQLString()) (b.ToSQLString())
                concatWithWhitespaces [setStr; order.ToSQLString()]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type ValueExprGenericMapper =
    { value : Value -> ValueExpr
      columnReference : ColumnRef -> ValueExpr
      placeholder : int -> ValueExpr
      query : SelectExpr -> SelectExpr
    }

let idValueExprGenericMapper =
    { value = VEValue
      columnReference = VEColumn
      placeholder = VEPlaceholder
      query = id
    }

let rec genericMapValueExpr (mapper : ValueExprGenericMapper) : ValueExpr -> ValueExpr =
    let rec traverse = function
        | VEValue value -> mapper.value value
        | VEColumn c -> mapper.columnReference c
        | VEPlaceholder i -> mapper.placeholder i
        | VENot e -> VENot <| traverse e
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VEEqAny (e, arr) -> VEEqAny (traverse e, traverse arr)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
        | VENotEqAll (e, arr) -> VENotEqAll (traverse e, traverse arr)
        | VELike (e, pat) -> VELike (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEInQuery (e, query) -> VEInQuery (traverse e, mapper.query query)
        | VENotInQuery (e, query) -> VENotInQuery (traverse e, mapper.query query)
        | VEIsNull e -> VEIsNull <| traverse e
        | VEIsNotNull e -> VEIsNotNull <| traverse e
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> VEAggFunc (name, mapAggExpr traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
        | VECase (es, els) ->
            let es' = Array.map (fun (cond, e) -> (traverse cond, traverse e)) es
            let els' = Option.map traverse els
            VECase (es', els')
        | VECoalesce vals -> VECoalesce <| Array.map traverse vals
        | VEJsonArrow (a, b) -> VEJsonArrow (traverse a, traverse b)
        | VEJsonTextArrow (a, b) -> VEJsonTextArrow (traverse a, traverse b)
        | VEArray vals -> VEArray <| Array.map traverse vals
        | VESubquery query -> VESubquery (mapper.query query)
    traverse

and mapAggExpr (func : ValueExpr -> ValueExpr) : AggExpr -> AggExpr = function
    | AEAll exprs -> AEAll (Array.map func exprs)
    | AEDistinct expr -> AEDistinct (func expr)
    | AEStar -> AEStar

type ValueExprMapper =
    { value : Value -> Value
      columnReference : ColumnRef -> ColumnRef
      placeholder : int -> int
      query : SelectExpr -> SelectExpr
    }

let idValueExprMapper =
    { value = id
      columnReference = id
      placeholder = id
      query = id
    }

let mapValueExpr (mapper : ValueExprMapper) : ValueExpr -> ValueExpr =
    genericMapValueExpr
        { value = mapper.value >> VEValue
          columnReference = mapper.columnReference >> VEColumn
          placeholder = mapper.placeholder >> VEPlaceholder
          query = mapper.query
        }

type ValueExprIter =
    { value : Value -> unit
      columnReference : ColumnRef -> unit
      placeholder : int -> unit
      query : SelectExpr -> unit
    }

let idValueExprIter =
    { value = fun _ -> ()
      columnReference = fun _ -> ()
      placeholder = fun _ -> ()
      query = fun _ -> ()
    }

let rec iterValueExpr (mapper : ValueExprIter) : ValueExpr -> unit =
    let rec traverse = function
        | VEValue value -> mapper.value value
        | VEColumn c -> mapper.columnReference c
        | VEPlaceholder i -> mapper.placeholder i
        | VENot e -> traverse e
        | VEAnd (a, b) -> traverse a; traverse b
        | VEOr (a, b) -> traverse a; traverse b
        | VEConcat (a, b) -> traverse a; traverse b
        | VEEq (a, b) -> traverse a; traverse b
        | VEEqAny (e, arr) -> traverse e; traverse arr
        | VENotEq (a, b) -> traverse a; traverse b
        | VENotEqAll (e, arr) -> traverse e; traverse arr
        | VELike (e, pat) -> traverse e; traverse pat
        | VENotLike (e, pat) -> traverse e; traverse pat
        | VELess (a, b) -> traverse a; traverse b
        | VELessEq (a, b) -> traverse a; traverse b
        | VEGreater (a, b) -> traverse a; traverse b
        | VEGreaterEq (a, b) -> traverse a; traverse b
        | VEIn (e, vals) -> traverse e; Array.iter traverse vals
        | VENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | VEInQuery (e, query) -> traverse e; mapper.query query
        | VENotInQuery (e, query) -> traverse e; mapper.query query
        | VEIsNull e -> traverse e
        | VEIsNotNull e -> traverse e
        | VEFunc (name, args) -> Array.iter traverse args
        | VEAggFunc (name, args) -> iterAggExpr traverse args
        | VECast (e, typ) -> traverse e
        | VECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | VECoalesce vals -> Array.iter traverse vals
        | VEJsonArrow (a, b) -> traverse a; traverse b
        | VEJsonTextArrow (a, b) -> traverse a; traverse b
        | VEArray vals -> Array.iter traverse vals
        | VESubquery query -> mapper.query query
    traverse

and iterAggExpr (func : ValueExpr -> unit) : AggExpr -> unit = function
    | AEAll exprs -> Array.iter func exprs
    | AEDistinct expr -> func expr
    | AEStar -> ()

[<NoComparison>]
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

[<NoComparison>]
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

[<NoComparison>]
type InsertExpr =
    { name : TableRef
      columns : (obj * ColumnName)[] // obj is extra metadata
      values : InsertValues
      returning : SelectedColumn[]
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let returningStr =
                if Array.isEmpty this.returning then
                    ""
                else
                    let resultsStr = this.returning |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let insertStr =
                sprintf "INSERT INTO %s (%s) %s"
                    (this.name.ToSQLString())
                    (this.columns |> Seq.map (fun (extra, x) -> x.ToSQLString()) |> String.concat ", ")
                    (this.values.ToSQLString())
            concatWithWhitespaces [insertStr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type UpdateExpr =
    { name : TableRef
      columns : Map<ColumnName, obj * ValueExpr> // obj is extra metadata
      where : ValueExpr option
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not <| Map.isEmpty this.columns)

            let valuesExpr = this.columns |> Map.toSeq |> Seq.map (fun (name, (extra, expr)) -> sprintf "%s = %s" (name.ToSQLString()) (expr.ToSQLString())) |> String.concat ", "
            let condExpr =
                match this.where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            let updateStr = sprintf "UPDATE %s SET %s" (this.name.ToSQLString()) valuesExpr
            concatWithWhitespaces [updateStr; condExpr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type DeleteExpr =
    { name : TableRef
      where : ValueExpr option
      extra : obj
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

let emptyOrderLimitClause = { orderBy = [||]; limit = None; offset = None }

let rec private normalizeArrayValue (constr : 'a -> Value) : ArrayValue<'a> -> ValueExpr = function
    | AVArray arr -> normalizeArray constr arr
    | AVNull -> VEValue VNull
    | AVValue v -> VEValue (constr v)

and normalizeArray (constr : 'a -> Value) (arr : ArrayValue<'a>[]) : ValueExpr =
    VEArray (Array.map (normalizeArrayValue constr) arr)
