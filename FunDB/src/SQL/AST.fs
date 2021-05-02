module FunWithFlags.FunDB.SQL.AST

// SQL module does not attempt to restrict SQL in any way apart from type safety for values (explicit strings, integers etc.).

open System
open NpgsqlTypes
open Newtonsoft.Json
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.SQL.Utils

type [<Struct>] SQLName = SQLName of string
    with
        override this.ToString () =
            match this with
            | SQLName name -> name

        member this.ToSQLString () =
            match this with
            | SQLName name -> renderSqlName name

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<Struct>] SQLRawString = SQLRawString of string
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
// Because '"character varying(1)"' is not a type but 'character varying(1)' is!
type TypeName = SQLRawString
type FunctionName = SQLName

// Values

type SchemaObject =
    { Schema : SchemaName option
      Name : SQLName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this.Schema with
            | None -> this.Name.ToSQLString()
            | Some schema -> sprintf "%s.%s" (schema.ToSQLString()) (this.Name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type TableRef = SchemaObject

type ResolvedColumnRef =
    { Table : TableRef
      Name : ColumnName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            sprintf "%s.%s" (this.Table.ToSQLString()) (this.Name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ColumnRef =
    { Table : TableRef option
      Name : ColumnName
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this.Table with
            | None -> this.Name.ToSQLString()
            | Some entity -> sprintf "%s.%s" (entity.ToSQLString()) (this.Name.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ArrayValue<'t> =
    | AVValue of 't
    | AVArray of ArrayValue<'t>[]
    | AVNull

and ValueArray<'t> = ArrayValue<'t>[]

let rec mapArrayValue (func : 'a -> 'b) : ArrayValue<'a> -> ArrayValue<'b> = function
    | AVValue a -> AVValue (func a)
    | AVArray vals -> AVArray (mapValueArray func vals)
    | AVNull -> AVNull

and mapValueArray (func : 'a -> 'b) (vals : ValueArray<'a>) : ValueArray<'b> =
    Array.map (mapArrayValue func) vals

type [<NoEquality; NoComparison>] Value =
    | VInt of int
    | VBigInt of int64
    | VDecimal of decimal
    | VString of string
    | VRegclass of SchemaObject
    | VBool of bool
    | VDateTime of NpgsqlDateTime
    | VDate of NpgsqlDate
    | VInterval of NpgsqlTimeSpan
    | VJson of JToken
    | VUuid of Guid
    | VIntArray of ValueArray<int>
    | VBigIntArray of ValueArray<int64>
    | VDecimalArray of ValueArray<decimal>
    | VStringArray of ValueArray<string>
    | VBoolArray of ValueArray<bool>
    | VDateTimeArray of ValueArray<NpgsqlDateTime>
    | VDateArray of ValueArray<NpgsqlDate>
    | VIntervalArray of ValueArray<NpgsqlTimeSpan>
    | VRegclassArray of ValueArray<SchemaObject>
    | VJsonArray of ValueArray<JToken>
    | VUuidArray of ValueArray<Guid>
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
            | VBigInt i -> renderSqlBigInt i
            | VDecimal d -> renderSqlDecimal d
            | VString s -> sprintf "%s :: text" (renderSqlString s)
            | VRegclass rc -> sprintf "%s :: regclass" (rc.ToSQLString() |> renderSqlString)
            | VBool b -> renderSqlBool b
            | VDateTime dt -> sprintf "%s :: timestamp" (dt |> string |> renderSqlString)
            | VDate d -> sprintf "%s :: date" (d |> string |> renderSqlString)
            | VInterval d -> sprintf "%s :: interval" (d |> string |> renderSqlString)
            | VJson j -> sprintf "%s :: jsonb" (j |> renderSqlJson |> renderSqlString)
            | VUuid u -> sprintf "%s :: uuid" (u |> string |> renderSqlString)
            | VIntArray vals -> renderArray renderSqlInt "int4" vals
            | VBigIntArray vals -> renderArray renderSqlBigInt "int8" vals
            | VDecimalArray vals -> renderArray renderSqlDecimal "decimal" vals
            | VStringArray vals -> renderArray renderSqlString "text" vals
            | VBoolArray vals -> renderArray renderSqlBool "bool" vals
            | VDateTimeArray vals -> renderArray string "timestamp" vals
            | VDateArray vals -> renderArray string "date" vals
            | VIntervalArray vals -> renderArray string "interval" vals
            | VRegclassArray vals -> renderArray (fun (x : SchemaObject) -> x.ToSQLString()) "regclass" vals
            | VJsonArray vals -> renderArray renderSqlJson "jsonb" vals
            | VUuidArray vals -> renderArray string "uuid" vals
            | VNull -> "NULL"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type ValuePrettyConverter () =
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
        | VInt i -> writer.WriteValue(i)
        | VBigInt i -> writer.WriteValue(i)
        | VDecimal d -> writer.WriteValue(d)
        | VString s -> writer.WriteValue(s)
        | VBool b -> writer.WriteValue(b)
        | VDateTime dt -> writer.WriteValue(dt.ToDateTime())
        | VDate dt -> writer.WriteValue(dt.ToString())
        | VInterval int -> writer.WriteValue(int.ToString())
        | VJson j -> j.WriteTo(writer)
        | VRegclass rc -> writer.WriteValue(string rc)
        | VUuid u -> writer.WriteValue(u)
        | VIntArray vals -> serializeArray id vals
        | VBigIntArray vals -> serializeArray id vals
        | VDecimalArray vals -> serializeArray id vals
        | VStringArray vals -> serializeArray id vals
        | VBoolArray vals -> serializeArray id vals
        | VDateTimeArray vals -> serializeArray (fun (dt : NpgsqlDateTime) -> dt.ToDateTime()) vals
        | VDateArray vals -> serializeArray string vals
        | VIntervalArray vals -> serializeArray string vals
        | VRegclassArray vals -> serializeArray string vals
        | VJsonArray vals -> serializeArray id vals
        | VUuidArray vals -> serializeArray id vals
        | VNull -> writer.WriteNull()

// Simplified list of PostgreSQL types. Other types are casted to those.
// Used when interpreting query results and for compiling FunQL.
type SimpleType =
    | [<CaseName("int")>] STInt
    | [<CaseName("bigint")>] STBigInt
    | [<CaseName("string")>] STString
    | [<CaseName("decimal")>] STDecimal
    | [<CaseName("bool")>] STBool
    | [<CaseName("datetime")>] STDateTime
    | [<CaseName("date")>] STDate
    | [<CaseName("interval")>] STInterval
    | [<CaseName("regclass")>] STRegclass
    | [<CaseName("json")>] STJson
    | [<CaseName("uuid")>] STUuid
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | STInt -> "int4"
            | STBigInt -> "int8"
            | STString -> "text"
            | STDecimal -> "numeric"
            | STBool -> "bool"
            | STDateTime -> "timestamp"
            | STDate -> "date"
            | STInterval -> "interval"
            | STRegclass -> "regclass"
            | STJson -> "jsonb"
            | STUuid -> "uuid"

        member this.ToSQLRawString () = SQLRawString (this.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

// Find the closest simple type to a given.
let findSimpleType (str : TypeName) : SimpleType option =
    match str.ToString() with
    | "int4" -> Some STInt
    | "integer" -> Some STInt
    | "int8" -> Some STBigInt
    | "bigint" -> Some STBigInt
    | "text" -> Some STString
    | "double precision" -> Some STDecimal
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
    | "interval" -> Some STInterval
    | "regclass" -> Some STRegclass
    | "jsonb" -> Some STJson
    | "json" -> Some STJson
    | "uuid" -> Some STUuid
    | _ -> None

[<SerializeAsObject("type")>]
type ValueType<'t> when 't :> ISQLString =
    | [<CaseName(null)>] VTScalar of Type : 't
    | [<CaseName("array")>] VTArray of Subtype : 't
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

type DBValueType = ValueType<TypeName>
type SimpleValueType = ValueType<SimpleType>

let valueSimpleType : Value -> SimpleValueType option = function
    | VInt i -> Some <| VTScalar STInt
    | VBigInt i -> Some <| VTScalar STBigInt
    | VDecimal d -> Some <| VTScalar STDecimal
    | VString s -> Some <| VTScalar STString
    | VBool b -> Some <| VTScalar STBool
    | VDateTime dt -> Some <| VTScalar STDateTime
    | VDate dt -> Some <| VTScalar STDate
    | VInterval int -> Some <| VTScalar STInterval
    | VRegclass rc -> Some <| VTScalar STRegclass
    | VJson j -> Some <| VTScalar STJson
    | VUuid u -> Some <| VTScalar STUuid
    | VIntArray vals -> Some <| VTArray STInt
    | VBigIntArray vals -> Some <| VTArray STBigInt
    | VDecimalArray vals -> Some <| VTArray STDecimal
    | VStringArray vals -> Some <| VTArray STString
    | VBoolArray vals -> Some <| VTArray STBool
    | VDateTimeArray vals -> Some <| VTArray STDateTime
    | VDateArray vals -> Some <| VTArray STDate
    | VIntervalArray vals -> Some <| VTArray STInterval
    | VRegclassArray vals -> Some <| VTArray STRegclass
    | VJsonArray vals -> Some <| VTArray STJson
    | VUuidArray vals -> Some <| VTArray STUuid
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

type TableAlias =
    { Name : TableName
      Columns : ColumnName[] option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let columnsStr =
                match this.Columns with
                | None -> ""
                | Some cols -> cols |> Array.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"
            String.concatWithWhitespaces ["AS"; this.Name.ToSQLString(); columnsStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type [<NoEquality; NoComparison>] BinaryOperator =
    | BOLess
    | BOLessEq
    | BOGreater
    | BOGreaterEq
    | BOEq
    | BONotEq
    | BOConcat
    | BOLike
    | BOILike
    | BONotLike
    | BONotILike
    | BOMatchRegex
    | BOMatchRegexCI
    | BONotMatchRegex
    | BONotMatchRegexCI
    | BOPlus
    | BOMinus
    | BOMultiply
    | BODivide
    | BOJsonArrow
    | BOJsonTextArrow
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | BOLess -> "<"
            | BOLessEq -> "<="
            | BOGreater -> ">"
            | BOGreaterEq -> ">="
            | BOEq -> "="
            | BONotEq -> "<>"
            | BOConcat -> "||"
            | BOLike -> "~~"
            | BOILike -> "~~*"
            | BONotLike -> "!~~"
            | BONotILike -> "!~~*"
            | BOMatchRegex -> "~"
            | BOMatchRegexCI -> "~*"
            | BONotMatchRegex -> "!~"
            | BONotMatchRegexCI -> "!~*"
            | BOPlus -> "+"
            | BOMinus -> "-"
            | BOMultiply -> "*"
            | BODivide -> "/"
            | BOJsonArrow -> "->"
            | BOJsonTextArrow -> "->>"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] SpecialFunction =
    | SFCoalesce
    | SFGreatest
    | SFLeast
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SFCoalesce -> "COALESCE"
            | SFGreatest -> "GREATEST"
            | SFLeast -> "LEAST"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

// Parameters go in same order they go in SQL commands (e.g. VECast (value, type) because "foo :: bar").
type [<NoEquality; NoComparison>] ValueExpr =
    | VEValue of Value
    | VEColumn of ColumnRef
    | VEPlaceholder of int
    | VENot of ValueExpr
    | VEOr of ValueExpr * ValueExpr
    | VEAnd of ValueExpr * ValueExpr
    | VEBinaryOp of ValueExpr * BinaryOperator * ValueExpr
    | VEDistinct of ValueExpr * ValueExpr
    | VENotDistinct of ValueExpr * ValueExpr
    | VEAny of ValueExpr * BinaryOperator * ValueExpr
    | VEAll of ValueExpr * BinaryOperator * ValueExpr
    | VESimilarTo of ValueExpr * ValueExpr
    | VENotSimilarTo of ValueExpr * ValueExpr
    | VEIn of ValueExpr * ValueExpr[]
    | VENotIn of ValueExpr * ValueExpr[]
    | VEInQuery of ValueExpr * SelectExpr
    | VENotInQuery of ValueExpr * SelectExpr
    | VEIsNull of ValueExpr
    | VEIsNotNull of ValueExpr
    | VESpecialFunc of SpecialFunction * ValueExpr[]
    | VEFunc of FunctionName * ValueExpr[]
    | VEAggFunc of FunctionName * AggExpr
    | VECast of ValueExpr * DBValueType
    | VECase of (ValueExpr * ValueExpr)[] * (ValueExpr option)
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
            | VEOr (a, b) -> sprintf "(%s) OR (%s)" (a.ToSQLString()) (b.ToSQLString())
            | VEAnd (a, b) -> sprintf "(%s) AND (%s)" (a.ToSQLString()) (b.ToSQLString())
            | VEBinaryOp (a, op, b) -> sprintf "(%s) %s (%s)" (a.ToSQLString()) (op.ToSQLString()) (b.ToSQLString())
            | VEDistinct (a, b) -> sprintf "(%s) IS DISTINCT FROM (%s)" (a.ToSQLString()) (b.ToSQLString())
            | VENotDistinct (a, b) -> sprintf "(%s) IS NOT DISTINCT FROM (%s)" (a.ToSQLString()) (b.ToSQLString())
            | VEAny (e, op, arr) -> sprintf "(%s) %s ANY (%s)" (e.ToSQLString()) (op.ToSQLString()) (arr.ToSQLString())
            | VEAll (e, op, arr) -> sprintf "(%s) %s ALL (%s)" (e.ToSQLString()) (op.ToSQLString()) (arr.ToSQLString())
            | VESimilarTo (e, pat) -> sprintf "(%s) SIMILAR TO (%s)" (e.ToSQLString()) (pat.ToSQLString())
            | VENotSimilarTo (e, pat) -> sprintf "(%s) NOT SIMILAR TO (%s)" (e.ToSQLString()) (pat.ToSQLString())
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
            | VESpecialFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map (fun arg -> arg.ToSQLString()) |> String.concat ", ")
            | VEFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map (fun arg -> arg.ToSQLString()) |> String.concat ", ")
            | VEAggFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args.ToSQLString())
            | VECast (e, typ) -> sprintf "(%s) :: %s" (e.ToSQLString()) (typ.ToSQLString())
            | VECase (es, els) ->
                let esStr = es |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToSQLString()) (e.ToSQLString())) |> String.concat " "
                let elsStr =
                    match els with
                    | None -> ""
                    | Some e -> sprintf "ELSE %s" (e.ToSQLString())
                String.concatWithWhitespaces ["CASE"; esStr; elsStr; "END"]
            | VEArray vals -> sprintf "ARRAY[%s]" (vals |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", ")
            | VESubquery query -> sprintf "(%s)" (query.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

and [<NoEquality; NoComparison>] AggExpr =
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

and [<NoEquality; NoComparison>] FromExpr =
    | FTable of obj * TableAlias option * TableRef // obj is extra meta info
    | FJoin of JoinExpr
    | FSubExpr of TableAlias * SelectExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FTable (_, malias, t) ->
                let aliasStr =
                    match malias with
                    | None -> ""
                    | Some alias -> alias.ToSQLString()
                String.concatWithWhitespaces [t.ToSQLString(); aliasStr]
            | FSubExpr (alias, expr) ->
                sprintf "(%s) %s" (expr.ToSQLString()) (alias.ToSQLString())
            | FJoin join -> join.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] JoinExpr =
    { Type : JoinType
      A : FromExpr
      B : FromExpr
      Condition : ValueExpr
    }
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            sprintf "(%s %s JOIN %s ON %s)" (this.A.ToSQLString()) (this.Type.ToSQLString()) (this.B.ToSQLString()) (this.Condition.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] SelectedColumn =
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

and [<NoEquality; NoComparison>] SingleSelectExpr =
    { Columns : SelectedColumn[]
      From : FromExpr option
      Where : ValueExpr option
      GroupBy : ValueExpr[]
      OrderLimit : OrderLimitClause
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let resultsStr = this.Columns |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
            let fromStr =
                match this.From with
                | None -> ""
                | Some from -> sprintf "FROM %s" (from.ToSQLString())
            let whereStr =
                match this.Where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToSQLString())
            let groupByStr =
                if Array.isEmpty this.GroupBy then
                    ""
                else
                    sprintf "GROUP BY %s" (this.GroupBy |> Array.map (fun x -> x.ToSQLString()) |> String.concat ", ")

            String.concatWithWhitespaces ["SELECT"; resultsStr; fromStr; whereStr; groupByStr; this.OrderLimit.ToSQLString()]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] OrderLimitClause =
    { OrderBy : (SortOrder * ValueExpr)[]
      Limit : ValueExpr option
      Offset : ValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
                let orderByStr =
                    if Array.isEmpty this.OrderBy
                    then ""
                    else sprintf "ORDER BY %s" (this.OrderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (expr.ToSQLString()) (ord.ToSQLString())) |> String.concat ", ")
                let limitStr =
                    match this.Limit with
                    | Some e -> sprintf "LIMIT %s" (e.ToSQLString())
                    | None -> ""
                let offsetStr =
                    match this.Offset with
                    | Some e -> sprintf "OFFSET %s" (e.ToSQLString())
                    | None -> ""
                String.concatWithWhitespaces [orderByStr; limitStr; offsetStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] SelectTreeExpr =
    | SSelect of SingleSelectExpr
    | SValues of ValueExpr[][]
    | SSetOp of SetOperationExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SSelect e -> e.ToSQLString()
            | SValues values ->
                assert not (Array.isEmpty values)
                let printOne (array : ValueExpr array) =
                    assert not (Array.isEmpty array)
                    array |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"
                let valuesStr = values |> Seq.map printOne |> String.concat ", "
                sprintf "VALUES %s" valuesStr
            | SSetOp setOp -> setOp.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] CommonTableExpr =
    { Fields : ColumnName[] option
      Materialized : bool option
      Expr : SelectExpr
    }

and [<NoEquality; NoComparison>] CommonTableExprs =
    { Recursive : bool
      Exprs : (TableName * CommonTableExpr)[]
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not (Array.isEmpty this.Exprs))
            let oneExpr (name : TableName, cte : CommonTableExpr) =
                let nameStr =
                    match cte.Fields with
                    | None ->
                        name.ToSQLString()
                    | Some args ->
                        assert (not (Array.isEmpty args))
                        let argsStr = args |> Array.map (fun x -> x.ToSQLString()) |> String.concat ", "
                        sprintf "%s(%s)" (name.ToSQLString()) argsStr
                let materialized =
                    match cte.Materialized with
                    | None -> ""
                    | Some false -> "NOT MATERIALIZED"
                    | Some true -> "MATERIALIZED"
                let exprStr = sprintf "(%s)" (cte.Expr.ToSQLString())
                String.concatWithWhitespaces [nameStr; "AS"; materialized; exprStr]
            let exprs =
                this.Exprs
                |> Seq.map oneExpr
                |> String.concat ", "
            let recursive = if this.Recursive then "RECURSIVE" else ""
            String.concatWithWhitespaces ["WITH"; recursive; exprs]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] SelectExpr =
    { CTEs : CommonTableExprs option
      Tree : SelectTreeExpr
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let ctesStr =
                match this.CTEs with
                | None -> ""
                | Some ctes -> ctes.ToSQLString()
            String.concatWithWhitespaces [ctesStr; this.Tree.ToSQLString()]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] SetOperationExpr =
    { Operation : SetOperation
      AllowDuplicates : bool
      A : SelectExpr
      B : SelectExpr
      OrderLimit : OrderLimitClause
    } with
        member this.ToSQLString () =
            let allowDuplicatesStr = if this.AllowDuplicates then "ALL" else ""
            let aStr = sprintf "(%s)" (this.A.ToSQLString())
            let bStr = sprintf "(%s)" (this.B.ToSQLString())
            String.concatWithWhitespaces [aStr; this.Operation.ToSQLString(); allowDuplicatesStr; bStr; this.OrderLimit.ToSQLString()]

        override this.ToString () = this.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type ValueExprGenericMapper =
    { Value : Value -> ValueExpr
      ColumnReference : ColumnRef -> ValueExpr
      Placeholder : int -> ValueExpr
      Query : SelectExpr -> SelectExpr
    }

let idValueExprGenericMapper =
    { Value = VEValue
      ColumnReference = VEColumn
      Placeholder = VEPlaceholder
      Query = id
    }

let rec genericMapValueExpr (mapper : ValueExprGenericMapper) : ValueExpr -> ValueExpr =
    let rec traverse = function
        | VEValue value -> mapper.Value value
        | VEColumn c -> mapper.ColumnReference c
        | VEPlaceholder i -> mapper.Placeholder i
        | VENot e -> VENot <| traverse e
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEBinaryOp (a, op, b) -> VEBinaryOp (traverse a, op, traverse b)
        | VEAny (a, op, b) -> VEAny (traverse a, op, traverse b)
        | VEAll (a, op, b) -> VEAll (traverse a, op, traverse b)
        | VEDistinct (a, b) -> VEDistinct (traverse a, traverse b)
        | VENotDistinct (a, b) -> VENotDistinct (traverse a, traverse b)
        | VESimilarTo (e, pat) -> VESimilarTo (traverse e, traverse pat)
        | VENotSimilarTo (e, pat) -> VENotSimilarTo (traverse e, traverse pat)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEInQuery (e, query) -> VEInQuery (traverse e, mapper.Query query)
        | VENotInQuery (e, query) -> VENotInQuery (traverse e, mapper.Query query)
        | VEIsNull e -> VEIsNull <| traverse e
        | VEIsNotNull e -> VEIsNotNull <| traverse e
        | VESpecialFunc (name, args) -> VESpecialFunc (name, Array.map traverse args)
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> VEAggFunc (name, mapAggExpr traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
        | VECase (es, els) ->
            let es' = Array.map (fun (cond, e) -> (traverse cond, traverse e)) es
            let els' = Option.map traverse els
            VECase (es', els')
        | VEArray vals -> VEArray <| Array.map traverse vals
        | VESubquery query -> VESubquery (mapper.Query query)
    traverse

and mapAggExpr (func : ValueExpr -> ValueExpr) : AggExpr -> AggExpr = function
    | AEAll exprs -> AEAll (Array.map func exprs)
    | AEDistinct expr -> AEDistinct (func expr)
    | AEStar -> AEStar

type ValueExprMapper =
    { Value : Value -> Value
      ColumnReference : ColumnRef -> ColumnRef
      Placeholder : int -> int
      Query : SelectExpr -> SelectExpr
    }

let idValueExprMapper =
    { Value = id
      ColumnReference = id
      Placeholder = id
      Query = id
    }

let mapValueExpr (mapper : ValueExprMapper) : ValueExpr -> ValueExpr =
    genericMapValueExpr
        { Value = mapper.Value >> VEValue
          ColumnReference = mapper.ColumnReference >> VEColumn
          Placeholder = mapper.Placeholder >> VEPlaceholder
          Query = mapper.Query
        }

type ValueExprIter =
    { Value : Value -> unit
      ColumnReference : ColumnRef -> unit
      Placeholder : int -> unit
      Query : SelectExpr -> unit
    }

let idValueExprIter =
    { Value = fun _ -> ()
      ColumnReference = fun _ -> ()
      Placeholder = fun _ -> ()
      Query = fun _ -> ()
    }

let rec iterValueExpr (mapper : ValueExprIter) : ValueExpr -> unit =
    let rec traverse = function
        | VEValue value -> mapper.Value value
        | VEColumn c -> mapper.ColumnReference c
        | VEPlaceholder i -> mapper.Placeholder i
        | VENot e -> traverse e
        | VEAnd (a, b) -> traverse a; traverse b
        | VEOr (a, b) -> traverse a; traverse b
        | VEBinaryOp (a, op, b) -> traverse a; traverse b
        | VEDistinct (a, b) -> traverse a; traverse b
        | VENotDistinct (a, b) -> traverse a; traverse b
        | VEAny (e, op, arr) -> traverse e; traverse arr
        | VEAll (e, op, arr) -> traverse e; traverse arr
        | VESimilarTo (e, pat) -> traverse e; traverse pat
        | VENotSimilarTo (e, pat) -> traverse e; traverse pat
        | VEIn (e, vals) -> traverse e; Array.iter traverse vals
        | VENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | VEInQuery (e, query) -> traverse e; mapper.Query query
        | VENotInQuery (e, query) -> traverse e; mapper.Query query
        | VEIsNull e -> traverse e
        | VEIsNotNull e -> traverse e
        | VESpecialFunc (name, args) -> Array.iter traverse args
        | VEFunc (name, args) -> Array.iter traverse args
        | VEAggFunc (name, args) -> iterAggExpr traverse args
        | VECast (e, typ) -> traverse e
        | VECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | VEArray vals -> Array.iter traverse vals
        | VESubquery query -> mapper.Query query
    traverse

and iterAggExpr (func : ValueExpr -> unit) : AggExpr -> unit = function
    | AEAll exprs -> Array.iter func exprs
    | AEDistinct expr -> func expr
    | AEStar -> ()

let emptyOrderLimitClause = { OrderBy = [||]; Limit = None; Offset = None }

let rec private normalizeArrayValue (constr : 'a -> Value) : ArrayValue<'a> -> ValueExpr = function
    | AVArray arr -> normalizeArray constr arr
    | AVNull -> VEValue VNull
    | AVValue v -> VEValue (constr v)

and normalizeArray (constr : 'a -> Value) (arr : ArrayValue<'a>[]) : ValueExpr =
    VEArray (Array.map (normalizeArrayValue constr) arr)
