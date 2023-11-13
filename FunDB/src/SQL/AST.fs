module FunWithFlags.FunDB.SQL.AST

// SQL module does not attempt to restrict SQL in any way apart from type safety for values (explicit strings, integers etc.).

open System
open Newtonsoft.Json
open NodaTime

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunUtils.Serialization.Json
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
type ParameterName = SQLName
type ConstraintName = SQLName
type OpClassName = SQLName

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

type [<StructuralEquality; NoComparison>] Value =
    | VInt of int
    | VBigInt of int64
    | VDecimal of decimal
    | VString of string
    | VRegclass of SchemaObject
    | VBool of bool
    | VDateTime of Instant
    | VLocalDateTime of LocalDateTime
    | VDate of LocalDate
    | VInterval of Period
    | VJson of ComparableJToken
    | VUuid of Guid
    | VIntArray of ValueArray<int>
    | VBigIntArray of ValueArray<int64>
    | VDecimalArray of ValueArray<decimal>
    | VStringArray of ValueArray<string>
    | VBoolArray of ValueArray<bool>
    | VDateTimeArray of ValueArray<Instant>
    | VLocalDateTimeArray of ValueArray<LocalDateTime>
    | VDateArray of ValueArray<LocalDate>
    | VIntervalArray of ValueArray<Period>
    | VRegclassArray of ValueArray<SchemaObject>
    | VJsonArray of ValueArray<ComparableJToken>
    | VUuidArray of ValueArray<Guid>
    | VNull
    | VInvalid
    with
        override this.ToString () =
            match this with
            | VInvalid -> "(invalid)"
            | _ -> this.ToSQLString()

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
            | VDateTime dt -> sprintf "%s :: timestamptz" (dt |> renderSqlDateTime |> renderSqlString)
            | VLocalDateTime dt -> sprintf "%s :: timestamp" (dt |> renderSqlLocalDateTime |> renderSqlString)
            | VDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
            | VInterval d -> sprintf "%s :: interval" (d |> renderSqlInterval |> renderSqlString)
            | VJson j -> sprintf "%s :: jsonb" (j.Json |> renderSqlJson |> renderSqlString)
            | VUuid u -> sprintf "%s :: uuid" (u |> string |> renderSqlString)
            | VIntArray vals -> renderArray renderSqlInt "int4" vals
            | VBigIntArray vals -> renderArray renderSqlBigInt "int8" vals
            | VDecimalArray vals -> renderArray renderSqlDecimal "decimal" vals
            | VStringArray vals -> renderArray renderSqlString "text" vals
            | VBoolArray vals -> renderArray renderSqlBool "bool" vals
            | VDateTimeArray vals -> renderArray renderSqlDateTime "timestamptz" vals
            | VLocalDateTimeArray vals -> renderArray renderSqlLocalDateTime "timestamp" vals
            | VDateArray vals -> renderArray renderSqlDate "date" vals
            | VIntervalArray vals -> renderArray renderSqlInterval "interval" vals
            | VRegclassArray vals -> renderArray toSQLString "regclass" vals
            | VJsonArray vals -> vals |> renderArray (fun j -> renderSqlJson j.Json) "jsonb"
            | VUuidArray vals -> renderArray string "uuid" vals
            | VNull -> "NULL"
            | VInvalid -> raisef NotSupportedException "Invalid values cannot be represented in SQL"

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
        // To avoid overflow in JavaScript.
        | VBigInt i -> writer.WriteValue(string i)
        | VDecimal d -> writer.WriteValue(d)
        | VString s -> writer.WriteValue(s)
        | VBool b -> writer.WriteValue(b)
        | VDateTime dt -> serialize dt
        | VLocalDateTime dt -> serialize dt
        | VDate dt -> serialize dt
        | VInterval int -> serialize int
        | VJson j -> j.Json.WriteTo(writer)
        | VRegclass rc -> writer.WriteValue(string rc)
        | VUuid u -> writer.WriteValue(u)
        | VIntArray vals -> serializeArray id vals
        | VBigIntArray vals -> serializeArray string vals
        | VDecimalArray vals -> serializeArray id vals
        | VStringArray vals -> serializeArray id vals
        | VBoolArray vals -> serializeArray id vals
        | VDateTimeArray vals -> serializeArray id vals
        | VLocalDateTimeArray vals -> serializeArray id vals
        | VDateArray vals -> serializeArray id vals
        | VIntervalArray vals -> serializeArray id vals
        | VRegclassArray vals -> serializeArray string vals
        | VJsonArray vals -> serializeArray id vals
        | VUuidArray vals -> serializeArray id vals
        | VNull -> writer.WriteNull()
        | VInvalid -> writer.WriteValue("__invalid__")

// Simplified list of PostgreSQL types. Other types are casted to those.
// Used when interpreting query results and for compiling FunQL.
[<SerializeAsObject("type")>]
type SimpleType =
    | [<CaseKey("int")>] STInt
    | [<CaseKey("bigint")>] STBigInt
    | [<CaseKey("string")>] STString
    | [<CaseKey("decimal")>] STDecimal
    | [<CaseKey("bool")>] STBool
    | [<CaseKey("datetime")>] STDateTime
    | [<CaseKey("localdatetime")>] STLocalDateTime
    | [<CaseKey("date")>] STDate
    | [<CaseKey("interval")>] STInterval
    | [<CaseKey("regclass")>] STRegclass
    | [<CaseKey("json")>] STJson
    | [<CaseKey("uuid")>] STUuid
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | STInt -> "int4"
            | STBigInt -> "int8"
            | STString -> "text"
            | STDecimal -> "numeric"
            | STBool -> "bool"
            | STDateTime -> "timestamptz"
            | STLocalDateTime -> "localdatetime"
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
    | "timestamp without time zone" -> Some STLocalDateTime
    | "timestamp with time zone" -> Some STDateTime
    | "timestamp" -> Some STLocalDateTime
    | "timestamptz" -> Some STDateTime
    | "date" -> Some STDate
    | "interval" -> Some STInterval
    | "regclass" -> Some STRegclass
    | "jsonb" -> Some STJson
    | "json" -> Some STJson
    | "uuid" -> Some STUuid
    | _ -> None

[<SerializeAsObject("type", AllowUnknownType=true)>]
type ValueType<'t> when 't :> ISQLString =
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] VTScalar of Type : 't
    | [<CaseKey("array")>] VTArray of Subtype : 't
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

type NullsOrder =
    | NullsFirst
    | NullsLast
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | NullsFirst -> "NULLS FIRST"
            | NullsLast -> "NULLS LAST"

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
                | Some cols -> cols |> Seq.map toSQLString |> String.concat ", " |> sprintf "(%s)"
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

type [<NoEquality; NoComparison>] LockStrength =
    | LSUpdate
    | LSNoKeyUpdate
    | LSShare
    | LSNoKeyShare
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | LSUpdate -> "UPDATE"
            | LSNoKeyUpdate -> "NO KEY UPDATE"
            | LSShare -> "SHARE"
            | LSNoKeyShare -> "NO KEY SHARE"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] LockWait =
    | LWNoWait
    | LWSkipLocked
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | LWNoWait -> "NO WAIT"
            | LWSkipLocked -> "SKIP LOCKED"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] LockClause =
    { Strength : LockStrength
      Tables : TableName[]
      Waiting : LockWait option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let tableStr =
                if Array.isEmpty this.Tables then
                    ""
                else
                    this.Tables |> Seq.map toSQLString |> String.concat ", " |> sprintf "OF %s"
            String.concatWithWhitespaces [this.Strength.ToSQLString(); tableStr; optionToSQLString this.Waiting]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] OperationTable =
    { Table : TableRef
      Alias : TableName option
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let aliasStr =
                match this.Alias with
                | None -> ""
                | Some alias -> sprintf "AS %s" (toSQLString alias)
            String.concatWithWhitespaces [toSQLString this.Table; aliasStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] FromTable =
    { Table : TableRef
      Alias : TableAlias option
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            String.concatWithWhitespaces [toSQLString this.Table; optionToSQLString this.Alias]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString ()

type [<NoEquality; NoComparison>] UpdateColumnName =
    { Name : ColumnName
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () = this.Name.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

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
                sprintf "(%s) IN (%s)" (e.ToSQLString()) (vals |> Seq.map toSQLString |> String.concat ", ")
            | VENotIn (e, vals) ->
                assert (not <| Array.isEmpty vals)
                sprintf "(%s) NOT IN (%s)" (e.ToSQLString()) (vals |> Seq.map toSQLString |> String.concat ", ")
            | VEInQuery (e, query) -> sprintf "(%s) IN (%s)" (e.ToSQLString()) (query.ToSQLString())
            | VENotInQuery (e, query) -> sprintf "(%s) NOT IN (%s)" (e.ToSQLString()) (query.ToSQLString())
            | VEIsNull a -> sprintf "(%s) IS NULL" (a.ToSQLString())
            | VEIsNotNull a -> sprintf "(%s) IS NOT NULL" (a.ToSQLString())
            | VESpecialFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map toSQLString |> String.concat ", ")
            | VEFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map toSQLString |> String.concat ", ")
            | VEAggFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args.ToSQLString())
            | VECast (e, typ) -> sprintf "(%s) :: %s" (e.ToSQLString()) (typ.ToSQLString())
            | VECase (es, els) ->
                let esStr = es |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToSQLString()) (e.ToSQLString())) |> String.concat " "
                let elsStr =
                    match els with
                    | None -> ""
                    | Some e -> sprintf "ELSE %s" (e.ToSQLString())
                String.concatWithWhitespaces ["CASE"; esStr; elsStr; "END"]
            | VEArray vals -> sprintf "ARRAY[%s]" (vals |> Seq.map toSQLString |> String.concat ", ")
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
                exprs |> Seq.map toSQLString |> String.concat ", "
            | AEDistinct expr -> sprintf "DISTINCT %s" (expr.ToSQLString())
            | AEStar -> "*"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] TableExpr =
    | TESelect of SelectExpr
    | TEFunc of FunctionName * ValueExpr[]
     with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TESelect sel -> sprintf "(%O)" sel
            | TEFunc (name, args) -> sprintf "%s(%s)" (name.ToSQLString()) (args |> Seq.map toSQLString |> String.concat ", ")

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] FromTableExpr =
    { Alias : TableAlias option
      Expression : TableExpr
      Lateral : bool
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let lateralStr =
                if this.Lateral then "LATERAL" else ""
            let aliasStr = optionToSQLString this.Alias
            String.concatWithWhitespaces [lateralStr; toSQLString this.Expression; aliasStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] FromExpr =
    | FTable of FromTable
    | FTableExpr of FromTableExpr
    | FJoin of JoinExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FTable ftable -> ftable.ToSQLString()
            | FTableExpr expr -> expr.ToSQLString()
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
      Locking : LockClause option
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let resultsStr = this.Columns |> Seq.map toSQLString |> String.concat ", "
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
                    sprintf "GROUP BY %s" (this.GroupBy |> Seq.map toSQLString |> String.concat ", ")

            String.concatWithWhitespaces ["SELECT"; resultsStr; fromStr; whereStr; groupByStr; this.OrderLimit.ToSQLString()]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] OrderColumn =
    { Expr : ValueExpr
      Order : SortOrder option
      Nulls : NullsOrder option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let orderStr = optionToSQLString this.Order
            let nullsStr = optionToSQLString this.Nulls
            String.concatWithWhitespaces [this.Expr.ToSQLString(); orderStr; nullsStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] OrderLimitClause =
    { OrderBy : OrderColumn[]
      Limit : ValueExpr option
      Offset : ValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
                let orderByStr =
                    if Array.isEmpty this.OrderBy
                    then ""
                    else sprintf "ORDER BY %s" (this.OrderBy |> Seq.map toSQLString |> String.concat ", ")
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
                    array |> Seq.map toSQLString |> String.concat ", " |> sprintf "(%s)"
                let valuesStr = values |> Seq.map printOne |> String.concat ", "
                sprintf "VALUES %s" valuesStr
            | SSetOp setOp -> setOp.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] CommonTableExpr =
    { Fields : ColumnName[] option
      Materialized : bool option
      Expr : DataExpr
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let fieldsStr =
                match this.Fields with
                | None -> ""
                | Some args ->
                    assert (not (Array.isEmpty args))
                    let argsStr = args |> Seq.map toSQLString |> String.concat ", "
                    sprintf "(%s)" argsStr
            let materialized =
                match this.Materialized with
                | None -> ""
                | Some false -> "NOT MATERIALIZED"
                | Some true -> "MATERIALIZED"
            let exprStr = sprintf "(%s)" (this.Expr.ToSQLString())
            String.concatWithWhitespaces [fieldsStr; "AS"; materialized; exprStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] CommonTableExprs =
    { Recursive : bool
      Exprs : (TableName * CommonTableExpr)[]
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not (Array.isEmpty this.Exprs))
            let oneExpr (name : TableName, cte : CommonTableExpr) =
                sprintf "%s %s" (name.ToSQLString()) (cte.ToSQLString())
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
            let ctesStr = optionToSQLString this.CTEs
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

and [<NoEquality; NoComparison>] InsertValue =
    | IVExpr of ValueExpr
    | IVDefault
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IVExpr e -> e.ToSQLString()
            | IVDefault -> "DEFAULT"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] InsertSource =
    | ISValues of InsertValue[][]
    | ISSelect of SelectExpr
    | ISDefaultValues
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | ISValues values ->
                let renderInsertValue (values : InsertValue[]) =
                    values |> Seq.map toSQLString |> String.concat ", " |> sprintf "(%s)"

                assert (not <| Array.isEmpty values)
                sprintf "VALUES %s" (values |> Seq.map renderInsertValue |> String.concat ", ")
            | ISSelect sel -> sel.ToSQLString()
            | ISDefaultValues -> "DEFAULT VALUES"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] InsertExpr =
    { CTEs : CommonTableExprs option
      Table : OperationTable
      Columns : (obj * ColumnName)[] // obj is extra metadata
      Source : InsertSource
      OnConflict : OnConflictExpr option
      Returning : SelectedColumn[]
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let ctesStr = optionToSQLString this.CTEs
            let returningStr =
                if Array.isEmpty this.Returning then
                    ""
                else
                    let resultsStr = this.Returning |> Seq.map toSQLString |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let insertStr =
                sprintf "INSERT INTO %s (%s) %s"
                    (this.Table.ToSQLString())
                    (this.Columns |> Seq.map (fun (extra, x) -> x.ToSQLString()) |> String.concat ", ")
                    (this.Source.ToSQLString())
            String.concatWithWhitespaces [ctesStr; insertStr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] UpdateAssignExpr =
    | UAESet of UpdateColumnName * InsertValue
    | UAESelect of UpdateColumnName[] * SelectExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | UAESet (name, expr) -> sprintf "%s = %s" (name.ToSQLString()) (expr.ToSQLString())
            | UAESelect (cols, select) ->
                assert (not <| Array.isEmpty cols)
                let colsStr = cols |> Seq.map toSQLString |> String.concat ", "
                sprintf "(%s) = (%O)" colsStr select

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] UpdateExpr =
    { CTEs : CommonTableExprs option
      Table : OperationTable
      Assignments : UpdateAssignExpr[] // obj is extra metadata
      From : FromExpr option
      Where : ValueExpr option
      Returning : SelectedColumn[]
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not <| Array.isEmpty this.Assignments)

            let ctesStr = optionToSQLString this.CTEs
            let assignsStr = this.Assignments |> Seq.map toSQLString |> String.concat ", "
            let fromStr =
                match this.From with
                | Some f -> sprintf "FROM %s" (f.ToSQLString())
                | None -> ""
            let condExpr =
                match this.Where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            let returningStr =
                if Array.isEmpty this.Returning then
                    ""
                else
                    let resultsStr = this.Returning |> Seq.map toSQLString |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let updateStr = sprintf "UPDATE %s SET %s" (this.Table.ToSQLString()) assignsStr
            String.concatWithWhitespaces [ctesStr; updateStr; fromStr; condExpr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] DeleteExpr =
    { CTEs : CommonTableExprs option
      Table : OperationTable
      Using : FromExpr option
      Where : ValueExpr option
      Returning : SelectedColumn[]
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let ctesStr = optionToSQLString this.CTEs
            let usingStr =
                match this.Using with
                | Some f -> sprintf "USING %s" (f.ToSQLString())
                | None -> ""
            let condExpr =
                match this.Where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            let returningStr =
                if Array.isEmpty this.Returning then
                    ""
                else
                    let resultsStr = this.Returning |> Seq.map toSQLString |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let deleteStr = sprintf "DELETE FROM %s" (this.Table.ToSQLString())
            String.concatWithWhitespaces [ctesStr; deleteStr; usingStr; condExpr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] DataExpr =
    | DESelect of SelectExpr
    | DEInsert of InsertExpr
    | DEUpdate of UpdateExpr
    | DEDelete of DeleteExpr
     with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | DESelect sel -> sel.ToSQLString()
            | DEInsert ins -> ins.ToSQLString()
            | DEUpdate upd -> upd.ToSQLString()
            | DEDelete del -> del.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<StructuralEquality; NoComparison>] IndexKey =
    | IKColumn of ColumnName
    | IKExpression of StringComparable<ValueExpr>
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IKColumn col -> col.ToSQLString()
            | IKExpression expr -> sprintf "(%O)" expr

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] OnConflictExpr =
    { Target : ConflictTarget option
      Action : ConflictAction
    } with
    override this.ToString () = this.ToSQLString()

    member this.ToSQLString () =
        String.concatWithWhitespaces ["ON CONFLICT"; optionToSQLString this.Target; this.Action.ToSQLString()]

    interface ISQLString with
        member this.ToSQLString () = this.ToSQLString()

and [<StructuralEquality; NoComparison>] ConflictColumn =
    { Key : IndexKey
      OpClass : OpClassName option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let opClassStr = optionToSQLString this.OpClass
            String.concatWithWhitespaces [this.Key.ToSQLString(); opClassStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] ConflictColumnsTarget =
    { Columns : ConflictColumn[]
      Predicate : ValueExpr option
    } with
    override this.ToString () = this.ToSQLString()

    member this.ToSQLString () =
        let columnsStr =
            assert (not <| Array.isEmpty this.Columns)
            this.Columns |> Seq.map toSQLString |> String.concat ", " |> sprintf "(%s)"
        let predStr =
            match this.Predicate with
            | None -> ""
            | Some pred -> sprintf "WHERE %O" pred
        String.concatWithWhitespaces [columnsStr; predStr]

    interface ISQLString with
        member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] ConflictTarget =
    | CTColumns of ConflictColumnsTarget
    | CTConstraint of ConstraintName
     with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | CTColumns cols -> cols.ToSQLString()
            | CTConstraint constr -> sprintf "ON CONSTRAINT %s" (constr.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] ConflictAction =
    | CANothing
    | CAUpdate of UpdateConflictAction
     with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | CANothing -> "DO NOTHING"
            | CAUpdate update -> sprintf "DO UPDATE %O" update

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

and [<NoEquality; NoComparison>] UpdateConflictAction =
    { Assignments : UpdateAssignExpr[]
      Where : ValueExpr option
    } with
    override this.ToString () = this.ToSQLString()

    member this.ToSQLString () =
        let valuesExpr = this.Assignments |> Seq.map toSQLString |> String.concat ", "
        let condExpr =
            match this.Where with
            | Some c -> sprintf "WHERE %s" (c.ToSQLString())
            | None -> ""
        let updateStr = sprintf "SET %s" valuesExpr
        String.concatWithWhitespaces [updateStr; condExpr]

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

let nullExpr = VEValue VNull

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
    | AVNull -> nullExpr
    | AVValue v -> VEValue (constr v)

and normalizeArray (constr : 'a -> Value) (arr : ArrayValue<'a>[]) : ValueExpr =
    VEArray (Array.map (normalizeArrayValue constr) arr)

let parseIntValue = function
    | VInt i -> int64 i
    | VBigInt i -> i
    | ret -> failwithf "Non-integer result: %O" ret

let parseSmallIntValue = function
    | VInt i -> i
    | VBigInt i -> int i
    | ret -> failwithf "Non-integer result: %O" ret

let parseStringValue = function
    | VString s -> s
    | ret -> failwithf "Non-string result: %O" ret

let parseBoolValue = function
    | VBool b -> b
    | ret -> failwithf "Non-boolean result: %O" ret

let emptySingleSelectExpr : SingleSelectExpr =
    { Columns = [||]
      From = None
      Where = None
      GroupBy = [||]
      OrderLimit = emptyOrderLimitClause
      Locking = None
      Extra = null
    }

let insertExpr (table : OperationTable) (source : InsertSource) : InsertExpr =
    { CTEs = None
      Table = table
      Columns = [||]
      Source = source
      Returning = [||]
      OnConflict = None
      Extra = null
    }

let updateExpr (table : OperationTable) : UpdateExpr =
    { CTEs = None
      Table = table
      Assignments = [||]
      From = None
      Where = None
      Returning = [||]
      Extra = null
    }

let deleteExpr (table : OperationTable) : DeleteExpr =
    { CTEs = None
      Table = table
      Using = None
      Where = None
      Returning = [||]
      Extra = null
    }

let subSelectExpr (alias : TableAlias) (select : SelectExpr) : FromTableExpr =
    { Alias = Some alias
      Expression = TESelect select
      Lateral = false
    }

let selectExpr (tree : SelectTreeExpr) : SelectExpr =
    { CTEs = None
      Tree = tree
      Extra = null
    }

let fromTable (ref : TableRef) : FromTable =
    { Extra = null
      Alias = None
      Table = ref
    }

let operationTable (ref : TableRef) : OperationTable =
    { Extra = null
      Alias = None
      Table = ref
    }

let updateColumnName (name : ColumnName) : UpdateColumnName =
    { Name = name
      Extra = null
    }

let commonTableExprs (exprs : (TableName * CommonTableExpr)[]) : CommonTableExprs =
    { Recursive = false
      Exprs = exprs
    }

let commonTableExpr (expr : DataExpr) : CommonTableExpr =
    { Fields = None
      Materialized = None
      Expr = expr
    }

let fromTableExpr (expr : TableExpr) : FromTableExpr =
    { Expression = expr
      Alias = None
      Lateral = false
    }

let operationTableRef (opTable : OperationTable) : TableRef =
    match opTable.Alias with
    | Some alias -> { Schema = None; Name = alias }
    | None -> opTable.Table