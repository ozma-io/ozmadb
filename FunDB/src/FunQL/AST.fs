module FunWithFlags.FunDB.FunQL.AST

open System
open System.ComponentModel
open Newtonsoft.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.Utils

type [<TypeConverter(typeof<ToNewtypeConverter<string>>)>] FunQLName = FunQLName of string
    with
        override this.ToString () =
            match this with
            | FunQLName name -> name

        member this.ToFunQLString () =
            match this with
            | FunQLName c -> renderSqlName c

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type SchemaName = FunQLName
type EntityName = FunQLName
type FieldName = FunQLName
type ConstraintName = FunQLName
type AttributeName = FunQLName

type EntityRef =
    { schema : SchemaName option
      name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.schema with
            | None -> this.name.ToFunQLString()
            | Some x -> sprintf "%s.%s" (x.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ResolvedEntityRef =
    { schema : SchemaName
      name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.schema.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldRef =
    { entity : EntityRef option
      name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.entity with
            | None -> this.name.ToFunQLString()
            | Some entity -> sprintf "%s.%s" (entity.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type [<JsonConverter(typeof<FieldValueConverter>)>] FieldValue =
    | FInt of int
    | FDecimal of decimal
    | FString of string
    | FBool of bool
    | FDateTime of DateTimeOffset
    | FDate of DateTimeOffset
    | FIntArray of int array
    | FDecimalArray of decimal array
    | FStringArray of string array
    | FBoolArray of bool array
    | FDateTimeArray of DateTimeOffset array
    | FDateArray of DateTimeOffset array
    | FNull
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let renderArray func typeName vals =
                let arrStr = sprintf "[%s]" (vals |> Seq.map func |> String.concat ", ")
                if Array.isEmpty vals && typeName <> "string" then
                    sprintf "%s :: array(%s)" arrStr typeName
                else
                    arrStr
            match this with
            | FInt i -> renderSqlInt i
            | FDecimal d -> renderSqlDecimal d
            | FString s -> renderSqlString s
            | FBool b -> renderSqlBool b
            | FDateTime dt -> sprintf "%s :: datetime" (dt |> renderSqlDateTime |> renderSqlString)
            | FDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
            | FIntArray vals -> renderArray renderSqlInt "int" vals
            | FDecimalArray vals -> renderArray renderSqlDecimal "decimal" vals
            | FStringArray vals -> renderArray escapeSqlDoubleQuotes "string" vals
            | FBoolArray vals -> renderArray renderSqlBool "bool" vals
            | FDateTimeArray vals -> renderArray (renderSqlDateTime >> escapeSqlDoubleQuotes) "datetime" vals
            | FDateArray vals -> renderArray (renderSqlDate >> escapeSqlDoubleQuotes) "date" vals
            | FNull -> "NULL"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and FieldValueConverter () =
    inherit JsonConverter<FieldValue> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : FieldValue =
        raise <| NotImplementedException()
 
    override this.WriteJson (writer : JsonWriter, value : FieldValue, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)

        match value with
        | FInt i -> serialize i
        | FDecimal d -> serialize d
        | FString s -> serialize s
        | FBool b -> serialize b
        | FDateTime dt -> serialize <| dt.ToUnixTimeSeconds()
        | FDate dt -> serialize <| dt.ToUnixTimeSeconds()
        | FIntArray vals -> serialize vals
        | FDecimalArray vals -> serialize vals
        | FStringArray vals -> serialize vals
        | FBoolArray vals -> serialize vals
        | FDateTimeArray vals -> serialize <| Array.map (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | FDateArray vals -> serialize <| Array.map (fun (dt : DateTimeOffset) -> dt.ToUnixTimeSeconds()) vals
        | FNull -> serialize null

type [<JsonConverter(typeof<ScalarFieldTypeConverter>)>] ScalarFieldType =
    | SFTInt
    | SFTDecimal
    | SFTString
    | SFTBool
    | SFTDateTime
    | SFTDate
    with
        override this.ToString () = this.ToJSONString()

        member this.ToFunQLString () =
            match this with
            | SFTInt -> "int"
            | SFTDecimal -> "decimal"
            | SFTString -> "string"
            | SFTBool -> "bool"
            | SFTDateTime -> "datetime"
            | SFTDate -> "date"
    
        member this.ToJSONString () = this.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and ScalarFieldTypeConverter () =
    inherit JsonConverter<ScalarFieldType> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : ScalarFieldType =
        raise <| NotImplementedException ()
 
    override this.WriteJson (writer : JsonWriter, value : ScalarFieldType, serializer : JsonSerializer) : unit =
        serializer.Serialize(writer, value.ToJSONString())

type [<JsonConverter(typeof<FieldExprTypeConverter>)>] FieldExprType =
    | FETScalar of ScalarFieldType
    | FETArray of ScalarFieldType
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FETScalar s -> s.ToJSONString()
            | FETArray valType -> sprintf "array(%s)" (valType.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and FieldExprTypeConverter () =
    inherit JsonConverter<FieldExprType> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : FieldExprType =
        raise <| NotImplementedException ()
 
    override this.WriteJson (writer : JsonWriter, value : FieldExprType, serializer : JsonSerializer) : unit =
        let dict =
            match value with
            | FETScalar st -> [("type", st :> obj)]
            | FETArray st -> [("type", "array" :> obj); ("subtype", st.ToJSONString() :> obj)]
        serializer.Serialize(writer, Map.ofList dict)

type [<JsonConverter(typeof<FieldTypeConverter>)>] FieldType<'e, 'f> when 'e :> IFunQLString and 'f :> IFunQLString =
    | FTType of FieldExprType
    | FTReference of 'e * FieldExpr<'f> option
    | FTEnum of Set<string>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FTType t -> t.ToFunQLString()
            | FTReference (e, None) -> sprintf "reference(%s)" (e.ToFunQLString())
            | FTReference (e, Some check) -> sprintf "reference(%s, %s)" (e.ToFunQLString()) (check.ToFunQLString())
            | FTEnum vals -> sprintf "enum(%s)" (vals |> Seq.map (sprintf "\"%s\"" << renderSqlString) |> String.concat ", ")

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and FieldTypeConverter () =
    inherit JsonConverter ()

    override this.CanConvert (objectType : Type) =
        objectType.GetGenericTypeDefinition() = typeof<FieldType<_, _>>

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, serializer : JsonSerializer) : obj =
        raise <| NotImplementedException()
 
    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)
        match castUnion<FieldType<IFunQLString, IFunQLString>> value with
        | Some (FTType st) -> serialize st
        | Some (FTReference (ref, where)) ->
            let cond =
                match where with
                | None -> null
                | Some cond -> cond.ToString()
            [("type", "reference":> obj); ("entity", ref :> obj); ("where", cond :> obj)] |> Map.ofList |> serialize
        | Some (FTEnum vals) ->
            [("type", "enum" :> obj); ("values", vals :> obj)] |> Map.ofList |> serialize
        | None -> failwith "impossible"

and Placeholder =
    | PLocal of string
    | PGlobal of string

and FieldExpr<'f> when 'f :> IFunQLString =
    | FEValue of FieldValue
    | FEColumn of 'f
    | FEPlaceholder of Placeholder
    | FENot of FieldExpr<'f>
    | FEAnd of FieldExpr<'f> * FieldExpr<'f>
    | FEOr of FieldExpr<'f> * FieldExpr<'f>
    | FEConcat of FieldExpr<'f> * FieldExpr<'f>
    | FEEq of FieldExpr<'f> * FieldExpr<'f>
    | FENotEq of FieldExpr<'f> * FieldExpr<'f>
    | FELike of FieldExpr<'f> * FieldExpr<'f>
    | FENotLike of FieldExpr<'f> * FieldExpr<'f>
    | FELess of FieldExpr<'f> * FieldExpr<'f>
    | FELessEq of FieldExpr<'f> * FieldExpr<'f>
    | FEGreater of FieldExpr<'f> * FieldExpr<'f>
    | FEGreaterEq of FieldExpr<'f> * FieldExpr<'f>
    | FEIn of FieldExpr<'f> * (FieldExpr<'f> array)
    | FENotIn of FieldExpr<'f> * (FieldExpr<'f> array)
    | FECast of FieldExpr<'f> * FieldExprType
    | FEIsNull of FieldExpr<'f>
    | FEIsNotNull of FieldExpr<'f>
    | FECase of ((FieldExpr<'f> * FieldExpr<'f>) array) * (FieldExpr<'f> option)
    | FECoalesce of FieldExpr<'f> array
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FEValue value -> value.ToFunQLString()
            | FEColumn c -> c.ToFunQLString()
            | FEPlaceholder (PLocal s) -> sprintf "$%s" (renderSqlName s)
            | FEPlaceholder (PGlobal s) -> sprintf "$$%s" (renderSqlName s)
            | FENot e -> sprintf "NOT (%s)" (e.ToFunQLString())
            | FEAnd (a, b) -> sprintf "(%s) AND (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEOr (a, b) -> sprintf "(%s) OR (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEConcat (a, b) -> sprintf "(%s) || (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEEq (a, b) -> sprintf "(%s) = (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FENotEq (a, b) -> sprintf "(%s) <> (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FELike (e, pat) -> sprintf "(%s) LIKE (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
            | FENotLike (e, pat) -> sprintf "(%s) NOT LIKE (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
            | FELess (a, b) -> sprintf "(%s) < (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FELessEq (a, b) -> sprintf "(%s) <= (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEGreater (a, b) -> sprintf "(%s) > (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEGreaterEq (a, b) -> sprintf "(%s) >= (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEIn (e, vals) ->
                assert (not <| Array.isEmpty vals)
                sprintf "(%s) IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
            | FENotIn (e, vals) ->
                assert (not <| Array.isEmpty vals)
                sprintf "(%s) NOT IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
            | FECast (e, typ) -> sprintf "(%s) :: %s" (e.ToFunQLString()) (typ.ToFunQLString())
            | FEIsNull e -> sprintf "(%s) IS NULL" (e.ToFunQLString())
            | FEIsNotNull e -> sprintf "(%s) IS NOT NULL" (e.ToFunQLString())
            | FECase (es, els) ->
                let esStr = es |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToFunQLString()) (e.ToFunQLString())) |> String.concat " "
                let elsStr =
                    match els with
                    | None -> ""
                    | Some e -> sprintf "ELSE %s" (e.ToFunQLString())
                concatWithWhitespaces ["CASE"; esStr; elsStr; "END"]
            | FECoalesce vals -> 
                assert (not <| Array.isEmpty vals)
                sprintf "COALESCE(%s)" (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

let mapFieldExpr (columnFunc : 'a -> 'b) (placeholderFunc : Placeholder -> Placeholder) : FieldExpr<'a> -> FieldExpr<'b> =
    let rec traverse = function
        | FEValue value -> FEValue value
        | FEColumn c -> FEColumn (columnFunc c)
        | FEPlaceholder s -> FEPlaceholder (placeholderFunc s)
        | FENot e -> FENot (traverse e)
        | FEAnd (a, b) -> FEAnd (traverse a, traverse b)
        | FEOr (a, b) -> FEOr (traverse a, traverse b)
        | FEConcat (a, b) -> FEConcat (traverse a, traverse b)
        | FEEq (a, b) -> FEEq (traverse a, traverse b)
        | FENotEq (a, b) -> FENotEq (traverse a, traverse b)
        | FELike (e, pat) -> FELike (traverse e, traverse pat)
        | FENotLike (e, pat) -> FENotLike (traverse e, traverse pat)
        | FELess (a, b) -> FELess (traverse a, traverse b)
        | FELessEq (a, b) -> FELessEq (traverse a, traverse b)
        | FEGreater (a, b) -> FEGreater (traverse a, traverse b)
        | FEGreaterEq (a, b) -> FEGreaterEq (traverse a, traverse b)
        | FEIn (e, vals) -> FEIn (traverse e, Array.map traverse vals)
        | FENotIn (e, vals) -> FENotIn (traverse e, Array.map traverse vals)
        | FECast (e, typ) -> FECast (traverse e, typ)
        | FEIsNull e -> FEIsNull (traverse e)
        | FEIsNotNull e -> FEIsNotNull (traverse e)
        | FECase (es, els) -> FECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | FECoalesce vals -> FECoalesce (Array.map traverse vals)
    traverse

let iterFieldExpr (colFunc : 'a -> unit) (placeholderFunc : Placeholder -> unit) : FieldExpr<'a> -> unit =
    let rec traverse = function
        | FEValue value -> ()
        | FEColumn c -> colFunc c
        | FEPlaceholder s -> placeholderFunc s
        | FENot e -> traverse e
        | FEAnd (a, b) -> traverse a; traverse b
        | FEOr (a, b) -> traverse a; traverse b
        | FEConcat (a, b) -> traverse a; traverse b
        | FEEq (a, b) -> traverse a; traverse b
        | FENotEq (a, b) -> traverse a; traverse b
        | FELike (e, pat) -> traverse e; traverse pat
        | FENotLike (e, pat) -> traverse e; traverse pat
        | FELess (a, b) -> traverse a; traverse b
        | FELessEq (a, b) -> traverse a; traverse b
        | FEGreater (a, b) -> traverse a; traverse b
        | FEGreaterEq (a, b) -> traverse a; traverse b
        | FEIn (e, vals) -> traverse e; Array.iter traverse vals
        | FENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | FECast (e, typ) -> traverse e
        | FEIsNull e -> traverse e
        | FEIsNotNull e -> traverse e
        | FECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | FECoalesce vals -> Array.iter traverse vals
    traverse

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | Asc -> "ASC"
            | Desc -> "DESC"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type QueryResult<'f> when 'f :> IFunQLString =
    { name : FunQLName
      expression : FieldExpr<'f>
    }
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s AS %s" (this.expression.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type JoinType =
    | Inner
    | Left
    | Right
    | Outer
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | Left -> "LEFT"
            | Right -> "RIGHT"
            | Inner -> "INNER"
            | Outer -> "OUTER"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()
           
type ConditionClause<'e, 'f> when 'e :> IFunQLString and 'f :> IFunQLString =
    { where: FieldExpr<'f> option
      orderBy: (SortOrder * FieldExpr<'f>)[]
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let whereStr =
                match this.where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToFunQLString())
            let orderByStr =
                if Array.isEmpty this.orderBy
                then ""
                else sprintf "ORDER BY %s" (this.orderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (ord.ToFunQLString()) (expr.ToFunQLString())) |> String.concat ", ")
            concatWithWhitespaces [whereStr; orderByStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FromClause<'e, 'f> when 'e :> IFunQLString and 'f :> IFunQLString =
    { from: FromExpr<'e, 'f>
      condition: ConditionClause<'e, 'f>
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
             sprintf "FROM %s" (concatWithWhitespaces [this.from.ToFunQLString(); this.condition.ToFunQLString()])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and QueryExpr<'e, 'f> when 'e :> IFunQLString and 'f :> IFunQLString =
    { results: QueryResult<'f>[]
      clause: FromClause<'e, 'f>
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let resultsStr = this.results |> Seq.map (fun res -> res.ToFunQLString()) |> String.concat ", "
            sprintf "SELECT %s" (concatWithWhitespaces [resultsStr; this.clause.ToFunQLString()])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

and FromExpr<'e, 'f> when 'e :> IFunQLString and 'f :> IFunQLString =
    | FEntity of 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * FieldExpr<'f>
    | FSubExpr of EntityName * QueryExpr<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FEntity e -> e.ToFunQLString()
            | FJoin (joinType, a, b, cond) -> sprintf "%s %s JOIN %s ON %s" (a.ToFunQLString()) (joinType.ToFunQLString()) (b.ToFunQLString()) (cond.ToFunQLString())
            | FSubExpr (name, expr) -> sprintf "(%s) AS %s" (expr.ToFunQLString()) (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type AttributeMap<'f> when 'f :> IFunQLString = Map<AttributeName, FieldExpr<'f>>

type ParsedFieldType = FieldType<EntityRef, FieldRef>

type LocalFieldExpr = FieldExpr<FieldName>

type FunQLVoid = private FunQLVoid of unit with
    interface IFunQLString with
        member this.ToFunQLString () = failwith "impossible"

type PureFieldExpr = FieldExpr<FunQLVoid>

type ViewResults<'f> when 'f :> IFunQLString = (AttributeMap<'f> * QueryResult<'f>)[]

let funId = FunQLName "Id"
let funSchema = FunQLName "public"

let funView = FunQLName "view"