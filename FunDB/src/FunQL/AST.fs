module FunWithFlags.FunDB.FunQL.AST

open System
open System.ComponentModel
open System.Threading.Tasks
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.FunQL.Utils

type IFunQLName =
    interface
        inherit IFunQLString

        abstract member ToName : unit -> FunQLName
    end

and [<TypeConverter(typeof<NewtypeConverter<FunQLName>>)>] FunQLName = FunQLName of string
    with
        override this.ToString () =
            match this with
            | FunQLName name -> name

        member this.ToFunQLString () =
            match this with
            | FunQLName c -> renderFunQLName c

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this

        interface IFunQLName with
            member this.ToName () = this

type SchemaName = FunQLName
type EntityName = FunQLName
type FieldName = FunQLName
type ConstraintName = FunQLName
type AttributeName = FunQLName
type ArgumentName = FunQLName
type UserViewName = FunQLName
type RoleName = FunQLName

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

        member this.ToName () = this.name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type UserViewRef = EntityRef

type ResolvedEntityRef =
    { schema : SchemaName
      name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.schema.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

let relaxEntityRef (ref : ResolvedEntityRef) : EntityRef =
    { schema = Some ref.schema; name = ref.name }

let tryResolveEntityRef (ref : EntityRef) : ResolvedEntityRef option =
    Option.map (fun schema -> { schema = schema; name = ref.name }) ref.schema

type ResolvedUserViewRef = ResolvedEntityRef

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

        member this.ToName () = this.name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type ResolvedFieldRef =
    { entity : ResolvedEntityRef
      name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.entity.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

let relaxFieldRef (ref : ResolvedFieldRef) : FieldRef =
    { entity = Some <| relaxEntityRef ref.entity; name = ref.name }

let tryResolveFieldRef (ref : FieldRef) : ResolvedFieldRef option =
    ref.entity |> Option.bind (tryResolveEntityRef >> Option.map (fun entity -> { entity = entity; name = ref.name }))

type Placeholder =
    | PLocal of ArgumentName
    | PGlobal of ArgumentName
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | PLocal arg -> sprintf "$%s" (arg.ToFunQLString())
            | PGlobal arg -> sprintf "$$%s" (arg.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

        member this.ToName () =
            match this with
            | PLocal name -> name
            | PGlobal name -> name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type [<NoEquality; NoComparison>] FieldValue =
    | FInt of int
    | FDecimal of decimal
    | FString of string
    | FBool of bool
    | FDateTime of DateTime
    | FDate of DateTime
    | FJson of JToken
    | FUserViewRef of UserViewRef
    | FIntArray of int[]
    | FDecimalArray of decimal[]
    | FStringArray of string[]
    | FBoolArray of bool[]
    | FDateTimeArray of DateTime[]
    | FDateArray of DateTime[]
    | FJsonArray of JToken[]
    | FUserViewRefArray of UserViewRef[]
    | FNull
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let renderArray func typeName vals =
                let arrStr = sprintf "ARRAY[%s]" (vals |> Seq.map func |> String.concat ", ")
                if Array.isEmpty vals && typeName <> "string" then
                    sprintf "%s :: array(%s)" arrStr typeName
                else
                    arrStr
            match this with
            | FInt i -> renderFunQLInt i
            | FDecimal d -> renderFunQLDecimal d
            | FString s -> renderFunQLString s
            | FBool b -> renderFunQLBool b
            | FDateTime dt -> sprintf "%s :: datetime" (dt |> renderFunQLDateTime |> renderFunQLString)
            | FDate d -> sprintf "%s :: date" (d |> renderFunQLDate |> renderFunQLString)
            | FJson j -> renderFunQLJson j
            | FUserViewRef r -> sprintf "&%s" (r.ToFunQLString())
            | FIntArray vals -> renderArray renderFunQLInt "int" vals
            | FDecimalArray vals -> renderArray renderFunQLDecimal "decimal" vals
            | FStringArray vals -> renderArray renderFunQLString "string" vals
            | FBoolArray vals -> renderArray renderFunQLBool "bool" vals
            | FDateTimeArray vals -> renderArray (renderFunQLDateTime >> renderFunQLString) "datetime" vals
            | FDateArray vals -> renderArray (renderFunQLDate >> renderFunQLString) "date" vals
            | FJsonArray vals -> renderArray renderFunQLJson "json" vals
            | FUserViewRefArray vals -> renderArray (fun (r : EntityRef) -> sprintf "&%s" (r.ToFunQLString())) "uvref" vals
            | FNull -> "NULL"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldValuePrettyConverter () =
    inherit JsonConverter<FieldValue> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : FieldValue =
        raise <| NotImplementedException()

    override this.WriteJson (writer : JsonWriter, value : FieldValue, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)

        match value with
        | FInt i -> writer.WriteValue(i)
        | FDecimal d -> writer.WriteValue(d)
        | FString s -> writer.WriteValue(s)
        | FBool b -> writer.WriteValue(b)
        | FDateTime dt -> writer.WriteValue(dt)
        | FDate dt -> writer.WriteValue(dt)
        | FJson j -> j.WriteTo(writer)
        | FUserViewRef r -> serialize r
        | FIntArray vals -> serialize vals
        | FDecimalArray vals -> serialize vals
        | FStringArray vals -> serialize vals
        | FBoolArray vals -> serialize vals
        | FDateTimeArray vals -> serialize vals
        | FDateArray vals -> serialize vals
        | FJsonArray vals -> serialize vals
        | FUserViewRefArray vals -> serialize vals
        | FNull -> writer.WriteNull()

type ScalarFieldType =
    | SFTInt
    | SFTDecimal
    | SFTString
    | SFTBool
    | SFTDateTime
    | SFTDate
    | SFTJson
    | SFTUserViewRef
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | SFTInt -> "int"
            | SFTDecimal -> "decimal"
            | SFTString -> "string"
            | SFTBool -> "bool"
            | SFTDateTime -> "datetime"
            | SFTDate -> "date"
            | SFTJson -> "json"
            | SFTUserViewRef -> "uvref"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ScalarFieldTypePrettyConverter () =
    inherit JsonConverter<ScalarFieldType> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : ScalarFieldType =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : ScalarFieldType, serializer : JsonSerializer) : unit =
        serializer.Serialize(writer, value.ToFunQLString())

type FieldExprType =
    | FETScalar of ScalarFieldType
    | FETArray of ScalarFieldType
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FETScalar s -> s.ToFunQLString()
            | FETArray valType -> sprintf "array(%s)" (valType.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldExprTypePrettyConverter () =
    inherit JsonConverter<FieldExprType> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : FieldExprType =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : FieldExprType, serializer : JsonSerializer) : unit =
        writer.WriteStartObject()
        writer.WritePropertyName("type")
        match value with
        | FETScalar st ->
            serializer.Serialize(writer, st)
        | FETArray st ->
            writer.WriteValue("array")
            writer.WritePropertyName("subtype")
            serializer.Serialize(writer, st)
        writer.WriteEndObject()

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

type SetOperation =
    | Union
    | Intersect
    | Except
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | Union -> "UNION"
            | Intersect -> "INTERSECT"
            | Except -> "EXCEPT"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

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

type LinkedRef<'f> when 'f :> IFunQLName =
    { ref : 'f
      path : FieldName[]
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            Seq.append (Seq.singleton <| this.ref.ToFunQLString()) (Seq.collect (fun (p : FieldName) -> ["=>"; p.ToFunQLString()]) this.path) |> String.concat ""

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () =
            if not (Array.isEmpty this.path) then
                Array.last this.path
            else
                this.ref.ToName ()

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type ValueRef<'f> when 'f :> IFunQLName =
    | VRColumn of 'f
    | VRPlaceholder of Placeholder
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | VRColumn c -> c.ToFunQLString()
            | VRPlaceholder p -> p.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () =
            match this with
            | VRColumn c -> c.ToName ()
            | VRPlaceholder p -> p.ToName ()

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type SubEntityRef =
    { ref : EntityRef
      extra : obj
    }

type [<NoEquality; NoComparison>] FieldType<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | FTType of FieldExprType
    | FTReference of 'e * FieldExpr<'e, 'f> option
    | FTEnum of Set<string>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FTType t -> t.ToFunQLString()
            | FTReference (e, None) -> sprintf "reference(%s)" (e.ToFunQLString())
            | FTReference (e, Some check) -> sprintf "reference(%s, %s)" (e.ToFunQLString()) (check.ToFunQLString())
            | FTEnum vals -> sprintf "enum(%s)" (vals |> Seq.map renderFunQLString |> String.concat ", ")

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and AttributeMap<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName = Map<AttributeName, FieldExpr<'e, 'f>>

and [<NoEquality; NoComparison>] FieldExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | FEValue of FieldValue
    | FERef of 'f
    | FENot of FieldExpr<'e, 'f>
    | FEAnd of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEOr of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEConcat of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEDistinct of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FENotDistinct of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEEq of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FENotEq of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FELike of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FENotLike of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FELess of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FELessEq of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEGreater of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEGreaterEq of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEIn of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>[]
    | FENotIn of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>[]
    | FEInQuery of FieldExpr<'e, 'f> * SelectExpr<'e, 'f>
    | FENotInQuery of FieldExpr<'e, 'f> * SelectExpr<'e, 'f>
    | FECast of FieldExpr<'e, 'f> * FieldExprType
    | FEIsNull of FieldExpr<'e, 'f>
    | FEIsNotNull of FieldExpr<'e, 'f>
    | FECase of (FieldExpr<'e, 'f> * FieldExpr<'e, 'f>)[] * (FieldExpr<'e, 'f> option)
    | FECoalesce of FieldExpr<'e, 'f>[]
    | FEJsonArray of FieldExpr<'e, 'f>[]
    | FEJsonObject of Map<FunQLName, FieldExpr<'e, 'f>>
    | FEJsonArrow of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEJsonTextArrow of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEPlus of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEMinus of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEMultiply of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEDivide of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEFunc of FunQLName * FieldExpr<'e, 'f>[]
    | FEAggFunc of FunQLName * AggExpr<'e, 'f>
    | FESubquery of SelectExpr<'e, 'f>
    | FEInheritedFrom of 'f * SubEntityRef
    | FEOfType of 'f * SubEntityRef
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FEValue value -> value.ToFunQLString()
            | FERef r -> r.ToFunQLString()
            | FENot e -> sprintf "NOT (%s)" (e.ToFunQLString())
            | FEAnd (a, b) -> sprintf "(%s) AND (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEOr (a, b) -> sprintf "(%s) OR (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEConcat (a, b) -> sprintf "(%s) || (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEDistinct (a, b) -> sprintf "(%s) IS DISTINCT FROM (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FENotDistinct (a, b) -> sprintf "(%s) IS NOT DISTINCT FROM (%s)" (a.ToFunQLString()) (b.ToFunQLString())
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
            | FEInQuery (e, query) -> sprintf "(%s) IN (%s)" (e.ToFunQLString()) (query.ToFunQLString())
            | FENotInQuery (e, query) -> sprintf "(%s) NOT IN (%s)" (e.ToFunQLString()) (query.ToFunQLString())
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
            | FEJsonArray vals -> vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", " |> sprintf "[%s]"
            | FEJsonObject obj -> obj |> Map.toSeq |> Seq.map (fun (k, v) -> sprintf "%s: %s" (k.ToFunQLString()) (v.ToFunQLString())) |> String.concat ", " |> sprintf "{%s}"
            | FEJsonArrow (a, b) -> sprintf "(%s)->(%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEJsonTextArrow (a, b) -> sprintf "(%s)->>(%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEPlus (a, b) -> sprintf "(%s) + (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEMinus (a, b) -> sprintf "(%s) - (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEMultiply (a, b) -> sprintf "(%s) * (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEDivide (a, b) -> sprintf "(%s) / (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FEFunc (name, args) -> sprintf "%s(%s)" (name.ToFunQLString()) (args |> Seq.map (fun arg -> arg.ToFunQLString()) |> String.concat ", ")
            | FEAggFunc (name, args) -> sprintf "%s(%s)" (name.ToFunQLString()) (args.ToFunQLString())
            | FESubquery q -> sprintf "(%s)" (q.ToFunQLString())
            | FEInheritedFrom (f, ref) -> sprintf "%s INHERITED FROM %s" (f.ToFunQLString()) (ref.ref.ToFunQLString())
            | FEOfType (f, ref) -> sprintf "%s OFTYPE %s" (f.ToFunQLString()) (ref.ref.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] AggExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | AEAll of FieldExpr<'e, 'f>[]
    | AEDistinct of FieldExpr<'e, 'f>
    | AEStar
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | AEAll exprs ->
                assert (not <| Array.isEmpty exprs)
                exprs |> Array.map (fun x -> x.ToFunQLString()) |> String.concat ", "
            | AEDistinct expr -> sprintf "DISTINCT %s" (expr.ToFunQLString())
            | AEStar -> "*"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] QueryResult<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { attributes : AttributeMap<'e, 'f>
      result : QueryResultExpr<'e, 'f>
    }
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let attrsStr =
                if Map.isEmpty this.attributes
                then ""
                else renderAttributesMap this.attributes

            concatWithWhitespaces [this.result.ToFunQLString(); attrsStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] QueryResultExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | QRExpr of FunQLName option * FieldExpr<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | QRExpr (None, expr) -> expr.ToFunQLString()
            | QRExpr (Some name, expr) -> sprintf "%s AS %s" (expr.ToFunQLString()) (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.TryToName () =
            match this with
            | QRExpr (None, FERef c) -> Some <| c.ToName ()
            | QRExpr (Some name, expr) -> Some name
            | _ -> None

        interface IFunQLName with
            member this.ToName () = this.TryToName () |> Option.get

and [<NoEquality; NoComparison>] OrderLimitClause<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { orderBy : (SortOrder * FieldExpr<'e, 'f>)[]
      limit : FieldExpr<'e, 'f> option
      offset : FieldExpr<'e, 'f> option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
                let orderByStr =
                    if Array.isEmpty this.orderBy
                    then ""
                    else sprintf "ORDER BY %s" (this.orderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (expr.ToFunQLString()) (ord.ToFunQLString())) |> String.concat ", ")
                let limitStr =
                    match this.limit with
                    | Some e -> sprintf "LIMIT %s" (e.ToFunQLString())
                    | None -> ""
                let offsetStr =
                    match this.offset with
                    | Some e -> sprintf "OFFSET %s" (e.ToFunQLString())
                    | None -> ""
                concatWithWhitespaces [orderByStr; limitStr; offsetStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] FromExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | FEntity of EntityName option * 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FSubExpr of EntityName * SelectExpr<'e, 'f>
    | FValues of EntityName * FieldName[] * FieldExpr<'e, 'f>[][]
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FEntity (name, e) ->
                match name with
                | Some n -> sprintf "%s AS %s" (e.ToFunQLString()) (n.ToFunQLString())
                | None ->  e.ToFunQLString()
            | FJoin (joinType, a, b, cond) -> sprintf "(%s) %s JOIN (%s) ON %s" (a.ToFunQLString()) (joinType.ToFunQLString()) (b.ToFunQLString()) (cond.ToFunQLString())
            | FSubExpr (name, expr) ->
                sprintf "(%s) AS %s" (expr.ToFunQLString()) (name.ToFunQLString())
            | FValues (name, fieldNames, values) ->
                assert not (Array.isEmpty values)
                let valuesStr = values |> Seq.map (fun array -> array |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", " |> sprintf "(%s)") |> String.concat ", "
                let fieldNamesStr = fieldNames |> Seq.map (fun n -> n.ToFunQLString()) |> String.concat ", "
                sprintf "(VALUES %s) AS %s (%s)" valuesStr (name.ToFunQLString()) fieldNamesStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] SingleSelectExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { attributes : AttributeMap<'e, 'f>
      results : QueryResult<'e, 'f>[]
      from : FromExpr<'e, 'f> option
      where : FieldExpr<'e, 'f> option
      groupBy : FieldExpr<'e, 'f>[]
      orderLimit : OrderLimitClause<'e, 'f>
      extra : obj
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let attributesStrs = this.attributes |> Map.toSeq |> Seq.map (fun (name, expr) -> sprintf "@%s = %s" (name.ToFunQLString()) (expr.ToFunQLString()))
            let resultsStrs = this.results |> Seq.map (fun res -> res.ToFunQLString())
            let resultStr = Seq.append attributesStrs resultsStrs |> String.concat ", "
            let fromStr =
                match this.from with
                | None -> ""
                | Some from -> sprintf "FROM %s" (from.ToFunQLString())
            let whereStr =
                match this.where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToFunQLString())
            let groupByStr =
                if Array.isEmpty this.groupBy then
                    ""
                else
                    sprintf "GROUP BY %s" (this.groupBy |> Array.map (fun x -> x.ToFunQLString()) |> String.concat ", ")

            sprintf "SELECT %s" (concatWithWhitespaces [resultStr; fromStr; whereStr; groupByStr; this.orderLimit.ToFunQLString()])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

and [<NoEquality; NoComparison>] SelectExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | SSelect of SingleSelectExpr<'e, 'f>
    | SSetOp of SetOperation * SelectExpr<'e, 'f> * SelectExpr<'e, 'f> * OrderLimitClause<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () : string = // FIXME: inference bug?
            match this with
            | SSelect e -> e.ToFunQLString()
            | SSetOp (op, a, b, order) ->
                let setStr = sprintf "(%s) %s (%s)" (a.ToFunQLString()) (op.ToFunQLString()) (b.ToFunQLString())
                concatWithWhitespaces [setStr; order.ToFunQLString()]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldTypePrettyConverter () =
    inherit JsonConverter ()

    override this.CanConvert (objectType : Type) =
        isUnionCase<FieldType<_, _>> objectType

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, serializer : JsonSerializer) : obj =
        raise <| NotImplementedException()

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)
        match castUnion<FieldType<IFunQLName, IFunQLName>> value with
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

type SubEntityContext =
    | SECInheritedFrom
    | SECOfType

type FieldExprMapper<'e1, 'f1, 'e2, 'f2> when 'e1 :> IFunQLName and 'f1 :> IFunQLName and 'e2 :> IFunQLName and 'f2 :> IFunQLName =
    { value : FieldValue -> FieldValue
      fieldReference : 'f1 -> 'f2
      query : SelectExpr<'e1, 'f1> -> SelectExpr<'e2, 'f2>
      aggregate : AggExpr<'e1, 'f1> -> AggExpr<'e1, 'f1>
      subEntity : SubEntityContext -> 'f2 -> SubEntityRef -> SubEntityRef
    }

let idFieldExprMapper (fieldReference : 'f1 -> 'f2) (query : SelectExpr<'e1, 'f1> -> SelectExpr<'e2, 'f2>) =
    { value = id
      fieldReference = fieldReference
      query = query
      aggregate = id
      subEntity = fun _ _ r -> r
    }

let rec mapFieldExpr (mapper : FieldExprMapper<'e1, 'f1, 'e2, 'f2>) : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2> =
    let rec traverse = function
        | FEValue value -> FEValue (mapper.value value)
        | FERef r -> FERef (mapper.fieldReference r)
        | FENot e -> FENot (traverse e)
        | FEAnd (a, b) -> FEAnd (traverse a, traverse b)
        | FEOr (a, b) -> FEOr (traverse a, traverse b)
        | FEConcat (a, b) -> FEConcat (traverse a, traverse b)
        | FEDistinct (a, b) -> FEDistinct (traverse a, traverse b)
        | FENotDistinct (a, b) -> FENotDistinct (traverse a, traverse b)
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
        | FEInQuery (e, query) -> FEInQuery (traverse e, mapper.query query)
        | FENotInQuery (e, query) -> FENotInQuery (traverse e, mapper.query query)
        | FECast (e, typ) -> FECast (traverse e, typ)
        | FEIsNull e -> FEIsNull (traverse e)
        | FEIsNotNull e -> FEIsNotNull (traverse e)
        | FECase (es, els) -> FECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | FECoalesce vals -> FECoalesce (Array.map traverse vals)
        | FEJsonArray vals -> FEJsonArray (Array.map traverse vals)
        | FEJsonObject obj -> FEJsonObject (Map.map (fun name -> traverse) obj)
        | FEJsonArrow (a, b) -> FEJsonArrow (traverse a, traverse b)
        | FEJsonTextArrow (a, b) -> FEJsonTextArrow (traverse a, traverse b)
        | FEPlus (a, b) -> FEPlus (traverse a, traverse b)
        | FEMinus (a, b) -> FEMinus (traverse a, traverse b)
        | FEMultiply (a, b) -> FEMultiply (traverse a, traverse b)
        | FEDivide (a, b) -> FEDivide (traverse a, traverse b)
        | FEFunc (name, args) -> FEFunc (name, Array.map traverse args)
        | FEAggFunc (name, args) -> FEAggFunc (name, mapAggExpr traverse (mapper.aggregate args))
        | FESubquery query -> FESubquery (mapper.query query)
        | FEInheritedFrom (f, nam) ->
            let ref = mapper.fieldReference f
            FEInheritedFrom (ref, mapper.subEntity SECInheritedFrom ref nam)
        | FEOfType (f, nam) ->
            let ref = mapper.fieldReference f
            FEOfType (ref, mapper.subEntity SECOfType ref nam)
    traverse

and mapAggExpr (func : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>) : AggExpr<'e1, 'f1> -> AggExpr<'e2, 'f2> = function
    | AEAll exprs -> AEAll (Array.map func exprs)
    | AEDistinct expr -> AEDistinct (func expr)
    | AEStar -> AEStar

type FieldExprTaskSyncMapper<'e1, 'f1, 'e2, 'f2> when 'e1 :> IFunQLName and 'f1 :> IFunQLName and 'e2 :> IFunQLName and 'f2 :> IFunQLName =
    { value : FieldValue -> Task<FieldValue>
      fieldReference : 'f1 -> Task<'f2>
      query : SelectExpr<'e1, 'f1> -> Task<SelectExpr<'e2, 'f2>>
      aggregate : AggExpr<'e1, 'f1> -> Task<AggExpr<'e1, 'f1>>
      subEntity : SubEntityContext -> 'f2 -> SubEntityRef -> Task<SubEntityRef>
    }

let idFieldExprTaskSyncMapper (fieldReference : 'f1 -> Task<'f2>) (query : SelectExpr<'e1, 'f1> -> Task<SelectExpr<'e2, 'f2>>) =
    { value = Task.result
      fieldReference = fieldReference
      query = query
      aggregate = Task.result
      subEntity = fun _ _ r -> Task.result r
    }

let rec mapTaskSyncFieldExpr (mapper : FieldExprTaskSyncMapper<'e1, 'f1, 'e2, 'f2>) : FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>> =
    let rec traverse = function
        | FEValue value -> Task.map FEValue (mapper.value value)
        | FERef r -> Task.map FERef (mapper.fieldReference r)
        | FENot e -> Task.map FENot (traverse e)
        | FEAnd (a, b) -> Task.map2Sync (curry FEAnd) (traverse a) (traverse b)
        | FEOr (a, b) -> Task.map2Sync (curry FEOr) (traverse a) (traverse b)
        | FEConcat (a, b) -> Task.map2Sync (curry FEConcat) (traverse a) (traverse b)
        | FEDistinct (a, b) -> Task.map2Sync (curry FEDistinct) (traverse a) (traverse b)
        | FENotDistinct (a, b) -> Task.map2Sync (curry FENotDistinct) (traverse a) (traverse b)
        | FEEq (a, b) -> Task.map2Sync (curry FEEq) (traverse a) (traverse b)
        | FENotEq (a, b) -> Task.map2Sync (curry FENotEq) (traverse a) (traverse b)
        | FELike (e, pat) -> Task.map2Sync (curry FELike) (traverse e) (traverse pat)
        | FENotLike (e, pat) -> Task.map2Sync (curry FENotLike) (traverse e) (traverse pat)
        | FELess (a, b) -> Task.map2Sync (curry FELess) (traverse a) (traverse b)
        | FELessEq (a, b) -> Task.map2Sync (curry FELessEq) (traverse a) (traverse b)
        | FEGreater (a, b) -> Task.map2Sync (curry FEGreater) (traverse a) (traverse b)
        | FEGreaterEq (a, b) -> Task.map2Sync (curry FEGreaterEq) (traverse a) (traverse b)
        | FEIn (e, vals) -> Task.map2Sync (curry FEIn) (traverse e) (Array.mapTaskSync traverse vals)
        | FENotIn (e, vals) -> Task.map2Sync (curry FENotIn) (traverse e) (Array.mapTaskSync traverse vals)
        | FEInQuery (e, query) -> Task.map2Sync (curry FEInQuery) (traverse e) (mapper.query query)
        | FENotInQuery (e, query) -> Task.map2Sync (curry FENotInQuery) (traverse e) (mapper.query query)
        | FECast (e, typ) -> Task.map (fun newE -> FECast (newE, typ)) (traverse e)
        | FEIsNull e -> Task.map FEIsNull (traverse e)
        | FEIsNotNull e -> Task.map FEIsNotNull (traverse e)
        | FECase (es, els) ->
            let mapOne (cond, e) = task {
                let! newCond = traverse cond
                let! newE = traverse e
                return (newCond, newE)
            }
            Task.map2Sync (curry FECase) (Array.mapTaskSync mapOne es) (Option.mapTask traverse els)
        | FECoalesce vals -> Task.map FECoalesce (Array.mapTaskSync traverse vals)
        | FEJsonArray vals -> Task.map FEJsonArray (Array.mapTaskSync traverse vals)
        | FEJsonObject obj -> Task.map FEJsonObject (Map.mapTaskSync (fun name -> traverse) obj)
        | FEJsonArrow (a, b) -> Task.map2Sync (curry FEJsonArrow) (traverse a) (traverse b)
        | FEJsonTextArrow (a, b) -> Task.map2Sync (curry FEJsonTextArrow) (traverse a) (traverse b)
        | FEPlus (a, b) -> Task.map2Sync (curry FEPlus) (traverse a) (traverse b)
        | FEMinus (a, b) -> Task.map2Sync (curry FEMinus) (traverse a) (traverse b)
        | FEMultiply (a, b) -> Task.map2Sync (curry FEMultiply) (traverse a) (traverse b)
        | FEDivide (a, b) -> Task.map2Sync (curry FEDivide) (traverse a) (traverse b)
        | FEFunc (name, args) -> Task.map (fun x -> FEFunc (name, x)) (Array.mapTaskSync traverse args)
        | FEAggFunc (name, args) ->
            task {
                let! args1 = mapper.aggregate args
                return! Task.map (fun x -> FEAggFunc (name, x)) (mapTaskSyncAggExpr traverse args1)
            }
        | FESubquery query -> Task.map FESubquery (mapper.query query)
        | FEInheritedFrom (f, nam) ->
            task {
                let! field = mapper.fieldReference f
                let! subEntity = mapper.subEntity SECInheritedFrom field nam
                return FEInheritedFrom (field, subEntity)
            }
        | FEOfType (f, nam) ->
            task {
                let! field = mapper.fieldReference f
                let! subEntity = mapper.subEntity SECOfType field nam
                return FEOfType (field, subEntity)
            }
    traverse

and mapTaskSyncAggExpr (func : FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>>) : AggExpr<'e1, 'f1> -> Task<AggExpr<'e2, 'f2>> = function
    | AEAll exprs -> Task.map AEAll (Array.mapTaskSync func exprs)
    | AEDistinct expr -> Task.map AEDistinct (func expr)
    | AEStar -> Task.result AEStar

type FieldExprIter<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { value : FieldValue -> unit
      fieldReference : 'f -> unit
      query : SelectExpr<'e, 'f> -> unit
      aggregate : AggExpr<'e, 'f> -> unit
      subEntity : SubEntityContext -> 'f -> SubEntityRef -> unit
    }

let idFieldExprIter =
    { value = fun _ -> ()
      fieldReference = fun _ -> ()
      query = fun _ -> ()
      aggregate = fun _ -> ()
      subEntity = fun _ _ _ -> ()
    }

let rec iterFieldExpr (mapper : FieldExprIter<'e, 'f>) : FieldExpr<'e, 'f> -> unit =
    let rec traverse = function
        | FEValue value -> mapper.value value
        | FERef r -> mapper.fieldReference r
        | FENot e -> traverse e
        | FEAnd (a, b) -> traverse a; traverse b
        | FEOr (a, b) -> traverse a; traverse b
        | FEConcat (a, b) -> traverse a; traverse b
        | FEDistinct (a, b) -> traverse a; traverse b
        | FENotDistinct (a, b) -> traverse a; traverse b
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
        | FEInQuery (e, query) -> traverse e; mapper.query query
        | FENotInQuery (e, query) -> traverse e; mapper.query query
        | FECast (e, typ) -> traverse e
        | FEIsNull e -> traverse e
        | FEIsNotNull e -> traverse e
        | FECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | FECoalesce vals -> Array.iter traverse vals
        | FEJsonArray vals -> Array.iter traverse vals
        | FEJsonObject obj -> Map.iter (fun name -> traverse) obj
        | FEJsonArrow (a, b) -> traverse a; traverse b
        | FEJsonTextArrow (a, b) -> traverse a; traverse b
        | FEPlus (a, b) -> traverse a; traverse b
        | FEMinus (a, b) -> traverse a; traverse b
        | FEMultiply (a, b) -> traverse a; traverse b
        | FEDivide (a, b) -> traverse a; traverse b
        | FEFunc (name, args) -> Array.iter traverse args
        | FEAggFunc (name, args) ->
            mapper.aggregate args
            iterAggExpr traverse args
        | FESubquery query -> mapper.query query
        | FEInheritedFrom (f, nam) -> mapper.fieldReference f; mapper.subEntity SECInheritedFrom f nam
        | FEOfType (f, nam) -> mapper.fieldReference f; mapper.subEntity SECOfType f nam
    traverse

and iterAggExpr (func : FieldExpr<'e, 'f> -> unit) : AggExpr<'e, 'f> -> unit = function
    | AEAll exprs -> Array.iter func exprs
    | AEDistinct expr -> func expr
    | AEStar -> ()

let emptyOrderLimitClause = { orderBy = [||]; limit = None; offset = None }

type FunQLVoid = private FunQLVoid of unit with
    interface IFunQLString with
        member this.ToFunQLString () = failwith "impossible"

    interface IFunQLName with
        member this.ToName () = failwith "impossible"

type LinkedFieldRef = LinkedRef<ValueRef<FieldRef>>
type LinkedFieldName = LinkedRef<ValueRef<FieldName>>

type ParsedFieldType = FieldType<EntityRef, LinkedFieldRef>

type LocalFieldExpr = FieldExpr<FunQLVoid, FieldName>

[<NoEquality; NoComparison>]
type BoundField =
    { ref : ResolvedFieldRef
      immediate : bool // Set if field references value from a table directly, not via a subexpression.
    }

[<NoEquality; NoComparison>]
type BoundRef<'f> when 'f :> IFunQLName =
    { ref : 'f
      bound : BoundField option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = this.ref.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.ref.ToName ()

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type LinkedBoundFieldRef = LinkedRef<ValueRef<BoundRef<FieldRef>>>

type ResolvedFieldExpr = FieldExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedSelectExpr = SelectExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedSingleSelectExpr = SingleSelectExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedQueryResult = QueryResult<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedQueryResultExpr = QueryResultExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedFromExpr = FromExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedAttributeMap = AttributeMap<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedOrderLimitClause = OrderLimitClause<ResolvedEntityRef, LinkedBoundFieldRef>

[<NoEquality; NoComparison>]
type Argument<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { argType: FieldType<'e, 'f>
      optional: bool
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let typeStr = this.argType.ToFunQLString()
            if this.optional then
                sprintf "%s NULL" typeStr
            else
                typeStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

type ArgumentFieldType = FieldType<ResolvedEntityRef, FunQLVoid>

type ParsedArgument = Argument<EntityRef, LinkedFieldRef>
type ResolvedArgument = Argument<ResolvedEntityRef, FunQLVoid>

let funId = FunQLName "id"
let funSubEntity = FunQLName "sub_entity"
let funSchema = FunQLName "public"
let funView = FunQLName "view"
let funMain = FunQLName "__main"
let funUsers = FunQLName "users"
let funEvents = FunQLName "events"

type UsedFields = Set<FieldName>
type UsedEntities = Map<EntityName, UsedFields>
type UsedSchemas = Map<SchemaName, UsedEntities>

let addUsedEntity (schemaName : SchemaName) (entityName : EntityName) (usedSchemas : UsedSchemas) : UsedSchemas =
    let oldSchema = Map.findWithDefault schemaName (fun () -> Map.empty) usedSchemas
    let oldEntity = Map.findWithDefault entityName (fun () -> Set.empty) oldSchema
    Map.add schemaName (Map.add entityName oldEntity oldSchema) usedSchemas

let addUsedEntityRef (ref : ResolvedEntityRef) =
    addUsedEntity ref.schema ref.name

let addUsedField (schemaName : SchemaName) (entityName : EntityName) (fieldName : FieldName) (usedSchemas : UsedSchemas) : UsedSchemas =
    assert (fieldName <> funId && fieldName <> funSubEntity)
    let oldSchema = Map.findWithDefault schemaName (fun () -> Map.empty) usedSchemas
    let oldEntity = Map.findWithDefault entityName (fun () -> Set.empty) oldSchema
    let newEntity = Set.add fieldName oldEntity
    Map.add schemaName (Map.add entityName newEntity oldSchema) usedSchemas

let addUsedFieldRef (ref : ResolvedFieldRef) =
    addUsedField ref.entity.schema ref.entity.name ref.name

let mergeUsedSchemas : UsedSchemas -> UsedSchemas -> UsedSchemas =
    Map.unionWith (fun _ -> Map.unionWith (fun _ -> Set.union))

// Map of registered global arguments. Should be in sync with RequestContext's globalArguments.
let globalArgumentTypes : Map<ArgumentName, ResolvedArgument> =
    Map.ofSeq
        [ (FunQLName "lang", { argType = FTType <| FETScalar SFTString
                               optional = false })
          (FunQLName "user", { argType = FTType <| FETScalar SFTString
                               optional = false })
          (FunQLName "user_id", { argType = FTReference ({ schema = funSchema; name = funUsers }, None)
                                  optional = true })
          (FunQLName "transaction_time", { argType = FTType <| FETScalar SFTDateTime
                                           optional = false })
        ]

let globalArgumentsMap = globalArgumentTypes |> Map.mapKeys PGlobal

let allowedAggregateFunctions =
    Set.ofSeq
        [ FunQLName "sum"
          FunQLName "avg"
          FunQLName "min"
          FunQLName "max"
          FunQLName "count"
          FunQLName "bool_and"
        ]

let allowedFunctions =
    Set.ofSeq
        [ FunQLName "abs"
        ]