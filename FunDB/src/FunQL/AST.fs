module FunWithFlags.FunDB.FunQL.AST

open System
open System.ComponentModel
open Microsoft.FSharp.Reflection
open System.Threading.Tasks
open NpgsqlTypes
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.SQL.Utils

module SQL = FunWithFlags.FunDB.SQL.AST

type IFunQLName =
    interface
        inherit IFunQLString

        abstract member ToName : unit -> FunQLName
    end

and [<TypeConverter(typeof<NewtypeConverter<FunQLName>>)>] [<Struct>] FunQLName = FunQLName of string
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
type IndexName = FunQLName
type AttributeName = FunQLName
type ArgumentName = FunQLName
type UserViewName = FunQLName
type RoleName = FunQLName
type FunctionName = FunQLName
type TriggerName = FunQLName
type ActionName = FunQLName

type EntityRef =
    { Schema : SchemaName option
      Name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.Schema with
            | None -> this.Name.ToFunQLString()
            | Some x -> sprintf "%s.%s" (x.ToFunQLString()) (this.Name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.Name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type UserViewRef = EntityRef

type ResolvedEntityRef =
    { Schema : SchemaName
      Name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.Schema.ToFunQLString()) (this.Name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.Name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

let relaxEntityRef (ref : ResolvedEntityRef) : EntityRef =
    { Schema = Some ref.Schema; Name = ref.Name }

let tryResolveEntityRef (ref : EntityRef) : ResolvedEntityRef option =
    Option.map (fun schema -> { Schema = schema; Name = ref.Name }) ref.Schema

type ResolvedUserViewRef = ResolvedEntityRef

type FieldRef =
    { Entity : EntityRef option
      Name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.Entity with
            | None -> this.Name.ToFunQLString()
            | Some entity -> sprintf "%s.%s" (entity.ToFunQLString()) (this.Name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.Name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type ResolvedFieldRef =
    { Entity : ResolvedEntityRef
      Name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.Entity.ToFunQLString()) (this.Name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.Name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

let relaxFieldRef (ref : ResolvedFieldRef) : FieldRef =
    { Entity = Some <| relaxEntityRef ref.Entity; Name = ref.Name }

let tryResolveFieldRef (ref : FieldRef) : ResolvedFieldRef option =
    ref.Entity |> Option.bind (tryResolveEntityRef >> Option.map (fun entity -> { Entity = entity; Name = ref.Name }))

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
    | FDateTime of NpgsqlDateTime
    | FDate of NpgsqlDate
    | FInterval of NpgsqlTimeSpan
    | FJson of JToken
    | FUuid of Guid
    | FUserViewRef of UserViewRef
    | FIntArray of int[]
    | FDecimalArray of decimal[]
    | FStringArray of string[]
    | FBoolArray of bool[]
    | FDateTimeArray of NpgsqlDateTime[]
    | FDateArray of NpgsqlDate[]
    | FIntervalArray of NpgsqlTimeSpan[]
    | FJsonArray of JToken[]
    | FUserViewRefArray of UserViewRef[]
    | FUuidArray of Guid[]
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
            | FDateTime dt -> renderFunQLDateTime dt
            | FDate d -> renderFunQLDate d
            | FInterval int -> renderFunQLInterval int
            | FJson j -> renderFunQLJson j
            | FUserViewRef r -> sprintf "&%s" (r.ToFunQLString())
            | FUuid j -> renderFunQLUuid j
            | FIntArray vals -> renderArray renderFunQLInt "int" vals
            | FDecimalArray vals -> renderArray renderFunQLDecimal "decimal" vals
            | FStringArray vals -> renderArray renderFunQLString "string" vals
            | FBoolArray vals -> renderArray renderFunQLBool "bool" vals
            | FDateTimeArray vals -> renderArray (string >> renderFunQLString) "datetime" vals
            | FDateArray vals -> renderArray (string >> renderFunQLString) "date" vals
            | FIntervalArray vals -> renderArray (string >> renderFunQLString) "interval" vals
            | FJsonArray vals -> renderArray renderFunQLJson "json" vals
            | FUserViewRefArray vals -> renderArray (fun (r : EntityRef) -> sprintf "&%s" (r.ToFunQLString())) "uvref" vals
            | FUuidArray vals -> renderArray renderFunQLUuid "uuid" vals
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
        | FDateTime dt -> writer.WriteValue(dt.ToDateTime())
        | FDate dt -> writer.WriteValue(dt.ToString())
        | FInterval int -> writer.WriteValue(int.ToString())
        | FJson j -> j.WriteTo(writer)
        | FUserViewRef r -> serialize r
        | FUuid uuid -> writer.WriteValue(uuid)
        | FIntArray vals -> serialize vals
        | FDecimalArray vals -> serialize vals
        | FStringArray vals -> serialize vals
        | FBoolArray vals -> serialize vals
        | FDateTimeArray vals -> serialize vals
        | FDateArray vals -> serialize vals
        | FIntervalArray vals -> serialize vals
        | FJsonArray vals -> serialize vals
        | FUserViewRefArray vals -> serialize vals
        | FUuidArray vals -> serialize vals
        | FNull -> writer.WriteNull()

type ScalarFieldType =
    | [<CaseName("int")>] SFTInt
    | [<CaseName("decimal")>] SFTDecimal
    | [<CaseName("string")>] SFTString
    | [<CaseName("bool")>] SFTBool
    | [<CaseName("datetime")>] SFTDateTime
    | [<CaseName("date")>] SFTDate
    | [<CaseName("interval")>] SFTInterval
    | [<CaseName("json")>] SFTJson
    | [<CaseName("uvref")>] SFTUserViewRef
    | [<CaseName("uuid")>] SFTUuid
    with
        static member private Fields = unionNames (unionCases typeof<ScalarFieldType>) |> Map.mapWithKeys (fun name case -> (case.Info.Name, Option.get name))

        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let (case, _) = FSharpValue.GetUnionFields(this, typeof<ScalarFieldType>)
            Map.find case.Name ScalarFieldType.Fields

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

[<SerializeAsObject("type")>]
type FieldExprType =
    | [<CaseName(null)>] FETScalar of Type : ScalarFieldType
    | [<CaseName("array")>] FETArray of Subtype : ScalarFieldType
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FETScalar s -> s.ToFunQLString()
            | FETArray valType -> sprintf "array(%s)" (valType.ToFunQLString())

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
    | [<CaseName("asc")>] Asc
    | [<CaseName("desc")>] Desc
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | Asc -> "ASC"
            | Desc -> "DESC"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type LinkedRef<'f> when 'f :> IFunQLName =
    { Ref : 'f
      Path : FieldName[]
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            Seq.append (Seq.singleton <| this.Ref.ToFunQLString()) (Seq.collect (fun (p : FieldName) -> ["=>"; p.ToFunQLString()]) this.Path) |> String.concat ""

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () =
            if not (Array.isEmpty this.Path) then
                Array.last this.Path
            else
                this.Ref.ToName ()

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
    { Ref : EntityRef
      Extra : ObjectMap
    }

type EntityAlias =
    { Name : EntityName
      Fields : FieldName[] option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let columnsStr =
                match this.Fields with
                | None -> ""
                | Some cols -> cols |> Array.map (fun x -> x.ToFunQLString()) |> String.concat ", " |> sprintf "(%s)"
            String.concatWithWhitespaces ["AS"; this.Name.ToFunQLString(); columnsStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

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
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
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

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

type [<NoEquality; NoComparison; SerializeAsObject("type")>] FieldType<'e> when 'e :> IFunQLName =
    | [<CaseName(null, InnerObject=true)>] FTType of FieldExprType
    | [<CaseName("reference")>] FTReference of reference : 'e
    | [<CaseName("enum")>] FTEnum of values : Set<string>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FTType t -> t.ToFunQLString()
            | FTReference e -> sprintf "reference(%s)" (e.ToFunQLString())
            | FTEnum vals -> sprintf "enum(%s)" (vals |> Seq.map renderFunQLString |> String.concat ", ")

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and AttributeMap<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName = Map<AttributeName, FieldExpr<'e, 'f>>

and FieldExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | FEValue of FieldValue
    | FERef of 'f
    | FENot of FieldExpr<'e, 'f>
    | FEAnd of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEOr of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEBinaryOp of FieldExpr<'e, 'f> * BinaryOperator * FieldExpr<'e, 'f>
    | FEDistinct of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FENotDistinct of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FESimilarTo of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FENotSimilarTo of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>
    | FEIn of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>[]
    | FENotIn of FieldExpr<'e, 'f> * FieldExpr<'e, 'f>[]
    | FEInQuery of FieldExpr<'e, 'f> * SelectExpr<'e, 'f>
    | FENotInQuery of FieldExpr<'e, 'f> * SelectExpr<'e, 'f>
    | FEAny of FieldExpr<'e, 'f> * BinaryOperator * FieldExpr<'e, 'f>
    | FEAll of FieldExpr<'e, 'f> * BinaryOperator * FieldExpr<'e, 'f>
    | FECast of FieldExpr<'e, 'f> * FieldExprType
    | FEIsNull of FieldExpr<'e, 'f>
    | FEIsNotNull of FieldExpr<'e, 'f>
    | FECase of (FieldExpr<'e, 'f> * FieldExpr<'e, 'f>)[] * (FieldExpr<'e, 'f> option)
    | FEJsonArray of FieldExpr<'e, 'f>[]
    | FEJsonObject of Map<FunQLName, FieldExpr<'e, 'f>>
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
            | FEBinaryOp (a, op, b) -> sprintf "(%s) %s (%s)" (a.ToFunQLString()) (op.ToFunQLString()) (b.ToFunQLString())
            | FEDistinct (a, b) -> sprintf "(%s) IS DISTINCT FROM (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FENotDistinct (a, b) -> sprintf "(%s) IS NOT DISTINCT FROM (%s)" (a.ToFunQLString()) (b.ToFunQLString())
            | FESimilarTo (e, pat) -> sprintf "(%s) SIMILAR TO (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
            | FENotSimilarTo (e, pat) -> sprintf "(%s) NOT SIMILAR TO (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
            | FEIn (e, vals) ->
                assert (not <| Array.isEmpty vals)
                sprintf "(%s) IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
            | FENotIn (e, vals) ->
                assert (not <| Array.isEmpty vals)
                sprintf "(%s) NOT IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
            | FEInQuery (e, query) -> sprintf "(%s) IN (%s)" (e.ToFunQLString()) (query.ToFunQLString())
            | FENotInQuery (e, query) -> sprintf "(%s) NOT IN (%s)" (e.ToFunQLString()) (query.ToFunQLString())
            | FEAny (e, op, arr) -> sprintf "(%s) %s ANY (%s)" (e.ToFunQLString()) (op.ToFunQLString()) (arr.ToFunQLString())
            | FEAll (e, op, arr) -> sprintf "(%s) %s ALL (%s)" (e.ToFunQLString()) (op.ToFunQLString()) (arr.ToFunQLString())
            | FECast (e, typ) -> sprintf "(%s) :: %s" (e.ToFunQLString()) (typ.ToFunQLString())
            | FEIsNull e -> sprintf "(%s) IS NULL" (e.ToFunQLString())
            | FEIsNotNull e -> sprintf "(%s) IS NOT NULL" (e.ToFunQLString())
            | FECase (es, els) ->
                let esStr = es |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToFunQLString()) (e.ToFunQLString())) |> String.concat " "
                let elsStr =
                    match els with
                    | None -> ""
                    | Some e -> sprintf "ELSE %s" (e.ToFunQLString())
                String.concatWithWhitespaces ["CASE"; esStr; elsStr; "END"]
            | FEJsonArray vals -> vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", " |> sprintf "[%s]"
            | FEJsonObject obj -> obj |> Map.toSeq |> Seq.map (fun (k, v) -> sprintf "%s: %s" (k.ToFunQLString()) (v.ToFunQLString())) |> String.concat ", " |> sprintf "{%s}"
            | FEFunc (name, args) -> sprintf "%s(%s)" (name.ToFunQLString()) (args |> Seq.map (fun arg -> arg.ToFunQLString()) |> String.concat ", ")
            | FEAggFunc (name, args) -> sprintf "%s(%s)" (name.ToFunQLString()) (args.ToFunQLString())
            | FESubquery q -> sprintf "(%s)" (q.ToFunQLString())
            | FEInheritedFrom (f, ref) -> sprintf "%s INHERITED FROM %s" (f.ToFunQLString()) (ref.Ref.ToFunQLString())
            | FEOfType (f, ref) -> sprintf "%s OFTYPE %s" (f.ToFunQLString()) (ref.Ref.ToFunQLString())

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
    | QRAll of EntityRef option
    | QRExpr of QueryColumnResult<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | QRAll None -> "*"
            | QRAll (Some ref) -> sprintf "%s.*" (ref.ToFunQLString())
            | QRExpr expr -> expr.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] QueryColumnResult<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Alias : EntityName option
      Attributes : AttributeMap<'e, 'f>
      Result : FieldExpr<'e, 'f>
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let attrsStr =
                if Map.isEmpty this.Attributes
                then ""
                else renderAttributesMap this.Attributes

            let aliasStr =
                match this.Alias with
                | None -> ""
                | Some name -> sprintf "AS %s" (name.ToFunQLString())

            let resultStr = this.Result.ToFunQLString()

            String.concatWithWhitespaces [resultStr; aliasStr; attrsStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.TryToName () =
            match this with
            | { Alias = Some name } -> Some name
            | { Alias = None; Result = FERef c } -> Some <| c.ToName ()
            | _ -> None

        interface IFunQLName with
            member this.ToName () = this.TryToName () |> Option.get

and [<NoEquality; NoComparison>] OrderLimitClause<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { OrderBy : (SortOrder * FieldExpr<'e, 'f>)[]
      Limit : FieldExpr<'e, 'f> option
      Offset : FieldExpr<'e, 'f> option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
                let orderByStr =
                    if Array.isEmpty this.OrderBy
                    then ""
                    else sprintf "ORDER BY %s" (this.OrderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (expr.ToFunQLString()) (ord.ToFunQLString())) |> String.concat ", ")
                let limitStr =
                    match this.Limit with
                    | Some e -> sprintf "LIMIT %s" (e.ToFunQLString())
                    | None -> ""
                let offsetStr =
                    match this.Offset with
                    | Some e -> sprintf "OFFSET %s" (e.ToFunQLString())
                    | None -> ""
                String.concatWithWhitespaces [orderByStr; limitStr; offsetStr]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] SingleSelectExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Attributes : AttributeMap<'e, 'f>
      Results : QueryResult<'e, 'f>[]
      From : FromExpr<'e, 'f> option
      Where : FieldExpr<'e, 'f> option
      GroupBy : FieldExpr<'e, 'f>[]
      OrderLimit : OrderLimitClause<'e, 'f>
      Extra : ObjectMap
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let attributesStrs = this.Attributes |> Map.toSeq |> Seq.map (fun (name, expr) -> sprintf "@%s = %s" (name.ToFunQLString()) (expr.ToFunQLString()))
            let resultsStrs = this.Results |> Seq.map (fun res -> res.ToFunQLString())
            let resultStr = Seq.append attributesStrs resultsStrs |> String.concat ", "
            let fromStr =
                match this.From with
                | None -> ""
                | Some from -> sprintf "FROM %s" (from.ToFunQLString())
            let whereStr =
                match this.Where with
                | None -> ""
                | Some cond -> sprintf "WHERE %s" (cond.ToFunQLString())
            let groupByStr =
                if Array.isEmpty this.GroupBy then
                    ""
                else
                    sprintf "GROUP BY %s" (this.GroupBy |> Array.map (fun x -> x.ToFunQLString()) |> String.concat ", ")

            sprintf "SELECT %s" (String.concatWithWhitespaces [resultStr; fromStr; whereStr; groupByStr; this.OrderLimit.ToFunQLString()])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

and [<NoEquality; NoComparison>] SelectTreeExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | SSelect of SingleSelectExpr<'e, 'f>
    | SValues of FieldExpr<'e, 'f>[][]
    | SSetOp of SetOperationExpr<'e, 'f>
    with
        member this.ToFunQLString () =
            match this with
            | SSelect e -> e.ToFunQLString()
            | SValues values ->
                assert not (Array.isEmpty values)
                let printOne (array : FieldExpr<'e, 'f> array) =
                    assert not (Array.isEmpty array)
                    array |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", " |> sprintf "(%s)"
                let valuesStr = values |> Seq.map printOne |> String.concat ", "
                sprintf "VALUES %s" valuesStr
            | SSetOp setOp -> setOp.ToFunQLString ()

        override this.ToString () = this.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] SelectExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { CTEs : CommonTableExprs<'e, 'f> option
      Tree : SelectTreeExpr<'e, 'f>
      Extra : ObjectMap
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let ctesStr =
                match this.CTEs with
                | None -> ""
                | Some ctes -> ctes.ToFunQLString()
            String.concatWithWhitespaces [ctesStr; this.Tree.ToFunQLString()]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] SetOperationExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Operation : SetOperation
      AllowDuplicates : bool
      A : SelectExpr<'e, 'f>
      B : SelectExpr<'e, 'f>
      OrderLimit : OrderLimitClause<'e, 'f>
    } with
        member this.ToFunQLString () =
            let allowDuplicatesStr = if this.AllowDuplicates then "ALL" else ""
            let aStr = sprintf "(%s)" (this.A.ToFunQLString())
            let bStr = sprintf "(%s)" (this.B.ToFunQLString())
            String.concatWithWhitespaces [aStr; this.Operation.ToFunQLString(); allowDuplicatesStr; bStr; this.OrderLimit.ToFunQLString()]

        override this.ToString () = this.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] FromExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    // We don't allow fields aliasing for entities, because we don't guarantee order of entity fields.
    | FEntity of EntityName option * 'e
    | FJoin of JoinExpr<'e, 'f>
    | FSubExpr of EntityAlias * SelectExpr<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | FEntity (malias, t) ->
                let aliasStr =
                    match malias with
                    | None -> ""
                    | Some alias -> alias.ToFunQLString()
                String.concatWithWhitespaces [t.ToFunQLString(); aliasStr]
            | FSubExpr (alias, expr) ->
                sprintf "(%s) %s" (expr.ToFunQLString()) (alias.ToFunQLString())
            | FJoin join -> join.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and [<NoEquality; NoComparison>] JoinExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Type : JoinType
      A : FromExpr<'e, 'f>
      B : FromExpr<'e, 'f>
      Condition : FieldExpr<'e, 'f>
    }
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            sprintf "(%s %s JOIN %s ON %s)" (this.A.ToFunQLString()) (this.Type.ToFunQLString()) (this.B.ToFunQLString()) (this.Condition.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and CommonTableExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Fields : FieldName[] option
      Expr : SelectExpr<'e, 'f>
      Extra : ObjectMap
    }

and [<NoEquality; NoComparison>] CommonTableExprs<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Recursive : bool
      Exprs : (EntityName * CommonTableExpr<'e, 'f>)[]
      Extra : ObjectMap
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            assert (not (Array.isEmpty this.Exprs))
            let convertOne (name : EntityName, cte : CommonTableExpr<'e, 'f>) =
                let nameStr =
                    match cte.Fields with
                    | None -> name.ToFunQLString()
                    | Some args ->
                        assert (not (Array.isEmpty args))
                        let argsStr = args |> Array.map (fun x -> x.ToFunQLString()) |> String.concat ", "
                        sprintf "%s(%s)" (name.ToFunQLString()) argsStr
                sprintf "%s AS (%s)" nameStr (cte.Expr.ToFunQLString())
            let exprs =
                this.Exprs
                |> Seq.map convertOne
                |> String.concat ", "
            let recursive = if this.Recursive then "RECURSIVE" else ""
            String.concatWithWhitespaces ["WITH"; recursive; exprs]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldTypePrettyConverter () =
    inherit JsonConverter ()

    override this.CanConvert (objectType : Type) =
        isUnionCase<FieldType<_>> objectType

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, serializer : JsonSerializer) : obj =
        raise <| NotImplementedException()

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)
        match castUnion<FieldType<IFunQLName>> value with
        | Some (FTType st) -> serialize st
        | Some (FTReference ref) ->
            [("type", "reference":> obj); ("entity", ref :> obj)] |> Map.ofList |> serialize
        | Some (FTEnum vals) ->
            [("type", "enum" :> obj); ("values", vals :> obj)] |> Map.ofList |> serialize
        | None -> failwith "impossible"

type SubEntityContext =
    | SECInheritedFrom
    | SECOfType

type FieldExprMapper<'e1, 'f1, 'e2, 'f2> when 'e1 :> IFunQLName and 'f1 :> IFunQLName and 'e2 :> IFunQLName and 'f2 :> IFunQLName =
    { Value : FieldValue -> FieldValue
      FieldReference : 'f1 -> 'f2
      Query : SelectExpr<'e1, 'f1> -> SelectExpr<'e2, 'f2>
      Aggregate : AggExpr<'e1, 'f1> -> AggExpr<'e1, 'f1>
      SubEntity : SubEntityContext -> 'f2 -> SubEntityRef -> SubEntityRef
    }

let idFieldExprMapper (fieldReference : 'f1 -> 'f2) (query : SelectExpr<'e1, 'f1> -> SelectExpr<'e2, 'f2>) =
    { Value = id
      FieldReference = fieldReference
      Query = query
      Aggregate = id
      SubEntity = fun _ _ r -> r
    }

let rec mapFieldExpr (mapper : FieldExprMapper<'e1, 'f1, 'e2, 'f2>) : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2> =
    let rec traverse = function
        | FEValue value -> FEValue (mapper.Value value)
        | FERef r -> FERef (mapper.FieldReference r)
        | FENot e -> FENot (traverse e)
        | FEAnd (a, b) -> FEAnd (traverse a, traverse b)
        | FEOr (a, b) -> FEOr (traverse a, traverse b)
        | FEDistinct (a, b) -> FEDistinct (traverse a, traverse b)
        | FENotDistinct (a, b) -> FENotDistinct (traverse a, traverse b)
        | FEBinaryOp (a, op, b) -> FEBinaryOp (traverse a, op, traverse b)
        | FESimilarTo (e, pat) -> FESimilarTo (traverse e, traverse pat)
        | FENotSimilarTo (e, pat) -> FENotSimilarTo (traverse e, traverse pat)
        | FEIn (e, vals) -> FEIn (traverse e, Array.map traverse vals)
        | FENotIn (e, vals) -> FENotIn (traverse e, Array.map traverse vals)
        | FEInQuery (e, query) -> FEInQuery (traverse e, mapper.Query query)
        | FENotInQuery (e, query) -> FENotInQuery (traverse e, mapper.Query query)
        | FEAny (e, op, arr) -> FEAny (traverse e, op, traverse arr)
        | FEAll (e, op, arr) -> FEAll (traverse e, op, traverse arr)
        | FECast (e, typ) -> FECast (traverse e, typ)
        | FEIsNull e -> FEIsNull (traverse e)
        | FEIsNotNull e -> FEIsNotNull (traverse e)
        | FECase (es, els) -> FECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | FEJsonArray vals -> FEJsonArray (Array.map traverse vals)
        | FEJsonObject obj -> FEJsonObject (Map.map (fun name -> traverse) obj)
        | FEFunc (name, args) -> FEFunc (name, Array.map traverse args)
        | FEAggFunc (name, args) -> FEAggFunc (name, mapAggExpr traverse (mapper.Aggregate args))
        | FESubquery query -> FESubquery (mapper.Query query)
        | FEInheritedFrom (f, nam) ->
            let ref = mapper.FieldReference f
            FEInheritedFrom (ref, mapper.SubEntity SECInheritedFrom ref nam)
        | FEOfType (f, nam) ->
            let ref = mapper.FieldReference f
            FEOfType (ref, mapper.SubEntity SECOfType ref nam)
    traverse

and mapAggExpr (func : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>) : AggExpr<'e1, 'f1> -> AggExpr<'e2, 'f2> = function
    | AEAll exprs -> AEAll (Array.map func exprs)
    | AEDistinct expr -> AEDistinct (func expr)
    | AEStar -> AEStar

type FieldExprTaskMapper<'e1, 'f1, 'e2, 'f2> when 'e1 :> IFunQLName and 'f1 :> IFunQLName and 'e2 :> IFunQLName and 'f2 :> IFunQLName =
    { Value : FieldValue -> Task<FieldValue>
      FieldReference : 'f1 -> Task<'f2>
      Query : SelectExpr<'e1, 'f1> -> Task<SelectExpr<'e2, 'f2>>
      Aggregate : AggExpr<'e1, 'f1> -> Task<AggExpr<'e1, 'f1>>
      SubEntity : SubEntityContext -> 'f2 -> SubEntityRef -> Task<SubEntityRef>
    }

let idFieldExprTaskMapper (fieldReference : 'f1 -> Task<'f2>) (query : SelectExpr<'e1, 'f1> -> Task<SelectExpr<'e2, 'f2>>) =
    { Value = Task.result
      FieldReference = fieldReference
      Query = query
      Aggregate = Task.result
      SubEntity = fun _ _ r -> Task.result r
    }

let rec mapTaskFieldExpr (mapper : FieldExprTaskMapper<'e1, 'f1, 'e2, 'f2>) : FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>> =
    let rec traverse = function
        | FEValue value -> Task.map FEValue (mapper.Value value)
        | FERef r -> Task.map FERef (mapper.FieldReference r)
        | FENot e -> Task.map FENot (traverse e)
        | FEAnd (a, b) -> Task.map2 (curry FEAnd) (traverse a) (traverse b)
        | FEOr (a, b) -> Task.map2 (curry FEOr) (traverse a) (traverse b)
        | FEDistinct (a, b) -> Task.map2 (curry FEDistinct) (traverse a) (traverse b)
        | FENotDistinct (a, b) -> Task.map2 (curry FENotDistinct) (traverse a) (traverse b)
        | FEBinaryOp (a, op, b) -> Task.map2 (fun a b -> FEBinaryOp (a, op, b)) (traverse a) (traverse b)
        | FESimilarTo (e, pat) -> Task.map2 (curry FESimilarTo) (traverse e) (traverse pat)
        | FENotSimilarTo (e, pat) -> Task.map2 (curry FENotSimilarTo) (traverse e) (traverse pat)
        | FEIn (e, vals) -> Task.map2 (curry FEIn) (traverse e) (Array.mapTask traverse vals)
        | FENotIn (e, vals) -> Task.map2 (curry FENotIn) (traverse e) (Array.mapTask traverse vals)
        | FEInQuery (e, query) -> Task.map2 (curry FEInQuery) (traverse e) (mapper.Query query)
        | FENotInQuery (e, query) -> Task.map2 (curry FENotInQuery) (traverse e) (mapper.Query query)
        | FEAny (e, op, arr) -> Task.map2 (fun e arr -> FEAny (e, op, arr)) (traverse e) (traverse arr)
        | FEAll (e, op, arr) -> Task.map2 (fun e arr -> FEAll (e, op, arr)) (traverse e) (traverse arr)
        | FECast (e, typ) -> Task.map (fun newE -> FECast (newE, typ)) (traverse e)
        | FEIsNull e -> Task.map FEIsNull (traverse e)
        | FEIsNotNull e -> Task.map FEIsNotNull (traverse e)
        | FECase (es, els) ->
            let mapOne (cond, e) =
                task {
                    let! newCond = traverse cond
                    let! newE = traverse e
                    return (newCond, newE)
                }
            Task.map2 (curry FECase) (Array.mapTask mapOne es) (Option.mapTask traverse els)
        | FEJsonArray vals -> Task.map FEJsonArray (Array.mapTask traverse vals)
        | FEJsonObject obj -> Task.map FEJsonObject (Map.mapTask (fun name -> traverse) obj)
        | FEFunc (name, args) -> Task.map (fun x -> FEFunc (name, x)) (Array.mapTask traverse args)
        | FEAggFunc (name, args) ->
            task {
                let! args1 = mapper.Aggregate args
                return! Task.map (fun x -> FEAggFunc (name, x)) (mapTaskAggExpr traverse args1)
            }
        | FESubquery query -> Task.map FESubquery (mapper.Query query)
        | FEInheritedFrom (f, nam) ->
            task {
                let! field = mapper.FieldReference f
                let! subEntity = mapper.SubEntity SECInheritedFrom field nam
                return FEInheritedFrom (field, subEntity)
            }
        | FEOfType (f, nam) ->
            task {
                let! field = mapper.FieldReference f
                let! subEntity = mapper.SubEntity SECOfType field nam
                return FEOfType (field, subEntity)
            }
    traverse

and mapTaskAggExpr (func : FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>>) : AggExpr<'e1, 'f1> -> Task<AggExpr<'e2, 'f2>> = function
    | AEAll exprs -> Task.map AEAll (Array.mapTask func exprs)
    | AEDistinct expr -> Task.map AEDistinct (func expr)
    | AEStar -> Task.result AEStar

type FieldExprIter<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    { Value : FieldValue -> unit
      FieldReference : 'f -> unit
      Query : SelectExpr<'e, 'f> -> unit
      Aggregate : AggExpr<'e, 'f> -> unit
      SubEntity : SubEntityContext -> 'f -> SubEntityRef -> unit
    }

let idFieldExprIter =
    { Value = fun _ -> ()
      FieldReference = fun _ -> ()
      Query = fun _ -> ()
      Aggregate = fun _ -> ()
      SubEntity = fun _ _ _ -> ()
    }

let rec iterFieldExpr (mapper : FieldExprIter<'e, 'f>) : FieldExpr<'e, 'f> -> unit =
    let rec traverse = function
        | FEValue value -> mapper.Value value
        | FERef r -> mapper.FieldReference r
        | FENot e -> traverse e
        | FEAnd (a, b) -> traverse a; traverse b
        | FEOr (a, b) -> traverse a; traverse b
        | FEDistinct (a, b) -> traverse a; traverse b
        | FENotDistinct (a, b) -> traverse a; traverse b
        | FEBinaryOp (a, op, b) -> traverse a; traverse b
        | FESimilarTo (e, pat) -> traverse e; traverse pat
        | FENotSimilarTo (e, pat) -> traverse e; traverse pat
        | FEIn (e, vals) -> traverse e; Array.iter traverse vals
        | FENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | FEInQuery (e, query) -> traverse e; mapper.Query query
        | FENotInQuery (e, query) -> traverse e; mapper.Query query
        | FEAny (e, op, arr) -> traverse e; traverse arr
        | FEAll (e, op, arr) -> traverse e; traverse arr
        | FECast (e, typ) -> traverse e
        | FEIsNull e -> traverse e
        | FEIsNotNull e -> traverse e
        | FECase (es, els) ->
            Array.iter (fun (cond, e) -> traverse cond; traverse e) es
            Option.iter traverse els
        | FEJsonArray vals -> Array.iter traverse vals
        | FEJsonObject obj -> Map.iter (fun name -> traverse) obj
        | FEFunc (name, args) -> Array.iter traverse args
        | FEAggFunc (name, args) ->
            mapper.Aggregate args
            iterAggExpr traverse args
        | FESubquery query -> mapper.Query query
        | FEInheritedFrom (f, nam) -> mapper.FieldReference f; mapper.SubEntity SECInheritedFrom f nam
        | FEOfType (f, nam) -> mapper.FieldReference f; mapper.SubEntity SECOfType f nam
    traverse

and iterAggExpr (func : FieldExpr<'e, 'f> -> unit) : AggExpr<'e, 'f> -> unit = function
    | AEAll exprs -> Array.iter func exprs
    | AEDistinct expr -> func expr
    | AEStar -> ()

let emptyOrderLimitClause = { OrderBy = [||]; Limit = None; Offset = None }

type FunQLVoid = private FunQLVoid of unit with
    interface IFunQLString with
        member this.ToFunQLString () = failwith "impossible"

    interface IFunQLName with
        member this.ToName () = failwith "impossible"

type LinkedFieldRef = LinkedRef<ValueRef<FieldRef>>
type LinkedFieldName = LinkedRef<ValueRef<FieldName>>

type ParsedFieldType = FieldType<EntityRef>
type ResolvedFieldType = FieldType<ResolvedEntityRef>

type LocalFieldExpr = FieldExpr<FunQLVoid, FieldName>

[<NoEquality; NoComparison>]
type BoundRef<'f> when 'f :> IFunQLName =
    { Ref : 'f
      Extra : ObjectMap
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = this.Ref.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        member this.ToName () = this.Ref.ToName ()

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type LinkedBoundFieldRef = BoundRef<LinkedRef<ValueRef<FieldRef>>>

type ResolvedFieldExpr = FieldExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSelectExpr = SelectExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSelectTreeExpr = SelectTreeExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSingleSelectExpr = SingleSelectExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedQueryResult = QueryResult<EntityRef, LinkedBoundFieldRef>
type ResolvedQueryColumnResult = QueryColumnResult<EntityRef, LinkedBoundFieldRef>
type ResolvedCommonTableExpr = CommonTableExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedCommonTableExprs = CommonTableExprs<EntityRef, LinkedBoundFieldRef>
type ResolvedFromExpr = FromExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedAttributeMap = AttributeMap<EntityRef, LinkedBoundFieldRef>
type ResolvedOrderLimitClause = OrderLimitClause<EntityRef, LinkedBoundFieldRef>
type ResolvedAggExpr = AggExpr<EntityRef, LinkedBoundFieldRef>

[<NoEquality; NoComparison>]
type Argument<'e> when 'e :> IFunQLName =
    { ArgType: FieldType<'e>
      Optional: bool
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let typeStr = this.ArgType.ToFunQLString()
            if this.Optional then
                sprintf "%s NULL" typeStr
            else
                typeStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

type ParsedArgument = Argument<EntityRef>
type ResolvedArgument = Argument<ResolvedEntityRef>

let funId = FunQLName "id"
let funSubEntity = FunQLName "sub_entity"
let funSchema = FunQLName "public"
let funView = FunQLName "view"
let funMain = FunQLName "__main"
let funUsers = FunQLName "users"
let funEvents = FunQLName "events"

let systemColumns = Set.ofList [funId; funSubEntity]

type UsedFields = Set<FieldName>
type UsedEntities = Map<EntityName, UsedFields>
type UsedSchemas = Map<SchemaName, UsedEntities>

let usedEntityFields (ref : ResolvedEntityRef) (usedSchemas : UsedSchemas) : UsedFields option =
    match Map.tryFind ref.Schema usedSchemas with
    | None -> None
    | Some usedEntities -> Map.tryFind ref.Name usedEntities

let isFieldUsed (ref : ResolvedFieldRef) (usedSchemas : UsedSchemas) : bool =
    match usedEntityFields ref.Entity usedSchemas with
    | None -> false
    | Some usedFields -> Set.contains ref.Name usedFields

let addUsedEntity (schemaName : SchemaName) (entityName : EntityName) (usedSchemas : UsedSchemas) : UsedSchemas =
    let oldSchema = Map.findWithDefault schemaName (fun () -> Map.empty) usedSchemas
    let oldEntity = Map.findWithDefault entityName (fun () -> Set.empty) oldSchema
    Map.add schemaName (Map.add entityName oldEntity oldSchema) usedSchemas

let addUsedEntityRef (ref : ResolvedEntityRef) =
    addUsedEntity ref.Schema ref.Name

let addUsedField (schemaName : SchemaName) (entityName : EntityName) (fieldName : FieldName) (usedSchemas : UsedSchemas) : UsedSchemas =
    let oldSchema = Map.findWithDefault schemaName (fun () -> Map.empty) usedSchemas
    let oldEntity = Map.findWithDefault entityName (fun () -> Set.empty) oldSchema
    let newEntity = Set.add fieldName oldEntity
    Map.add schemaName (Map.add entityName newEntity oldSchema) usedSchemas

let addUsedFieldRef (ref : ResolvedFieldRef) =
    addUsedField ref.Entity.Schema ref.Entity.Name ref.Name

let mergeUsedSchemas : UsedSchemas -> UsedSchemas -> UsedSchemas =
    Map.unionWith (fun _ -> Map.unionWith (fun _ -> Set.union))

type UsedArguments = Set<Placeholder>

type LocalArgumentsMap = Map<ArgumentName, FieldValue>

// Map of registered global arguments. Should be in sync with RequestContext's globalArguments.
let globalArgumentTypes : Map<ArgumentName, ResolvedArgument> =
    Map.ofSeq
        [ (FunQLName "lang", { ArgType = FTType <| FETScalar SFTString
                               Optional = false })
          (FunQLName "user", { ArgType = FTType <| FETScalar SFTString
                               Optional = false })
          (FunQLName "user_id", { ArgType = FTReference ({ Schema = funSchema; Name = funUsers })
                                  Optional = true })
          (FunQLName "transaction_time", { ArgType = FTType <| FETScalar SFTDateTime
                                           Optional = false })
          (FunQLName "transaction_id", { ArgType = FTType <| FETScalar SFTInt
                                         Optional = false })
        ]

let globalArgumentsMap = globalArgumentTypes |> Map.mapKeys PGlobal

let allowedAggregateFunctions : Map<FunctionName, SQL.FunctionName> =
    Map.ofList
        [ // Generic
          (FunQLName "count", SQL.SQLName "count")
          // Numbers
          (FunQLName "sum", SQL.SQLName "sum")
          (FunQLName "avg", SQL.SQLName "avg")
          (FunQLName "min", SQL.SQLName "min")
          (FunQLName "max", SQL.SQLName "max")
          // Booleans
          (FunQLName "bool_and", SQL.SQLName "bool_and")
          (FunQLName "every", SQL.SQLName "every")
          // Strings
          (FunQLName "string_agg", SQL.SQLName "string_agg")
          // JSON
          (FunQLName "json_agg", SQL.SQLName "jsonb_agg")
          (FunQLName "json_object_agg", SQL.SQLName "jsonb_object_agg")
        ]

type FunctionRepr =
    | FRFunction of SQL.SQLName
    | FRSpecial of SQL.SpecialFunction

let allowedFunctions : Map<FunctionName, FunctionRepr> =
    Map.ofList
        [ // Numbers
          (FunQLName "abs", FRFunction <| SQL.SQLName "abs")
          (FunQLName "isfinite", FRFunction <| SQL.SQLName "isfinite")
          (FunQLName "round", FRFunction <| SQL.SQLName "round")
          // Strings
          (FunQLName "to_char", FRFunction <| SQL.SQLName "to_char")
          // Dates
          (FunQLName "age", FRFunction <| SQL.SQLName "age")
          (FunQLName "date_part", FRFunction <| SQL.SQLName "date_part")
          (FunQLName "date_trunc", FRFunction <| SQL.SQLName "date_trunc")
          // Special
          (FunQLName "coalesce", FRSpecial SQL.SFCoalesce)
          (FunQLName "least", FRSpecial SQL.SFLeast)
          (FunQLName "greatest", FRSpecial SQL.SFGreatest)
        ]

let private parseSingleValue (constrFunc : 'A -> FieldValue option) (isNullable : bool) (tok: JToken) : FieldValue option =
    if tok.Type = JTokenType.Null then
        if isNullable then
            Some FNull
        else
            None
    else
        try
            constrFunc <| tok.ToObject()
        with
        | :? JsonSerializationException -> None

let private parseValueFromJson' (fieldExprType : FieldExprType) : bool -> JToken -> FieldValue option =
    let parseSingleValueStrict f = parseSingleValue (f >> Some)
    match fieldExprType with
    | FETArray SFTString -> parseSingleValueStrict FStringArray
    | FETArray SFTInt -> parseSingleValueStrict FIntArray
    | FETArray SFTDecimal -> parseSingleValueStrict FDecimalArray
    | FETArray SFTBool -> parseSingleValueStrict FBoolArray
    | FETArray SFTDateTime -> parseSingleValue (Array.map convertDateTime >> FDateTimeArray >> Some)
    | FETArray SFTDate -> parseSingleValue (Seq.traverseOption trySqlDate >> Option.map (Array.ofSeq >> FDateArray))
    | FETArray SFTInterval -> parseSingleValue (Seq.traverseOption trySqlInterval >> Option.map (Array.ofSeq >> FIntervalArray))
    | FETArray SFTJson -> parseSingleValueStrict FJsonArray
    | FETArray SFTUserViewRef -> parseSingleValueStrict FUserViewRefArray
    | FETArray SFTUuid -> parseSingleValueStrict FUuidArray
    | FETScalar SFTString -> parseSingleValueStrict FString
    | FETScalar SFTInt -> parseSingleValueStrict FInt
    | FETScalar SFTDecimal -> parseSingleValueStrict FDecimal
    | FETScalar SFTBool -> parseSingleValueStrict FBool
    | FETScalar SFTDateTime -> parseSingleValue (convertDateTime >> FDateTime >> Some)
    | FETScalar SFTDate -> parseSingleValue (trySqlDate >> Option.map FDate)
    | FETScalar SFTInterval -> parseSingleValue (trySqlInterval >> Option.map FInterval)
    | FETScalar SFTJson -> parseSingleValueStrict FJson
    | FETScalar SFTUserViewRef -> parseSingleValueStrict FUserViewRef
    | FETScalar SFTUuid -> parseSingleValueStrict FUuid

let parseValueFromJson (fieldType : FieldType<_>) (isNullable : bool) (tok : JToken) : FieldValue option =
    match fieldType with
    | FTType feType -> parseValueFromJson' feType isNullable tok
    | FTReference ref -> parseSingleValue (FInt >> Some) isNullable tok
    | FTEnum values ->
        let checkAndEncode v =
            if Set.contains v values then
                Some <| FString v
            else
                None
        parseSingleValue checkAndEncode isNullable tok
