module OzmaDB.OzmaQL.AST

open System
open System.ComponentModel
open System.Threading.Tasks
open FSharpPlus
open NodaTime
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System.Runtime.Serialization

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Reflection
open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.OzmaUtils.Serialization.Json
open OzmaDB.OzmaQL.Utils
open OzmaDB.SQL.Utils

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Typecheck

type UnexpectedExprException(message: string, innerException: Exception) =
    inherit Exception(message, innerException)

    new(message: string) = UnexpectedExprException(message, null)

type IOzmaQLName =
    interface
        inherit IOzmaQLString

        abstract member ToName: unit -> OzmaQLName
    end

and [<TypeConverter(typeof<NewtypeConverter<OzmaQLName>>); Struct>] OzmaQLName =
    | OzmaQLName of string

    override this.ToString() =
        match this with
        | OzmaQLName name -> name

    member this.ToOzmaQLString() =
        match this with
        | OzmaQLName c -> renderOzmaQLName c

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this

    interface IOzmaQLName with
        member this.ToName() = this

type SchemaName = OzmaQLName
type EntityName = OzmaQLName
type FieldName = OzmaQLName
type ConstraintName = OzmaQLName
type IndexName = OzmaQLName
type AttributeName = OzmaQLName
type ArgumentName = OzmaQLName
type UserViewName = OzmaQLName
type RoleName = OzmaQLName
type FunctionName = OzmaQLName
type TriggerName = OzmaQLName
type ActionName = OzmaQLName
type PragmaName = OzmaQLName

type EntityRef =
    { Schema: SchemaName option
      Name: EntityName }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this.Schema with
        | None -> this.Name.ToOzmaQLString()
        | Some x -> sprintf "%s.%s" (x.ToOzmaQLString()) (this.Name.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this.Name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type UserViewRef = EntityRef

type ResolvedEntityRef =
    { Schema: SchemaName
      Name: EntityName }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        sprintf "%s.%s" (this.Schema.ToOzmaQLString()) (this.Name.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this.Name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

let relaxEntityRef (ref: ResolvedEntityRef) : EntityRef =
    { Schema = Some ref.Schema
      Name = ref.Name }

let getResolvedEntityRef (ref: EntityRef) : ResolvedEntityRef =
    match ref.Schema with
    | Some schema -> { Schema = schema; Name = ref.Name }
    | None -> failwith "No schema specified"

type ResolvedUserViewRef = ResolvedEntityRef

type FieldRef =
    { Entity: EntityRef option
      Name: FieldName }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this.Entity with
        | None -> this.Name.ToOzmaQLString()
        | Some entity -> sprintf "%s.%s" (entity.ToOzmaQLString()) (this.Name.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this.Name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type ResolvedFieldRef =
    { Entity: ResolvedEntityRef
      Name: FieldName }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        sprintf "%s.%s" (this.Entity.ToOzmaQLString()) (this.Name.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this.Name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

let relaxFieldRef (ref: ResolvedFieldRef) : FieldRef =
    { Entity = Some <| relaxEntityRef ref.Entity
      Name = ref.Name }

type ArgumentRef =
    | PLocal of ArgumentName
    | PGlobal of ArgumentName

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | PLocal arg -> sprintf "$%s" (arg.ToOzmaQLString())
        | PGlobal arg -> sprintf "$$%s" (arg.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() =
        match this with
        | PLocal name -> name
        | PGlobal name -> name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type ArgumentRefConverter() =
    inherit JsonConverter<ArgumentRef>()

    override this.CanRead = false

    override this.ReadJson
        (reader: JsonReader, objectType: Type, existingValue, hasExistingValue, serializer: JsonSerializer)
        : ArgumentRef =
        raise <| NotImplementedException()

    override this.WriteJson(writer: JsonWriter, value: ArgumentRef, serializer: JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)

        match value with
        | PLocal name -> writer.WriteValue("$" + string name)
        | PGlobal name -> writer.WriteValue("$$" + string name)

let argumentIsLocal =
    function
    | PLocal name -> true
    | PGlobal name -> false

[<StructuralEquality; NoComparison>]
type FieldValue =
    | FInt of int
    | FDecimal of decimal
    | FString of string
    | FBool of bool
    | FDateTime of Instant
    | FDate of LocalDate
    | FInterval of Period
    | FJson of ComparableJToken
    | FUuid of Guid
    | FUserViewRef of UserViewRef
    | FIntArray of int[]
    | FDecimalArray of decimal[]
    | FStringArray of string[]
    | FBoolArray of bool[]
    | FDateTimeArray of Instant[]
    | FDateArray of LocalDate[]
    | FIntervalArray of Period[]
    | FJsonArray of ComparableJToken[]
    | FUserViewRefArray of UserViewRef[]
    | FUuidArray of Guid[]
    | FNull

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let renderArray func typeName vals =
            let arrStr = sprintf "ARRAY[%s]" (vals |> Seq.map func |> String.concat ", ")

            if Array.isEmpty vals && typeName <> "string" then
                sprintf "%s :: array(%s)" arrStr typeName
            else
                arrStr

        match this with
        | FInt i -> renderOzmaQLInt i
        | FDecimal d -> renderOzmaQLDecimal d
        | FString s -> renderOzmaQLString s
        | FBool b -> renderOzmaQLBool b
        | FDateTime dt -> renderOzmaQLDateTime dt
        | FDate d -> renderOzmaQLDate d
        | FInterval int -> renderOzmaQLInterval int
        | FJson j -> renderOzmaQLJson j.Json
        | FUserViewRef r -> sprintf "&%s" (r.ToOzmaQLString())
        | FUuid u -> renderOzmaQLUuid u
        | FIntArray vals -> renderArray renderOzmaQLInt "int" vals
        | FDecimalArray vals -> renderArray renderOzmaQLDecimal "decimal" vals
        | FStringArray vals -> renderArray renderOzmaQLString "string" vals
        | FBoolArray vals -> renderArray renderOzmaQLBool "bool" vals
        | FDateTimeArray vals -> renderArray (renderSqlDateTime >> renderOzmaQLString) "datetime" vals
        | FDateArray vals -> renderArray (renderSqlDate >> renderOzmaQLString) "date" vals
        | FIntervalArray vals -> renderArray (renderSqlInterval >> renderOzmaQLString) "interval" vals
        | FJsonArray vals -> vals |> renderArray (fun j -> renderOzmaQLJson j.Json) "json"
        | FUserViewRefArray vals -> renderArray (fun (r: EntityRef) -> sprintf "&%s" (r.ToOzmaQLString())) "uvref" vals
        | FUuidArray vals -> renderArray renderOzmaQLUuid "uuid" vals
        | FNull -> "NULL"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type FieldValuePrettyConverter() =
    inherit JsonConverter<FieldValue>()

    override this.CanRead = false

    override this.ReadJson
        (reader: JsonReader, objectType: Type, existingValue, hasExistingValue, serializer: JsonSerializer)
        : FieldValue =
        raise <| NotImplementedException()

    override this.WriteJson(writer: JsonWriter, value: FieldValue, serializer: JsonSerializer) : unit =
        let serialize value = serializer.Serialize(writer, value)

        match value with
        | FInt i -> writer.WriteValue(i)
        | FDecimal d -> writer.WriteValue(d)
        | FString s -> writer.WriteValue(s)
        | FBool b -> writer.WriteValue(b)
        | FDateTime dt -> serialize dt
        | FDate date -> serialize date
        | FInterval int -> serialize int
        | FJson j -> j.Json.WriteTo(writer)
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

[<StructuralEquality; NoComparison>]
type ReferenceDeleteAction =
    | [<CaseKey("noAction")>] RDANoAction
    | [<CaseKey("cascade")>] RDACascade
    | [<CaseKey("setNull")>] RDASetNull
    | [<CaseKey("setDefault")>] RDASetDefault

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | RDANoAction -> "NO ACTION"
        | RDACascade -> "CASCADE"
        | RDASetNull -> "SET NULL"
        | RDASetDefault -> "SET DEFAULT"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

[<StructuralEquality; NoComparison; SerializeAsObject("type")>]
type ScalarFieldType<'e> when 'e :> IOzmaQLName =
    | [<CaseKey("int")>] SFTInt
    | [<CaseKey("decimal")>] SFTDecimal
    | [<CaseKey("string")>] SFTString
    | [<CaseKey("bool")>] SFTBool
    | [<CaseKey("datetime")>] SFTDateTime
    | [<CaseKey("date")>] SFTDate
    | [<CaseKey("interval")>] SFTInterval
    | [<CaseKey("json")>] SFTJson
    | [<CaseKey("uvref")>] SFTUserViewRef
    | [<CaseKey("uuid")>] SFTUuid
    | [<CaseKey("reference")>] SFTReference of Entity: 'e * OnDelete: ReferenceDeleteAction option
    | [<CaseKey("enum")>] SFTEnum of Values: OrderedSet<string>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | SFTInt -> "int"
        | SFTDecimal -> "decimal"
        | SFTString -> "string"
        | SFTBool -> "bool"
        | SFTDateTime -> "datetime"
        | SFTDate -> "date"
        | SFTInterval -> "interval"
        | SFTJson -> "json"
        | SFTUserViewRef -> "uvref"
        | SFTUuid -> "uuid"
        | SFTReference(e, mopts) ->
            let optsStr =
                match mopts with
                | None -> ""
                | Some opts -> sprintf "ON DELETE %s" (toOzmaQLString opts)

            String.concatWithWhitespaces [ sprintf "reference(%s)" (e.ToOzmaQLString()); optsStr ]
        | SFTEnum vals -> sprintf "enum(%s)" (vals |> Seq.map renderOzmaQLString |> String.concat ", ")

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

[<StructuralEquality; NoComparison; SerializeAsObject("type", AllowUnknownType = true)>]
type FieldType<'e> when 'e :> IOzmaQLName =
    | [<CaseKey(null, Type = CaseSerialization.InnerObject)>] FTScalar of Type: ScalarFieldType<'e>
    | [<CaseKey("array")>] FTArray of Subtype: ScalarFieldType<'e>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | FTScalar s -> s.ToOzmaQLString()
        | FTArray valType -> sprintf "array(%s)" (valType.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type JoinType =
    | Inner
    | Left
    | Right
    | Outer

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | Left -> "LEFT"
        | Right -> "RIGHT"
        | Inner -> "INNER"
        | Outer -> "OUTER"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type SetOperation =
    | Union
    | Intersect
    | Except

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | Union -> "UNION"
        | Intersect -> "INTERSECT"
        | Except -> "EXCEPT"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type SortOrder =
    | [<CaseKey("asc")>] Asc
    | [<CaseKey("desc")>] Desc

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | Asc -> "ASC"
        | Desc -> "DESC"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type NullsOrder =
    | [<CaseKey("first")>] NullsFirst
    | [<CaseKey("last")>] NullsLast

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | NullsFirst -> "NULLS FIRST"
        | NullsLast -> "NULLS LAST"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type PathArrow =
    { Name: FieldName
      AsRoot: bool }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let refStr = this.Name.ToOzmaQLString()
        let asRootStr = if this.AsRoot then "!" else ""
        sprintf "=>%s%s" refStr asRootStr

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type LinkedRef<'f> when 'f :> IOzmaQLName =
    { Ref: 'f
      AsRoot: bool
      Path: PathArrow[] }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let refStr = this.Ref.ToOzmaQLString()
        let asRootStr = if this.AsRoot then "!" else ""

        let refSeq =
            seq {
                refStr
                asRootStr
            }

        Seq.append refSeq (this.Path |> Seq.map toOzmaQLString) |> String.concat ""

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() =
        if not (Array.isEmpty this.Path) then
            let arrow = Array.last this.Path
            arrow.Name
        else
            this.Ref.ToName()

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type ValueRef<'f> when 'f :> IOzmaQLName =
    | VRColumn of 'f
    | VRArgument of ArgumentRef

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | VRColumn c -> c.ToOzmaQLString()
        | VRArgument p -> p.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() =
        match this with
        | VRColumn c -> c.ToName()
        | VRArgument p -> p.ToName()

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type SubEntityRef = { Ref: EntityRef; Extra: ObjectMap }

type EntityAlias =
    { Name: EntityName
      Fields: FieldName[] option }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let columnsStr =
            match this.Fields with
            | None -> ""
            | Some cols -> cols |> Seq.map toOzmaQLString |> String.concat ", " |> sprintf "(%s)"

        String.concatWithWhitespaces [ "AS"; this.Name.ToOzmaQLString(); columnsStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type DomainExprInfo =
    { AsRoot: bool }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        if this.AsRoot then "WITH SUPERUSER ROLE" else ""

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

[<NoEquality; NoComparison>]
type BinaryOperator =
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

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
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

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

[<NoEquality; NoComparison>]
type FromEntity<'e> when 'e :> IOzmaQLName =
    { Alias: EntityName option
      AsRoot: bool
      Only: bool
      Ref: 'e
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let onlyStr = if this.Only then "ONLY" else ""

        let aliasStr =
            match this.Alias with
            | Some name -> sprintf "AS %s" (name.ToOzmaQLString())
            | None -> ""

        let roleStr = if this.AsRoot then "WITH SUPERUSER ROLE" else ""
        String.concatWithWhitespaces [ onlyStr; this.Ref.ToOzmaQLString(); roleStr; aliasStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type OperationEntity<'e> when 'e :> IOzmaQLName = FromEntity<'e>

type SerializedBoundMappingEntry = { When: FieldValue; Value: FieldValue }

type BoundMappingEntriesPrettyConverter() =
    inherit JsonConverter<HashMap<FieldValue, FieldValue>>()

    override this.CanRead = false

    override this.ReadJson
        (reader: JsonReader, someType, existingValue, hasExistingValue, serializer: JsonSerializer)
        : HashMap<FieldValue, FieldValue> =
        raise <| NotImplementedException()

    override this.WriteJson
        (writer: JsonWriter, value: HashMap<FieldValue, FieldValue>, serializer: JsonSerializer)
        : unit =
        writer.WriteStartArray()

        for KeyValue(k, v) in value do
            let convInfo = { When = k; Value = v }
            serializer.Serialize(writer, convInfo)

        writer.WriteEndArray()

[<NoEquality; NoComparison>]
type BoundMapping =
    { [<JsonConverter(typeof<BoundMappingEntriesPrettyConverter>)>]
      Entries: HashMap<FieldValue, FieldValue>
      [<DataMember(EmitDefaultValue = false)>]
      Default: FieldValue option }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let esStr =
            this.Entries
            |> HashMap.toSeq
            |> Seq.map (fun (value, e) -> sprintf "WHEN %s THEN %s" (toOzmaQLString value) (toOzmaQLString e))
            |> String.concat " "

        let elsStr =
            match this.Default with
            | None -> ""
            | Some e -> sprintf "ELSE %s" (e.ToOzmaQLString())

        String.concatWithWhitespaces [ "MAPPING"; esStr; elsStr; "END" ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type ResolvedExprFlags =
    { HasSubqueries: bool
      HasFetches: bool
      HasArrows: bool
      HasAggregates: bool
      HasArguments: bool
      HasFields: bool }

let emptyResolvedExprFlags: ResolvedExprFlags =
    { HasSubqueries = false
      HasFetches = false
      HasArrows = false
      HasAggregates = false
      HasArguments = false
      HasFields = false }

let unknownResolvedExprFlags: ResolvedExprFlags =
    { HasSubqueries = true
      HasFetches = true
      HasArrows = true
      HasAggregates = false
      HasArguments = true
      HasFields = true }

let fieldResolvedExprFlags: ResolvedExprFlags =
    { emptyResolvedExprFlags with
        HasFields = true }

let unionResolvedExprFlags (a: ResolvedExprFlags) (b: ResolvedExprFlags) =
    { HasSubqueries = a.HasSubqueries || b.HasSubqueries
      HasFetches = a.HasFetches || b.HasFetches
      HasArrows = a.HasArrows || b.HasArrows
      HasAggregates = a.HasAggregates || b.HasAggregates
      HasArguments = a.HasArguments || b.HasArguments
      HasFields = a.HasFields || b.HasFields }

let exprIsLocal (flags: ResolvedExprFlags) =
    not (flags.HasFetches || flags.HasSubqueries || flags.HasArrows)

type DependencyStatus =
    | DSConst
    | DSSingle
    | DSPerRow

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | DSConst -> "CONST"
        | DSSingle -> "SINGLE"
        | DSPerRow -> "PER ROW"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

let unionDependencyStatus (a: DependencyStatus) (b: DependencyStatus) : DependencyStatus =
    match (a, b) with
    | (DSConst, DSConst) -> DSConst
    | (DSConst, DSSingle)
    | (DSSingle, DSConst)
    | (DSSingle, DSSingle) -> DSSingle
    | (DSPerRow, _)
    | (_, DSPerRow) -> DSPerRow

type AttributesMap<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName = Map<AttributeName, Attribute<'e, 'f>>

and BoundAttributesMap<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName = Map<AttributeName, BoundAttribute<'e, 'f>>

and [<NoEquality; NoComparison>] BoundAttributeExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | BAExpr of FieldExpr<'e, 'f>
    | BAMapping of BoundMapping
    | BAArrayMapping of BoundMapping

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | BAExpr e -> toOzmaQLString e
        | BAMapping mapping -> toOzmaQLString mapping
        | BAArrayMapping mapping -> sprintf "ARRAY %O" mapping

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] BoundAttribute<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Expression: BoundAttributeExpr<'e, 'f>
      Dependency: DependencyStatus
      Internal: bool }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let internalStr = if this.Internal then "INTERNAL" else ""
        String.concatWithWhitespaces [ internalStr; toOzmaQLString this.Dependency; toOzmaQLString this.Expression ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] Attribute<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Expression: FieldExpr<'e, 'f>
      Dependency: DependencyStatus
      Internal: bool }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let internalStr = if this.Internal then "INTERNAL" else ""
        String.concatWithWhitespaces [ internalStr; toOzmaQLString this.Dependency; toOzmaQLString this.Expression ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and FieldExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | FEValue of FieldValue
    | FERef of 'f
    | FEEntityAttr of 'e * AttributeName
    | FEFieldAttr of 'f * AttributeName
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
    | FECast of FieldExpr<'e, 'f> * FieldType<'e>
    | FEIsNull of FieldExpr<'e, 'f>
    | FEIsNotNull of FieldExpr<'e, 'f>
    | FECase of (FieldExpr<'e, 'f> * FieldExpr<'e, 'f>)[] * FieldExpr<'e, 'f> option
    | FEMatch of FieldExpr<'e, 'f> * (FieldExpr<'e, 'f> * FieldExpr<'e, 'f>)[] * FieldExpr<'e, 'f> option
    | FEJsonArray of FieldExpr<'e, 'f>[]
    | FEJsonObject of Map<OzmaQLName, FieldExpr<'e, 'f>>
    | FEFunc of FunctionName * FieldExpr<'e, 'f>[]
    | FEAggFunc of FunctionName * AggExpr<'e, 'f>
    | FESubquery of SelectExpr<'e, 'f>
    | FEInheritedFrom of 'f * SubEntityRef
    | FEOfType of 'f * SubEntityRef

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | FEValue value -> value.ToOzmaQLString()
        | FERef r -> r.ToOzmaQLString()
        | FEEntityAttr(r, attr) -> sprintf "%s.@@%s" (r.ToOzmaQLString()) (attr.ToOzmaQLString())
        | FEFieldAttr(r, attr) -> sprintf "%s.@%s" (r.ToOzmaQLString()) (attr.ToOzmaQLString())
        | FENot e -> sprintf "NOT (%s)" (e.ToOzmaQLString())
        | FEAnd(a, b) -> sprintf "(%s) AND (%s)" (a.ToOzmaQLString()) (b.ToOzmaQLString())
        | FEOr(a, b) -> sprintf "(%s) OR (%s)" (a.ToOzmaQLString()) (b.ToOzmaQLString())
        | FEBinaryOp(a, op, b) -> sprintf "(%s) %s (%s)" (a.ToOzmaQLString()) (op.ToOzmaQLString()) (b.ToOzmaQLString())
        | FEDistinct(a, b) -> sprintf "(%s) IS DISTINCT FROM (%s)" (a.ToOzmaQLString()) (b.ToOzmaQLString())
        | FENotDistinct(a, b) -> sprintf "(%s) IS NOT DISTINCT FROM (%s)" (a.ToOzmaQLString()) (b.ToOzmaQLString())
        | FESimilarTo(e, pat) -> sprintf "(%s) SIMILAR TO (%s)" (e.ToOzmaQLString()) (pat.ToOzmaQLString())
        | FENotSimilarTo(e, pat) -> sprintf "(%s) NOT SIMILAR TO (%s)" (e.ToOzmaQLString()) (pat.ToOzmaQLString())
        | FEIn(e, vals) ->
            assert (not <| Array.isEmpty vals)
            sprintf "(%s) IN (%s)" (e.ToOzmaQLString()) (vals |> Seq.map toOzmaQLString |> String.concat ", ")
        | FENotIn(e, vals) ->
            assert (not <| Array.isEmpty vals)
            sprintf "(%s) NOT IN (%s)" (e.ToOzmaQLString()) (vals |> Seq.map toOzmaQLString |> String.concat ", ")
        | FEInQuery(e, query) -> sprintf "(%s) IN (%s)" (e.ToOzmaQLString()) (query.ToOzmaQLString())
        | FENotInQuery(e, query) -> sprintf "(%s) NOT IN (%s)" (e.ToOzmaQLString()) (query.ToOzmaQLString())
        | FEAny(e, op, arr) ->
            sprintf "(%s) %s ANY (%s)" (e.ToOzmaQLString()) (op.ToOzmaQLString()) (arr.ToOzmaQLString())
        | FEAll(e, op, arr) ->
            sprintf "(%s) %s ALL (%s)" (e.ToOzmaQLString()) (op.ToOzmaQLString()) (arr.ToOzmaQLString())
        | FECast(e, typ) -> sprintf "(%s) :: %s" (e.ToOzmaQLString()) (typ.ToOzmaQLString())
        | FEIsNull e -> sprintf "(%s) IS NULL" (e.ToOzmaQLString())
        | FEIsNotNull e -> sprintf "(%s) IS NOT NULL" (e.ToOzmaQLString())
        | FECase(es, els) ->
            let esStr =
                es
                |> Seq.map (fun (cond, e) -> sprintf "WHEN %s THEN %s" (cond.ToOzmaQLString()) (e.ToOzmaQLString()))
                |> String.concat " "

            let elsStr =
                match els with
                | None -> ""
                | Some e -> sprintf "ELSE %s" (e.ToOzmaQLString())

            String.concatWithWhitespaces [ "CASE"; esStr; elsStr; "END" ]
        | FEMatch(expr, es, els) ->
            let esStr =
                es
                |> Seq.map (fun (value, e) -> sprintf "WHEN %s THEN %s" (toOzmaQLString value) (toOzmaQLString e))
                |> String.concat " "

            let elsStr =
                match els with
                | None -> ""
                | Some e -> sprintf "ELSE %s" (e.ToOzmaQLString())

            String.concatWithWhitespaces [ "MATCH ON"; toOzmaQLString expr; esStr; elsStr; "END" ]
        | FEJsonArray vals -> vals |> Seq.map toOzmaQLString |> String.concat ", " |> sprintf "[%s]"
        | FEJsonObject obj ->
            obj
            |> Map.toSeq
            |> Seq.map (fun (k, v) -> sprintf "%s: %s" (k.ToOzmaQLString()) (v.ToOzmaQLString()))
            |> String.concat ", "
            |> sprintf "{%s}"
        | FEFunc(name, args) ->
            sprintf "%s(%s)" (name.ToOzmaQLString()) (args |> Seq.map toOzmaQLString |> String.concat ", ")
        | FEAggFunc(name, args) -> sprintf "%s(%s)" (name.ToOzmaQLString()) (args.ToOzmaQLString())
        | FESubquery q -> sprintf "(%s)" (q.ToOzmaQLString())
        | FEInheritedFrom(f, ref) -> sprintf "%s INHERITED FROM %s" (f.ToOzmaQLString()) (ref.Ref.ToOzmaQLString())
        | FEOfType(f, ref) -> sprintf "%s OFTYPE %s" (f.ToOzmaQLString()) (ref.Ref.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] AggExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | AEAll of FieldExpr<'e, 'f>[]
    | AEDistinct of FieldExpr<'e, 'f>
    | AEStar

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | AEAll exprs ->
            assert (not <| Array.isEmpty exprs)
            exprs |> Array.map toOzmaQLString |> String.concat ", "
        | AEDistinct expr -> sprintf "DISTINCT %s" (expr.ToOzmaQLString())
        | AEStar -> "*"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] QueryResult<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | QRAll of EntityRef option
    | QRExpr of QueryColumnResult<'e, 'f>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | QRAll None -> "*"
        | QRAll(Some ref) -> sprintf "%s.*" (ref.ToOzmaQLString())
        | QRExpr expr -> expr.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] QueryColumnResult<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Alias: EntityName option
      Attributes: BoundAttributesMap<'e, 'f>
      Result: FieldExpr<'e, 'f> }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let attrsStr =
            if Map.isEmpty this.Attributes then
                ""
            else
                renderAttributesMap this.Attributes

        let aliasStr =
            match this.Alias with
            | None -> ""
            | Some name -> sprintf "AS %s" (name.ToOzmaQLString())

        let resultStr = this.Result.ToOzmaQLString()

        String.concatWithWhitespaces [ resultStr; aliasStr; attrsStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.TryToName() =
        match this with
        | { Alias = Some name } -> Some name
        | { Alias = None; Result = FERef c } -> Some <| c.ToName()
        | _ -> None

    interface IOzmaQLName with
        member this.ToName() = this.TryToName() |> Option.get

and [<NoEquality; NoComparison>] OrderColumn<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Expr: FieldExpr<'e, 'f>
      Order: SortOrder option
      Nulls: NullsOrder option }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let orderStr = optionToOzmaQLString this.Order
        let nullsStr = optionToOzmaQLString this.Nulls
        String.concatWithWhitespaces [ this.Expr.ToOzmaQLString(); orderStr; nullsStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] OrderLimitClause<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { OrderBy: OrderColumn<'e, 'f>[]
      Limit: FieldExpr<'e, 'f> option
      Offset: FieldExpr<'e, 'f> option }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let orderByStr =
            if Array.isEmpty this.OrderBy then
                ""
            else
                sprintf "ORDER BY %s" (this.OrderBy |> Seq.map toOzmaQLString |> String.concat ", ")

        let limitStr =
            match this.Limit with
            | Some e -> sprintf "LIMIT %s" (e.ToOzmaQLString())
            | None -> ""

        let offsetStr =
            match this.Offset with
            | Some e -> sprintf "OFFSET %s" (e.ToOzmaQLString())
            | None -> ""

        String.concatWithWhitespaces [ orderByStr; limitStr; offsetStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] SingleSelectExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Attributes: AttributesMap<'e, 'f>
      Results: QueryResult<'e, 'f>[]
      From: FromExpr<'e, 'f> option
      Where: FieldExpr<'e, 'f> option
      GroupBy: FieldExpr<'e, 'f>[]
      OrderLimit: OrderLimitClause<'e, 'f>
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let attributesStrs =
            this.Attributes
            |> Map.toSeq
            |> Seq.map (fun (name, expr) -> sprintf "@%s = %s" (name.ToOzmaQLString()) (expr.ToOzmaQLString()))

        let resultsStrs = this.Results |> Seq.map toOzmaQLString
        let resultStr = Seq.append attributesStrs resultsStrs |> String.concat ", "

        let fromStr =
            match this.From with
            | None -> ""
            | Some from -> sprintf "FROM %s" (from.ToOzmaQLString())

        let whereStr =
            match this.Where with
            | None -> ""
            | Some cond -> sprintf "WHERE %s" (cond.ToOzmaQLString())

        let groupByStr =
            if Array.isEmpty this.GroupBy then
                ""
            else
                sprintf "GROUP BY %s" (this.GroupBy |> Array.map toOzmaQLString |> String.concat ", ")

        sprintf
            "SELECT %s"
            (String.concatWithWhitespaces [ resultStr; fromStr; whereStr; groupByStr; this.OrderLimit.ToOzmaQLString() ])

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] ValuesValue<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | VVExpr of FieldExpr<'e, 'f>
    | VVDefault

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | VVExpr e -> e.ToOzmaQLString()
        | VVDefault -> "DEFAULT"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] SelectTreeExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | SSelect of SingleSelectExpr<'e, 'f>
    // TODO: Add a SelectExpr node to support nested WITH clauses.
    | SValues of ValuesValue<'e, 'f>[][]
    | SSetOp of SetOperationExpr<'e, 'f>

    member this.ToOzmaQLString() =
        match this with
        | SSelect e -> e.ToOzmaQLString()
        | SValues values ->
            assert not (Array.isEmpty values)

            let printOne (array: ValuesValue<'e, 'f>[]) =
                assert not (Array.isEmpty array)
                array |> Seq.map toOzmaQLString |> String.concat ", " |> sprintf "(%s)"

            let valuesStr = values |> Seq.map printOne |> String.concat ", "
            sprintf "VALUES %s" valuesStr
        | SSetOp setOp -> setOp.ToOzmaQLString()

    override this.ToString() = this.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] SelectExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { CTEs: CommonTableExprs<'e, 'f> option
      Tree: SelectTreeExpr<'e, 'f>
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let ctesStr = optionToOzmaQLString this.CTEs
        String.concatWithWhitespaces [ ctesStr; this.Tree.ToOzmaQLString() ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] SetOperationExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Operation: SetOperation
      AllowDuplicates: bool
      A: SelectExpr<'e, 'f>
      B: SelectExpr<'e, 'f>
      OrderLimit: OrderLimitClause<'e, 'f> }

    member this.ToOzmaQLString() =
        let allowDuplicatesStr = if this.AllowDuplicates then "ALL" else ""
        let aStr = sprintf "(%s)" (this.A.ToOzmaQLString())
        let bStr = sprintf "(%s)" (this.B.ToOzmaQLString())

        String.concatWithWhitespaces
            [ aStr
              this.Operation.ToOzmaQLString()
              allowDuplicatesStr
              bStr
              this.OrderLimit.ToOzmaQLString() ]

    override this.ToString() = this.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] TableExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | TESelect of SelectExpr<'e, 'f>
    | TEDomain of 'f * DomainExprInfo
    | TEFieldDomain of 'e * FieldName * DomainExprInfo
    | TETypeDomain of FieldType<'e> * DomainExprInfo

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | TESelect sel -> sprintf "(%O)" sel
        | TEDomain(ref, info) ->
            let domainStr = sprintf "DOMAIN OF %s" (toOzmaQLString ref)
            let infoStr = toOzmaQLString info
            String.concatWithWhitespaces [ domainStr; infoStr ]
        | TEFieldDomain(entity, field, info) ->
            let domainStr = sprintf "DOMAIN OF FIELD %O.%s" entity (toOzmaQLString field)
            let infoStr = toOzmaQLString info
            String.concatWithWhitespaces [ domainStr; infoStr ]
        | TETypeDomain(typ, info) ->
            let domainStr = sprintf "DOMAIN OF TYPE %O" typ
            let infoStr = toOzmaQLString info
            String.concatWithWhitespaces [ domainStr; infoStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] FromTableExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Alias: EntityAlias
      Expression: TableExpr<'e, 'f>
      Lateral: bool }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let lateralStr = if this.Lateral then "LATERAL" else ""
        String.concatWithWhitespaces [ lateralStr; toOzmaQLString this.Expression; toOzmaQLString this.Alias ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] FromExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    // TODO: We don't have fields aliasing implemented for entities yet, because internally we don't guarantee order of entity fields.
    | FEntity of FromEntity<'e>
    | FTableExpr of FromTableExpr<'e, 'f>
    | FJoin of JoinExpr<'e, 'f>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | FEntity ent -> ent.ToOzmaQLString()
        | FTableExpr expr -> expr.ToOzmaQLString()
        | FJoin join -> join.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] JoinExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Type: JoinType
      A: FromExpr<'e, 'f>
      B: FromExpr<'e, 'f>
      Condition: FieldExpr<'e, 'f> }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        sprintf
            "(%s %s JOIN %s ON %s)"
            (this.A.ToOzmaQLString())
            (this.Type.ToOzmaQLString())
            (this.B.ToOzmaQLString())
            (this.Condition.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and CommonTableExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Fields: FieldName[] option
      Materialized: bool option
      Expr: SelectExpr<'e, 'f>
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let fieldsStr =
            match this.Fields with
            | None -> ""
            | Some args ->
                assert (not (Array.isEmpty args))
                let argsStr = args |> Seq.map toOzmaQLString |> String.concat ", "
                sprintf "(%s)" argsStr

        let materializedStr =
            match this.Materialized with
            | None -> ""
            | Some true -> "MATERIALIZED"
            | Some false -> "NOT MATERIALIZED"

        String.concatWithWhitespaces [ fieldsStr; "AS"; materializedStr; this.Expr.ToOzmaQLString() ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] CommonTableExprs<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Recursive: bool
      Exprs: (EntityName * CommonTableExpr<'e, 'f>)[]
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        assert (not (Array.isEmpty this.Exprs))

        let convertOne (name: EntityName, cte: CommonTableExpr<'e, 'f>) =
            let nameStr =
                match cte.Fields with
                | None -> name.ToOzmaQLString()
                | Some args ->
                    assert (not (Array.isEmpty args))
                    let argsStr = args |> Seq.map toOzmaQLString |> String.concat ", "
                    sprintf "%s(%s)" (name.ToOzmaQLString()) argsStr

            let materializedStr =
                match cte.Materialized with
                | None -> ""
                | Some true -> "MATERIALIZED"
                | Some false -> "NOT MATERIALIZED"

            String.concatWithWhitespaces [ nameStr; "AS"; materializedStr; cte.Expr.ToOzmaQLString() ]

        let exprs = this.Exprs |> Seq.map convertOne |> String.concat ", "
        let recursive = if this.Recursive then "RECURSIVE" else ""
        String.concatWithWhitespaces [ "WITH"; recursive; exprs ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] InsertSource<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | ISSelect of SelectExpr<'e, 'f>
    | ISDefaultValues

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | ISSelect sel -> sel.ToOzmaQLString()
        | ISDefaultValues -> "DEFAULT VALUES"

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] InsertExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { CTEs: CommonTableExprs<'e, 'f> option
      Entity: OperationEntity<'e>
      Fields: FieldName[]
      Source: InsertSource<'e, 'f>
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let ctesStr = optionToOzmaQLString this.CTEs

        let insertStr =
            sprintf
                "INSERT INTO %s (%s) %s"
                (this.Entity.ToOzmaQLString())
                (this.Fields |> Seq.map toOzmaQLString |> String.concat ", ")
                (this.Source.ToOzmaQLString())

        String.concatWithWhitespaces [ ctesStr; insertStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] UpdateAssignExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | UAESet of FieldName * ValuesValue<'e, 'f>
    | UAESelect of FieldName[] * SelectExpr<'e, 'f>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | UAESet(name, expr) -> sprintf "%s = %s" (name.ToOzmaQLString()) (expr.ToOzmaQLString())
        | UAESelect(cols, select) ->
            assert (not <| Array.isEmpty cols)
            let colsStr = cols |> Seq.map toOzmaQLString |> String.concat ", "
            sprintf "(%s) = (%O)" colsStr select

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] UpdateExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { CTEs: CommonTableExprs<'e, 'f> option
      Entity: OperationEntity<'e>
      Assignments: UpdateAssignExpr<'e, 'f>[]
      From: FromExpr<'e, 'f> option
      Where: FieldExpr<'e, 'f> option
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        assert (not <| Array.isEmpty this.Assignments)

        let ctesStr = optionToOzmaQLString this.CTEs
        let assignsStr = this.Assignments |> Seq.map toOzmaQLString |> String.concat ", "

        let fromStr =
            match this.From with
            | Some f -> sprintf "FROM %s" (f.ToOzmaQLString())
            | None -> ""

        let condExpr =
            match this.Where with
            | Some c -> sprintf "WHERE %s" (c.ToOzmaQLString())
            | None -> ""

        let updateStr = sprintf "UPDATE %s SET %s" (this.Entity.ToOzmaQLString()) assignsStr
        String.concatWithWhitespaces [ ctesStr; updateStr; fromStr; condExpr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] DeleteExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { CTEs: CommonTableExprs<'e, 'f> option
      Entity: OperationEntity<'e>
      Using: FromExpr<'e, 'f> option
      Where: FieldExpr<'e, 'f> option
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let ctesStr = optionToOzmaQLString this.CTEs

        let usingStr =
            match this.Using with
            | Some f -> sprintf "USING %s" (f.ToOzmaQLString())
            | None -> ""

        let condExpr =
            match this.Where with
            | Some c -> sprintf "WHERE %s" (c.ToOzmaQLString())
            | None -> ""

        let deleteStr = sprintf "DELETE FROM %s" (this.Entity.ToOzmaQLString())
        String.concatWithWhitespaces [ ctesStr; deleteStr; usingStr; condExpr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

and [<NoEquality; NoComparison>] DataExpr<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    | DESelect of SelectExpr<'e, 'f>
    | DEInsert of InsertExpr<'e, 'f>
    | DEUpdate of UpdateExpr<'e, 'f>
    | DEDelete of DeleteExpr<'e, 'f>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | DESelect sel -> sel.ToOzmaQLString()
        | DEInsert ins -> ins.ToOzmaQLString()
        | DEUpdate upd -> upd.ToOzmaQLString()
        | DEDelete del -> del.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

[<NoEquality; NoComparison>]
type IndexColumn<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Expr: FieldExpr<'e, 'f>
      OpClass: OzmaQLName option
      Order: SortOrder option
      Nulls: NullsOrder option }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let opClassStr = optionToOzmaQLString this.OpClass
        let orderStr = optionToOzmaQLString this.Order
        let nullsStr = optionToOzmaQLString this.Nulls
        String.concatWithWhitespaces [ this.Expr.ToOzmaQLString(); opClassStr; orderStr; nullsStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

// If a query has a main entity, all rows can be bound to some entry of that entity.
// If FOR INSERT is specified, columns must additionally contain all required fields of that entity.
[<NoEquality; NoComparison>]
type MainEntity<'e> when 'e :> IOzmaQLName =
    { Entity: 'e
      ForInsert: bool
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        if this.ForInsert then
            sprintf "FOR INSERT INTO %s" (this.Entity.ToOzmaQLString())
        else
            sprintf "FOR UPDATE OF %s" (this.Entity.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type SubEntityContext =
    | SECInheritedFrom
    | SECOfType

type FieldExprMapper<'e1, 'f1, 'e2, 'f2>
    when 'e1 :> IOzmaQLName and 'f1 :> IOzmaQLName and 'e2 :> IOzmaQLName and 'f2 :> IOzmaQLName =
    { Value: FieldValue -> FieldValue
      FieldReference: 'f1 -> 'f2
      EntityReference: 'e1 -> 'e2
      Query: SelectExpr<'e1, 'f1> -> SelectExpr<'e2, 'f2>
      PreAggregate: AggExpr<'e1, 'f1> -> AggExpr<'e1, 'f1>
      SubEntity: SubEntityContext -> 'f2 -> SubEntityRef -> SubEntityRef }

let idFieldExprMapper: FieldExprMapper<'a, 'b, 'a, 'b> =
    { Value = id
      FieldReference = id
      EntityReference = id
      Query = id
      PreAggregate = id
      SubEntity = fun _ _ r -> r }

let queryFieldExprMapper (fieldFunc: 'f1 -> 'f2) (queryFunc: SelectExpr<'e, 'f1> -> SelectExpr<'e, 'f2>) =
    { Value = id
      FieldReference = fieldFunc
      EntityReference = id
      Query = queryFunc
      PreAggregate = id
      SubEntity = fun _ _ r -> r }

let onlyFieldExprMapper (fieldFunc: 'f1 -> 'f2) =
    { Value = id
      FieldReference = fieldFunc
      EntityReference = id
      Query = fun query -> raisef UnexpectedExprException "Unexpected subquery in expression"
      PreAggregate = fun agg -> raisef UnexpectedExprException "Unexpected aggregate in expression"
      SubEntity = fun _ _ r -> r }

let mapAggExpr (func: FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>) : AggExpr<'e1, 'f1> -> AggExpr<'e2, 'f2> =
    function
    | AEAll exprs -> AEAll(Array.map func exprs)
    | AEDistinct expr -> AEDistinct(func expr)
    | AEStar -> AEStar

let mapScalarFieldType (func: 'e1 -> 'e2) : ScalarFieldType<'e1> -> ScalarFieldType<'e2> =
    function
    | SFTInt -> SFTInt
    | SFTDecimal -> SFTDecimal
    | SFTString -> SFTString
    | SFTBool -> SFTBool
    | SFTDateTime -> SFTDateTime
    | SFTDate -> SFTDate
    | SFTInterval -> SFTInterval
    | SFTJson -> SFTJson
    | SFTUserViewRef -> SFTUserViewRef
    | SFTUuid -> SFTUuid
    | SFTReference(e, opts) -> SFTReference(func e, opts)
    | SFTEnum vals -> SFTEnum vals

let mapFieldType (func: 'e1 -> 'e2) : FieldType<'e1> -> FieldType<'e2> =
    function
    | FTScalar typ -> FTScalar <| mapScalarFieldType func typ
    | FTArray typ -> FTArray <| mapScalarFieldType func typ

let mapBoundAttributeExpr
    (f: FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>)
    : BoundAttributeExpr<'e1, 'f1> -> BoundAttributeExpr<'e2, 'f2> =
    function
    | BAExpr e -> BAExpr(f e)
    | BAMapping mapping -> BAMapping mapping
    | BAArrayMapping mapping -> BAArrayMapping mapping

let rec mapFieldExpr (mapper: FieldExprMapper<'e1, 'f1, 'e2, 'f2>) : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2> =
    let rec traverse =
        function
        | FEValue value -> FEValue(mapper.Value value)
        | FERef r -> FERef(mapper.FieldReference r)
        | FEEntityAttr(eref, attr) -> FEEntityAttr(mapper.EntityReference eref, attr)
        | FEFieldAttr(fref, attr) -> FEFieldAttr(mapper.FieldReference fref, attr)
        | FENot e -> FENot(traverse e)
        | FEAnd(a, b) -> FEAnd(traverse a, traverse b)
        | FEOr(a, b) -> FEOr(traverse a, traverse b)
        | FEDistinct(a, b) -> FEDistinct(traverse a, traverse b)
        | FENotDistinct(a, b) -> FENotDistinct(traverse a, traverse b)
        | FEBinaryOp(a, op, b) -> FEBinaryOp(traverse a, op, traverse b)
        | FESimilarTo(e, pat) -> FESimilarTo(traverse e, traverse pat)
        | FENotSimilarTo(e, pat) -> FENotSimilarTo(traverse e, traverse pat)
        | FEIn(e, vals) -> FEIn(traverse e, Array.map traverse vals)
        | FENotIn(e, vals) -> FENotIn(traverse e, Array.map traverse vals)
        | FEInQuery(e, query) -> FEInQuery(traverse e, mapper.Query query)
        | FENotInQuery(e, query) -> FENotInQuery(traverse e, mapper.Query query)
        | FEAny(e, op, arr) -> FEAny(traverse e, op, traverse arr)
        | FEAll(e, op, arr) -> FEAll(traverse e, op, traverse arr)
        | FECast(e, typ) -> FECast(traverse e, mapFieldType mapper.EntityReference typ)
        | FEIsNull e -> FEIsNull(traverse e)
        | FEIsNotNull e -> FEIsNotNull(traverse e)
        | FECase(es, els) ->
            FECase(Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | FEMatch(expr, es, els) ->
            FEMatch(traverse expr, Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | FEJsonArray vals -> FEJsonArray(Array.map traverse vals)
        | FEJsonObject obj -> FEJsonObject(Map.map (fun name -> traverse) obj)
        | FEFunc(name, args) -> FEFunc(name, Array.map traverse args)
        | FEAggFunc(name, args) -> FEAggFunc(name, mapAggExpr traverse (mapper.PreAggregate args))
        | FESubquery query -> FESubquery(mapper.Query query)
        | FEInheritedFrom(f, nam) ->
            let ref = mapper.FieldReference f
            FEInheritedFrom(ref, mapper.SubEntity SECInheritedFrom ref nam)
        | FEOfType(f, nam) ->
            let ref = mapper.FieldReference f
            FEOfType(ref, mapper.SubEntity SECOfType ref nam)

    traverse

type FieldExprTaskMapper<'e1, 'f1, 'e2, 'f2>
    when 'e1 :> IOzmaQLName and 'f1 :> IOzmaQLName and 'e2 :> IOzmaQLName and 'f2 :> IOzmaQLName =
    { Value: FieldValue -> Task<FieldValue>
      FieldReference: 'f1 -> Task<'f2>
      EntityReference: 'e1 -> Task<'e2>
      Query: SelectExpr<'e1, 'f1> -> Task<SelectExpr<'e2, 'f2>>
      PreAggregate: AggExpr<'e1, 'f1> -> Task<AggExpr<'e1, 'f1>>
      SubEntity: SubEntityContext -> 'f2 -> SubEntityRef -> Task<SubEntityRef> }

let idFieldExprTaskMapper: FieldExprTaskMapper<'e, 'f, 'e, 'f> =
    { Value = Task.result
      FieldReference = Task.result
      EntityReference = Task.result
      Query = Task.result
      PreAggregate = Task.result
      SubEntity = fun _ _ r -> Task.result r }

let onlyFieldExprTaskMapper (fieldFunc: 'f1 -> Task<'f2>) : FieldExprTaskMapper<'e, 'f1, 'e, 'f2> =
    { Value = Task.result
      FieldReference = fieldFunc
      EntityReference = Task.result
      Query = fun query -> raisef UnexpectedExprException "Unexpected subquery in expression"
      PreAggregate = fun agg -> raisef UnexpectedExprException "Unexpected aggregate in expression"
      SubEntity = fun _ _ r -> Task.result r }

let mapTaskAggExpr
    (func: FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>>)
    : AggExpr<'e1, 'f1> -> Task<AggExpr<'e2, 'f2>> =
    function
    | AEAll exprs -> Task.map AEAll (Array.mapTask func exprs)
    | AEDistinct expr -> Task.map AEDistinct (func expr)
    | AEStar -> Task.result AEStar

let mapTaskScalarFieldType (func: 'e1 -> Task<'e2>) : ScalarFieldType<'e1> -> Task<ScalarFieldType<'e2>> =
    function
    | SFTInt -> Task.result SFTInt
    | SFTDecimal -> Task.result SFTDecimal
    | SFTString -> Task.result SFTString
    | SFTBool -> Task.result SFTBool
    | SFTDateTime -> Task.result SFTDateTime
    | SFTDate -> Task.result SFTDate
    | SFTInterval -> Task.result SFTInterval
    | SFTJson -> Task.result SFTJson
    | SFTUserViewRef -> Task.result SFTUserViewRef
    | SFTUuid -> Task.result SFTUuid
    | SFTReference(e, opts) -> Task.map (fun e -> SFTReference(e, opts)) (func e)
    | SFTEnum vals -> Task.result <| SFTEnum vals

let mapTaskFieldType (func: 'e1 -> Task<'e2>) : FieldType<'e1> -> Task<FieldType<'e2>> =
    function
    | FTScalar typ -> Task.map FTScalar (mapTaskScalarFieldType func typ)
    | FTArray typ -> Task.map FTArray (mapTaskScalarFieldType func typ)

let mapTaskFieldExpr
    (mapper: FieldExprTaskMapper<'e1, 'f1, 'e2, 'f2>)
    : FieldExpr<'e1, 'f1> -> Task<FieldExpr<'e2, 'f2>> =
    let rec traverse =
        function
        | FEValue value -> Task.map FEValue (mapper.Value value)
        | FERef r -> Task.map FERef (mapper.FieldReference r)
        | FEEntityAttr(eref, attr) -> Task.map (fun eref -> FEEntityAttr(eref, attr)) (mapper.EntityReference eref)
        | FEFieldAttr(fref, attr) -> Task.map (fun fref -> FEFieldAttr(fref, attr)) (mapper.FieldReference fref)
        | FENot e -> Task.map FENot (traverse e)
        | FEAnd(a, b) -> Task.map2 (curry FEAnd) (traverse a) (traverse b)
        | FEOr(a, b) -> Task.map2 (curry FEOr) (traverse a) (traverse b)
        | FEDistinct(a, b) -> Task.map2 (curry FEDistinct) (traverse a) (traverse b)
        | FENotDistinct(a, b) -> Task.map2 (curry FENotDistinct) (traverse a) (traverse b)
        | FEBinaryOp(a, op, b) -> Task.map2 (fun a b -> FEBinaryOp(a, op, b)) (traverse a) (traverse b)
        | FESimilarTo(e, pat) -> Task.map2 (curry FESimilarTo) (traverse e) (traverse pat)
        | FENotSimilarTo(e, pat) -> Task.map2 (curry FENotSimilarTo) (traverse e) (traverse pat)
        | FEIn(e, vals) -> Task.map2 (curry FEIn) (traverse e) (Array.mapTask traverse vals)
        | FENotIn(e, vals) -> Task.map2 (curry FENotIn) (traverse e) (Array.mapTask traverse vals)
        | FEInQuery(e, query) -> Task.map2 (curry FEInQuery) (traverse e) (mapper.Query query)
        | FENotInQuery(e, query) -> Task.map2 (curry FENotInQuery) (traverse e) (mapper.Query query)
        | FEAny(e, op, arr) -> Task.map2 (fun e arr -> FEAny(e, op, arr)) (traverse e) (traverse arr)
        | FEAll(e, op, arr) -> Task.map2 (fun e arr -> FEAll(e, op, arr)) (traverse e) (traverse arr)
        | FECast(e, typ) ->
            Task.map2 (fun e typ -> FECast(e, typ)) (traverse e) (mapTaskFieldType mapper.EntityReference typ)
        | FEIsNull e -> Task.map FEIsNull (traverse e)
        | FEIsNotNull e -> Task.map FEIsNotNull (traverse e)
        | FECase(es, els) ->
            let mapOne (cond, e) =
                task {
                    let! newCond = traverse cond
                    let! newE = traverse e
                    return (newCond, newE)
                }

            Task.map2 (curry FECase) (Array.mapTask mapOne es) (Option.mapTask traverse els)
        | FEMatch(expr, es, els) ->
            let mapOne (cond, e) =
                task {
                    let! newCond = traverse cond
                    let! newE = traverse e
                    return (newCond, newE)
                }

            Task.map3 (curryN FEMatch) (traverse expr) (Array.mapTask mapOne es) (Option.mapTask traverse els)
        | FEJsonArray vals -> Task.map FEJsonArray (Array.mapTask traverse vals)
        | FEJsonObject obj -> Task.map FEJsonObject (Map.mapTask (fun name -> traverse) obj)
        | FEFunc(name, args) -> Task.map (fun x -> FEFunc(name, x)) (Array.mapTask traverse args)
        | FEAggFunc(name, args) ->
            task {
                let! args1 = mapper.PreAggregate args
                return! Task.map (fun x -> FEAggFunc(name, x)) (mapTaskAggExpr traverse args1)
            }
        | FESubquery query -> Task.map FESubquery (mapper.Query query)
        | FEInheritedFrom(f, nam) ->
            task {
                let! field = mapper.FieldReference f
                let! subEntity = mapper.SubEntity SECInheritedFrom field nam
                return FEInheritedFrom(field, subEntity)
            }
        | FEOfType(f, nam) ->
            task {
                let! field = mapper.FieldReference f
                let! subEntity = mapper.SubEntity SECOfType field nam
                return FEOfType(field, subEntity)
            }

    traverse

type FieldExprIter<'e, 'f> when 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { Value: FieldValue -> unit
      FieldReference: 'f -> unit
      EntityReference: 'e -> unit
      Query: SelectExpr<'e, 'f> -> unit
      Aggregate: AggExpr<'e, 'f> -> unit
      SubEntity: SubEntityContext -> 'f -> SubEntityRef -> unit }

let idFieldExprIter =
    { Value = fun _ -> ()
      FieldReference = fun _ -> ()
      EntityReference = fun _ -> ()
      Query = fun _ -> ()
      Aggregate = fun _ -> ()
      SubEntity = fun _ _ _ -> () }

let iterScalarFieldType (func: 'e1 -> unit) : ScalarFieldType<'e1> -> unit =
    function
    | SFTInt -> ()
    | SFTDecimal -> ()
    | SFTString -> ()
    | SFTBool -> ()
    | SFTDateTime -> ()
    | SFTDate -> ()
    | SFTInterval -> ()
    | SFTJson -> ()
    | SFTUserViewRef -> ()
    | SFTUuid -> ()
    | SFTReference(e, opts) -> func e
    | SFTEnum vals -> ()

let iterFieldType (func: 'e1 -> unit) : FieldType<'e1> -> unit =
    function
    | FTScalar typ -> iterScalarFieldType func typ
    | FTArray typ -> iterScalarFieldType func typ

let iterAggExpr (func: FieldExpr<'e, 'f> -> unit) : AggExpr<'e, 'f> -> unit =
    function
    | AEAll exprs -> Array.iter func exprs
    | AEDistinct expr -> func expr
    | AEStar -> ()

let iterFieldExpr (mapper: FieldExprIter<'e, 'f>) : FieldExpr<'e, 'f> -> unit =
    let rec traverse =
        function
        | FEValue value -> mapper.Value value
        | FERef r -> mapper.FieldReference r
        | FEEntityAttr(eref, attr) -> mapper.EntityReference eref
        | FEFieldAttr(fref, attr) -> mapper.FieldReference fref
        | FENot e -> traverse e
        | FEAnd(a, b) ->
            traverse a
            traverse b
        | FEOr(a, b) ->
            traverse a
            traverse b
        | FEDistinct(a, b) ->
            traverse a
            traverse b
        | FENotDistinct(a, b) ->
            traverse a
            traverse b
        | FEBinaryOp(a, op, b) ->
            traverse a
            traverse b
        | FESimilarTo(e, pat) ->
            traverse e
            traverse pat
        | FENotSimilarTo(e, pat) ->
            traverse e
            traverse pat
        | FEIn(e, vals) ->
            traverse e
            Array.iter traverse vals
        | FENotIn(e, vals) ->
            traverse e
            Array.iter traverse vals
        | FEInQuery(e, query) ->
            traverse e
            mapper.Query query
        | FENotInQuery(e, query) ->
            traverse e
            mapper.Query query
        | FEAny(e, op, arr) ->
            traverse e
            traverse arr
        | FEAll(e, op, arr) ->
            traverse e
            traverse arr
        | FECast(e, typ) ->
            iterFieldType mapper.EntityReference typ
            traverse e
        | FEIsNull e -> traverse e
        | FEIsNotNull e -> traverse e
        | FECase(es, els) ->
            Array.iter
                (fun (cond, e) ->
                    traverse cond
                    traverse e)
                es

            Option.iter traverse els
        | FEMatch(expr, es, els) ->
            traverse expr

            Array.iter
                (fun (cond, e) ->
                    traverse cond
                    traverse e)
                es

            Option.iter traverse els
        | FEJsonArray vals -> Array.iter traverse vals
        | FEJsonObject obj -> Map.iter (fun name -> traverse) obj
        | FEFunc(name, args) -> Array.iter traverse args
        | FEAggFunc(name, args) ->
            mapper.Aggregate args
            iterAggExpr traverse args
        | FESubquery query -> mapper.Query query
        | FEInheritedFrom(f, nam) ->
            mapper.FieldReference f
            mapper.SubEntity SECInheritedFrom f nam
        | FEOfType(f, nam) ->
            mapper.FieldReference f
            mapper.SubEntity SECOfType f nam

    traverse

let emptyOrderLimitClause =
    { OrderBy = [||]
      Limit = None
      Offset = None }

type OzmaQLVoid =
    private
    | OzmaQLVoid of unit

    interface IOzmaQLString with
        member this.ToOzmaQLString() = failwith "impossible"

    interface IOzmaQLName with
        member this.ToName() = failwith "impossible"

type LinkedFieldRef = LinkedRef<ValueRef<FieldRef>>
type LinkedFieldName = LinkedRef<ValueRef<FieldName>>

type ParsedFieldType = FieldType<EntityRef>
type ParsedScalarFieldType = ScalarFieldType<EntityRef>

type ResolvedFieldType = FieldType<ResolvedEntityRef>
type ResolvedScalarFieldType = ScalarFieldType<ResolvedEntityRef>

type LocalFieldExpr = FieldExpr<OzmaQLVoid, FieldName>

[<NoEquality; NoComparison>]
type BoundRef<'f> when 'f :> IOzmaQLName =
    { Ref: 'f
      Extra: ObjectMap }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() = this.Ref.ToOzmaQLString()

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() = this.Ref.ToName()

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

type LinkedBoundFieldRef = BoundRef<LinkedRef<ValueRef<FieldRef>>>

type ResolvedFieldExpr = FieldExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSelectExpr = SelectExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSelectTreeExpr = SelectTreeExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedSingleSelectExpr = SingleSelectExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedQueryResult = QueryResult<EntityRef, LinkedBoundFieldRef>
type ResolvedQueryColumnResult = QueryColumnResult<EntityRef, LinkedBoundFieldRef>
type ResolvedCommonTableExpr = CommonTableExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedCommonTableExprs = CommonTableExprs<EntityRef, LinkedBoundFieldRef>
type ResolvedFromEntity = FromEntity<EntityRef>
type ResolvedFromExpr = FromExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedAttributesMap = AttributesMap<EntityRef, LinkedBoundFieldRef>
type ResolvedBoundAttributesMap = BoundAttributesMap<EntityRef, LinkedBoundFieldRef>
type ResolvedOrderLimitClause = OrderLimitClause<EntityRef, LinkedBoundFieldRef>
type ResolvedAggExpr = AggExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedOrderColumn = OrderColumn<EntityRef, LinkedBoundFieldRef>
type ResolvedValuesValue = ValuesValue<EntityRef, LinkedBoundFieldRef>
type ResolvedInsertExpr = InsertExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedUpdateExpr = UpdateExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedDeleteExpr = DeleteExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedDataExpr = DataExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedOperationEntity = OperationEntity<EntityRef>
type ResolvedInsertSource = InsertSource<EntityRef, LinkedBoundFieldRef>
type ResolvedUpdateAssignExpr = UpdateAssignExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedBoundAttribute = BoundAttribute<EntityRef, LinkedBoundFieldRef>
type ResolvedAttribute = Attribute<EntityRef, LinkedBoundFieldRef>
type ResolvedBoundAttributeExpr = BoundAttributeExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedTableExpr = TableExpr<EntityRef, LinkedBoundFieldRef>
type ResolvedFromTableExpr = FromTableExpr<EntityRef, LinkedBoundFieldRef>

type ResolvedIndexColumn = IndexColumn<EntityRef, LinkedBoundFieldRef>
type ResolvedMainEntity = MainEntity<EntityRef>

type PragmasMap = Map<PragmaName, FieldValue>

[<NoEquality; NoComparison>]
type Argument<'te, 'e, 'f> when 'te :> IOzmaQLName and 'e :> IOzmaQLName and 'f :> IOzmaQLName =
    { ArgType: FieldType<'te>
      Optional: bool
      DefaultValue: FieldValue option
      Attributes: BoundAttributesMap<'e, 'f> }

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        let typeStr = this.ArgType.ToOzmaQLString()
        let optionalStr = if this.Optional then "NULL" else typeStr

        let defaultStr =
            match this.DefaultValue with
            | None -> ""
            | Some def -> sprintf "DEFAULT %s" (def.ToOzmaQLString())

        let attrsStr =
            if Map.isEmpty this.Attributes then
                ""
            else
                renderAttributesMap this.Attributes

        String.concatWithWhitespaces [ typeStr; optionalStr; defaultStr; attrsStr ]

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type ResolvedArgument = Argument<ResolvedEntityRef, EntityRef, LinkedBoundFieldRef>

let argument (typ: FieldType<'te>) (optional: bool) =
    { ArgType = typ
      Optional = optional
      DefaultValue = None
      Attributes = Map.empty }

let funId = OzmaQLName "id"
let funSubEntity = OzmaQLName "sub_entity"
let funSchema = OzmaQLName "public"
let funView = OzmaQLName "view"
let funMain = OzmaQLName "__main"
let funUsers = OzmaQLName "users"
let funEvents = OzmaQLName "events"

let compileName (OzmaQLName name) = SQL.SQLName name

let decompileName (SQL.SQLName name) = OzmaQLName name

let sqlFunId = compileName funId
let sqlFunSubEntity = compileName funSubEntity

let systemColumns = Set.ofList [ funId; funSubEntity ]

type UsedField =
    { Select: bool
      Update: bool
      Insert: bool }

let emptyUsedField: UsedField =
    { Select = false
      Update = false
      Insert = false }

let usedFieldSelect = { emptyUsedField with Select = true }

let usedFieldUpdate = { emptyUsedField with Update = true }

let usedFieldInsert = { emptyUsedField with Insert = true }

let unionUsedFields (a: UsedField) (b: UsedField) : UsedField =
    { Select = a.Select || b.Select
      Update = a.Update || b.Update
      Insert = a.Insert || b.Insert }

type UsedEntity =
    { Select: bool
      Update: bool
      Insert: bool
      Delete: bool
      Fields: Map<FieldName, UsedField> }

let emptyUsedEntity: UsedEntity =
    { Select = false
      Update = false
      Insert = false
      Delete = false
      Fields = Map.empty }

let usedEntitySelect: UsedEntity = { emptyUsedEntity with Select = true }

let usedEntityInsert = { emptyUsedEntity with Insert = true }

let usedEntityUpdate = { emptyUsedEntity with Update = true }

let usedEntityDelete = { emptyUsedEntity with Delete = true }

let unionUsedEntities (a: UsedEntity) (b: UsedEntity) : UsedEntity =
    { Select = a.Select || b.Select
      Insert = a.Insert || b.Insert
      Update = a.Update || b.Update
      Delete = a.Delete || b.Delete
      Fields = Map.unionWith unionUsedFields a.Fields b.Fields }

let addUsedEntityField (fieldName: FieldName) (usedField: UsedField) (usedEntity: UsedEntity) =
    let newField =
        match Map.tryFind fieldName usedEntity.Fields with
        | None -> usedField
        | Some oldField -> unionUsedFields oldField usedField

    { usedEntity with
        // Propagate field access to entity.
        Insert = usedEntity.Insert || usedField.Insert
        Select = usedEntity.Select || usedField.Select
        Update = usedEntity.Update || usedField.Update
        Fields = Map.add fieldName newField usedEntity.Fields }

// Making these structs to remove runtime cost of wrapping.
[<Struct>]
type UsedSchema =
    { Entities: Map<EntityName, UsedEntity> }

let emptyUsedSchema: UsedSchema = { Entities = Map.empty }

let unionUsedSchemas (a: UsedSchema) (b: UsedSchema) : UsedSchema =
    { Entities = Map.unionWith unionUsedEntities a.Entities b.Entities }

[<Struct>]
type UsedDatabase =
    { Schemas: Map<SchemaName, UsedSchema> }

    member this.FindEntity(ref: ResolvedEntityRef) =
        match Map.tryFind ref.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind ref.Name schema.Entities

    member this.FindField (ref: ResolvedEntityRef) (name: FieldName) =
        match this.FindEntity ref with
        | None -> None
        | Some entity -> Map.tryFind name entity.Fields

let emptyUsedDatabase: UsedDatabase = { Schemas = Map.empty }

let unionUsedDatabases (a: UsedDatabase) (b: UsedDatabase) : UsedDatabase =
    { Schemas = Map.unionWith unionUsedSchemas a.Schemas b.Schemas }

let addUsedEntity
    (schemaName: SchemaName)
    (entityName: EntityName)
    (usedEntity: UsedEntity)
    (usedDatabase: UsedDatabase)
    : UsedDatabase =
    let oldSchema = Map.findWithDefault schemaName emptyUsedSchema usedDatabase.Schemas

    let newEntity =
        match Map.tryFind entityName oldSchema.Entities with
        | None -> usedEntity
        | Some oldEntity -> unionUsedEntities oldEntity usedEntity

    let newSchema = { Entities = Map.add entityName newEntity oldSchema.Entities }
    { Schemas = Map.add schemaName newSchema usedDatabase.Schemas }

let addUsedEntityRef (ref: ResolvedEntityRef) (usedEntity: UsedEntity) (usedDatabase: UsedDatabase) =
    addUsedEntity ref.Schema ref.Name usedEntity usedDatabase

let addUsedField
    (schemaName: SchemaName)
    (entityName: EntityName)
    (fieldName: FieldName)
    (usedField: UsedField)
    (usedDatabase: UsedDatabase)
    : UsedDatabase =
    let oldSchema = Map.findWithDefault schemaName emptyUsedSchema usedDatabase.Schemas
    let oldEntity = Map.findWithDefault entityName emptyUsedEntity oldSchema.Entities
    let newEntity = addUsedEntityField fieldName usedField oldEntity
    let newSchema = { Entities = Map.add entityName newEntity oldSchema.Entities }
    { Schemas = Map.add schemaName newSchema usedDatabase.Schemas }

let addUsedFieldRef (ref: ResolvedFieldRef) =
    addUsedField ref.Entity.Schema ref.Entity.Name ref.Name

let tryFindUsedEntity
    (schemaName: SchemaName)
    (entityName: EntityName)
    (usedDatabase: UsedDatabase)
    : UsedEntity option =
    let schema = Map.findWithDefault schemaName emptyUsedSchema usedDatabase.Schemas
    Map.tryFind entityName schema.Entities

let tryFindUsedEntityRef (ref: ResolvedEntityRef) = tryFindUsedEntity ref.Schema ref.Name

let tryFindUsedField
    (schemaName: SchemaName)
    (entityName: EntityName)
    (fieldName: FieldName)
    (usedDatabase: UsedDatabase)
    : UsedField option =
    let entity =
        tryFindUsedEntity schemaName entityName usedDatabase
        |> Option.defaultValue emptyUsedEntity

    Map.tryFind fieldName entity.Fields

let tryFindUsedFieldRef (ref: ResolvedFieldRef) =
    tryFindUsedField ref.Entity.Schema ref.Entity.Name ref.Name

type UsedArguments = Set<ArgumentRef>

type LocalArgumentsMap = Map<ArgumentName, FieldValue>

let requiredArgument (argType: FieldType<'te>) : Argument<'te, 'e, 'f> =
    { ArgType = argType
      Optional = false
      DefaultValue = None
      Attributes = Map.empty }

// Map of registered global arguments. Should be in sync with RequestContext's globalArguments.
let globalArgumentTypes: Map<ArgumentName, ResolvedArgument> =
    Map.ofSeq
        [ (OzmaQLName "lang", requiredArgument (FTScalar SFTString))
          (OzmaQLName "user", requiredArgument (FTScalar SFTString))
          (OzmaQLName "user_id",
           requiredArgument (FTScalar(SFTReference({ Schema = funSchema; Name = funUsers }, None))))
          (OzmaQLName "transaction_time", requiredArgument (FTScalar SFTDateTime))
          (OzmaQLName "transaction_id", requiredArgument (FTScalar SFTInt)) ]

let globalArgumentsMap = globalArgumentTypes |> Map.mapKeys PGlobal

let allowedAggregateFunctions: Map<FunctionName, SQL.FunctionName> =
    Map.ofList
        [ // Generic
          (OzmaQLName "count", SQL.SQLName "count")
          // Numbers
          (OzmaQLName "sum", SQL.SQLName "sum")
          (OzmaQLName "avg", SQL.SQLName "avg")
          (OzmaQLName "min", SQL.SQLName "min")
          (OzmaQLName "max", SQL.SQLName "max")
          // Booleans
          (OzmaQLName "bool_and", SQL.SQLName "bool_and")
          (OzmaQLName "every", SQL.SQLName "every")
          // Strings
          (OzmaQLName "string_agg", SQL.SQLName "string_agg")
          // Arrays
          (OzmaQLName "array_agg", SQL.SQLName "array_agg")
          // JSON
          (OzmaQLName "json_agg", SQL.SQLName "jsonb_agg")
          (OzmaQLName "json_object_agg", SQL.SQLName "jsonb_object_agg") ]

let inline private parseSingleValue<'A>
    ([<InlineIfLambda>] constrFunc: 'A -> FieldValue option)
    (tok: JToken)
    : FieldValue option =
    if tok.Type = JTokenType.Null then
        Some FNull
    else
        try
            constrFunc <| tok.ToObject()
        with :? JsonException ->
            None

let inline private parseSingleValueStrict f = parseSingleValue (f >> Some)

let parseValueFromJson (fieldExprType: FieldType<'e>) : JToken -> FieldValue option =
    match fieldExprType with
    | FTArray SFTString -> parseSingleValueStrict FStringArray
    | FTArray SFTInt -> parseSingleValueStrict FIntArray
    | FTArray SFTDecimal -> parseSingleValueStrict FDecimalArray
    | FTArray SFTBool -> parseSingleValueStrict FBoolArray
    | FTArray SFTDateTime -> parseSingleValueStrict FDateTimeArray
    | FTArray SFTDate -> parseSingleValueStrict FDateArray
    | FTArray SFTInterval -> parseSingleValueStrict FIntervalArray
    | FTArray SFTJson -> parseSingleValueStrict FJsonArray
    | FTArray SFTUserViewRef -> parseSingleValueStrict FUserViewRefArray
    | FTArray SFTUuid -> parseSingleValueStrict FUuidArray
    | FTArray(SFTReference _) -> parseSingleValueStrict FIntArray
    | FTArray(SFTEnum vals) ->
        parseSingleValue (fun xs ->
            if Seq.forall (fun x -> vals.Contains x) xs then
                Some(FStringArray xs)
            else
                None)
    | FTScalar SFTString -> parseSingleValueStrict FString
    | FTScalar SFTInt -> parseSingleValueStrict FInt
    | FTScalar SFTDecimal -> parseSingleValueStrict FDecimal
    | FTScalar SFTBool -> parseSingleValueStrict FBool
    | FTScalar SFTDateTime -> parseSingleValueStrict FDateTime
    | FTScalar SFTDate -> parseSingleValueStrict FDate
    | FTScalar SFTInterval -> parseSingleValueStrict FInterval
    | FTScalar SFTJson -> parseSingleValueStrict FJson
    | FTScalar SFTUserViewRef -> parseSingleValueStrict FUserViewRef
    | FTScalar SFTUuid -> parseSingleValueStrict FUuid
    | FTScalar(SFTReference _) -> parseSingleValueStrict FInt
    | FTScalar(SFTEnum vals) -> parseSingleValue (fun x -> if vals.Contains x then Some(FString x) else None)

let fromEntity (entityRef: 'e) : FromEntity<'e> =
    { Ref = entityRef
      Alias = None
      Only = false
      AsRoot = false
      Extra = ObjectMap.empty }

let linkedRef (ref: 'f) : LinkedRef<'f> =
    { Ref = ref
      Path = [||]
      AsRoot = false }

let resolvedRefFieldExpr (ref: 'f) : FieldExpr<'e, BoundRef<LinkedRef<'f>>> =
    FERef
        { Ref = linkedRef ref
          Extra = ObjectMap.empty }

let emptySingleSelectExpr: SingleSelectExpr<'e, 'f> =
    { Attributes = Map.empty
      Results = [||]
      From = None
      Where = None
      GroupBy = [||]
      OrderLimit = emptyOrderLimitClause
      Extra = ObjectMap.empty }

let selectExpr (tree: SelectTreeExpr<'e, 'f>) : SelectExpr<'e, 'f> =
    { CTEs = None
      Tree = tree
      Extra = ObjectMap.empty }


let insertExpr (entity: OperationEntity<'e>) (source: InsertSource<'e, 'f>) : InsertExpr<'e, 'f> =
    { CTEs = None
      Entity = entity
      Fields = [||]
      Source = source
      Extra = ObjectMap.empty }

let updateExpr (entity: OperationEntity<'e>) : UpdateExpr<'e, 'f> =
    { CTEs = None
      Entity = entity
      Assignments = [||]
      From = None
      Where = None
      Extra = ObjectMap.empty }

let deleteExpr (entity: OperationEntity<'e>) : DeleteExpr<'e, 'f> =
    { CTEs = None
      Entity = entity
      Using = None
      Where = None
      Extra = ObjectMap.empty }

let queryColumnResult (expr: FieldExpr<'e, 'f>) : QueryColumnResult<'e, 'f> =
    { Alias = None
      Attributes = Map.empty
      Result = expr }

let entityAlias (name: EntityName) : EntityAlias = { Name = name; Fields = None }

let fromSubSelectExpr (select: SelectExpr<'e, 'f>) (alias: EntityAlias) : FromTableExpr<'e, 'f> =
    { Expression = TESelect select
      Alias = alias
      Lateral = false }

let commonTableExprs (exprs: (EntityName * CommonTableExpr<'e, 'f>)[]) : CommonTableExprs<'e, 'f> =
    { Recursive = false
      Exprs = exprs
      Extra = ObjectMap.empty }

let commonTableExpr (expr: SelectExpr<'e, 'f>) : CommonTableExpr<'e, 'f> =
    { Fields = None
      Materialized = None
      Expr = expr
      Extra = ObjectMap.empty }
