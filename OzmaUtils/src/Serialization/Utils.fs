[<AutoOpen>]
module OzmaDB.OzmaUtils.Serialization.Utils

open System
open System.Globalization
open System.Collections.Generic
open System.Runtime.Serialization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Reflection

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Class)>]
type SerializeAsObjectAttribute(caseFieldName: string) =
    inherit Attribute()
    member this.CaseFieldName = caseFieldName
    member val AllowUnknownType = false with get, set

type CaseSerialization =
    | Normal = 0
    | InnerObject = 1
    | InnerValue = 2

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Method)>]
type CaseKeyAttribute(caseKey: string) =
    inherit Attribute()
    member this.CaseKey = caseKey
    member val Type = CaseSerialization.Normal with get, set
    // Work around https://github.com/fsharp/fslang-suggestions/issues/684
    member val IgnoreFields: string[] = [||] with get, set

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Method)>]
type DefaultCaseAttribute() =
    inherit Attribute()

type CaseTag = int
type CaseKey = string option

let parseCaseName (case: UnionCaseInfo) (attribute: CaseKeyAttribute option) : CaseKey =
    match attribute with
    | None -> Some case.Name
    | Some(nameAttr: CaseKeyAttribute) when isNull nameAttr.CaseKey -> None
    | Some nameAttr -> Some nameAttr.CaseKey

let caseKey (case: UnionCaseInfo) : CaseKey =
    case.GetCustomAttributes typeof<CaseKeyAttribute>
    |> Seq.cast
    |> Seq.first
    |> parseCaseName case

let defaultUnionCase (cases: UnionCase seq) : obj option =
    let getDefaultCase case =
        match
            case.Info.GetCustomAttributes typeof<DefaultCaseAttribute>
            |> Seq.cast
            |> Seq.first
        with
        | None -> None
        | Some defaultAttr when not (Array.isEmpty case.Fields) -> failwith "Default case must not have any fields"
        | Some defaultAttr -> Some <| FSharpValue.MakeUnion(case.Info, [||])

    cases |> Seq.mapMaybe getDefaultCase |> Seq.first

type UnionEnumCase =
    { Info: UnionCaseInfo
      Value: obj
      Name: CaseKey }

type UnionEnumInfo =
    { Cases: Map<CaseTag, UnionEnumCase>
      GetTag: obj -> CaseTag }

let getUnionEnum (objectType: Type) : UnionEnumInfo option =
    let cases = unionCases objectType

    let enumCase case =
        if Array.isEmpty case.Fields then
            let caseInfo =
                { Info = case.Info
                  Value = FSharpValue.MakeUnion(case.Info, [||])
                  Name = caseKey case.Info }

            Some(case.Info.Tag, caseInfo)
        else
            None

    match cases |> Seq.traverseOption enumCase with
    | None -> None
    | Some enumCases ->
        Some
            { Cases = Map.ofSeq enumCases
              GetTag = FSharpValue.PreComputeUnionTagReader objectType }

let getTypeDefaultValue (objectType: Type) : obj option =
    if objectType = typeof<string> then
        None
    else if objectType = typeof<JsonMap> then
        Some JsonMap.empty
    else if objectType = typeof<bool> then
        Some(false :> obj)
    else if isNullableType objectType then
        Some null
    else if FSharpType.IsUnion objectType then
        let cases = unionCases objectType

        match getOption cases with
        | Some option -> Some option.NoneValue
        | None when
            objectType.IsGenericType
            && objectType.GetGenericTypeDefinition() = typedefof<_ list>
            ->
            let emptyCase =
                FSharpType.GetUnionCases(objectType)
                |> Array.find (fun case -> case.Name = "Empty")

            Some <| FSharpValue.MakeUnion(emptyCase, [||])
        | None -> defaultUnionCase cases
    else if objectType.IsArray then
        Some(Array.CreateInstance(objectType.GetElementType(), 0) :> obj)
    else if objectType.IsGenericType then
        let genType = objectType.GetGenericTypeDefinition()

        if genType = typedefof<Map<_, _>> then
            let typePars = objectType.GetGenericArguments()
            let tupleType = FSharpType.MakeTupleType typePars
            let emptyArray = Array.CreateInstance(tupleType, 0)
            Some <| Activator.CreateInstance(objectType, emptyArray)
        else if genType = typedefof<Set<_>> then
            let typePars = objectType.GetGenericArguments()
            let emptyArray = Array.CreateInstance(typePars.[0], 0)
            Some <| Activator.CreateInstance(objectType, emptyArray)
        else
            None
    else
        None

let getPropertyDefaultValue (info: MemberInfo) : obj option =
    match info.GetCustomAttribute<DefaultValueAttribute>() with
    | null -> getTypeDefaultValue (memberInnerType info |> Option.get)
    | attr ->
        match attr.Value with
        | null -> None
        | v -> Some v

type SerializableField =
    { Member: MemberInfo
      InnerType: Type
      GetValue: obj -> obj
      DefaultValue: obj option
      Name: string
      IsNullable: bool
      EmitDefaultValue: bool option
      Ignore: bool }

type SerializableFieldOptions =
    { DefaultIgnore: bool option
      DefaultName: string option }

let emptySerializableFieldOptions =
    { DefaultIgnore = None
      DefaultName = None }

let serializableField (opts: SerializableFieldOptions) (memberInfo: MemberInfo) : SerializableField =
    let objectType = memberInnerType memberInfo |> Option.get
    let defaultName = Option.defaultValue memberInfo.Name opts.DefaultName

    let (name, emitDefaultValue, hasDataMember) =
        match memberInfo.GetCustomAttribute<DataMemberAttribute>() with
        | null -> (defaultName, None, false)
        | attr ->
            let name = if attr.IsNameSetExplicitly then attr.Name else defaultName
            (name, Some attr.EmitDefaultValue, true)

    let hasIgnoreAlways =
        match memberInfo.GetCustomAttribute<IgnoreDataMemberAttribute>() with
        | null -> false
        | attr -> true

    let ignore =
        if hasIgnoreAlways then
            true
        else if hasDataMember then
            false
        else if
            FSharpType.IsRecord memberInfo.DeclaringType
            || FSharpType.IsUnion memberInfo.DeclaringType
        then
            // Ignore non-fields.
            match memberInfo.GetCustomAttribute<CompilationMappingAttribute>() with
            | RefNull -> true
            | attr -> attr.SourceConstructFlags <> SourceConstructFlags.Field
        else
            Option.defaultValue false opts.DefaultIgnore

    let defaultValue = getPropertyDefaultValue memberInfo
    let isNullable = isNullableType objectType
    let getValue = getMemberValue memberInfo |> Option.get

    { Member = memberInfo
      InnerType = objectType
      GetValue = getValue
      DefaultValue = defaultValue
      Name = name
      IsNullable = isNullable
      EmitDefaultValue = emitDefaultValue
      Ignore = ignore }

type CaseSerializationType =
    | CSTNormal
    | CSTInnerObject
    | CSTInnerValue

type CaseAsObjectInfo =
    { Name: CaseKey
      Info: UnionCaseInfo
      Fields: SerializableField[]
      Type: CaseSerializationType
      ConvertFrom: obj -> obj[]
      ConvertTo: obj[] -> obj }

type UnionAsObjectInfo =
    { CaseField: string
      AllowUnknownType: bool
      ValueCase: CaseTag option
      Cases: Map<CaseTag, CaseAsObjectInfo>
      GetTag: obj -> CaseTag }

let unionAsObject (getOpts: PropertyInfo -> SerializableFieldOptions) (objectType: Type) : UnionAsObjectInfo option =
    match objectType.GetCustomAttribute<SerializeAsObjectAttribute>() with
    | null -> None
    | asObject ->
        let mutable valueCase = None

        let caseInfo (case: UnionCase) =
            let maybeAttr =
                case.Info.GetCustomAttributes(typeof<CaseKeyAttribute>) |> Seq.cast |> Seq.first

            let name = parseCaseName case.Info maybeAttr

            let getField (prop: PropertyInfo) =
                let field = serializableField (getOpts prop) prop

                match maybeAttr with
                | Some caseKey when Array.contains field.Member.Name caseKey.IgnoreFields ->
                    { field with Ignore = true }
                | _ -> field

            let fields = case.Fields |> Array.map getField
            let serializedCount = fields |> Seq.filter (fun f -> not f.Ignore) |> Seq.length

            let serType =
                match maybeAttr with
                | Some caseKey when caseKey.Type = CaseSerialization.InnerValue ->
                    if Option.isSome valueCase then
                        failwithf "Only one case can be declared as inner value case: %O" case.Info.Name

                    if serializedCount > 1 then
                        failwithf
                            "Serialization as inner object requires no more than one field in a union case %s"
                            case.Info.Name

                    valueCase <- Some case.Info.Tag
                    CSTInnerValue
                | Some caseKey when caseKey.Type = CaseSerialization.InnerObject ->
                    if serializedCount > 1 then
                        failwithf
                            "Serialization as inner object requires no more than one field in a union case %s"
                            case.Info.Name

                    CSTInnerObject
                | _ ->
                    if Option.isSome name then
                        for field in case.Fields do
                            if field.Name = asObject.CaseFieldName then
                                failwithf
                                    "Field name %s of case %s clashes with case field name"
                                    field.Name
                                    case.Info.Name

                    CSTNormal

            let ret =
                { Name = name
                  Info = case.Info
                  Fields = fields
                  Type = serType
                  ConvertFrom = FSharpValue.PreComputeUnionReader case.Info
                  ConvertTo = FSharpValue.PreComputeUnionConstructor case.Info }

            (case.Info.Tag, ret)

        let cases = unionCases objectType |> Seq.map caseInfo |> Map.ofSeq

        let ret =
            { CaseField = asObject.CaseFieldName
              AllowUnknownType = asObject.AllowUnknownType
              ValueCase = valueCase
              Cases = cases
              GetTag = FSharpValue.PreComputeUnionTagReader objectType }

        Some ret

let enumCases<'a> : (UnionCase * 'a) seq =
    let processOne (case: UnionCase) =
        (case, FSharpValue.MakeUnion(case.Info, [||]) :?> 'a)

    unionCases typeof<'a> |> Seq.map processOne

type private LookupCaseEnum<'a> when 'a: equality =
    static member Table =
        enumCases<'a> |> Seq.map (fun (case, v) -> (v, caseKey case.Info)) |> dict

let typeHasEquality (typ: Type) =
    typ.GetCustomAttributes<NoEqualityAttribute>() |> Seq.isEmpty

let prepareLookupCaseKey<'a> : 'a -> CaseKey =
    let typ = typeof<'a>

    match getUnionEnum typ with
    | Some enumInfo when typeHasEquality typ ->
        // This trickery is because we can't guarantee we satisfy the constraints statically,
        // even though we check them in runtime.
        let lookupInstance = typedefof<LookupCaseEnum<_>>.MakeGenericType([| typ |])

        let table =
            lookupInstance
                .GetProperty("Table", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Static)
                .GetValue(null)
            :?> IDictionary<'a, CaseKey>

        fun v -> table.[v]
    | _ ->
        let table =
            unionCases typ
            |> Seq.map (fun case -> (case.Info.Tag, caseKey case.Info))
            |> dict

        let lookupTag = FSharpValue.PreComputeUnionTagReader typ
        fun v -> table.[lookupTag v]
