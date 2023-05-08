[<AutoOpen>]
module FunWithFlags.FunUtils.Serialization.Utils

open System
open System.Globalization
open System.Runtime.Serialization
open System.Text.Json.Serialization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils

type UnionCase =
    { Info : UnionCaseInfo
      Fields : PropertyInfo[]
      ConvertFrom : obj -> obj[]
      ConvertTo : obj[] -> obj
    }

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Class)>]
type SerializeAsObjectAttribute (caseFieldName : string) =
    inherit Attribute ()
    member this.CaseFieldName = caseFieldName
    member val AllowUnknownType = false with get, set

type CaseSerialization =
    | Normal = 0
    | InnerObject = 1
    | InnerValue = 2

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Method)>]
type CaseKeyAttribute (caseKey : string) =
    inherit Attribute ()
    member this.CaseKey = caseKey
    member val Type = CaseSerialization.Normal with get, set
    // Work around https://github.com/fsharp/fslang-suggestions/issues/684
    member val IgnoreFields : string[] = [||] with get, set

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Method)>]
type DefaultCaseAttribute () =
    inherit Attribute ()

type CaseTag = int
type CaseKey = string option

let memberInnerType (memberInfo : MemberInfo) =
    match memberInfo with
    | :? PropertyInfo as info -> Some info.PropertyType
    | :? FieldInfo as info -> Some info.FieldType
    | _ -> None

let getMemberValue (memberInfo : MemberInfo) : (obj -> obj) option =
    match memberInfo with
    | :? PropertyInfo as info -> Some info.GetValue
    | :? FieldInfo as info -> Some info.GetValue
    | _ -> None

// https://stackoverflow.com/questions/56600268/how-do-i-check-t-for-types-not-being-allowed-to-be-null
let isNullableType (objectType : Type) =
    if FSharpType.IsRecord objectType then
        false
    else if FSharpType.IsUnion objectType then
        match Attribute.GetCustomAttribute(objectType, typeof<CompilationRepresentationAttribute>) with
        | null -> false
        | representation -> (representation :?> CompilationRepresentationAttribute).Flags.HasFlag(CompilationRepresentationFlags.UseNullAsTrueValue)
    else if objectType = typeof<string> then
        false
    else
        match Attribute.GetCustomAttribute(objectType, typeof<CompilationMappingAttribute>) with
        | :? CompilationMappingAttribute as mapping when mapping.SourceConstructFlags.HasFlag(SourceConstructFlags.ObjectType) ->
            not (isNull <| Attribute.GetCustomAttribute(objectType, typeof<AllowNullLiteralAttribute>))
        | _ -> not objectType.IsValueType || not (isNull <| Nullable.GetUnderlyingType(objectType))

let unionCases (objectType : Type) : UnionCase[] =
    let cases = FSharpType.GetUnionCases(objectType)

    let unionCase (info : UnionCaseInfo) : UnionCase =
        let fields = info.GetFields()
        { Info = info
          Fields = fields
          ConvertFrom = FSharpValue.PreComputeUnionReader info
          ConvertTo = FSharpValue.PreComputeUnionConstructor info
        }

    cases |> Array.map unionCase

let parseCaseName (case : UnionCaseInfo) (attribute : CaseKeyAttribute option) : CaseKey =
    match attribute with
    | None -> Some case.Name
    | Some (nameAttr : CaseKeyAttribute) when isNull nameAttr.CaseKey -> None
    | Some nameAttr -> Some nameAttr.CaseKey

let caseKey (case : UnionCaseInfo) : CaseKey =
    case.GetCustomAttributes typeof<CaseKeyAttribute> |> Seq.cast |> Seq.first |> parseCaseName case

let defaultUnionCase (cases : UnionCase seq) : obj option =
    let getDefaultCase case =
        match case.Info.GetCustomAttributes typeof<DefaultCaseAttribute> |> Seq.cast |> Seq.first with
        | None -> None
        | Some defaultAttr when not (Array.isEmpty case.Fields) -> failwith "Default case must not have any fields"
        | Some defaultAttr -> Some <| FSharpValue.MakeUnion(case.Info, [||])
    cases |> Seq.mapMaybe getDefaultCase |> Seq.first

type UnionEnumCase =
    { Info : UnionCaseInfo
      Value : obj
      Name : CaseKey
    }

type UnionEnumInfo =
    { Cases : Map<CaseTag, UnionEnumCase>
      GetTag : obj -> CaseTag
    }

let getUnionEnum (objectType : Type) : UnionEnumInfo option =
    let cases = unionCases objectType
    let enumCase case =
        if Array.isEmpty case.Fields then
            let caseInfo =
                { Info = case.Info
                  Value = FSharpValue.MakeUnion(case.Info, [||])
                  Name = caseKey case.Info
                }
            Some (case.Info.Tag, caseInfo)
        else
            None
    match cases |> Seq.traverseOption enumCase with
    | None -> None
    | Some enumCases ->
        Some
            { Cases = Map.ofSeq enumCases
              GetTag = FSharpValue.PreComputeUnionTagReader objectType
            }

let private getNewtypeCase (case : UnionCase) : (UnionCaseInfo * PropertyInfo) option =
    if Array.length case.Fields = 1 then
        Some (case.Info, case.Fields.[0])
    else
        None

type NewtypeInfo =
    { Case : UnionCaseInfo
      Type : PropertyInfo
      ConvertFrom : obj -> obj
      ConvertTo : obj -> obj
      ValueIsNullable : bool
    }

let getNewtype (map : UnionCase[]) : NewtypeInfo option =
    if Array.length map = 1 then
        match getNewtypeCase map.[0] with
        | None -> None
        | Some (case, typ) ->
            let convertFrom = FSharpValue.PreComputeUnionReader case
            let convertTo = FSharpValue.PreComputeUnionConstructor case
            Some
                { Case = case
                  Type = typ
                  ConvertFrom = fun obj -> (convertFrom obj).[0]
                  ConvertTo = fun arg -> convertTo [|arg|]
                  ValueIsNullable = isNullableType typ.PropertyType
                }
    else
        None

type OptionInfo =
    { SomeCase : UnionCaseInfo
      SomeType : PropertyInfo
      ConvertFromSome : obj -> obj
      ConvertToSome : obj -> obj
      NoneValue : obj
      ValueIsNullable : bool
    }

let getOption (map : UnionCase[]) : OptionInfo option =
    let checkNone case =
        if Array.isEmpty case.Fields then
            Some case.Info
        else
            None
    if Array.length map = 2 then
        let types =
            match (getNewtypeCase map.[0], checkNone map.[1]) with
            | (Some (someCase, someType), Some none) -> Some (someCase, someType, none)
            | _ ->
                match (getNewtypeCase map.[1], checkNone map.[0]) with
                | (Some (someCase, someType), Some none) -> Some (someCase, someType, none)
                | _ -> None
        match types with
        | None -> None
        | Some (someCase, someType, noneCase) ->
            let convertFrom = FSharpValue.PreComputeUnionReader someCase
            let convertTo = FSharpValue.PreComputeUnionConstructor someCase
            Some
                { SomeCase = someCase
                  SomeType = someType
                  ConvertFromSome = fun obj -> (convertFrom obj).[0]
                  ConvertToSome = fun arg -> convertTo [|arg|]
                  NoneValue = FSharpValue.MakeUnion(noneCase, [||])
                  ValueIsNullable = isNullableType someType.PropertyType
                }
    else
        None

let getTypeDefaultValue (objectType : Type) : obj option =
    if objectType = typeof<string> then
        None
    else if isNullableType objectType then
        Some null
    else if FSharpType.IsUnion objectType then
        let cases = unionCases objectType
        match getOption cases with
        | Some option ->
            Some option.NoneValue
        | None when objectType.IsGenericType && objectType.GetGenericTypeDefinition () = typedefof<_ list> ->
            let emptyCase = FSharpType.GetUnionCases(objectType) |> Array.find (fun case -> case.Name = "Empty")
            Some <| FSharpValue.MakeUnion(emptyCase, [||])
        | None -> defaultUnionCase cases
    else if objectType.IsArray then
        Some (Array.CreateInstance(objectType.GetElementType(), 0) :> obj)
    else if objectType.IsGenericType then
        let genType = objectType.GetGenericTypeDefinition ()
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
    else if objectType = typeof<bool> then
        Some (false :> obj)
    else
        None

let getPropertyDefaultValue (info : MemberInfo) : obj option =
    match Attribute.GetCustomAttribute(info, typeof<DefaultValueAttribute>) with
    | null -> getTypeDefaultValue (memberInnerType info |> Option.get)
    | attrObj ->
        let attr = attrObj :?> DefaultValueAttribute
        match attr.Value with
        | null -> None
        | v -> Some v

type SerializableField =
    { Member : MemberInfo
      InnerType : Type
      GetValue : obj -> obj
      DefaultValue : obj option
      Name : string
      IsNullable : bool
      EmitDefaultValue : bool option
      Ignore : bool
    }

let serializableField (prop : MemberInfo) : SerializableField =
    let objectType = memberInnerType prop |> Option.get
    let ignore =
        match Attribute.GetCustomAttribute(prop, typeof<JsonIgnoreAttribute>) with
        | null ->
            // Ignore internal F# fields if encountered.
            prop.Name.EndsWith("@")
        | attrObj ->
            let attr = attrObj :?> JsonIgnoreAttribute
            attr.Condition = JsonIgnoreCondition.Always
    let defaultValue = getPropertyDefaultValue prop
    let isNullable = isNullableType objectType
    let (name, emitDefaultValue) =
        match Attribute.GetCustomAttribute(prop, typeof<DataMemberAttribute>) with
        | null -> (prop.Name, None)
        | attrObj ->
            let attr = attrObj :?> DataMemberAttribute
            let name = if attr.IsNameSetExplicitly then attr.Name else prop.Name
            (name, Some attr.EmitDefaultValue)
    let getValue = getMemberValue prop |> Option.get
    { Member = prop
      InnerType = objectType
      GetValue = getValue
      DefaultValue = defaultValue
      Name = name
      IsNullable = isNullable
      EmitDefaultValue = emitDefaultValue
      Ignore = ignore
    }

type CaseSerializationType =
    | CSTNormal
    | CSTInnerObject
    | CSTInnerValue

type CaseAsObjectInfo =
    { Name : CaseKey
      Info : UnionCaseInfo
      Fields : SerializableField[]
      Type : CaseSerializationType
      ConvertFrom : obj -> obj[]
      ConvertTo : obj[] -> obj
    }

type UnionAsObjectInfo =
    { CaseField : string
      AllowUnknownType : bool
      ValueCase : CaseTag option
      Cases : Map<CaseTag, CaseAsObjectInfo>
      GetTag : obj -> CaseTag
    }

let unionAsObject (objectType : Type) : UnionAsObjectInfo option =
    match  Attribute.GetCustomAttribute(objectType, typeof<SerializeAsObjectAttribute>) with
    | null -> None
    | asObjectObj ->
        let asObject = asObjectObj :?> SerializeAsObjectAttribute
        let mutable valueCase = None

        let caseInfo (case : UnionCase) =
            let maybeAttr = case.Info.GetCustomAttributes(typeof<CaseKeyAttribute>) |> Seq.cast |> Seq.first
            let name = parseCaseName case.Info maybeAttr
            let getField (prop : PropertyInfo) =
                let field = serializableField prop
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
                        failwithf "Serialization as inner object requires no more than one field in a union case %s" case.Info.Name
                    valueCase <- Some case.Info.Tag
                    CSTInnerValue
                | Some caseKey when caseKey.Type = CaseSerialization.InnerObject ->
                    if serializedCount > 1 then
                        failwithf "Serialization as inner object requires no more than one field in a union case %s" case.Info.Name
                    CSTInnerObject
                | _ ->
                    if Option.isSome name then
                        for field in case.Fields do
                            if field.Name = asObject.CaseFieldName then
                                failwithf "Field name %s of case %s clashes with case field name" field.Name case.Info.Name
                    CSTNormal
            let ret =
                { Name = name
                  Info = case.Info
                  Fields = fields
                  Type = serType
                  ConvertFrom = FSharpValue.PreComputeUnionReader case.Info
                  ConvertTo = FSharpValue.PreComputeUnionConstructor case.Info
                }
            (case.Info.Tag, ret)

        let cases = unionCases objectType |> Seq.map caseInfo |> Map.ofSeq
        let ret =
            { CaseField = asObject.CaseFieldName
              AllowUnknownType = asObject.AllowUnknownType
              ValueCase = valueCase
              Cases = cases
              GetTag = FSharpValue.PreComputeUnionTagReader objectType
            }
        Some ret

// Needed for dictionary keys.
type NewtypeConverter<'nt> () =
    inherit TypeConverter ()

    let info = unionCases typeof<'nt> |> getNewtype |> Option.get
    let convertFrom = info.ConvertFrom
    let convertTo = info.ConvertTo
    let memberType = info.Type.PropertyType

    override this.CanConvertFrom (context : ITypeDescriptorContext, sourceType : Type) : bool =
        memberType = sourceType

    override this.CanConvertTo (context : ITypeDescriptorContext, destinationType : Type) : bool =
        typeof<'nt> = destinationType

    override this.ConvertTo (context : ITypeDescriptorContext, culture: CultureInfo, value : obj, destinationType : Type) : obj =
        convertFrom value

    override this.ConvertFrom (context : ITypeDescriptorContext, culture : CultureInfo, value : obj) : obj =
        convertTo value

let caseNames (unionType : Type) : Map<CaseKey, UnionCase> =
    unionCases unionType |> Seq.map (fun case -> (caseKey case.Info, case)) |> Map.ofSeqUnique
