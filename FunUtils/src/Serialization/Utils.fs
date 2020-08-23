module FunWithFlags.FunUtils.Serialization.Utils

open System
open System.Globalization
open System.Runtime.Serialization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils

type UnionCase =
    { Info : UnionCaseInfo
      Fields : PropertyInfo[]
    }

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Class)>]
type SerializeAsObjectAttribute (caseFieldName : string) =
    inherit Attribute ()
    member this.CaseFieldName = caseFieldName

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Method)>]
type CaseNameAttribute (caseName : string) =
    inherit Attribute ()
    member this.CaseName = caseName
    member val InnerObject = false with get, set

let memberInnerType (memberInfo : MemberInfo) =
    match memberInfo.MemberType with
    | MemberTypes.Property -> Some (memberInfo :?> PropertyInfo).PropertyType
    | MemberTypes.Field -> Some (memberInfo :?> FieldInfo).FieldType
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
        }

    cases |> Array.map unionCase

let unionName (case : UnionCaseInfo) : string option =
    match case.GetCustomAttributes typeof<CaseNameAttribute> |> Seq.cast |> Seq.first with
    | None -> Some case.Name
    | Some (nameAttr : CaseNameAttribute) when isNull nameAttr.CaseName -> None
    | Some nameAttr -> Some nameAttr.CaseName

let unionNames (cases : UnionCase seq) : Map<string option, UnionCase> =
    cases |> Seq.map (fun case -> (unionName case.Info, case)) |> Map.ofSeqUnique

let isUnionEnum (cases : UnionCase seq) : Map<string option, UnionCaseInfo * obj> option =
    let enumCase case =
        if Array.isEmpty case.Fields then
            Some (unionName case.Info, (case.Info, FSharpValue.MakeUnion(case.Info, [||])))
        else
            None
    cases |> Seq.traverseOption enumCase |> Option.map Map.ofSeq

let getNewtype (case : UnionCase) : (UnionCaseInfo * PropertyInfo) option =
    if Array.length case.Fields = 1 then
        Some (case.Info, case.Fields.[0])
    else
        None

type NewtypeInfo =
    { Case : UnionCaseInfo
      Type : PropertyInfo
      ValueIsNullable : bool
    }

let isNewtype (map : UnionCase[]) : NewtypeInfo option =
    if Array.length map = 1 then
        getNewtype map.[0] |> Option.map (fun (case, typ) -> { Case = case; Type = typ; ValueIsNullable = isNullableType typ.PropertyType })
    else
        None

type OptionInfo =
    { SomeCase : UnionCaseInfo
      SomeType : PropertyInfo
      NoneValue : obj
      ValueIsNullable : bool
    }

let isOption (map : UnionCase[]) : OptionInfo option =
    let checkNone case =
        if Array.isEmpty case.Fields then
            Some case.Info
        else
            None
    if Array.length map = 2 then
        let types =
            match (getNewtype map.[0], checkNone map.[1]) with
            | (Some (someCase, someType), Some none) -> Some (someCase, someType, none)
            | _ ->
                match (getNewtype map.[1], checkNone map.[0]) with
                | (Some (someCase, someType), Some none) -> Some (someCase, someType, none)
                | _ -> None
        match types with
        | None -> None
        | Some (someCase, someType, noneCase) ->
            Some
                { SomeCase = someCase
                  SomeType = someType
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
        match isOption cases with
        | Some option ->
            Some option.NoneValue
        | None when objectType.IsGenericType && objectType.GetGenericTypeDefinition () = typedefof<_ list> ->
            let emptyCase = FSharpType.GetUnionCases(objectType) |> Array.find (fun case -> case.Name = "Empty")
            Some <| FSharpValue.MakeUnion(emptyCase, [||])
        | None -> None
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
    | attr -> Some (attr :?> DefaultValueAttribute).Value

type SerializableField =
    { Property : PropertyInfo
      DefaultValue : obj option
      Name : string
      IsNullable : bool
      EmitDefaultValue : bool option
    }

let serializableField (prop : PropertyInfo) =
    let defaultValue = getPropertyDefaultValue prop
    let isNullable = isNullableType prop.PropertyType
    match Attribute.GetCustomAttribute(prop, typeof<DataMemberAttribute>) with
    | null ->
        { Property = prop
          DefaultValue = defaultValue
          Name = prop.Name
          IsNullable = isNullable
          EmitDefaultValue = None
        }
    | attrObj ->
        let attr = attrObj :?> DataMemberAttribute
        { Property = prop
          DefaultValue = defaultValue
          Name = if attr.IsNameSetExplicitly then attr.Name else prop.Name
          IsNullable = isNullable
          EmitDefaultValue = Some attr.EmitDefaultValue
        }

type CaseAsObjectInfo =
    { Info : UnionCaseInfo
      Fields : SerializableField[]
      InnerObject : bool
    }

type UnionAsObjectInfo =
    { CaseField : string
      Cases : Map<string option, CaseAsObjectInfo>
    }

let unionAsObject (objectType : Type) : UnionAsObjectInfo option =
    match Attribute.GetCustomAttribute(objectType, typeof<SerializeAsObjectAttribute>) with
    | null -> None
    | :? SerializeAsObjectAttribute as asObject ->
        let caseInfo (case : UnionCase) =
            let name = unionName case.Info
            let innerObject = 
                match case.Info.GetCustomAttributes(typeof<CaseNameAttribute>) |> Seq.cast |> Seq.first with
                | Some (caseName : CaseNameAttribute) when caseName.InnerObject ->
                    if Array.length case.Fields > 1 then
                        failwithf "Serialization as inner object requires no more than one field in a union case %s" case.Info.Name
                    true
                | _ ->
                    if Option.isSome name then
                        for field in case.Fields do
                            if field.Name = asObject.CaseFieldName then
                                failwithf "Field name %s of case %s clashes with case field name" field.Name case.Info.Name
                    false
            let ret =
                { Info = case.Info
                  Fields = case.Fields |> Array.map serializableField
                  InnerObject = innerObject
                }
            (name, ret)
        let cases = unionCases objectType |> Seq.map caseInfo |> Map.ofSeq
        let ret =
            { CaseField = asObject.CaseFieldName
              Cases = cases
            }
        Some ret
    | _ -> failwith "Impossible"

// Needed for dictionary keys.
type NewtypeConverter<'nt> () =
    inherit TypeConverter ()

    let info = unionCases typeof<'nt> |> isNewtype |> Option.get

    override this.CanConvertFrom (context : ITypeDescriptorContext, sourceType : Type) : bool =
        info.Type.PropertyType = sourceType

    override this.CanConvertTo (context : ITypeDescriptorContext, destinationType : Type) : bool =
        typeof<'nt> = destinationType

    override this.ConvertTo (context : ITypeDescriptorContext, culture: CultureInfo, value : obj, destinationType : Type) : obj =
        let (case, args) = FSharpValue.GetUnionFields(value, typeof<'nt>)
        args.[0]

    override this.ConvertFrom (context : ITypeDescriptorContext, culture : CultureInfo, value : obj) : obj =
        FSharpValue.MakeUnion(info.Case, [|value|])
