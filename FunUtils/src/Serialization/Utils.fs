module FunWithFlags.FunUtils.Serialization.Utils

open System
open System.Globalization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils.Utils

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

// https://stackoverflow.com/questions/56600268/how-do-i-check-t-for-types-not-being-allowed-to-be-null
let isNullableType (objectType : Type) =
    if FSharpType.IsRecord objectType then
        false
    else if FSharpType.IsUnion objectType then
        match Attribute.GetCustomAttribute(objectType, typeof<CompilationRepresentationAttribute>) with
        | null -> false
        | :? CompilationRepresentationAttribute as representation ->
            representation.Flags.HasFlag(CompilationRepresentationFlags.UseNullAsTrueValue)
        | _ -> failwith "Impossible"
    else
        match Attribute.GetCustomAttribute(objectType, typeof<CompilationMappingAttribute>) with
        | null -> true
        | :? CompilationMappingAttribute as mapping when mapping.SourceConstructFlags.HasFlag(SourceConstructFlags.ObjectType) ->
            match Attribute.GetCustomAttribute(objectType, typeof<AllowNullLiteralAttribute>) with
            | null -> false
            | :? AllowNullLiteralAttribute -> true
            | _ -> failwith "Impossible"
        | :? CompilationMappingAttribute -> true
        | _ -> failwith "Impossible"

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

let isUnionEnum : UnionCase seq -> bool =
    Seq.forall (fun case -> Array.isEmpty case.Fields)

let getNewtype (case : UnionCase) : (UnionCaseInfo * PropertyInfo) option =
    if Array.length case.Fields = 1 then
        Some (case.Info, case.Fields.[0])
    else
        None

let isNewtype (map : UnionCase[]) : (UnionCaseInfo * PropertyInfo) option =
    if Seq.length map = 1 then
        getNewtype (Seq.first map |> Option.get)
    else
        None

type OptionInfo =
    { SomeCase : UnionCaseInfo
      SomeType : PropertyInfo
      NoneCase : UnionCaseInfo
    }

let isOption (map : UnionCase[]) : OptionInfo option =
    let checkNone case =
        if Array.isEmpty case.Fields then
            Some case.Info
        else
            None
    if Array.length map = 2 then
        match (getNewtype map.[0], checkNone map.[1]) with
        | (Some (someCase, someType), Some none) -> Some { SomeCase = someCase; SomeType = someType; NoneCase = none }
        | _ ->
            match (getNewtype map.[1], checkNone map.[0]) with
            | (Some (someCase, someType), Some none) -> Some { SomeCase = someCase; SomeType = someType; NoneCase = none }
            | _ -> None
    else
        None

type CaseAsObjectInfo =
    { Case : UnionCase
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
                    if Option.isSome name then
                        failwithf "Serialization as inner object requires removed case label in a union case %s" case.Info.Name
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
                { Case = case
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

let getTypeDefaultValue (objectType : Type) : obj option =
    if isNullableType objectType then
        Some null
    else if FSharpType.IsUnion objectType then
        let cases = unionCases objectType
        match isOption cases with
        | Some option ->
            let nullValue = FSharpValue.MakeUnion(option.NoneCase, [||])
            Some nullValue
        | None -> None
    else
        None

// Needed for dictionary keys.
type NewtypeConverter<'nt> () =
    inherit TypeConverter ()

    let (case, argType) = unionCases typeof<'nt> |> isNewtype |> Option.get

    override this.CanConvertFrom (context : ITypeDescriptorContext, sourceType : Type) : bool =
        argType.PropertyType = sourceType

    override this.CanConvertTo (context : ITypeDescriptorContext, destinationType : Type) : bool =
        typeof<'nt> = destinationType

    override this.ConvertTo (context : ITypeDescriptorContext, culture: CultureInfo, value : obj, destinationType : Type) : obj =
        let (case, args) = FSharpValue.GetUnionFields(value, typeof<'nt>)
        args.[0]

    override this.ConvertFrom (context : ITypeDescriptorContext, culture : CultureInfo, value : obj) : obj =
        FSharpValue.MakeUnion(case, [|value|])
