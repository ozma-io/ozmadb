module FunWithFlags.FunDB.Serialization.Utils

open System
open System.Globalization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

open FunWithFlags.FunDB.Utils

type UnionCases = (UnionCaseInfo * PropertyInfo[])[]

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

let unionCases (objectType : Type) : UnionCases =
    FSharpType.GetUnionCases(objectType) |> Array.map (fun case -> (case, case.GetFields()))

let isUnionEnum : UnionCases -> bool =
    Seq.forall <| fun (case, fields) -> Array.isEmpty fields

let getNewtype (case, fields) =
    if Array.length fields = 1 then
        Some (case, fields.[0])
    else
        None

let isNewtype (map : UnionCases) : (UnionCaseInfo * PropertyInfo) option =
    if Array.length map = 1 then
        getNewtype map.[0]
    else
        None

type OptionInfo =
    { someCase : UnionCaseInfo
      someType : PropertyInfo
      noneCase : UnionCaseInfo
    }

let isOption (map : UnionCases) : OptionInfo option =
    let checkNone (case, fields) =
        if Array.isEmpty fields then
            Some case
        else
            None
    if Array.length map = 2 then
        match (getNewtype map.[0], checkNone map.[1]) with
        | (Some (someCase, someType), Some none) -> Some { someCase = someCase; someType = someType; noneCase = none }
        | _ ->
            match (getNewtype map.[1], checkNone map.[0]) with
            | (Some (someCase, someType), Some none) -> Some { someCase = someCase; someType = someType; noneCase = none }
            | _ -> None
    else
        None

let unionAsObject (objectType : Type) =
    match Attribute.GetCustomAttribute(objectType, typeof<SerializeAsObjectAttribute>) with
    | null -> None
    | :? SerializeAsObjectAttribute as asObject ->
        let getName (case : UnionCaseInfo, fields: PropertyInfo[]) =
            let name =
                match case.GetCustomAttributes typeof<CaseNameAttribute> |> Seq.cast |> Seq.first with
                | None -> case.Name
                | Some (nameAttr : CaseNameAttribute) -> nameAttr.CaseName
            for field in fields do
                if field.Name = name then
                    failwith <| sprintf "Field name %s of case %s clashes with case field name" field.Name case.Name
            name
        let nameCases = unionCases objectType |> Seq.map (function c -> (getName c, c)) |> Map.ofSeqUnique
        Some (asObject.CaseFieldName, nameCases)
    | _ -> failwith "Impossible"    

let getTypeDefaultValue (objectType : Type) : obj option =
    if isNullableType objectType then
        Some null
    else if FSharpType.IsUnion objectType then
        let cases = unionCases objectType
        match isOption cases with
        | Some option ->
            let nullValue = FSharpValue.MakeUnion(option.noneCase, [||])
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
