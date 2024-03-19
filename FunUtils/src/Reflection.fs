module FunWithFlags.FunUtils.Reflection

open System
open System.Globalization
open System.Reflection
open System.ComponentModel
open Microsoft.FSharp.Reflection

type UnionCase =
    { Info : UnionCaseInfo
      Fields : PropertyInfo[]
      ConvertFrom : obj -> obj[]
      ConvertTo : obj[] -> obj
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

// https://stackoverflow.com/questions/56600268/how-do-i-check-t-for-types-not-being-allowed-to-be-null
let isNullableType (objectType : Type) =
    let genericType = lazy (objectType.GetGenericTypeDefinition())
    if FSharpType.IsRecord objectType then
        false
    else if FSharpType.IsUnion objectType then
        match objectType.GetCustomAttribute<CompilationRepresentationAttribute>() with
        | RefNull -> false
        | representation -> representation.Flags.HasFlag(CompilationRepresentationFlags.UseNullAsTrueValue)
    else if objectType = typeof<string> then
        false
    else if objectType.IsGenericType && genericType.Value = typedefof<seq<_>> then
        false
    else
        match objectType.GetCustomAttribute<CompilationMappingAttribute>() with
        | mapping
            when not (isRefNull mapping) && mapping.SourceConstructFlags.HasFlag(SourceConstructFlags.ObjectType) ->
                not (isRefNull <| objectType.GetCustomAttribute<AllowNullLiteralAttribute>())
        | _ -> not objectType.IsValueType || not (isNull <| Nullable.GetUnderlyingType(objectType))

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
