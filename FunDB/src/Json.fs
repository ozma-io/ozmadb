module FunWithFlags.FunDB.Json

open System
open System.Reflection
open System.ComponentModel
open System.Globalization
open Microsoft.FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Newtonsoft.Json.Linq
open Microsoft.FSharp.Collections

open FunWithFlags.FunDB.Utils

type UnixDateTimeOffsetConverter () =
    inherit JsonConverter<DateTimeOffset> ()

    override this.ReadJson (reader : JsonReader, objectType, existingValue, hasExistingValue, serializer : JsonSerializer) : DateTimeOffset =
        let secs = serializer.Deserialize<int64>(reader)
        DateTimeOffset.FromUnixTimeSeconds(secs)
 
    override this.WriteJson (writer : JsonWriter, value : DateTimeOffset, serializer : JsonSerializer) : unit =
        serializer.Serialize(writer, value.ToUnixTimeSeconds())

type UnixDateTimeConverter () =
    inherit JsonConverter<DateTime> ()

    override this.ReadJson (reader : JsonReader, objectType, existingValue, hasExistingValue, serializer : JsonSerializer) : DateTime =
        let secs = serializer.Deserialize<int64>(reader)
        DateTimeOffset.FromUnixTimeSeconds(secs).UtcDateTime
 
    override this.WriteJson (writer : JsonWriter, value : DateTime, serializer : JsonSerializer) : unit =
        serializer.Serialize(writer, DateTimeOffset(value).ToUnixTimeSeconds())

type private UnionCases = (UnionCaseInfo * PropertyInfo[])[]

let private unionCases (objectType : Type) : UnionCases =
    FSharpType.GetUnionCases(objectType) |> Array.map (fun case -> (case, case.GetFields()))

let private isUnionEnum : UnionCases -> bool =
    Seq.forall <| fun (case, fields) -> Array.isEmpty fields

let private getNewtype (case, fields) =
    if Array.length fields = 1 then
        Some (case, fields.[0])
    else
        None

let private isNewtype (map : UnionCases) : (UnionCaseInfo * PropertyInfo) option =
    if Array.length map = 1 then
        getNewtype map.[0]
    else
        None

let private isOption (map : UnionCases) : ((UnionCaseInfo * PropertyInfo) * UnionCaseInfo) option =
    let checkNone (case, fields) =
        if Array.isEmpty fields then
            Some case
        else
            None
    if Array.length map = 2 then
        match (getNewtype map.[0], checkNone map.[1]) with
        | (Some just, Some none) -> Some (just, none)
        | _ ->
            match (getNewtype map.[1], checkNone map.[0]) with
            | (Some just, Some none) -> Some (just, none)
            | _ -> None
    else
        None

type UnionConverter () =
    inherit JsonConverter ()

    override this.CanConvert (objectType : Type) : bool =
        FSharpType.IsUnion objectType

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, serializer : JsonSerializer) : obj =
        let cases = unionCases objectType

        let searchUnion () =
            let tryCase : (UnionCaseInfo * PropertyInfo[]) -> obj option =
                if isUnionEnum cases then
                    let name = serializer.Deserialize<string>(reader)
                    fun (case, _) ->
                        if case.Name = name then
                            Some <| FSharpValue.MakeUnion(case, [||])
                        else
                            None
                else
                    let array = JArray.Load reader
                    let nameToken = array.[0]
                    if nameToken.Type <> JTokenType.String then
                        raise <| JsonException("Invalid union name")
                    let name = JToken.op_Explicit nameToken : string
                    fun (case, fields) ->
                        if case.Name = name then
                            let args = fields |> Array.mapi (fun i field -> array.[i + 1].ToObject(field.PropertyType))
                            Some <| FSharpValue.MakeUnion(case, args)
                        else
                            None
            match cases |> Seq.mapMaybe tryCase |> Seq.first with
            | Some ret -> ret
            | None -> raise <| JsonException("Unknown union case")

        match isNewtype cases with
        | Some (case, argProperty) ->
            let arg = serializer.Deserialize(reader, argProperty.PropertyType)
            FSharpValue.MakeUnion(case, [|arg|])
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match isOption cases with
            | Some ((someCase, someProperty), noneCase) ->
                if reader.TokenType = JsonToken.Null then
                    ignore <| reader.Read()
                    FSharpValue.MakeUnion(noneCase, [||])
                else
                    let arg = serializer.Deserialize(reader, someProperty.PropertyType)
                    FSharpValue.MakeUnion(someCase, [|arg|])
            | None -> searchUnion ()
 
    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let objectType = value.GetType()
        let cases = unionCases objectType
        let (case, args) = FSharpValue.GetUnionFields(value, objectType)

        match isNewtype cases with
        | Some (case, argProperty) ->
            serializer.Serialize(writer, args.[0])
        | None ->
            match isOption cases with
            | Some ((someCase, someProperty), noneCase) ->
                if case = noneCase then
                    writer.WriteNull()
                else if case = someCase then
                    serializer.Serialize(writer, args.[0])
                else
                    failwith "Sanity check failed"
            | None ->
                if isUnionEnum cases then
                    serializer.Serialize(writer, case.Name)
                else
                    serializer.Serialize(writer, Array.append [|case.Name :> obj|] args)

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

// Default converters for several types
type ConverterContractResolver () =
    inherit DefaultContractResolver ()

    override this.CreateContract (objectType : Type) : JsonContract =
        let contract = base.CreateContract objectType

        if isNull contract.Converter then
            if FSharpType.IsUnion objectType then
                contract.Converter <- UnionConverter ()
            if objectType = typeof<DateTime> then
                contract.Converter <- UnixDateTimeConverter ()
            else if objectType = typeof<DateTimeOffset> then
                contract.Converter <- UnixDateTimeOffsetConverter ()
    
        contract

    override this.CreateProperty (memberInfo : MemberInfo, serialization : MemberSerialization) : JsonProperty =
        let prop = base.CreateProperty (memberInfo, serialization)

        if FSharpType.IsRecord memberInfo.DeclaringType || FSharpType.IsUnion memberInfo.DeclaringType then
            let fieldType =
                match memberInfo.MemberType with
                | MemberTypes.Property -> Some (memberInfo :?> PropertyInfo).PropertyType
                | MemberTypes.Field -> Some (memberInfo :?> FieldInfo).FieldType
                | _ -> None

            let setDefault v =
                if isNull prop.DefaultValue then
                    prop.DefaultValue <- v
            let (hasDefault, isOption) =
                match fieldType with
                | None -> (false, false)
                | Some ftype ->
                    if ftype.IsArray then
                        setDefault (Array.CreateInstance(ftype.GetElementType(), 0))
                        (true, false)
                    else if ftype.IsGenericType then
                        let genType = ftype.GetGenericTypeDefinition ()
                        if genType = typedefof<Map<_, _>> then
                            let typePars = ftype.GetGenericArguments()
                            let tupleType = FSharpType.MakeTupleType typePars
                            let emptyArray = Array.CreateInstance(tupleType, 0)
                            let emptyMap = Activator.CreateInstance(ftype, emptyArray)
                            setDefault emptyMap
                            (true, false)
                        else if genType = typedefof<Set<_>> then
                            let typePars = ftype.GetGenericArguments()
                            let emptyArray = Array.CreateInstance(typePars.[0], 0)
                            let emptySet = Activator.CreateInstance(ftype, emptyArray)
                            setDefault emptySet
                            (true, false)
                        else if genType = typedefof<_ list> then
                            let emptyCase = FSharpType.GetUnionCases(ftype) |> Array.find (fun case -> case.Name = "Empty")
                            let emptyList = FSharpValue.MakeUnion(emptyCase, [||])
                            setDefault emptyList
                            (true, false)
                        else
                            (false, genType = typedefof<_ option>) 
                    else
                        (false, false)                                          

            if prop.Required = Required.Default && not isOption then
                prop.Required <- Required.Always
    
        prop      

let defaultJsonSerializerSettings =
    JsonSerializerSettings(
        ContractResolver = ConverterContractResolver (),
        DefaultValueHandling = DefaultValueHandling.Populate
    )