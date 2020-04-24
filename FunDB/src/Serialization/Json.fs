module FunWithFlags.FunDB.Serialization.Json

open System
open System.Collections.Generic
open System.Reflection
open Microsoft.FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Newtonsoft.Json.Linq
open Microsoft.FSharp.Collections

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Serialization.Utils

type JToken = Linq.JToken

type UnionConverter (objectType : Type) =
    inherit JsonConverter ()

    let cases = unionCases objectType
    let casesMap = cases |> Seq.map (function (case, _) as c -> (case.Name, c)) |> Map.ofSeq

    let searchCases (name : string) (doCase : (UnionCaseInfo * PropertyInfo[]) -> obj) : obj =
        match Map.tryFind name casesMap with
            | Some c -> doCase c
            | None -> raise <| JsonException(sprintf "Unknown union case \"%s\"" name)

    let readJson : JsonReader -> JsonSerializer -> obj =
        match isNewtype cases with
        | Some (case, argProperty) ->
            fun reader serializer ->
                let arg = serializer.Deserialize(reader, argProperty.PropertyType)
                FSharpValue.MakeUnion(case, [|arg|])
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match isOption cases with
            | Some option ->
                let nullValue = FSharpValue.MakeUnion(option.noneCase, [||])
                fun reader serializer ->
                    if reader.TokenType = JsonToken.Null then
                        reader.Skip()
                        nullValue
                    else
                        let arg = serializer.Deserialize(reader, option.someType.PropertyType)
                        FSharpValue.MakeUnion(option.someCase, [|arg|])
            | None ->
                if isUnionEnum cases then
                    fun reader serializer ->
                        let name = serializer.Deserialize<string>(reader)
                        searchCases name <| fun (case, _) ->
                            FSharpValue.MakeUnion(case, [||])
                else
                    match unionAsObject objectType with
                    | Some (caseFieldName, caseNames) ->
                        fun reader serializer ->
                            let obj = JObject.Load reader
                            let name =
                                match obj.TryGetValue(caseFieldName) with
                                | (true, nameToken) -> JToken.op_Explicit nameToken : string
                                | (false, _) -> raise <| JsonException(sprintf "Couldn't find case property \"%s\"" caseFieldName)
                            match Map.tryFind name caseNames with
                            | Some (case, fields) ->
                                let getField (prop : PropertyInfo) =
                                    match obj.TryGetValue(prop.Name) with
                                    | (true, valueToken) -> valueToken.ToObject(prop.PropertyType, serializer)
                                    | (false, _) -> raise <| JsonException(sprintf "Couldn't find required field \"%s\"" prop.Name)
                                let args = fields |> Array.map getField
                                FSharpValue.MakeUnion (case, args)
                            | None -> raise <| JsonException(sprintf "Unknown union case \"%s\"" name)
                    | None ->
                        fun reader serializer ->
                            let array = JArray.Load reader
                            let nameToken = array.[0]
                            if nameToken.Type <> JTokenType.String then
                                raise <| JsonException("Invalid union name")
                            let name = JToken.op_Explicit nameToken : string
                            searchCases name <| fun (case, fields) ->
                                if Array.length fields <> array.Count - 1 then
                                    raise <| JsonException(sprintf "Case \"%s\" has %i arguments but %i given" name (Array.length fields) (array.Count - 1))

                                let args = fields |> Array.mapi (fun i field -> array.[i + 1].ToObject(field.PropertyType, serializer))
                                FSharpValue.MakeUnion(case, args)

    let writeJson : obj -> JsonWriter -> JsonSerializer -> unit =
        match isNewtype cases with
        | Some (case, argProperty) ->
            fun value  writer serializer ->
                let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                serializer.Serialize(writer, args.[0])
        | None ->
            match isOption cases with
            | Some option ->
                fun value writer serializer ->
                    let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                    if case = option.noneCase then
                        writer.WriteNull()
                    else if case = option.someCase then
                        serializer.Serialize(writer, args.[0])
                    else
                        failwith "Sanity check failed"
            | None ->
                if isUnionEnum cases then
                    fun value writer serializer ->
                        let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                        writer.WriteValue(case.Name)
                else
                    match unionAsObject objectType with
                    | Some (caseFieldName, caseNames) ->
                        let reverseNames = caseNames |> Map.toSeq |> Seq.map (fun (name, (case, fields)) -> (case.Name, (name, fields))) |> Map.ofSeq
                        fun value writer serializer ->
                            let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                            let (caseName, fields) = Map.find case.Name reverseNames
                            writer.WriteStartObject()
                            writer.WritePropertyName(caseFieldName)
                            writer.WriteValue(caseName)
                            for (field, arg) in Seq.zip fields args do
                                writer.WritePropertyName(field.Name)
                                serializer.Serialize(writer, arg)
                            writer.WriteEndObject()
                    | None ->
                        fun value writer serializer ->
                            let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                            serializer.Serialize(writer, Seq.append (Seq.singleton (case.Name :> obj)) args)

    override this.CanConvert (someType : Type) : bool =
        someType = objectType

    override this.ReadJson (reader : JsonReader, someType : Type, existingValue, serializer : JsonSerializer) : obj =
        readJson reader serializer

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        writeJson value writer serializer

// Default converters for several types
type ConverterContractResolver (converterConstructors : (Type -> JsonConverter option) array) =
    inherit DefaultContractResolver ()

    override this.CreateContract (objectType : Type) : JsonContract =
        let contract = base.CreateContract objectType

        if isNull contract.Converter then
            let applyConverter (converter : JsonConverter) =
                if converter.CanConvert objectType then
                    contract.Converter <- converter
                    false
                else
                    true
            converterConstructors |> Seq.mapMaybe (fun getConv -> getConv objectType) |> Seq.iterStop applyConverter

        if isNull contract.Converter then
            if FSharpType.IsUnion objectType then
                contract.Converter <- UnionConverter objectType

        contract

    override this.CreateProperty (memberInfo : MemberInfo, serialization : MemberSerialization) : JsonProperty =
        let prop = base.CreateProperty (memberInfo, serialization)

        if FSharpType.IsRecord memberInfo.DeclaringType || FSharpType.IsUnion memberInfo.DeclaringType then
            let maybeFieldType =
                match memberInfo.MemberType with
                | MemberTypes.Property -> Some (memberInfo :?> PropertyInfo).PropertyType
                | MemberTypes.Field -> Some (memberInfo :?> FieldInfo).FieldType
                | _ -> None
            match maybeFieldType with
            | None -> ()
            | Some ftype ->
                let setDefault v =
                    if isNull prop.DefaultValue then
                        prop.DefaultValue <- v

                let isRequired =
                    if ftype.IsArray then
                        setDefault (Array.CreateInstance(ftype.GetElementType(), 0))
                        true
                    else if ftype.IsGenericType then
                        let genType = ftype.GetGenericTypeDefinition ()
                        if genType = typedefof<Map<_, _>> then
                            let typePars = ftype.GetGenericArguments()
                            let tupleType = FSharpType.MakeTupleType typePars
                            let emptyArray = Array.CreateInstance(tupleType, 0)
                            let emptyMap = Activator.CreateInstance(ftype, emptyArray)
                            setDefault emptyMap
                            true
                        else if genType = typedefof<Set<_>> then
                            let typePars = ftype.GetGenericArguments()
                            let emptyArray = Array.CreateInstance(typePars.[0], 0)
                            let emptySet = Activator.CreateInstance(ftype, emptyArray)
                            setDefault emptySet
                            true
                        else if genType = typedefof<_ list> then
                            let emptyCase = FSharpType.GetUnionCases(ftype) |> Array.find (fun case -> case.Name = "Empty")
                            let emptyList = FSharpValue.MakeUnion(emptyCase, [||])
                            setDefault emptyList
                            true
                        else
                            genType <> typedefof<_ option>
                    else
                        ftype <> typeof<obj>

                if not prop.IsRequiredSpecified && isRequired then
                    prop.Required <- Required.Always

        prop
    
(* Default settings for F# types:
   * All fields are always required, unless type is Option;
   * Default values for containers are provided.
*)
let makeDefaultJsonSerializerSettings (converterConstructors : (Type -> JsonConverter option) array) =
    JsonSerializerSettings(
        ContractResolver = ConverterContractResolver (converterConstructors),
        DefaultValueHandling = DefaultValueHandling.Populate
    )

let tryJson (str : string) : JToken option =
    try
        Some <| JToken.Parse(str)
    with
    | :? JsonException -> None

let jsonObject (vals : (string * JToken) seq) : JObject =
    let o = JObject()
    for (k, v) in vals do
        o.[k] <- v
    o

let jsonArray (vals : JToken seq) : JArray =
    let arr = JArray()
    for v in vals do
        arr.Add(v)
    arr