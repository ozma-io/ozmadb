module FunWithFlags.FunUtils.Serialization.Json

open System
open System.Reflection
open Microsoft.FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Newtonsoft.Json.Linq
open Microsoft.FSharp.Collections

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunUtils.Serialization.Utils

type JToken = Linq.JToken

let private tokenToString (token : JToken) : string option =
    match token.Type with
    | JTokenType.String -> Some (JToken.op_Explicit token)
    | JTokenType.Null -> None
    | _ -> raisef JsonException "Invalid union name"

type UnionConverter (objectType : Type) =
    inherit JsonConverter ()

    let cases = unionCases objectType
    let casesMap = unionNames cases
    let reverseNames = casesMap |> Map.mapWithKeys (fun name case -> (case.Info.Name, name))
    let reverseUnionObjectInfo = unionAsObject objectType |> Option.map (fun info -> (info, info.Cases |> Map.mapWithKeys (fun name case -> (case.Info.Name, (name, case)))))

    let searchCases (name : string option) : UnionCase =
        match Map.tryFind name casesMap with
        | Some c -> c
        | None ->
            match name with
            | Some n -> raisef JsonException "Unknown union case \"%s\"" n
            | None -> raisef JsonException "No default union case"

    let readJson : JsonReader -> JsonSerializer -> obj =
        match isNewtype cases with
        | Some info ->
            fun reader serializer ->
                let arg = serializer.Deserialize(reader, info.Type.PropertyType)
                if not info.ValueIsNullable && isNull arg then
                    raisef JsonException "Attempted to set null to non-nullable value"
                FSharpValue.MakeUnion(info.Case, [|arg|])
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match isOption cases with
            | Some option ->
                let nullValue = FSharpValue.MakeUnion(option.NoneCase, [||])
                fun reader serializer ->
                    if reader.TokenType = JsonToken.Null then
                        reader.Skip()
                        nullValue
                    else
                        let arg = serializer.Deserialize(reader, option.SomeType.PropertyType)
                        if not option.ValueIsNullable && isNull arg then
                            raisef JsonException "Attempted to set null to non-nullable value"
                        FSharpValue.MakeUnion(option.SomeCase, [|arg|])
            | None ->
                if isUnionEnum cases then
                    fun reader serializer ->
                        let name = serializer.Deserialize<string>(reader)
                        let case = searchCases (Option.ofNull name)
                        FSharpValue.MakeUnion(case.Info, [||])
                else
                    match reverseUnionObjectInfo with
                    | Some (objInfo, _) ->
                        fun reader serializer ->
                            let obj = JObject.Load reader
                            let name =
                                match obj.TryGetValue(objInfo.CaseField) with
                                | (true, nameToken) -> tokenToString nameToken
                                | (false, _) -> None
                            match Map.tryFind name objInfo.Cases with
                            | None ->
                                match name with
                                | Some n -> raisef JsonException "Unknown union case \"%s\"" n
                                | None -> raisef JsonException "Couldn't find case property \"%s\"" objInfo.CaseField
                            | Some case ->
                                let args =
                                    if case.InnerObject then
                                        case.Fields |> Array.map (fun field -> obj.ToObject(field.Property.PropertyType, serializer))
                                    else
                                        let resolver =
                                            match serializer.ContractResolver with
                                            | :? DefaultContractResolver as r -> r
                                            | _ -> null
                                        let contract = serializer.ContractResolver.ResolveContract 
                                        let getField (field : SerializableField) =
                                            let name = if isNull resolver then field.Name else resolver.GetResolvedPropertyName(field.Name)
                                            match obj.TryGetValue(name) with
                                            | (true, valueToken) ->
                                                let value = valueToken.ToObject(field.Property.PropertyType, serializer)
                                                if not field.IsNullable && isNull value then
                                                    raisef JsonException "Attempted to set null to non-nullable field \"%s\"" name
                                                value
                                            | (false, _) ->
                                                match field.DefaultValue with
                                                | Some def -> def
                                                | None -> raisef JsonException "Couldn't find required field \"%s\"" name
                                        Seq.map getField case.Fields |> Array.ofSeq
                                FSharpValue.MakeUnion (case.Info, args)
                    | None ->
                        fun reader serializer ->
                            let array = JArray.Load reader
                            let nameToken = array.[0]
                            let name = tokenToString nameToken
                            let case = searchCases name
                            if Array.length case.Fields <> array.Count - 1 then
                                raisef JsonException "Case %s has %i arguments but %i given" case.Info.Name (Array.length case.Fields) (array.Count - 1)

                            let args = case.Fields |> Array.mapi (fun i field -> array.[i + 1].ToObject(field.PropertyType, serializer))
                            FSharpValue.MakeUnion(case.Info, args)

    let writeJson : obj -> JsonWriter -> JsonSerializer -> unit =
        match isNewtype cases with
        | Some info ->
            fun value writer serializer ->
                let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                serializer.Serialize(writer, args.[0])
        | None ->
            match isOption cases with
            | Some option ->
                fun value writer serializer ->
                    let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                    if case = option.NoneCase then
                        writer.WriteNull()
                    else if case = option.SomeCase then
                        serializer.Serialize(writer, args.[0])
                    else
                        failwith "Sanity check failed"
            | None ->
                if isUnionEnum cases then
                    fun value writer serializer ->
                        let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                        let caseName = Map.find case.Name reverseNames
                        match caseName with
                        | None -> writer.WriteNull()
                        | Some cn -> writer.WriteValue(cn)
                else
                    match reverseUnionObjectInfo with
                    | Some (objInfo, reverseCases) ->
                        fun value writer serializer ->
                            let (caseInfo, args) = FSharpValue.GetUnionFields(value, objectType)
                            let (caseName, caseObject) = Map.find caseInfo.Name reverseCases
                            if caseObject.InnerObject then
                                match Array.length args with
                                | 0 ->
                                    writer.WriteStartObject()
                                    writer.WriteEndObject()
                                | 1 -> serializer.Serialize(writer, args.[0])
                                | _ -> failwith "Impossible"
                            else
                                writer.WriteStartObject()
                                match caseName with
                                | Some cn ->
                                    writer.WritePropertyName(objInfo.CaseField)
                                    writer.WriteValue(cn)
                                | None -> ()
                                let resolver =
                                    match serializer.ContractResolver with
                                    | :? DefaultContractResolver as r -> r
                                    | _ -> null
                                for (field, arg) in Seq.zip caseObject.Fields args do
                                    match field.DefaultValue with
                                    | Some d when not (Option.defaultValue true field.EmitDefaultValue) && d = arg -> ()
                                    | _ ->
                                        let name = if isNull resolver then field.Name else resolver.GetResolvedPropertyName(field.Name)
                                        writer.WritePropertyName(name)
                                        serializer.Serialize(writer, arg)
                                // Add extra properties -- stuff from `with` block.
                                let contract = serializer.ContractResolver.ResolveContract (value.GetType()) :?> JsonObjectContract
                                for prop in contract.Properties do
                                    let fieldValue = prop.ValueProvider.GetValue(value)
                                    let skipDefaultValue = prop.DefaultValueHandling.GetValueOrDefault(DefaultValueHandling.Populate) &&& DefaultValueHandling.Ignore = DefaultValueHandling.Ignore
                                    if not (skipDefaultValue && prop.DefaultValue = fieldValue) then
                                        writer.WritePropertyName(prop.PropertyName)
                                        serializer.Serialize(writer, fieldValue)
                                writer.WriteEndObject()
                    | None ->
                        fun value writer serializer ->
                            let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                            let caseName = Map.find case.Name reverseNames
                            serializer.Serialize(writer, Seq.append (Seq.singleton (caseName :> obj)) args)

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

        if prop.Order.HasValue then 
            eprintfn "Order for %s.%s is %O" memberInfo.DeclaringType.Name memberInfo.Name prop.Order

        if FSharpType.IsRecord memberInfo.DeclaringType || FSharpType.IsUnion memberInfo.DeclaringType then
            let propInfo = memberInfo :?> PropertyInfo
            let fieldInfo = serializableField propInfo
            match fieldInfo.DefaultValue with
            | Some v when isNull prop.DefaultValue ->
                prop.DefaultValue <- v
            | _ -> ()
            match fieldInfo.EmitDefaultValue with
            | Some v when not prop.DefaultValueHandling.HasValue ->
                prop.DefaultValueHandling <- Nullable((if v then DefaultValueHandling.Include else DefaultValueHandling.Ignore) ||| DefaultValueHandling.Populate)
            | _ -> ()
            if not prop.IsRequiredSpecified && not (isNullableType prop.PropertyType) then
                if isNull prop.DefaultValue then
                    prop.Required <- Required.Always
                else
                    prop.Required <- Required.DisallowNull
                    // FIXME: No idea why doesn't DefaultValueHandling.Ignore doesn't work by itself.
                    if prop.DefaultValueHandling.GetValueOrDefault(DefaultValueHandling.Populate) &&& DefaultValueHandling.Ignore = DefaultValueHandling.Ignore then
                        prop.ShouldSerialize <- fun obj ->
                            let value = propInfo.GetValue(obj)
                            value <> prop.DefaultValue

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