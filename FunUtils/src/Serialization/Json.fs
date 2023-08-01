module FunWithFlags.FunUtils.Serialization.Json

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Newtonsoft.Json.Linq
open Microsoft.FSharp.Collections

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils

type JToken = Linq.JToken

let private tokenToString (token : JToken) : string option =
    match token.Type with
    | JTokenType.String -> Some (JToken.op_Explicit token)
    | JTokenType.Null -> None
    | _ -> raisef JsonException "Invalid union name"

let private getInnerObject (token : JToken) (serializer : JsonSerializer) (field : SerializableField) =
    if field.Ignore then
        match field.DefaultValue with
        | None -> raisef JsonException "Field is ignored and no default value is set"
        | Some v -> v
    else
        token.ToObject(field.InnerType, serializer)

let private getJsonSerializableOpts (prop : JsonProperty) =
    { DefaultIgnore = Some prop.Ignored
      DefaultName = Some prop.PropertyName
    }

type UnionConverter (objectType : Type, resolver : ConverterContractResolver, contract : JsonContract) =
    inherit JsonConverter ()

    let cases = unionCases objectType

    let getSerializableOpts (prop : PropertyInfo) =
        let p = resolver.BaseCreateProperty(prop, MemberSerialization.OptOut)
        getJsonSerializableOpts p

    let (readJson, writeJson) =
        match unionAsObject getSerializableOpts objectType with
        | Some objInfo ->
            let casesMap =
                objInfo.Cases
                |> Map.values
                |> Seq.map (fun i -> (i.Name, i))
                |> Map.ofSeq

            let objectContract = contract :?> JsonObjectContract

            let read =
                if Map.count casesMap < Map.count objInfo.Cases then
                    fun (reader : JsonReader) (serializer : JsonSerializer) ->
                        raisef NotSupportedException "Case keys mapping is not bijective"
                else
                    fun (reader : JsonReader) (serializer : JsonSerializer) ->
                        match JToken.Load reader with
                        | :? JObject as obj ->
                            let name =
                                match obj.TryGetValue(objInfo.CaseField) with
                                | (true, nameToken) -> tokenToString nameToken
                                | (false, _) -> None
                            let case =
                                match Map.tryFind name casesMap with
                                | Some case -> case
                                | None ->
                                    match name with
                                    | Some n when objInfo.AllowUnknownType ->
                                        match Map.tryFind None casesMap with
                                        | Some case -> case
                                        | None -> raisef JsonException "Unknown union case \"%s\"" n
                                    | Some n -> raisef JsonException "Unknown union case \"%s\"" n
                                    | None -> raisef JsonException "Couldn't find case property \"%s\"" objInfo.CaseField
                            let args =
                                match case.Type with
                                | CSTInnerValue
                                | CSTInnerObject ->
                                    case.Fields |> Array.map (getInnerObject obj serializer)
                                | CSTNormal ->
                                    let getField (field : SerializableField) =
                                        if field.Ignore then
                                            match field.DefaultValue with
                                            | Some def -> def
                                            | None -> raisef JsonException "Field \"%s\" is ignored and no default value is set" field.Name
                                        else
                                            match obj.TryGetValue(field.Name) with
                                            | (true, valueToken) ->
                                                let value = valueToken.ToObject(field.InnerType, serializer)
                                                if not field.IsNullable && isNull value then
                                                    raisef JsonException "Attempted to set null to non-nullable field \"%s\"" field.Name
                                                value
                                            | (false, _) ->
                                                match field.DefaultValue with
                                                | Some def -> def
                                                | None -> raisef JsonException "Couldn't find required field \"%s\"" field.Name
                                    Seq.map getField case.Fields |> Array.ofSeq
                            case.ConvertTo args
                        | token ->
                            match objInfo.ValueCase with
                            | None -> raisef JsonException "Object expected"
                            | Some name ->
                                let case = objInfo.Cases.[name]
                                let args = case.Fields |> Array.map (getInnerObject token serializer)
                                case.ConvertTo args

            let write =
                fun value (writer : JsonWriter) (serializer : JsonSerializer) ->
                    let tag = objInfo.GetTag value
                    let case = objInfo.Cases.[tag]
                    let args = case.ConvertFrom(value)
                    match case.Type with
                    | CSTInnerValue
                    | CSTInnerObject ->
                        let singleField = case.Fields |> Array.tryFindIndex (fun f -> not f.Ignore)
                        match singleField with
                        | None ->
                            writer.WriteStartObject()
                            match case.Name with
                            | None -> ()
                            | _ when case.Type = CSTInnerValue -> ()
                            | Some cn ->
                                writer.WritePropertyName(objInfo.CaseField)
                                writer.WriteValue(cn)
                            writer.WriteEndObject()
                        | Some singleIndex ->
                            match case.Name with
                            | None -> serializer.Serialize(writer, args.[singleIndex])
                            | _ when case.Type = CSTInnerValue -> serializer.Serialize(writer, args.[singleIndex])
                            | Some cn ->
                                let obj = JObject.FromObject(args.[singleIndex], serializer)
                                obj.[objInfo.CaseField] <- JToken.op_Implicit cn
                                obj.WriteTo(writer)
                    | CSTNormal ->
                        writer.WriteStartObject()
                        match case.Name with
                        | Some cn ->
                            writer.WritePropertyName(objInfo.CaseField)
                            writer.WriteValue(cn)
                        | None -> ()
                        for (field, arg) in Seq.zip case.Fields args do
                            match field.DefaultValue with
                            | _ when field.Ignore -> ()
                            | Some d when not (Option.defaultValue true field.EmitDefaultValue) && d = arg -> ()
                            | _ ->
                                writer.WritePropertyName(field.Name)
                                serializer.Serialize(writer, arg)
                        // Add extra properties -- stuff from `with` block.
                        // We just use properties from the contract, as
                        // they are built for the "base" union type, and only contain
                        // these shared fields.
                        for prop in objectContract.Properties do
                            if not prop.Ignored then
                                let fieldValue = prop.ValueProvider.GetValue(value)
                                let skipDefaultValue = prop.DefaultValueHandling.GetValueOrDefault(DefaultValueHandling.Populate) &&& DefaultValueHandling.Ignore = DefaultValueHandling.Ignore
                                if not (skipDefaultValue && prop.DefaultValue = fieldValue) then
                                    writer.WritePropertyName(prop.PropertyName)
                                    serializer.Serialize(writer, fieldValue)
                        writer.WriteEndObject()
            (read, write)
        | None ->
            match getNewtype cases with
            | Some info ->
                let read =
                    fun (reader : JsonReader) (serializer : JsonSerializer) ->
                        let arg = serializer.Deserialize(reader, info.Type.PropertyType)
                        if not info.ValueIsNullable && isNull arg then
                            raisef JsonException "Attempted to set null to non-nullable value"
                        FSharpValue.MakeUnion(info.Case, [|arg|])
                let write =
                    fun value (writer : JsonWriter) (serializer : JsonSerializer) ->
                        let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                        serializer.Serialize(writer, args.[0])
                (read, write)
            | None ->
                // XXX: We do lose information here is we serialize e.g. ('a option option).
                match getOption cases with
                | Some option ->
                    let read =
                        fun (reader : JsonReader) (serializer : JsonSerializer) ->
                            if reader.TokenType = JsonToken.Null then
                                reader.Skip()
                                option.NoneValue
                            else
                                let arg = serializer.Deserialize(reader, option.SomeType.PropertyType)
                                if not option.ValueIsNullable && isNull arg then
                                    raisef JsonException "Attempted to set null to non-nullable value"
                                FSharpValue.MakeUnion(option.SomeCase, [|arg|])
                    let write =
                        fun value (writer : JsonWriter) (serializer : JsonSerializer) ->
                            if value = option.NoneValue then
                                writer.WriteNull()
                            else
                                let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                                assert (case = option.SomeCase)
                                assert (Array.length args = 1)
                                serializer.Serialize(writer, args.[0])
                    (read, write)
                | None ->
                    match getUnionEnum objectType with
                    | Some options ->
                        let optionsMap = options.Cases |> Map.mapWithKeys (fun tag case -> (case.Name, case.Value))
                        let read =
                            fun (reader : JsonReader) (serializer : JsonSerializer) ->
                                let name = serializer.Deserialize<string>(reader)
                                match Map.tryFind (Option.ofObj name) optionsMap with
                                | Some v -> v
                                | None -> raisef JsonException "Unknown enum case %s" name
                        let write =
                            fun value (writer : JsonWriter) (serializer : JsonSerializer) ->
                                let tag = options.GetTag value
                                let case = Map.find tag options.Cases
                                match case.Name with
                                | None -> writer.WriteNull()
                                | Some cn -> writer.WriteValue(cn)
                        (read, write)
                    | None ->
                        let casesMap = cases |> Seq.map (fun case -> caseKey case.Info, case) |> Map.ofSeq
                        let read =
                            fun (reader : JsonReader) (serializer : JsonSerializer) ->
                                let array = JArray.Load reader
                                let nameToken = array.[0]
                                let name = tokenToString nameToken
                                let case =
                                    match Map.tryFind name casesMap with
                                    | Some c -> c
                                    | None ->
                                        match name with
                                        | Some n -> raisef JsonException "Unknown union case \"%s\"" n
                                        | None -> raisef JsonException "No default union case"
                                if Array.length case.Fields <> array.Count - 1 then
                                    raisef JsonException "Case %s has %i arguments but %i given" case.Info.Name (Array.length case.Fields) (array.Count - 1)

                                let args = case.Fields |> Array.mapi (fun i field -> array.[i + 1].ToObject(field.PropertyType, serializer))
                                FSharpValue.MakeUnion(case.Info, args)
                        let reverseNames = casesMap |> Map.mapWithKeys (fun name case -> (case.Info.Name, name))
                        let write =
                            fun value (writer : JsonWriter) (serializer : JsonSerializer) ->
                                let (case, args) = FSharpValue.GetUnionFields(value, objectType)
                                let caseName = Map.find case.Name reverseNames
                                serializer.Serialize(writer, Seq.append (Seq.singleton (caseName :> obj)) args)
                        (read, write)

    override this.CanConvert (someType : Type) : bool =
        someType = objectType

    override this.ReadJson (reader : JsonReader, someType : Type, existingValue, serializer : JsonSerializer) : obj =
        readJson reader serializer

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        writeJson value writer serializer

// Default converters for several types
and ConverterContractResolver (converterConstructors : (Type -> JsonConverter option) array) =
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
                contract.Converter <- UnionConverter (objectType, this, contract)

        contract

    member this.BaseCreateProperty (memberInfo : MemberInfo, serialization : MemberSerialization) =
        base.CreateProperty (memberInfo, serialization)

    override this.CreateProperty (memberInfo : MemberInfo, serialization : MemberSerialization) : JsonProperty =
        let prop = base.CreateProperty (memberInfo, serialization)

        if FSharpType.IsRecord memberInfo.DeclaringType || FSharpType.IsUnion memberInfo.DeclaringType then
            let serializableOpts = getJsonSerializableOpts prop
            let fieldInfo = serializableField serializableOpts memberInfo
            let fieldInfo = { fieldInfo with Name = this.ResolvePropertyName fieldInfo.Name }
            prop.Ignored <- fieldInfo.Ignore
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
                            let value = fieldInfo.GetValue(obj)
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

let jtokenComparer = new JTokenEqualityComparer()

[<Struct; CustomEquality; NoComparison>]
type ComparableJToken = ComparableJToken of JToken
    with
        member this.Json =
            let (ComparableJToken tok) = this
            tok

        override x.Equals yobj =
            match yobj with
            | :? ComparableJToken as y ->
                jtokenComparer.Equals(x.Json, y.Json)
            | _ -> false

        override this.GetHashCode () =
            jtokenComparer.GetHashCode(this.Json)

        interface IEquatable<ComparableJToken> with
            override x.Equals y = jtokenComparer.Equals(x.Json, y.Json)