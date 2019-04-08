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

type JToken = Newtonsoft.Json.Linq.JToken

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

let private unionAsObject (objectType : Type) =
    let asObject = Attribute.GetCustomAttribute(objectType, typeof<SerializeAsObjectAttribute>) :?> SerializeAsObjectAttribute
    if isNull asObject then
        None
    else
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
            | Some ((someCase, someProperty), noneCase) ->
                fun reader serializer ->
                    if reader.TokenType = JsonToken.Null then
                        ignore <| reader.Read()
                        FSharpValue.MakeUnion(noneCase, [||])
                    else
                        let arg = serializer.Deserialize(reader, someProperty.PropertyType)
                        FSharpValue.MakeUnion(someCase, [|arg|])
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
                                    | (true, valueToken) -> valueToken.ToObject(prop.PropertyType)
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
                                let args = fields |> Array.mapi (fun i field -> array.[i + 1].ToObject(field.PropertyType))
                                FSharpValue.MakeUnion(case, args)

    let writeJson : UnionCaseInfo -> obj[] -> JsonWriter -> JsonSerializer -> unit =
        match isNewtype cases with
        | Some (case, argProperty) ->
            fun case args writer serializer ->
                serializer.Serialize(writer, args.[0])
        | None ->
            match isOption cases with
            | Some ((someCase, someProperty), noneCase) ->
                fun case args writer serializer ->
                    if case = noneCase then
                        writer.WriteNull()
                    else if case = someCase then
                        serializer.Serialize(writer, args.[0])
                    else
                        failwith "Sanity check failed"
            | None ->
                if isUnionEnum cases then
                    fun case args writer serializer ->
                        serializer.Serialize(writer, case.Name)
                else
                    match unionAsObject objectType with
                    | Some (caseFieldName, caseNames) ->
                        let reverseNames = caseNames |> Map.toSeq |> Seq.map (fun (name, (case, fields)) -> (case.Name, (name, fields))) |> Map.ofSeq
                        fun case args writer serializer ->
                            let (caseName, fields) = Map.find case.Name reverseNames
                            writer.WriteStartObject()
                            writer.WritePropertyName(caseFieldName)
                            writer.WriteValue(caseName)
                            for (field, arg) in Seq.zip fields args do
                                writer.WritePropertyName(field.Name)
                                serializer.Serialize(writer, arg)
                            writer.WriteEndObject()
                    | None ->
                        fun case args writer serializer ->
                            serializer.Serialize(writer, Array.append [|case.Name :> obj|] args)

    override this.CanConvert (someType : Type) : bool =
        someType = objectType

    override this.ReadJson (reader : JsonReader, someType : Type, existingValue, serializer : JsonSerializer) : obj =
        readJson reader serializer

    override this.WriteJson (writer : JsonWriter, value : obj, serializer : JsonSerializer) : unit =
        let (case, args) = FSharpValue.GetUnionFields(value, objectType)
        writeJson case args writer serializer

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
                contract.Converter <- UnionConverter objectType
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

(* Default settings for F# types:
   * All fields are always required, unless type is Option;
   * Default values for containers are provided.
*)
let defaultJsonSerializerSettings =
    JsonSerializerSettings(
        ContractResolver = ConverterContractResolver (),
        DefaultValueHandling = DefaultValueHandling.Populate
    )

let tryJson (str : string) : JToken option =
    try
        Some <| JToken.Parse(str)
    with
    | :? JsonException -> None