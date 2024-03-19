module FunWithFlags.FunUtils.Serialization.Yaml

open System
open System.Globalization
open System.Collections.Generic
open System.Collections.Concurrent
open Newtonsoft.Json.Linq
open YamlDotNet.Core
open YamlDotNet.Core.Events
open YamlDotNet.Serialization
open YamlDotNet.Serialization.NamingConventions
open YamlDotNet.Serialization.NodeDeserializers
open YamlDotNet.Serialization.Utilities
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Reflection
open FunWithFlags.FunUtils.Serialization.Utils

type TypedScalar =
    | TSString of string
    | TSInt of int
    | TSFloat of double
    | TSBool of bool
    | TSNull

let parseScalar (scalar : Scalar) : TypedScalar =
    match scalar.Style with
    | ScalarStyle.Plain ->
        // https://yaml.org/spec/1.2-old/spec.html#id2804356
        match scalar.Value with
        | ""
        | "~"
        | "null"
        | "Null"
        | "NULL" -> TSNull
        | "true"
        | "True"
        | "TRUE" -> TSBool true
        | "false"
        | "False"
        | "FALSE" -> TSBool false
        | ".inf"
        | ".Inf"
        | ".INF" -> TSFloat Double.PositiveInfinity
        | "-.inf"
        | "-.Inf"
        | "-.INF" -> TSFloat Double.NegativeInfinity
        | ".nan"
        | ".NaN"
        | ".NAN" -> TSFloat Double.NaN
        | value ->
            match Int32.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture) with
            | (true, ret) -> TSInt ret
            | (false, _) ->
                match Double.TryParse(value, NumberStyles.AllowLeadingSign ||| NumberStyles.AllowDecimalPoint ||| NumberStyles.AllowExponent, CultureInfo.InvariantCulture) with
                | (true, ret) -> TSFloat ret
                | (false, _) -> TSString scalar.Value
    | _ -> TSString scalar.Value

// https://github.com/aaubry/YamlDotNet/issues/459
// We pass serializers to converters after instantiating them with mutation. Ugh.
[<AbstractClass>]
type CrutchTypeConverter () =
    let mutable valueSerializer : IValueSerializer = null
    let mutable valueDeserializer : IValueDeserializer = null
    let mutable namingConvention : INamingConvention = null

    member this.ValueSerializer
        with get () = valueSerializer
        and internal set (value) = valueSerializer <- value

    member this.ValueDeserializer
        with get () = valueDeserializer
        and internal set (value) = valueDeserializer <- value

    member this.NamingConvention
        with get () = namingConvention
        and internal set (value) = namingConvention <- value

    member this.Serialize(emitter : IEmitter, value : obj) =
        this.ValueSerializer.SerializeValue(emitter, value, null)

    member this.Deserialize(parser : IParser, someType : Type) : obj =
        use state = new SerializerState()
        this.ValueDeserializer.DeserializeValue(parser, someType, state, this.ValueDeserializer)

    member this.Deserialize<'a>(parser : IParser) : 'a =
        downcast this.Deserialize(parser, typeof<'a>)

    abstract member Accepts : Type -> bool
    abstract member ReadYaml : IParser -> Type -> obj
    abstract member WriteYaml : IEmitter -> obj -> Type -> unit

    interface IYamlTypeConverter with
        override this.Accepts someType = this.Accepts someType
        override this.ReadYaml (parser, someType) = this.ReadYaml parser someType
        override this.WriteYaml (emitter, value, someType) = this.WriteYaml emitter value someType

[<AbstractClass>]
type GenericTypeConverter () =
    inherit CrutchTypeConverter ()
    let convertersCache = ConcurrentDictionary<Type, IYamlTypeConverter>()

    abstract member GetConverterType : Type -> Type

    member private this.GetConverter (someType : Type) : IYamlTypeConverter =
        match convertersCache.TryGetValue(someType) with
        | (true, c) -> c
        | (false, _) ->
            let addOne someType =
                let convType = this.GetConverterType someType
                let constr = convType.GetConstructor([|typeof<CrutchTypeConverter>|])
                constr.Invoke([|this|]) :?> IYamlTypeConverter
            convertersCache.AddOrUpdate(someType, addOne, fun key c -> c)

    override this.ReadYaml (parser : IParser) (someType : Type) : obj =
        let converter = this.GetConverter someType
        converter.ReadYaml(parser, someType)

    override this.WriteYaml (emitter : IEmitter) (value : obj) (someType : Type) : unit =
        let converter = this.GetConverter someType
        converter.WriteYaml(emitter, value, someType)

[<AbstractClass>]
type SimpleTypeConverter<'a> () =
    inherit CrutchTypeConverter ()

    let myType = typeof<'a>

    abstract member ReadTypedYaml : IParser -> 'a
    abstract member WriteTypedYaml : IEmitter -> 'a -> unit

    override this.Accepts (someType : Type) = someType = myType

    override this.ReadYaml (parser : IParser) (someType : Type) : obj =
        this.ReadTypedYaml parser :> obj

    override this.WriteYaml (emitter : IEmitter) (value : obj) (someType : Type) : unit =
        this.WriteTypedYaml emitter (value :?> 'a)

[<AbstractClass>]
type SpecializationTypeConverter<'a> () =
    inherit GenericTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<'a>.MakeGenericType([|someType|])

type SpecializedUnionConverter<'a> (converter : CrutchTypeConverter) =
    let cases = unionCases typeof<'a>

    let (readYaml, writeYaml) =
        match getNewtype cases with
        | Some info ->
            let read =
                fun (reader : IParser) ->
                    let arg = converter.Deserialize(reader, info.Type.PropertyType)
                    if not info.ValueIsNullable && isNull arg then
                        raisef YamlException "Attempted to set null to non-nullable value"
                    info.ConvertTo arg
            let write =
                fun value (writer : IEmitter) ->
                    let arg = info.ConvertFrom value
                    converter.Serialize(writer, arg)
            (read, write)
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match getOption cases with
            | Some option ->
                let nullDeserializer = NullNodeDeserializer() :> INodeDeserializer
                let read =
                    fun (reader : IParser) ->
                        match nullDeserializer.Deserialize(reader, null, null) with
                        | (true, _) -> option.NoneValue
                        | (false, _) ->
                            let arg = converter.Deserialize(reader, option.SomeType.PropertyType)
                            if not option.ValueIsNullable && isNull arg then
                                raisef YamlException "Attempted to set null to non-nullable value"
                            option.ConvertToSome arg
                let write =
                    fun value (writer : IEmitter) ->
                        if value = option.NoneValue then
                            writer.Emit(Scalar("null"))
                        else
                            let arg = option.ConvertFromSome value
                            converter.Serialize(writer, arg)
                (read, write)
            | None ->
                match getUnionEnum typeof<'a> with
                | Some enumInfo ->
                    let optionsMap = enumInfo.Cases |> Map.mapWithKeys (fun tag case -> (case.Name, case.Value))
                    let read =
                        fun (reader : IParser) ->
                            let name = converter.Deserialize<string>(reader)
                            match Map.tryFind (Option.ofObj name) optionsMap with
                            | Some v -> v
                            | None -> raisef YamlException "Unknown enum case %s" name
                    let write =
                        fun value (writer : IEmitter) ->
                            let tag = enumInfo.GetTag value
                            let case = enumInfo.Cases.[tag]
                            match case.Name with
                            | None -> writer.Emit(Scalar("null"))
                            | Some cn -> writer.Emit(Scalar(cn))
                    (read, write)
                | None ->
                    failwith "(De)serialization for arbitrary unions is not implemented"

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'a>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = writeYaml value emitter

type UnionTypeConverter () =
    inherit SpecializationTypeConverter<SpecializedUnionConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsUnion someType

type SpecializedRecordConverter<'a> (converter : CrutchTypeConverter) =
    let getFieldInfo prop =
        let field = serializableField emptySerializableFieldOptions prop
        let name = converter.NamingConvention.Apply field.Name
        (name, field)

    let fields = FSharpType.GetRecordFields(typeof<'a>) |> Array.map getFieldInfo
    let fieldsMap = Map.ofSeq fields
    let defaultValuesMap = fields |> Seq.mapMaybe (fun (name, field) -> Option.map (fun def -> (name, def)) field.DefaultValue) |> Map.ofSeq
    let makeRecord = FSharpValue.PreComputeRecordConstructor typeof<'a>

    let readYaml (parser : IParser) : obj =
        match parser.Current with
        | :? MappingStart -> ignore <| parser.MoveNext()
        | _ -> raisef YamlException "Invalid YAML value"

        let rec getNextField (values : Map<string, obj>) =
            match parser.Current with
            | :? MappingEnd ->
                ignore <| parser.MoveNext()
                values
            | :? Scalar as scalar ->
                ignore <| parser.MoveNext()
                match Map.tryFind scalar.Value fieldsMap with
                | Some field when not field.Ignore ->
                    let value = converter.Deserialize(parser, field.InnerType)
                    if not field.IsNullable && isNull value then
                        raisef YamlException "Attempted to set null to non-nullable field \"%s\"" scalar.Value
                    let values = Map.add scalar.Value value values
                    getNextField values
                | _ ->
                    parser.SkipThisAndNestedEvents()
                    getNextField values
            | _ -> raisef YamlException "Invalid YAML value"

        let valuesMap = getNextField defaultValuesMap
        let lookupField (name, field) =
            match Map.tryFind name valuesMap with
            | None -> raisef YamlException "Field \"%s\" not found" name
            | Some o -> o
        let values = fields |> Array.map lookupField
        makeRecord values

    let writeYaml (obj : obj) (writer : IEmitter) : unit =
        writer.Emit(MappingStart())
        let emitValue (name, field) =
            let value = field.GetValue(obj)
            match field.DefaultValue with
            | Some d when not (Option.defaultValue true field.EmitDefaultValue) && d = value -> ()
            | _ ->
                writer.Emit(Scalar(name))
                converter.Serialize(writer, value)
        Seq.iter emitValue fields
        writer.Emit(MappingEnd())

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'a>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = writeYaml value emitter

type RecordTypeConverter () =
    inherit SpecializationTypeConverter<SpecializedRecordConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsRecord someType

type SpecializedMapConverter<'k, 'v when 'k : comparison> (converter : CrutchTypeConverter) =
    interface IYamlTypeConverter with
        override this.Accepts someType =
            someType = typeof<Map<'k, 'v>>

        override this.ReadYaml (parser : IParser, someType : Type) =
            let seq = converter.Deserialize<Dictionary<'k, 'v>>(parser)
            seq |> Seq.map (function KeyValue(k, v) -> (k, v)) |> Map.ofSeq :> obj

        override this.WriteYaml (emitter, value, someType) = failwith "Not implemented"

type MapTypeConverter () =
    inherit GenericTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<SpecializedMapConverter<_, _>>.MakeGenericType(someType.GetGenericArguments())

    override this.Accepts (someType : Type) : bool =
        someType.IsGenericType && someType.GetGenericTypeDefinition() = typedefof<Map<_, _>>

type SpecializedSetConverter<'k when 'k : comparison> (converter : CrutchTypeConverter) =
    interface IYamlTypeConverter with
        override this.Accepts someType =
            someType = typeof<Set<'k>>

        override this.ReadYaml (parser : IParser, someType : Type) =
            let seq = converter.Deserialize<List<'k>>(parser)
            seq |> Set.ofSeq :> obj

        override this.WriteYaml (emitter, value, someType) = failwith "Not implemented"

type SetTypeConverter () =
    inherit GenericTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<SpecializedSetConverter<_>>.MakeGenericType(someType.GetGenericArguments())

    override this.Accepts (someType : Type) : bool =
        someType.IsGenericType && someType.GetGenericTypeDefinition() = typedefof<Set<_>>

let private readJsonValue (scalar : Scalar) =
    match parseScalar scalar with
    | TSInt i -> JValue(i)
    | TSString s -> JValue(s)
    | TSFloat f -> JValue(f)
    | TSBool b -> JValue(b)
    | TSNull -> JValue.CreateNull()

type JValueConverter () =
    inherit SimpleTypeConverter<JValue>()

    override this.ReadTypedYaml (parser : IParser) =
        match parser.TryConsume<Scalar>() with
        | (true, scalar) -> readJsonValue scalar
        | (false, _) -> raisef YamlException "Expected scalar"

    override this.WriteTypedYaml (emitter : IEmitter) (value : JValue) =
        this.Serialize(emitter, value.Value)

type JTokenConverter () =
    inherit SimpleTypeConverter<JToken>()

    override this.ReadTypedYaml (parser : IParser) =
        match parser.Current with
        | :? Scalar as scalar ->
            let ret = readJsonValue scalar :> JToken
            ignore  <| parser.MoveNext()
            ret
        | :? SequenceStart -> this.Deserialize<JArray>(parser) :> JToken
        | :? MappingStart -> this.Deserialize<JObject>(parser) :> JToken
        | _ -> raisef YamlException "Expected scalar, sequence or a mapping"

    override this.WriteTypedYaml (emitter : IEmitter) (value : JToken) =
        failwith "Expected to use a specific class converter"

type JCollectionConverter () =
    inherit SimpleTypeConverter<JContainer>()

    override this.ReadTypedYaml (parser : IParser) =
        match parser.Current with
        | :? SequenceStart -> this.Deserialize<JArray>(parser) :> JContainer
        | :? MappingStart -> this.Deserialize<JObject>(parser) :> JContainer
        | _ -> raisef YamlException "Expected sequence or a mapping"

    override this.WriteTypedYaml (emitter : IEmitter) (value : JContainer) =
        failwith "Expected to use a specific class converter"

type YamlSerializerSettings =
    { NamingConvention : INamingConvention
      CrutchTypeConverters : unit -> CrutchTypeConverter seq
    }

let defaultYamlSerializerSettings =
    { NamingConvention = NullNamingConvention.Instance
      CrutchTypeConverters = fun () -> Seq.empty
    }

let makeYamlSerializer (settings : YamlSerializerSettings) : ISerializer =
    let myConverters =
        seq {
            UnionTypeConverter() :> CrutchTypeConverter
            RecordTypeConverter() :> CrutchTypeConverter

            JValueConverter() :> CrutchTypeConverter
        }
    let converters = Seq.append myConverters (settings.CrutchTypeConverters ()) |> Seq.cache
    let builder =
        SerializerBuilder()
            .ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull)
            .WithNamingConvention(settings.NamingConvention)
            .DisableAliases()
    for conv in converters do
        ignore <| builder.WithTypeConverter(conv)
    let valueSerializer = builder.BuildValueSerializer()
    for conv in converters do
        conv.ValueSerializer <- valueSerializer
        conv.NamingConvention <- settings.NamingConvention
    let serializer = builder.Build()
    serializer

let defaultYamlSerializer = makeYamlSerializer defaultYamlSerializerSettings

type YamlDeserializerSettings =
    { NamingConvention : INamingConvention
      CrutchTypeConverters : unit -> CrutchTypeConverter seq
    }

let defaultYamlDeserializerSettings =
    { NamingConvention = NullNamingConvention.Instance
      CrutchTypeConverters = fun () -> Seq.empty
    }

let makeYamlDeserializer (settings : YamlDeserializerSettings) : IDeserializer =
    let myConverters =
        seq {
            UnionTypeConverter() :> CrutchTypeConverter
            RecordTypeConverter() :> CrutchTypeConverter
            MapTypeConverter() :> CrutchTypeConverter
            SetTypeConverter() :> CrutchTypeConverter

            JValueConverter() :> CrutchTypeConverter
            JTokenConverter() :> CrutchTypeConverter
            JCollectionConverter() :> CrutchTypeConverter
        }
    let converters = Seq.append myConverters (settings.CrutchTypeConverters ()) |> Seq.cache
    let builder = DeserializerBuilder()
    for conv in converters do
        ignore <| builder.WithTypeConverter(conv)
    let valueDeserializer = builder.BuildValueDeserializer()
    for conv in converters do
        conv.ValueDeserializer <- valueDeserializer
        conv.NamingConvention <- settings.NamingConvention
    let deserializer = builder.Build()
    deserializer

let defaultYamlDeserializer = makeYamlDeserializer defaultYamlDeserializerSettings
