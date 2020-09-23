module FunWithFlags.FunUtils.Serialization.Yaml

open System
open System.Collections.Generic
open YamlDotNet.Core
open YamlDotNet.Core.Events
open YamlDotNet.Serialization
open YamlDotNet.Serialization.NamingConventions
open YamlDotNet.Serialization.NodeDeserializers
open YamlDotNet.Serialization.Utilities
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils

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

    member this.Deserialize<'A>(parser : IParser) : 'A =
        downcast this.Deserialize(parser, typeof<'A>)

    abstract member Accepts : Type -> bool
    abstract member ReadYaml : IParser -> Type -> obj
    abstract member WriteYaml : IEmitter -> obj -> Type -> unit

    interface IYamlTypeConverter with
        override this.Accepts someType = this.Accepts someType
        override this.ReadYaml (parser, someType) = this.ReadYaml parser someType
        override this.WriteYaml (emitter, value, someType) = this.WriteYaml emitter value someType

[<AbstractClass>]
type SpecializedTypeConverter () =
    inherit CrutchTypeConverter ()
    let convertersCache = Dictionary<Type, IYamlTypeConverter>()

    abstract member GetConverterType : Type -> Type

    member private this.GetConverter (someType : Type) : IYamlTypeConverter =
        match convertersCache.TryGetValue(someType) with
        | (true, c) -> c
        | (false, _) ->
            let convType = this.GetConverterType someType
            let constr = convType.GetConstructor([|typeof<CrutchTypeConverter>|])
            let c = constr.Invoke([|this|]) :?> IYamlTypeConverter
            convertersCache.Add(someType, c)
            c

    override this.ReadYaml (parser : IParser) (someType : Type) : obj =
        let converter = this.GetConverter someType
        converter.ReadYaml(parser, someType)

    override this.WriteYaml (emitter : IEmitter) (value : obj) (someType : Type) : unit =
        let converter = this.GetConverter someType
        converter.WriteYaml(emitter, value, someType)

[<AbstractClass>]
type SpecializedSingleTypeConverter<'A> () =
    inherit SpecializedTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<'A>.MakeGenericType([|someType|])

type SpecializedUnionConverter<'A> (converter : CrutchTypeConverter) =
    let cases = unionCases typeof<'A>

    let (readYaml, writeYaml) =
        match isNewtype cases with
        | Some info ->
            let read =
                fun (reader : IParser) ->
                    let arg = converter.Deserialize(reader, info.Type.PropertyType)
                    if not info.ValueIsNullable && isNull arg then
                        raisef YamlException "Attempted to set null to non-nullable value"
                    FSharpValue.MakeUnion(info.Case, [|arg|])
            let write =
                fun value (writer : IEmitter) ->
                    let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                    converter.Serialize(writer, args.[0])
            (read, write)
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match isOption cases with
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
                            FSharpValue.MakeUnion(option.SomeCase, [|arg|])
                let write =
                    fun value (writer : IEmitter) ->
                        if value = option.NoneValue then
                            writer.Emit(Scalar("null"))
                        else
                            let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                            assert (case = option.SomeCase)
                            assert (Array.length args = 1)
                            converter.Serialize(writer, args.[0])
                (read, write)
            | None ->
                match isUnionEnum cases with
                | Some options ->
                    let read =
                        fun (reader : IParser) ->
                            let name = converter.Deserialize<string>(reader)
                            match Map.tryFind (Option.ofNull name) options with
                            | Some (_, v) -> v
                            | None -> raisef YamlException "Unknown enum case %s" name
                    let reverseOptions = options |> Map.mapWithKeys (fun name (case, v) -> (case.Name, name))
                    let write =
                        fun value (writer : IEmitter) ->
                            let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                            match Map.find case.Name reverseOptions with
                            | None -> writer.Emit(Scalar("null"))
                            | Some cn -> writer.Emit(Scalar(cn))
                    (read, write)
                | None ->
                    failwith "(De)serialization for arbitrary unions is not implemented"

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'A>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = writeYaml value emitter

type UnionTypeConverter () =
    inherit SpecializedSingleTypeConverter<SpecializedUnionConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsUnion someType

type SpecializedRecordConverter<'A> (converter : CrutchTypeConverter) =
    let getFieldInfo prop =
        let field = serializableField prop
        (converter.NamingConvention.Apply field.Name, field)

    let fields = FSharpType.GetRecordFields(typeof<'A>) |> Array.map getFieldInfo
    let fieldsMap = Map.ofSeq fields
    let defaultValuesMap = fields |> Seq.mapMaybe (fun (name, field) -> Option.map (fun def -> (name, def)) field.DefaultValue) |> Map.ofSeq

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
                | Some field ->
                    let value = converter.Deserialize(parser, field.Property.PropertyType)
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
        FSharpValue.MakeRecord(typeof<'A>, values)

    let writeYaml (value : obj) (writer : IEmitter) : unit =
        writer.Emit(MappingStart())
        let emitValue (name, field) =
            let value = field.Property.GetValue(value, null)
            match field.DefaultValue with
            | Some d when not (Option.defaultValue true field.EmitDefaultValue) && d = value -> ()
            | _ ->
                writer.Emit(Scalar(name))
                converter.Serialize(writer, value)
        Seq.iter emitValue fields
        writer.Emit(MappingEnd())

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'A>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = writeYaml value emitter

type RecordTypeConverter () =
    inherit SpecializedSingleTypeConverter<SpecializedRecordConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsRecord someType

type SpecializedMapConverter<'K, 'V when 'K : comparison> (converter : CrutchTypeConverter) =
    interface IYamlTypeConverter with
        override this.Accepts someType =
            someType = typeof<Map<'K, 'V>>

        override this.ReadYaml (parser : IParser, someType : Type) =
            let seq = converter.Deserialize<Dictionary<'K, 'V>>(parser)
            seq |> Seq.map (function KeyValue(k, v) -> (k, v)) |> Map.ofSeq :> obj

        override this.WriteYaml (emitter, value, someType) = failwith "Not implemented"

type MapTypeConverter () =
    inherit SpecializedTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<SpecializedMapConverter<_, _>>.MakeGenericType(someType.GetGenericArguments())

    override this.Accepts (someType : Type) : bool =
        someType.IsGenericType && someType.GetGenericTypeDefinition() = typedefof<Map<_, _>>

type SpecializedSetConverter<'K when 'K : comparison> (converter : CrutchTypeConverter) =
    interface IYamlTypeConverter with
        override this.Accepts someType =
            someType = typeof<Set<'K>>

        override this.ReadYaml (parser : IParser, someType : Type) =
            let seq = converter.Deserialize<List<'K>>(parser)
            seq |> Set.ofSeq :> obj

        override this.WriteYaml (emitter, value, someType) = failwith "Not implemented"

type SetTypeConverter () =
    inherit SpecializedTypeConverter ()

    override this.GetConverterType (someType : Type) : Type =
        typedefof<SpecializedSetConverter<_>>.MakeGenericType(someType.GetGenericArguments())

    override this.Accepts (someType : Type) : bool =
        someType.IsGenericType && someType.GetGenericTypeDefinition() = typedefof<Set<_>>

type YamlSerializerSettings =
    { NamingConvention : INamingConvention
      CrutchTypeConverters : unit -> CrutchTypeConverter seq
    }

let defaultYamlSerializerSettings =
    { NamingConvention = NullNamingConvention.Instance
      CrutchTypeConverters = fun () -> Seq.empty
    }

let makeYamlSerializer (settings : YamlSerializerSettings) : ISerializer =
    let myConverters = seq {
        UnionTypeConverter() :> CrutchTypeConverter
        RecordTypeConverter() :> CrutchTypeConverter
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
    let myConverters = seq {
        UnionTypeConverter() :> CrutchTypeConverter
        RecordTypeConverter() :> CrutchTypeConverter
        MapTypeConverter() :> CrutchTypeConverter
        SetTypeConverter() :> CrutchTypeConverter
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
