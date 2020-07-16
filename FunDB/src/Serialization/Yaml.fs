module FunWithFlags.FunDB.Serialization.Yaml

open System
open System.Reflection
open System.Collections.Generic
open YamlDotNet.Core
open YamlDotNet.Core.Events
open YamlDotNet.Serialization
open YamlDotNet.Serialization.NodeDeserializers
open YamlDotNet.Serialization.Utilities
open Microsoft.FSharp.Reflection

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Serialization.Utils

// https://github.com/aaubry/YamlDotNet/issues/459
// We pass serializers to converters after instantiating them with mutation. Ugh.
[<AbstractClass>]
type CrutchTypeConverter () =
    member val ValueSerializer : IValueSerializer = null with get, set
    member val ValueDeserializer : IValueDeserializer = null with get, set

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
    let casesMap = cases |> unionNames
    let reverseNames = casesMap |> Map.toSeq |> Seq.map (fun (name, case) -> (case.info.Name, (name, case.fields))) |> Map.ofSeq

    let searchCases (name : string) (doCase : UnionCase -> obj) : obj =
        match Map.tryFind name casesMap with
            | Some c -> doCase c
            | None -> raisef YamlException "Unknown union case \"%s\"" name

    let readYaml : IParser -> obj =
        match isNewtype cases with
        | Some (case, argProperty) ->
            fun reader ->
                let arg = converter.Deserialize(reader, argProperty.PropertyType)
                FSharpValue.MakeUnion(case, [|arg|])
        | None ->
            // XXX: We do lose information here is we serialize e.g. ('a option option).
            match isOption cases with
            | Some option ->
                let nullDeserializer = NullNodeDeserializer() :> INodeDeserializer
                let nullValue = FSharpValue.MakeUnion(option.noneCase, [||])
                fun reader ->
                    match nullDeserializer.Deserialize(reader, null, null) with
                    | (true, _) -> nullValue
                    | (false, _) ->
                        let arg = converter.Deserialize(reader, option.someType.PropertyType)
                        FSharpValue.MakeUnion(option.someCase, [|arg|])
            | None ->
                if isUnionEnum cases then
                    fun reader ->
                        let name = converter.Deserialize<string>(reader)
                        let ret = searchCases name <| fun case ->
                            FSharpValue.MakeUnion(case.info, [||])
                        ret
                else
                    failwith "Deserialization for arbitrary unions is not implemented"

    let writeYaml : obj -> IEmitter -> unit =
        match isNewtype cases with
        | Some (case, argProperty) ->
            fun value writer ->
                let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                converter.Serialize(writer, args.[0])
        | None ->
            match isOption cases with
            | Some option ->
                fun value writer ->
                    let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                    if case = option.noneCase then
                        writer.Emit(Scalar("null"))
                    else if case = option.someCase then
                        converter.Serialize(writer, args.[0])
                    else
                        failwith "Sanity check failed"
            | None ->
                if isUnionEnum cases then
                    fun value writer ->
                        let (case, args) = FSharpValue.GetUnionFields(value, typeof<'A>)
                        let (caseName, _) = Map.find case.Name reverseNames
                        writer.Emit(Scalar(caseName))
                else
                    failwith "Serialization for arbitrary unions is not implemented"

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'A>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = writeYaml value emitter

type UnionTypeConverter () =
    inherit SpecializedSingleTypeConverter<SpecializedUnionConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsUnion someType

type SpecializedRecordConverter<'A> (converter : CrutchTypeConverter) =
    let fields = FSharpType.GetRecordFields(typeof<'A>)
    let fieldTypes = fields |> Seq.map (fun prop -> (prop.Name, prop.PropertyType)) |> Map.ofSeq
    let defaultValues =
        fields
            |> Seq.mapMaybe (fun prop -> getTypeDefaultValue prop.PropertyType |> Option.map (fun def -> (prop.Name, def)))
            |> Map.ofSeq

    let readYaml (parser : IParser) : obj =
        match parser.Current with
        | :? MappingStart -> ignore <| parser.MoveNext()
        | _ -> raisef YamlException "Invalid YAML value"

        let rec getNextField (fields : Map<string, obj>) =
            match parser.Current with
            | :? MappingEnd ->
                ignore <| parser.MoveNext()
                fields
            | :? Scalar as scalar ->
                ignore <| parser.MoveNext()
                match Map.tryFind scalar.Value fieldTypes with
                | Some valueType ->
                    let value = converter.Deserialize(parser, valueType)
                    let fields = Map.add scalar.Value value fields
                    getNextField fields
                | _ ->
                    parser.SkipThisAndNestedEvents()
                    getNextField fields
            | _ -> raisef YamlException "Invalid YAML value"

        let valuesMap = getNextField defaultValues
        let lookupField (prop : PropertyInfo) =
            match Map.tryFind prop.Name valuesMap with
            | None -> raisef YamlException "Field \"%s\" not found" prop.Name
            | Some o -> o
        let values = fields |> Array.map lookupField
        FSharpValue.MakeRecord(typeof<'A>, values)

    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<'A>
        override this.ReadYaml (parser, someType) = readYaml parser
        override this.WriteYaml (emitter, value, someType) = failwith "Not implemented"

type RecordTypeConverter () =
    inherit SpecializedSingleTypeConverter<SpecializedRecordConverter<obj>> ()

    override this.Accepts (someType : Type) : bool =
        FSharpType.IsRecord someType

type SpecializedMapConverter<'K, 'V when 'K : comparison> (converter : CrutchTypeConverter) =
    interface IYamlTypeConverter with
        override this.Accepts someType = someType = typeof<Map<'K, 'V>>

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

let makeYamlSerializer (build : SerializerBuilder -> unit) : ISerializer =
    let myConverters = [|
        UnionTypeConverter() :> CrutchTypeConverter
    |]
    let builder =
        SerializerBuilder()
            .ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull)
            .DisableAliases()
    for conv in myConverters do
        ignore <| builder.WithTypeConverter(conv)
    build builder
    let valueSerializer = builder.BuildValueSerializer()
    for conv in myConverters do
        conv.ValueSerializer <- valueSerializer
    let serializer = builder.Build()
    serializer

let defaultYamlSerializer = makeYamlSerializer ignore

let makeYamlDeserializer (build : DeserializerBuilder -> unit) : IDeserializer =
    let myConverters = [|
        UnionTypeConverter() :> CrutchTypeConverter
        RecordTypeConverter() :> CrutchTypeConverter
        MapTypeConverter() :> CrutchTypeConverter
    |]
    let builder = DeserializerBuilder()
    for conv in myConverters do
        ignore <| builder.WithTypeConverter(conv)
    build builder
    let valueDeserializer = builder.BuildValueDeserializer()
    for conv in myConverters do
        conv.ValueDeserializer <- valueDeserializer
    let deserializer = builder.Build()
    deserializer

let defaultYamlDeserializer = makeYamlDeserializer ignore
