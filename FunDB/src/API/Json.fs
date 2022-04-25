module FunWithFlags.FunDB.API.Json

open System
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open NodaTime
open NodaTime.Serialization.JsonNet

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json

module FunQL = FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type InstantFromDateTimeConverter () =
    inherit JsonConverter<Instant> ()

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : Instant =
        let ret =
            match reader.Value with
            | :? string as str ->
                let res = NodaTime.Text.InstantPattern.General.Parse str
                if res.Success then
                    res.Value
                else
                    raisefWithInner JsonSerializationException res.Exception ""
            | :? DateTimeOffset as dt ->
                Instant.FromDateTimeOffset dt
            | _ -> raisef JsonSerializationException "Failed to parse Instant"
        ignore <| reader.Read()
        ret

    override this.WriteJson (writer : JsonWriter, value : Instant, serializer : JsonSerializer) : unit =
        writer.WriteValue(NodaTime.Text.InstantPattern.General.Format(value))

// Specialized converter to convert Instants to JavaScript Dates.
type InstantFromToDateTimeConverter () =
    inherit InstantFromDateTimeConverter ()

    override this.WriteJson (writer : JsonWriter, value : Instant, serializer : JsonSerializer) : unit =
        writer.WriteValue(value.ToDateTimeOffset())

let funDBJsonSettings (extraConverters : JsonConverter seq) =
    let converters : JsonConverter seq =
        seq {
            yield! extraConverters
            yield FunQL.FieldValuePrettyConverter () :> JsonConverter
            yield SQL.ValuePrettyConverter () :> JsonConverter
            yield NodaConverters.NormalizingIsoPeriodConverter
            yield InstantFromDateTimeConverter () :> JsonConverter
        }
    let constructors = Seq.map (fun conv -> fun _ -> Some conv) converters
    let jsonSettings = makeDefaultJsonSerializerSettings (Seq.toArray constructors)
    ignore <| jsonSettings.ConfigureForNodaTime(DateTimeZoneProviders.Tzdb)
    let resolver = jsonSettings.ContractResolver :?> ConverterContractResolver
    resolver.NamingStrategy <- CamelCaseNamingStrategy(
        OverrideSpecifiedNames = false
    )
    jsonSettings.NullValueHandling <- NullValueHandling.Ignore
    jsonSettings.DateParseHandling <- DateParseHandling.None
    jsonSettings