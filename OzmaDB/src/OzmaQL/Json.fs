module OzmaDB.OzmaQL.Json

open System
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open NodaTime
open NodaTime.Serialization.JsonNet

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization.Json

module OzmaQL = OzmaDB.OzmaQL.AST
module SQL = OzmaDB.SQL.AST

// Needed to properly convert Instants to and from JavaScript Dates.
type InstantDateTimeConverter() =
    inherit JsonConverter<Instant>()

    override this.ReadJson
        (reader: JsonReader, objectType: Type, existingValue, hasExistingValue, serializer: JsonSerializer)
        : Instant =
        let ret =
            match reader.Value with
            | :? string as str ->
                let res = NodaTime.Text.InstantPattern.ExtendedIso.Parse str

                if res.Success then
                    res.Value
                else
                    raisefWithInner JsonSerializationException res.Exception ""
            | :? DateTime as dt ->
                try
                    Instant.FromDateTimeUtc dt
                with :? ArgumentException as e ->
                    raisefWithInner JsonSerializationException e ""
            | :? DateTimeOffset as dt -> Instant.FromDateTimeOffset dt
            | _ -> raisef JsonSerializationException "Failed to parse Instant"

        ignore <| reader.Read()
        ret

    override this.WriteJson(writer: JsonWriter, value: Instant, serializer: JsonSerializer) : unit =
        writer.WriteValue(value.ToDateTimeUtc())

let defaultJsonSettings =
    let converters: JsonConverter seq =
        seq {
            yield OzmaQL.FieldValuePrettyConverter()
            yield OzmaQL.ArgumentRefConverter()
            yield SQL.ValuePrettyConverter()
            yield InstantDateTimeConverter()
        }

    let constructors = Seq.map (fun conv -> fun _ -> Some conv) converters
    let jsonSettings = makeDefaultJsonSerializerSettings (Seq.toArray constructors)
    ignore <| jsonSettings.ConfigureForNodaTime(DateTimeZoneProviders.Tzdb)
    let resolver = jsonSettings.ContractResolver :?> ConverterContractResolver
    resolver.NamingStrategy <- CamelCaseNamingStrategy(OverrideSpecifiedNames = false)
    jsonSettings.NullValueHandling <- NullValueHandling.Ignore
    jsonSettings.DateParseHandling <- DateParseHandling.None
    jsonSettings.DateTimeZoneHandling <- DateTimeZoneHandling.Utc
    jsonSettings
