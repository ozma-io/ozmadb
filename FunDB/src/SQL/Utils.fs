module FunWithFlags.FunDB.SQL.Utils

// NpgsqlInterval
#nowarn "44"

open System
open System.Text
open System.Globalization
open Npgsql.NameTranslation
open NpgsqlTypes
open NodaTime
open Newtonsoft.Json
open Newtonsoft.Json.Linq

let escapeSqlDoubleQuotes (str : string) : string = sprintf "\"%s\"" (str.Replace("\"", "\"\""))

// Internal restriction of PostgreSQL; any identifier longer is truncated. Try to create table with long name, for example.
let sqlIdentifierLength = 63

// PostgreSQL C-style escape string constants but without E
let private genericEscapeSqlSingleQuotes (strBuilder : StringBuilder) (str : string) : string =
    ignore <| strBuilder.Append('\'')
    for c in str do
        match c with
        | '\\' ->
            ignore <| strBuilder.Append("\\\\")
        | '\x08' ->
            ignore <| strBuilder.Append("\\b")
        | '\x0c' ->
            ignore <| strBuilder.Append("\\f")
        | '\n' ->
            ignore <| strBuilder.Append("\\n")
        | '\r' ->
            ignore <| strBuilder.Append("\\r")
        | '\t' ->
            ignore <| strBuilder.Append("\\t")
        | '\'' ->
            ignore <| strBuilder.Append("\\'")
        | c when c < ' ' ->
            ignore <| strBuilder.AppendFormat("\\x{0:X2}", int c)
        | c ->
            ignore <| strBuilder.Append(c)
    ignore <| strBuilder.Append('\'')
    strBuilder.ToString()

let escapeSqlSingleQuotes (str : string) =
    let strBuilder = StringBuilder (2 * str.Length)
    genericEscapeSqlSingleQuotes strBuilder str

let renderSqlName str =
    if String.length str > sqlIdentifierLength then
        failwith <| sprintf "SQL identifier %s is too long" str
    escapeSqlDoubleQuotes str

let renderSqlString (str : string) =
    let strBuilder = StringBuilder (2 * str.Length)
    ignore <| strBuilder.Append('E')
    genericEscapeSqlSingleQuotes strBuilder str

let renderSqlBool : bool -> string = function
    | true -> "TRUE"
    | false -> "FALSE"

let renderSqlDecimal (f : decimal) = f.ToString(CultureInfo.InvariantCulture)

let renderSqlInt (i : int) : string = i.ToString(CultureInfo.InvariantCulture)

let renderSqlBigInt (i : int64) : string = i.ToString(CultureInfo.InvariantCulture)

let renderSqlJson (j : JToken) : string = j.ToString(Formatting.None)

let private ofNodaParseResult (result : NodaTime.Text.ParseResult<'a>) : 'a option =
    if result.Success then
        Some result.Value
    else
        None

let private datePattern = NodaTime.Text.LocalDatePattern.Iso
let private dateTimeRenderPattern = NodaTime.Text.InstantPattern.CreateWithInvariantCulture("uuuu-MM-dd HH:mm:ss.ffffff+00")

let renderSqlDateTime = dateTimeRenderPattern.Format

let private dateTimeSecPattern = NodaTime.Text.InstantPattern.CreateWithInvariantCulture("uuuu-MM-dd HH:mm:ss")
let private subsecSeparator = [|';'; '.'|]
let private subsecMultiplier = function
    | 1 -> 100000
    | 2 -> 10000
    | 3 -> 1000
    | 4 -> 100
    | 5 -> 10
    | 6 -> 1
    | _ -> failwith "Invalid format"

let trySqlDateTime (dateTimeStr : string) : Instant option =
    try
        let timePoint = dateTimeStr.IndexOf(' ')
        let dateStr =
            if timePoint = -1 then
                dateTimeStr
            else
                dateTimeStr.Substring(0, timePoint)
        let date = datePattern.Parse(dateStr).GetValueOrThrow()
        let res = Instant.FromUtc(date.Year, date.Month, date.Day, 0, 0)
        if timePoint = -1 then
            Some res
        else
            let timeStr = dateTimeStr.Substring(timePoint + 1)
            let subsecDotPoint = timeStr.IndexOfAny(subsecSeparator)
            let tzPlusPoint = timeStr.IndexOf('+')
            let secsStr =
                if subsecDotPoint = -1 then
                    if tzPlusPoint <> -1 then
                        timeStr.Substring(0, tzPlusPoint)
                    else
                        timeStr
                else
                    if tzPlusPoint <> -1 && subsecDotPoint <= tzPlusPoint then
                        failwith "Invalid format"
                    timeStr.Substring(0, subsecDotPoint)
            let timeParts = secsStr.Split(':')
            let hours = Int32.Parse(timeParts.[0])
            let minutes = Int32.Parse(timeParts.[1])
            let seconds =
                match timeParts.Length with
                | 2 -> 0
                | 3 -> Int32.Parse(timeParts.[2])
                | _ -> failwith "Invalid format"
            let usecs =
                if subsecDotPoint = -1 then
                    0
                else
                    let subsecPoint = subsecDotPoint + 1

                    let subsecEndPoint =
                        if tzPlusPoint = -1 then
                            timeStr.Length
                        else
                            tzPlusPoint
                    let subsecLength = subsecEndPoint - subsecPoint
                    let subsecPart = timeStr.Substring(subsecPoint, subsecLength)
                    let subsecs = Int32.Parse subsecPart
                    subsecs * subsecMultiplier subsecLength
            if tzPlusPoint <> -1 && timeStr.Substring(tzPlusPoint) <> "+00" then
                failwith "Invalid format"
            let totalTicks =
                int64 usecs * NpgsqlTimeSpan.TicksPerMicrosecond +
                int64 seconds * NpgsqlTimeSpan.TicksPerSecond +
                int64 minutes * NpgsqlTimeSpan.TicksPerMinute +
                int64 hours * NpgsqlTimeSpan.TicksPerHour
            Some <| res.PlusTicks(totalTicks)
    with
    | _ -> None

let renderSqlDate = datePattern.Format
let trySqlDate (s : string) : LocalDate option = datePattern.Parse(s) |> ofNodaParseResult

let renderSqlInterval (p : Period) : string =
    let months = p.Years * NpgsqlTimeSpan.MonthsPerYear + p.Months
    let days = p.Days
    let ticks =
        p.Hours * NpgsqlTimeSpan.TicksPerHour + 
        p.Minutes * NpgsqlTimeSpan.TicksPerMinute +
        p.Seconds * NpgsqlTimeSpan.TicksPerSecond +
        p.Milliseconds * NpgsqlTimeSpan.TicksPerMillsecond +
        p.Ticks +
        p.Nanoseconds / 100L
    NpgsqlTimeSpan(months, days, ticks) |> string

let trySqlInterval (s : string) : Period option =
    match NpgsqlTimeSpan.TryParse(s) with
    | (false, _) -> None
    | (true, raw) ->
        let builder = PeriodBuilder(
            Months = raw.Months,
            Days = raw.Days,
            Ticks = raw.Ticks
        )
        Some <| builder.Build().Normalize()

type ISQLString =
    abstract member ToSQLString : unit -> string

let toSQLString<'a when 'a :> ISQLString> (o : 'a) = o.ToSQLString()

let optionToSQLString<'a when 'a :> ISQLString> : 'a option -> string = function
    | None -> ""
    | Some o -> o.ToSQLString()

let convertComments : string option -> string = function
    | None -> ""
    | Some comments -> sprintf "-- %s\n" (comments.Replace("\n", "\n-- "))

let snakeCaseName = NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase