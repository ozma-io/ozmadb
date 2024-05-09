module OzmaDB.OzmaQL.Utils

open System
open System.Collections.Generic
open Newtonsoft.Json.Linq
open NodaTime
open NpgsqlTypes

open OzmaDB.SQL.Utils

type IOzmaQLString =
    abstract member ToOzmaQLString : unit -> string

let goodName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || name.Contains("__"))

let renderOzmaQLName = escapeSqlDoubleQuotes
let renderOzmaQLString = escapeSqlSingleQuotes
let renderOzmaQLInt = renderSqlInt
let renderOzmaQLBool = renderSqlBool
let renderOzmaQLDecimal = renderSqlDecimal

let renderOzmaQLDateTime (j : Instant) : string = sprintf "%s :: datetime" (j |> renderSqlDateTime |> renderOzmaQLString)
let renderOzmaQLDate (j : LocalDate) : string = sprintf "%s :: date" (j |> renderSqlDate |> renderOzmaQLString)
let renderOzmaQLInterval (ts : Period) : string = sprintf "%s :: interval" (ts |> renderSqlInterval |> renderOzmaQLString)
let renderOzmaQLUuid (uuid : Guid) : string = sprintf "%s :: uuid" (uuid |> string |> renderOzmaQLString)

let rec renderOzmaQLJson (j : JToken) : string =
    match j.Type with
    | JTokenType.Object ->
        let obj = j :?> JObject :> KeyValuePair<string, JToken> seq
        obj |> Seq.map (fun (KeyValue (k, v)) -> sprintf "%s: %s" (escapeSqlDoubleQuotes k) (renderOzmaQLJson v)) |> String.concat "," |> sprintf "{ %s }"
    | JTokenType.Array ->
        let arr = j :?> JArray :> JToken seq
        arr |> Seq.map renderOzmaQLJson |> String.concat ", " |> sprintf "[%s]"
    | JTokenType.Integer -> renderOzmaQLInt <| JToken.op_Explicit j
    | JTokenType.Float -> renderOzmaQLDecimal <| JToken.op_Explicit j
    | JTokenType.String -> renderOzmaQLString <| JToken.op_Explicit j
    | JTokenType.Boolean -> renderOzmaQLBool <| JToken.op_Explicit j
    | JTokenType.Date ->
        let dt = JToken.op_Explicit j : DateTimeOffset
        Instant.FromDateTimeOffset dt |> renderOzmaQLDateTime
    | JTokenType.Null -> "null"
    | typ -> failwith <| sprintf "Unexpected token type %O" typ

let renderAttributesMap<'e, 'f when 'e :> IOzmaQLString and 'e : comparison and 'f :> IOzmaQLString>(attrs : Map<'e, 'f>) =
    attrs |> Map.toSeq |> Seq.map (fun (name, e) -> sprintf "%s = %s" (name.ToOzmaQLString()) (e.ToOzmaQLString())) |> String.concat ", " |> sprintf "@{ %s }"

let toOzmaQLString<'a when 'a :> IOzmaQLString> (o : 'a) = o.ToOzmaQLString()

let optionToOzmaQLString<'a when 'a :> IOzmaQLString> : 'a option -> string = function
    | None -> ""
    | Some o -> o.ToOzmaQLString()