module FunWithFlags.FunDB.FunQL.Utils

open System
open System.Collections.Generic
open Newtonsoft.Json.Linq
open NpgsqlTypes

open FunWithFlags.FunDB.SQL.Utils

type IFunQLString =
    abstract member ToFunQLString : unit -> string

let goodName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || name.Contains("__"))

let renderFunQLName = escapeSqlDoubleQuotes
let renderFunQLString = escapeSqlSingleQuotes
let renderFunQLInt = renderSqlInt
let renderFunQLBool = renderSqlBool
let renderFunQLDecimal = renderSqlDecimal

let renderFunQLDateTime (j : NpgsqlDateTime) : string = sprintf "%s :: datetime" (j |> string |> renderFunQLString)
let renderFunQLDate (j : NpgsqlDate) : string = sprintf "%s :: date" (j |> string |> renderFunQLString)
let renderFunQLInterval (ts : NpgsqlTimeSpan) : string = sprintf "%s :: interval" (ts |> string |> renderFunQLString)
let renderFunQLUUID (uuid : Guid) : string = sprintf "%s :: uuid" (uuid |> string |> renderFunQLString)

let rec renderFunQLJson (j : JToken) : string =
    match j.Type with
    | JTokenType.Object ->
        let obj = j :?> JObject :> KeyValuePair<string, JToken> seq
        obj |> Seq.map (fun (KeyValue (k, v)) -> sprintf "%s: %s" (escapeSqlDoubleQuotes k) (renderFunQLJson v)) |> String.concat "," |> sprintf "{ %s }"
    | JTokenType.Array ->
        let arr = j :?> JArray :> JToken seq
        arr |> Seq.map renderFunQLJson |> String.concat ", " |> sprintf "[%s]"
    | JTokenType.Integer -> renderFunQLInt <| JToken.op_Explicit j
    | JTokenType.Float -> renderFunQLDecimal <| JToken.op_Explicit j
    | JTokenType.String -> renderFunQLString <| JToken.op_Explicit j
    | JTokenType.Boolean -> renderFunQLBool <| JToken.op_Explicit j
    | JTokenType.Date -> renderFunQLDateTime <| NpgsqlDateTime (JToken.op_Explicit j : DateTime)
    | JTokenType.Null -> "null"
    | typ -> failwith <| sprintf "Unknown JSON token type %O" typ

let renderAttributesMap<'e, 'f when 'e :> IFunQLString and 'e : comparison and 'f :> IFunQLString>(attrs : Map<'e, 'f>) =
    attrs |> Map.toSeq |> Seq.map (fun (name, e) -> sprintf "%s = %s" (name.ToFunQLString()) (e.ToFunQLString())) |> String.concat ", " |> sprintf "@{ %s }"