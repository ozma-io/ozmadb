module FunWithFlags.FunDB.FunQL.Utils

open System.Collections.Generic
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.SQL.Utils

type IFunQLString =
    abstract member ToFunQLString : unit -> string

let goodSystemName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || (name.Contains("__") && not (name.StartsWith("__"))))

let goodName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || name.Contains("__"))

let rec renderFunQLJson (j : JToken) : string =
    match j.Type with
    | JTokenType.Object ->
        let obj = j :?> JObject :> KeyValuePair<string, JToken> seq
        obj |> Seq.map (fun (KeyValue (k, v)) -> sprintf "%s:%s" (renderSqlName k) (renderFunQLJson v)) |> String.concat "," |> sprintf "{%s}"
    | JTokenType.Array ->
        let arr = j :?> JArray :> JToken seq
        arr |> Seq.map renderFunQLJson |> String.concat "," |> sprintf "[%s]"
    | JTokenType.Integer -> renderSqlInt <| JToken.op_Explicit j
    | JTokenType.Float -> renderSqlDecimal <| JToken.op_Explicit j
    | JTokenType.String -> renderSqlString <| JToken.op_Explicit j
    | JTokenType.Boolean -> renderSqlBool <| JToken.op_Explicit j
    | JTokenType.Null -> "null"
    | typ -> failwith <| sprintf "Unknown JSON token type %O" typ