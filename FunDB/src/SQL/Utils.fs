module FunWithFlags.FunDB.SQL.Utils

open System
open System.Text
open System.Globalization
open Newtonsoft.Json
open Newtonsoft.Json.Linq

let escapeSqlDoubleQuotes (str : string) : string = sprintf "\"%s\"" (str.Replace("\"", "\"\""))

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

let renderSqlName = escapeSqlDoubleQuotes

let renderSqlString (str : string) =
    let strBuilder = StringBuilder (2 * str.Length)
    ignore <| strBuilder.Append('E')
    genericEscapeSqlSingleQuotes strBuilder str

let renderSqlBool : bool -> string = function
    | true -> "TRUE"
    | false -> "FALSE"

let renderSqlDecimal (f : decimal) = f.ToString(CultureInfo.InvariantCulture)

let renderSqlInt (i : int) : string = i.ToString(CultureInfo.InvariantCulture)

let renderSqlDateTime (dt : DateTimeOffset) : string = dt.ToString("O", CultureInfo.InvariantCulture)

let renderSqlDate (dt : DateTimeOffset) : string = dt.ToString("d", CultureInfo.InvariantCulture)

let renderSqlJson (j : JToken) : string = j.ToString(Formatting.None)

type ISQLString =
    abstract member ToSQLString : unit -> string
