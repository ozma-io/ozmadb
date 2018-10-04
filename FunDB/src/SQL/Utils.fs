module FunWithFlags.FunDB.SQL.Utils

open System
open System.Globalization

let escapeSymbols (str : string) : string = str.Replace("\\", "\\\\")

let escapeDoubleQuotes (str : string) : string = sprintf "\"%s\"" ((escapeSymbols str).Replace("\"", "\\\""))

let escapeSingleQuotes (str : string) : string = sprintf "'%s'" ((escapeSymbols str).Replace("'", "''"))

let renderSqlName = escapeDoubleQuotes
let renderSqlString = escapeSingleQuotes

let renderSqlBool : bool -> string = function
    | true -> "TRUE"
    | false -> "FALSE"

let renderSqlInt (i : int) : string = i.ToString(CultureInfo.InvariantCulture)

let renderSqlDateTime (dt : DateTimeOffset) : string = dt.ToString("O", CultureInfo.InvariantCulture)

let renderSqlDate (dt : DateTimeOffset) : string = dt.ToString("d", CultureInfo.InvariantCulture)

type ISQLString =
    abstract member ToSQLString : unit -> string
