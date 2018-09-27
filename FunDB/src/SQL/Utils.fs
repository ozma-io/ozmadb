module FunWithFlags.FunDB.SQL.Utils

open System
open System.Globalization

let escapeDoubleQuotes (str : string) = sprintf "\"%s\"" (str.Replace("\"", "\\\""))

let escapeSingleQuotes (str : string) = sprintf "'%s'" (str.Replace("'", "''"))

let renderSqlName = escapeDoubleQuotes
let renderSqlString = escapeSingleQuotes

let renderSqlBool = function
    | true -> "TRUE"
    | false -> "FALSE"

let renderSqlInt (i : int) = i.ToString(CultureInfo.InvariantCulture)

let renderSqlDateTime (dt : DateTime) = dt.ToString("O", CultureInfo.InvariantCulture)

let renderSqlDate (dt : DateTime) = dt.ToString("d", CultureInfo.InvariantCulture)

type ISQLString =
    abstract member ToSQLString : unit -> string
