module FunWithFlags.FunDB.SQL.Utils

open System
open System.Globalization

let escapeSqlSymbols (str : string) : string = str.Replace("\\", "\\\\")

let escapeSqlDoubleQuotes (str : string) : string = sprintf "\"%s\"" ((escapeSqlSymbols str).Replace("\"", "\\\""))

let escapeSqlSingleQuotes (str : string) : string = sprintf "'%s'" ((escapeSqlSymbols str).Replace("'", "''"))

let renderSqlName = escapeSqlDoubleQuotes
let renderSqlString = escapeSqlSingleQuotes

let renderSqlBool : bool -> string = function
    | true -> "TRUE"
    | false -> "FALSE"

let renderSqlDecimal (f : decimal) = f.ToString(CultureInfo.InvariantCulture)

let renderSqlInt (i : int) : string = i.ToString(CultureInfo.InvariantCulture)

let renderSqlDateTime (dt : DateTimeOffset) : string = dt.ToString("O", CultureInfo.InvariantCulture)

let renderSqlDate (dt : DateTimeOffset) : string = dt.ToString("d", CultureInfo.InvariantCulture)

type ISQLString =
    abstract member ToSQLString : unit -> string
