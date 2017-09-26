module internal FunWithFlags.FunDB.Escape

let renderSqlName (str : string) = sprintf "\"%s\"" (str.Replace("\"", "\\\""))

let renderSqlString (str : string) = sprintf "'%s'" (str.Replace("'", "''"))

let renderBool = function
    | true -> "TRUE"
    | false -> "FALSE"
