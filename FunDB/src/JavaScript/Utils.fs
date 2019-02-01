module FunWithFlags.FunDB.JavaScript.Utils

open System.Globalization

type IJSString =
    abstract member ToJSString : unit -> string

let renderJsBool : bool -> string = function
    | true -> "true"
    | false -> "false"

let renderJsString (str : string) = sprintf "\"%s\"" (str.Replace("\\", "\\\\").Replace("\"", "\\\""))

let renderJsNumber (f : double) = f.ToString(CultureInfo.InvariantCulture)
