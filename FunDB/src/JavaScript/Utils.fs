module FunWithFlags.FunDB.JavaScript.Utils

type IJSString =
    abstract member ToJSString : unit -> string
let renderJsBool : bool -> string = function
    | true -> "true"
    | false -> "false"

let renderJsString (str : string) = sprintf "\"%s\"" (str.Replace("\\", "\\\\").Replace("\"", "\\\""))