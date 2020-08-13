module FunWithFlags.FunUtils.String

open System.Text.RegularExpressions

let truncate (len : int) (s : string) =
    if String.length s <= len then s else s.Substring(0, len)

let concatWithWhitespaces (strs : seq<string>) : string =
    let filterEmpty = function
        | "" -> None
        | str -> Some str
    strs |> Seq.mapMaybe filterEmpty |> String.concat " "

let private normalizeNewlinesRegex = Regex("\r\n|\n|\r", RegexOptions.Compiled)

let normalizeNewlines (str : string) : string =
    normalizeNewlinesRegex.Replace(str, "\n")