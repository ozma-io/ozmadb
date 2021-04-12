module FunWithFlags.FunUtils.String

open System
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

let hexBytes (bytes : byte seq) : string =
    bytes |> Seq.map (fun b -> b.ToString("x2")) |> String.concat ""

type StringComparable<'a> (inner : 'a) =
    let innerString = string inner

    member this.String = innerString
    member this.Value = inner

    override this.ToString () = innerString

    override x.Equals yobj =
        match yobj with
        | :? StringComparable<'a> as y -> x.String = y.String
        | _ -> false

    override this.GetHashCode () = hash this.String

    interface IComparable with
       member x.CompareTo yobj =
          match yobj with
          | :? StringComparable<'a> as y -> compare x.String y.String
          | _ -> invalidArg "yobj" "cannot compare value of different types"

    interface IComparable<StringComparable<'a>> with
       member x.CompareTo y = compare x.String y.String

let comparable a = StringComparable a