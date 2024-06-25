namespace OzmaDB.OzmaUtils

open System
open System.Text.RegularExpressions
open FSharpPlus

type StringComparable<'a>(inner: 'a) =
    let mutable innerString = None

    member this.String =
        match innerString with
        | None ->
            let str = string inner
            innerString <- Some str
            str
        | Some str -> str

    member this.Value = inner

    override this.ToString() = this.String

    override x.Equals yobj =
        match yobj with
        | :? StringComparable<'a> as y -> x.String = y.String
        | _ -> false

    override this.GetHashCode() = hash this.String

    interface IComparable with
        member x.CompareTo yobj =
            match yobj with
            | :? StringComparable<'a> as y -> compare x.String y.String
            | _ -> invalidArg "yobj" "cannot compare value of different types"

    interface IComparable<StringComparable<'a>> with
        member x.CompareTo y = compare x.String y.String

[<RequireQualifiedAccess>]
module String =
    let truncate (len: int) (s: string) =
        if String.length s <= len then s else s.Substring(0, len)

    let concatWithWhitespaces (strs: seq<string>) : string =
        let filterEmpty =
            function
            | "" -> None
            | str -> Some str

        strs |> Seq.mapMaybe filterEmpty |> String.concat " "

    let private normalizeNewlinesRegex = Regex("\r\n|\n|\r", RegexOptions.Compiled)

    let normalizeNewlines (str: string) : string =
        normalizeNewlinesRegex.Replace(str, "\n")

    let hexBytes (bytes: byte seq) : string =
        bytes |> Seq.map (fun b -> b.ToString("x2")) |> String.concat ""

    let comparable a = StringComparable a

    let removeSuffix (suffix: string) (s: string) =
        if String.endsWith suffix s then
            s.Substring(0, s.Length - suffix.Length)
        else
            s
