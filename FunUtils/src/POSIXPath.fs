[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.POSIXPath

open System.Text.RegularExpressions

type Path = string

// We implement methods for handling paths by ourselves, because we use a virtual POSIX fs and System.IO.Path is platform-specific.

let private extensionRegex = Regex(@"\.([^/.]+)$")
let trimExtension (path : Path) : Path =
    extensionRegex.Replace(path, "")

let extension (path : Path) =
    let m = extensionRegex.Match(path)
    if m.Success then
        Some m.Groups.[1].Value
    else
        None

let isAbsolute (path : Path) =
    path.[0] = '/'

let addDelimiter (path : Path) : Path =
    if path.EndsWith('/') then
        path
    else
        path + "/"

let private dirNameRegex = Regex(@"/[^/]*$")
let dirName (path : Path) : Path =
    dirNameRegex.Replace(path, "")

let rec private shortenReplaceContinuously' (regex : Regex) (replace : Match -> string) (s : string) =
    let r = regex.Replace(s, MatchEvaluator replace)
    if String.length r = String.length s then
        s
    else
        shortenReplaceContinuously' regex replace r

let rec private shortenReplaceContinuously (regex : Regex) (replace : string) (s : string) =
    let r = regex.Replace(s, replace)
    if String.length r = String.length s then
        s
    else
        shortenReplaceContinuously regex replace r

let private removeDotsRegex = Regex(@"(^|/)\./")
let private removeDotsReplace (m : Match) = m.Groups.[1].Value
let private removeRepeatingSlashesRegex = Regex(@"//+")
let private removeRepeatingSlashesReplace  = "/"
let private removeForwardBacksRegex = Regex(@"([^/]+)/\.\./")
let private replaceForwardBacksReplace (m : Match) =
    if m.Groups.[1].Value = ".." then
        "../../"
    else
        ""
let normalize (path : Path) : Path =
    path
        |> shortenReplaceContinuously' removeDotsRegex removeDotsReplace
        |> shortenReplaceContinuously removeRepeatingSlashesRegex removeRepeatingSlashesReplace
        |> shortenReplaceContinuously' removeForwardBacksRegex replaceForwardBacksReplace

let private startingBacksRegex = Regex(@"^/\.\./")
let goesBack (path : Path) =
    startingBacksRegex.IsMatch path

let combine (a : Path) (b : Path) =
    if String.length b > 0 && b.[0] = '/' then
        b
    else
        sprintf "%s/%s" (a.TrimEnd('/')) b

let splitComponents (path : Path) : Path[] =
    path.Split('/') |> Array.filter (fun x -> x <> "")