
module FunWithFlags.FunUtils.POSIXPath

open System.Text.RegularExpressions

// We implement methods for handling paths by ourselves, because we use a virtual POSIX fs and System.IO.Path is platform-specific.

let private trimExtensionRegex = Regex(@"([^/]+)\.[^/.]+$")
let private trimExtensionReplace (m : Match) = m.Groups.[1].Value
let trimExtension (path : string) =
    trimExtensionRegex.Replace(path, trimExtensionReplace)

let private dirNameRegex = Regex(@"/[^/]*$")
let dirName (path : string) =
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
let private removeStartingBacksRegex = Regex(@"^/\.\./")
let private removeStartingBacksReplace = "/"

let normalizePath (path : string) =
    path
        |> shortenReplaceContinuously' removeDotsRegex removeDotsReplace
        |> shortenReplaceContinuously removeRepeatingSlashesRegex removeRepeatingSlashesReplace 
        |> shortenReplaceContinuously' removeForwardBacksRegex replaceForwardBacksReplace 
        |> shortenReplaceContinuously removeStartingBacksRegex removeStartingBacksReplace 

let combinePath (a : string) (b : string) =
    if String.length b > 0 && b.[0] = '/' then
        b
    else
        sprintf "%s/%s" (a.TrimEnd('/')) b