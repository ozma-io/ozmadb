module OzmaDB.OzmaUtils.Parsing

open System
open System.Text.RegularExpressions
open System.Globalization

let tryInt (culture: CultureInfo) (str: string) : int option =
    match Int32.TryParse(str, NumberStyles.Integer ||| NumberStyles.AllowDecimalPoint, culture) with
    | (true, res) -> Some res
    | _ -> None

let tryIntInvariant: string -> int option = tryInt CultureInfo.InvariantCulture
let tryIntCurrent: string -> int option = tryInt CultureInfo.CurrentCulture

let tryInt64 (culture: CultureInfo) (str: string) : int64 option =
    match Int64.TryParse(str, NumberStyles.Integer ||| NumberStyles.AllowDecimalPoint, culture) with
    | (true, res) -> Some res
    | _ -> None

let tryInt64Invariant: string -> int64 option =
    tryInt64 CultureInfo.InvariantCulture

let tryInt64Current: string -> int64 option = tryInt64 CultureInfo.CurrentCulture

let tryDecimal (culture: CultureInfo) (str: string) : decimal option =
    match Decimal.TryParse(str, NumberStyles.Currency, culture) with
    | (true, res) -> Some res
    | _ -> None

let tryDecimalInvariant: string -> decimal option =
    tryDecimal CultureInfo.InvariantCulture

let tryDecimalCurrent: string -> decimal option =
    tryDecimal CultureInfo.CurrentCulture

let tryBool (str: string) : bool option =
    match str.ToLower() with
    | "true" -> Some true
    | "t" -> Some true
    | "1" -> Some true
    | "yes" -> Some true
    | "y" -> Some true
    | "false" -> Some false
    | "f" -> Some false
    | "0" -> Some false
    | "no" -> Some false
    | "n" -> Some false
    | _ -> None

let tryUuid (str: string) : Guid option =
    match Guid.TryParse(str) with
    | (true, uuid) -> Some uuid
    | (false, _) -> None

let regexMatch (regex: string) (flags: RegexOptions) (str: string) : (string list) option =
    let m = Regex.Match(str, regex, flags)

    if m.Success then
        Some <| List.tail [ for x in m.Groups -> x.Value ]
    else
        None

let (|Regex|_|) (regex: string) (str: string) : (string list) option = regexMatch regex RegexOptions.None str

let (|CIRegex|_|) (regex: string) (str: string) : (string list) option =
    regexMatch regex RegexOptions.IgnoreCase str

let (|RegexOpts|_|) (regex: string) (flags: RegexOptions) (str: string) : (string list) option =
    regexMatch regex flags str

module NumBases =
    let octChar (c: char) =
        if c >= '0' && c <= '7' then
            Some <| int c - int '0'
        else
            None

    let hexChar (c: char) =
        if c >= '0' && c <= '9' then
            Some <| int c - int '0'
        else if c >= 'a' && c <= 'f' then
            Some <| 10 + int c - int 'a'
        else if c >= 'A' && c <= 'F' then
            Some <| 10 + int c - int 'A'
        else
            None
