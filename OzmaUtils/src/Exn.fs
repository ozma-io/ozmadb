[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Exn

let rec fullMessage (e: exn) : string =
    if isNull e.InnerException then
        e.Message
    else
        let inner = fullMessage e.InnerException

        if e.Message = "" then inner
        else if inner = "" then e.Message
        else sprintf "%s: %s" e.Message inner

let rec innermost (e: exn) : exn =
    if isNull e.InnerException then
        e
    else
        innermost e.InnerException

let (|Innermost|) e = innermost e
