module FunWithFlags.FunDB.FunQL.Utils

type IFunQLString =
    abstract member ToFunQLString : unit -> string

let goodSystemName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || (name.Contains("__") && not (name.StartsWith("__"))))

let goodName (name : string) : bool =
    not (name = "" || name.Contains(' ') || name.Contains('/') || name.Contains("__"))