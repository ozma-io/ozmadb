module FunWithFlags.FunDB.FunQL.Utils

type IFunQLString =
    abstract member ToFunQLString : unit -> string

let goodName (name : string) : bool = not (name.Contains("__") || name = "" || name.Contains(' ') || name.Contains('/'))