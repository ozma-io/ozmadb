module FunWithFlags.FunDB.FunQL.Utils

open FunWithFlags.FunDB.SQL.Utils

type IFunQLString =
    abstract member ToFunQLString : unit -> string
