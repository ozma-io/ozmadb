module FunWithFlags.FunDB.FunQL.Utils

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Utils

type IFunQLString =
    abstract member ToFunQLString : () -> string
