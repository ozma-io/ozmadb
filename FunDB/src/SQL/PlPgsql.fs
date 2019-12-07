module FunWithFlags.FunDB.SQL.PlPgsql

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL

type Statement =
    | StDefinition of SchemaOperation