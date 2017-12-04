module FunWithFlags.FunDB.FunQL.Utils

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Utils

let internal renderEntityName (e : Entity) =
    if e.SchemaId.HasValue then
        sprintf "%s.%s" (renderSqlName e.Schema.Name) (renderSqlName e.Name)
    else
        renderSqlName e.Name

let internal renderFieldName (f : Field) = sprintf "%s.%s" (renderEntityName f.Entity) (renderSqlName f.Name)
