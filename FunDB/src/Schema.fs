module FunWithFlags.FunDB.Schema

open Microsoft.EntityFrameworkCore
open Npgsql.NameTranslation

open FunWithFlags.FunUtils.Utils

let updateDifference (db : DbContext) (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
    let updateObject (name, newObject) =
        match Map.tryFind name existingObjects with
        | Some existingObject ->
            updateFunc name newObject existingObject
            (name, existingObject)
        | None ->
            let newExistingObject = createFunc name
            updateFunc name newObject newExistingObject
            (name, newExistingObject)
    let updatedObjects = newObjects |> Map.toSeq |> Seq.map updateObject |> Map.ofSeq
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            ignore <| db.Remove(existingObject)
    updatedObjects

let snakeCaseName = NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase