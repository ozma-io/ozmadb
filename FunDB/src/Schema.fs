module FunWithFlags.FunDB.Schema

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Npgsql.NameTranslation

open FunWithFlags.FunUtils

let updateDifference (db : DbContext) (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (deleteFunc : 'k -> 'eobj -> unit) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
    let updateObject (name, newObject) =
        match Map.tryFind name existingObjects with
        | Some existingObject ->
            updateFunc name newObject existingObject
            (name, existingObject)
        | None ->
            let newExistingObject = createFunc name
            ignore <| db.Add(newExistingObject)
            updateFunc name newObject newExistingObject
            (name, newExistingObject)
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            deleteFunc name existingObject
    newObjects |> Map.toSeq |> Seq.map updateObject |> Map.ofSeq

let snakeCaseName = NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase
