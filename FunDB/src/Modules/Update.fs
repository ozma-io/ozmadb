module FunWithFlags.FunDB.Modules.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Modules.Source
open FunWithFlags.FunDBSchema.System

type UpdateModulesException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UpdateModulesException (message, null)

type private ModulesUpdater (db : SystemContext) =
    let updateModulesField (modul : SourceModule) (existingModule : Module) : unit =
        existingModule.Source <- modul.Source

    let updateModulesDatabase (schema : SourceModulesSchema) (existingSchema : Schema) : unit =
        let oldModulesMap =
            existingSchema.Modules |> Seq.map (fun modul -> (modul.Path, modul)) |> Map.ofSeq

        let updateFunc _ = updateModulesField
        let createFunc modulePath =
            let newModule =
                Module (
                    Path = modulePath
                )
            existingSchema.Modules.Add(newModule)
            newModule
        ignore <| updateDifference db updateFunc createFunc schema.Modules oldModulesMap

    let updateSchemas (schemas : Map<SchemaName, SourceModulesSchema>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateModulesDatabase
        let createFunc name = raisef UpdateModulesException "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateModules (db : SystemContext) (modules : SourceModules) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    task {
        let! _ = serializedSaveChangesAsync db cancellationToken

        let currentSchemas = db.GetModulesObjects ()
        // We don't touch in any way schemas not in layout.
        let wantedSchemas = modules.Schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
        let! allSchemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name modules.Schemas)
            |> Map.ofSeq

        // See `Layout.Update` for explanation on why is this in a lambda.
        return fun () ->
            task {
                let updater = ModulesUpdater(db)
                updater.UpdateSchemas modules.Schemas schemasMap
                let! changedEntries = serializedSaveChangesAsync db cancellationToken
                return changedEntries
            }
    }
