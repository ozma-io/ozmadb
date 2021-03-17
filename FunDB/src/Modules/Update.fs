module FunWithFlags.FunDB.Modules.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Modules.Source
open FunWithFlags.FunDBSchema.System

type private ModulesUpdater (db : SystemContext) as this =
    inherit SystemUpdater(db)

    let updateModulesField (modul : SourceModule) (existingModule : Module) : unit =
        existingModule.Source <- modul.Source

    let updateModulesDatabase (schema : SourceModulesSchema) (existingSchema : Schema) : unit =
        let oldModulesMap =
            existingSchema.Modules |> Seq.map (fun modul -> (modul.Path, modul)) |> Map.ofSeq

        let updateFunc _ = updateModulesField
        let createFunc modulePath =
            Module (
                Path = modulePath,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc schema.Modules oldModulesMap

    let updateSchemas (schemas : Map<SchemaName, SourceModulesSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateModulesDatabase schema existingSchema
            with
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e.InnerException "In schema %O: %s" name e.Message
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateModules (db : SystemContext) (modules : SourceModules) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetModulesObjects ()
            // We don't touch in any way schemas not in layout.
            let wantedSchemas = modules.Schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
            let! allSchemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name modules.Schemas)
                |> Map.ofSeq

            let updater = ModulesUpdater(db)
            ignore <| updater.UpdateSchemas modules.Schemas schemasMap
            return updater
        }
