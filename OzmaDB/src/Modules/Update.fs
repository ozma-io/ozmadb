module OzmaDB.Modules.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils
open OzmaDB.Operations.Update
open OzmaDB.OzmaQL.AST
open OzmaDB.Modules.Source
open OzmaDB.Modules.Types
open OzmaDBSchema.System

type private ModulesUpdater (db : SystemContext) as this =
    inherit SystemUpdater(db)

    let updateModulesField (modul : SourceModule) (existingModule : Module) : unit =
        existingModule.Source <- modul.Source
        existingModule.AllowBroken <- modul.AllowBroken

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
            | e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateModules (db : SystemContext) (modules : SourceModules) (cancellationToken : CancellationToken) : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetModulesObjects ()
            // We don't touch in any way schemas not in layout.
            let wantedSchemas = modules.Schemas |> Map.toSeq |> Seq.map (fun (OzmaQLName name, schema) -> name) |> Seq.toArray
            let! allSchemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (OzmaQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name modules.Schemas)
                |> Map.ofSeq

            let updater = ModulesUpdater(db)
            ignore <| updater.UpdateSchemas modules.Schemas schemasMap
            return updater
        }

let private findBrokenModulesSchema (schemaName : SchemaName) (schema : ModulesSchema) : ModuleRef seq =
    seq {
        for KeyValue(modulePath, maybeModule) in schema.Modules do
            match maybeModule with
            | Error e when not e.AllowBroken -> yield { Schema = schemaName; Path = modulePath }
            | _ -> ()
    }

let private findBrokenModules (modules : ResolvedModules) : ModuleRef seq =
    seq {
        for KeyValue(schemaName, schema) in modules.Schemas do
            yield! findBrokenModulesSchema schemaName schema
    }

let private checkModuleName (ref : ModuleRef) : Expr<Module -> bool> =
    let checkSchema = checkSchemaName ref.Schema
    let name = string ref.Path
    <@ fun action -> (%checkSchema) action.Schema && action.Path = name @>

let markBrokenModules (db : SystemContext) (modules : ResolvedModules) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let checks = findBrokenModules modules |> Seq.map checkModuleName
        do! genericMarkBroken db.Modules checks cancellationToken
    }
