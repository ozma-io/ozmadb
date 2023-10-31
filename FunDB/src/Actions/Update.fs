module FunWithFlags.FunDB.Actions.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Microsoft.FSharp.Quotations

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.Actions.Run

type private ActionsUpdater (db : SystemContext) as this =
    inherit SystemUpdater(db)

    let updateActionsField (action : SourceAction) (existingAction : Action) : unit =
        existingAction.AllowBroken <- action.AllowBroken
        existingAction.Function <- action.Function

    let updateActionsDatabase (schema : SourceActionsSchema) (existingSchema : Schema) : unit =
        let oldActionsMap =
            existingSchema.Actions |> Seq.map (fun action -> (FunQLName action.Name, action)) |> Map.ofSeq

        let updateFunc _ = updateActionsField
        let createFunc (FunQLName actionName) =
            Action (
                Name = actionName,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc schema.Actions oldActionsMap

    let updateSchemas (schemas : Map<SchemaName, SourceActionsSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateActionsDatabase schema existingSchema
            with
            | e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateActions (db : SystemContext) (actions : SourceActions) (cancellationToken : CancellationToken) : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetActionsObjects ()
            // We don't touch in any way schemas not in layout.
            let wantedSchemas = actions.Schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
            let! allSchemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name actions.Schemas)
                |> Map.ofSeq

            let updater = ActionsUpdater(db)
            ignore <| updater.UpdateSchemas actions.Schemas schemasMap
            return updater
        }

let private findBrokenActionsSchema (schemaName : SchemaName) (schema : PreparedActionsSchema) : ActionRef seq =
    seq {
        for KeyValue(actionName, maybeAction) in schema.Actions do
            match maybeAction with
            | Error e when not e.AllowBroken -> yield { Schema = schemaName; Name = actionName }
            | _ -> ()
    }

let private findBrokenActions (actions : PreparedActions) : ActionRef seq =
    seq {
        for KeyValue(schemaName, schema) in actions.Schemas do
            yield! findBrokenActionsSchema schemaName schema
    }

let private checkActionName (ref : ActionRef) : Expr<Action -> bool> =
    let checkSchema = checkSchemaName ref.Schema
    let name = string ref.Name
    <@ fun action -> (%checkSchema) action.Schema && action.Name = name @>

let markBrokenActions (db : SystemContext) (actions : PreparedActions) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let checks = findBrokenActions actions |> Seq.map checkActionName
        do! genericMarkBroken db.Actions checks <@ fun x -> Action(AllowBroken = true) @> cancellationToken
    }
