module FunWithFlags.FunDB.Actions.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Microsoft.FSharp.Quotations

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDBSchema.System

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
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e.InnerException "In schema %O: %s" name e.Message
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateActions (db : SystemContext) (actions : SourceActions) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
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

let private findBrokenActionsSchema (schemaName : SchemaName) (schema : ErroredActionsSchema) : ActionRef seq =
    seq {
        for KeyValue(actionName, action) in schema do
            yield { schema = schemaName; name = actionName }
    }

let private findBrokenActions (actions : ErroredActions) : ActionRef seq =
    seq {
        for KeyValue(schemaName, schema) in actions do
            yield! findBrokenActionsSchema schemaName schema
    }

let private checkActionName (ref : ActionRef) : Expr<Action -> bool> =
    let checkSchema = checkSchemaName ref.schema
    let uvName = string ref.name
    <@ fun action -> (%checkSchema) action.Schema && action.Name = uvName @>

let markBrokenActions (db : SystemContext) (actions : ErroredActions) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let checks = findBrokenActions actions |> Seq.map checkActionName
        do! genericMarkBroken db.Actions checks <@ fun x -> Action(AllowBroken = true) @> cancellationToken
    }
