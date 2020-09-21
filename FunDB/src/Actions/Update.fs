module FunWithFlags.FunDB.Actions.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Microsoft.FSharp.Quotations

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDBSchema.System

type UpdateActionsException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UpdateActionsException (message, null)

type private ActionsUpdater (db : SystemContext) =
    let updateActionsField (action : SourceAction) (existingAction : Action) : unit =
        existingAction.AllowBroken <- action.AllowBroken
        existingAction.Function <- action.Function

    let updateActionsDatabase (schema : SourceActionsSchema) (existingSchema : Schema) : unit =
        let oldActionsMap =
            existingSchema.Actions |> Seq.map (fun action -> (FunQLName action.Name, action)) |> Map.ofSeq

        let updateFunc _ = updateActionsField
        let createFunc (FunQLName actionName) =
            let newAction =
                Action (
                    Name = actionName
                )
            existingSchema.Actions.Add(newAction)
            newAction
        ignore <| updateDifference db updateFunc createFunc schema.Actions oldActionsMap

    let updateSchemas (schemas : Map<SchemaName, SourceActionsSchema>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateActionsDatabase
        let createFunc name = raisef UpdateActionsException "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateActions (db : SystemContext) (actions : SourceActions) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync(cancellationToken)

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
        updater.UpdateSchemas actions.Schemas schemasMap
        let! changedEntries = db.SaveChangesAsync(cancellationToken)
        return changedEntries > 0
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