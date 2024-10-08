module OzmaDB.Actions.Update

open System.Threading
open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Quotations
open System.Linq.Expressions

open OzmaDB.OzmaUtils
open OzmaDBSchema.System
open OzmaDB.Operations.Update
open OzmaDB.OzmaQL.AST
open OzmaDB.Actions.Source
open OzmaDB.Actions.Types
open OzmaDB.Actions.Run

type private ActionsUpdater(db: SystemContext) as this =
    inherit SystemUpdater(db)

    let updateActionsField (action: SourceAction) (existingAction: Action) : unit =
        existingAction.AllowBroken <- action.AllowBroken
        existingAction.Function <- action.Function

    let updateActionsDatabase (schema: SourceActionsSchema) (existingSchema: Schema) : unit =
        let oldActionsMap =
            existingSchema.Actions
            |> Seq.map (fun action -> (OzmaQLName action.Name, action))
            |> Map.ofSeq

        let updateFunc _ = updateActionsField

        let createFunc (OzmaQLName actionName) =
            Action(Name = actionName, Schema = existingSchema)

        ignore
        <| this.UpdateDifference updateFunc createFunc schema.Actions oldActionsMap

    let updateSchemas (schemas: Map<SchemaName, SourceActionsSchema>) (existingSchemas: Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateActionsDatabase schema existingSchema
            with e ->
                raisefWithInner SystemUpdaterException e "In schema %O" name

        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateActions
    (db: SystemContext)
    (actions: SourceActions)
    (cancellationToken: CancellationToken)
    : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken
    <| fun () ->
        task {
            let currentSchemas = db.GetActionsObjects()
            // We don't touch in any way schemas not in layout.
            let wantedSchemas =
                actions.Schemas
                |> Map.toSeq
                |> Seq.map (fun (OzmaQLName name, schema) -> name)
                |> Seq.toArray

            let! allSchemas =
                currentSchemas
                    .AsTracking()
                    .Where(fun schema -> wantedSchemas.Contains(schema.Name))
                    .ToListAsync(cancellationToken)

            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (OzmaQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name actions.Schemas)
                |> Map.ofSeq

            let updater = ActionsUpdater(db)
            ignore <| updater.UpdateSchemas actions.Schemas schemasMap
            return updater
        }

let private findBrokenActionsSchema (schemaName: SchemaName) (schema: PreparedActionsSchema) : ActionRef seq =
    seq {
        for KeyValue(actionName, maybeAction) in schema.Actions do
            match maybeAction with
            | Error e when not e.AllowBroken ->
                yield
                    { Schema = schemaName
                      Name = actionName }
            | _ -> ()
    }

let private findBrokenActions (actions: PreparedActions) : ActionRef seq =
    seq {
        for KeyValue(schemaName, schema) in actions.Schemas do
            yield! findBrokenActionsSchema schemaName schema
    }

let private checkActionName (ref: ActionRef) : Expr<Action -> bool> =
    let checkSchema = checkSchemaName ref.Schema
    let name = string ref.Name
    <@ fun action -> (%checkSchema) action.Schema && action.Name = name @>

let markBrokenActions (db: SystemContext) (actions: PreparedActions) (cancellationToken: CancellationToken) : Task =
    task {
        let checks = findBrokenActions actions |> Seq.map checkActionName
        do! genericMarkBroken db.Actions checks cancellationToken
    }
