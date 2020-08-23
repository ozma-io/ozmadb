module FunWithFlags.FunDB.Triggers.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private TriggersUpdater (db : SystemContext, allSchemas : Schema seq) =
    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateTriggersField (trigger : SourceTrigger) (existingTrigger : Trigger) : unit =
        existingTrigger.AllowBroken <- trigger.AllowBroken
        existingTrigger.Priority <- trigger.Priority
        existingTrigger.Time <- trigger.Time.ToString()
        existingTrigger.OnInsert <- trigger.OnInsert
        existingTrigger.OnUpdateFields <- Array.map (fun x -> x.ToString()) trigger.OnUpdateFields
        existingTrigger.OnDelete <- trigger.OnDelete
        existingTrigger.Procedure <- trigger.Procedure

    let updateTriggersDatabase (schema : SourceTriggersDatabase) (existingSchema : Schema) : unit =
        let addOldTriggerKey (trigger: Trigger) =
            (({ schema = FunQLName trigger.TriggerEntity.Schema.Name; name = FunQLName trigger.TriggerEntity.Name }, FunQLName trigger.Name), trigger)
        let oldTriggersMap =
            existingSchema.Triggers |> Seq.map addOldTriggerKey |> Map.ofSeq

        let addNewTriggerKey schemaName entityName (triggerName, trig : SourceTrigger) =
            (({ schema = schemaName; name = entityName }, triggerName), trig)
        let addNewTriggersEntityKey schemaName (entityName, entity : SourceTriggersEntity) =
            entity.Triggers |> Map.toSeq |> Seq.map (addNewTriggerKey schemaName entityName)
        let addNewTriggersSchemaKey (schemaName, schema : SourceTriggersSchema) =
            schema.Entities |> Map.toSeq |> Seq.collect (addNewTriggersEntityKey schemaName)
        let newTriggersMap = schema.Schemas |> Map.toSeq |> Seq.collect addNewTriggersSchemaKey |> Map.ofSeq

        let updateFunc _ = updateTriggersField
        let createFunc (entityRef, FunQLName triggerName) =
            let entityId = Map.find entityRef allEntitiesMap
            let newTrigger =
                Trigger (
                    TriggerEntityId = entityId,
                    Name = triggerName
                )
            existingSchema.Triggers.Add(newTrigger)
            newTrigger
        ignore <| updateDifference db updateFunc createFunc newTriggersMap oldTriggersMap

    let updateSchemas (schemas : Map<SchemaName, SourceTriggersDatabase>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateTriggersDatabase
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateTriggers (db : SystemContext) (triggers : SourceTriggers) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync(cancellationToken)

        let currentSchemas = db.GetTriggersObjects ()

        let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
        // We don't touch in any way schemas not in layout.
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name triggers.Schemas)
            |> Map.ofSeq

        let updater = TriggersUpdater(db, allSchemas)
        updater.UpdateSchemas triggers.Schemas schemasMap
        let! changedEntries = db.SaveChangesAsync(cancellationToken)
        return changedEntries > 0
    }

let markBrokenTriggers (db : SystemContext) (triggers : ErroredTriggers) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let currentSchemas = db.GetTriggersObjects ()

        let! schemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)

        for schema in schemas do
            match Map.tryFind (FunQLName schema.Name) triggers with
            | None -> ()
            | Some dbErrors ->
                for trigger in schema.Triggers do
                    match Map.tryFind (FunQLName trigger.TriggerEntity.Schema.Name) dbErrors with
                    | None -> ()
                    | Some schemaErrors ->
                        match Map.tryFind (FunQLName trigger.TriggerEntity.Name) schemaErrors with
                        | None -> ()
                        | Some entityErrors ->
                            if Map.containsKey (FunQLName trigger.Name) entityErrors then
                                trigger.AllowBroken <- true

        let! _ = db.SaveChangesAsync(cancellationToken)
        return ()
    }