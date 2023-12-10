module FunWithFlags.FunDB.Triggers.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Quotations
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Run

type private TriggersUpdater (db : SystemContext, allSchemas : Schema seq) as this =
    inherit SystemUpdater(db)

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
            (({ Schema = FunQLName trigger.TriggerEntity.Schema.Name; Name = FunQLName trigger.TriggerEntity.Name }, FunQLName trigger.Name), trigger)
        let oldTriggersMap =
            existingSchema.Triggers |> Seq.map addOldTriggerKey |> Map.ofSeq

        let addNewTriggerKey schemaName entityName (triggerName, trig : SourceTrigger) =
            (({ Schema = schemaName; Name = entityName }, triggerName), trig)
        let addNewTriggersEntityKey schemaName (entityName, entity : SourceTriggersEntity) =
            entity.Triggers |> Map.toSeq |> Seq.map (addNewTriggerKey schemaName entityName)
        let addNewTriggersSchemaKey (schemaName, schema : SourceTriggersSchema) =
            schema.Entities |> Map.toSeq |> Seq.collect (addNewTriggersEntityKey schemaName)
        let newTriggersMap = schema.Schemas |> Map.toSeq |> Seq.collect addNewTriggersSchemaKey |> Map.ofSeq

        let updateFunc _ = updateTriggersField
        let createFunc (entityRef, FunQLName triggerName) =
            let entity =
                match Map.tryFind entityRef allEntitiesMap with
                | Some id -> id
                | None -> raisef SystemUpdaterException "Unknown entity %O for trigger %O" entityRef triggerName
            Trigger (
                TriggerEntity = entity,
                Name = triggerName,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc newTriggersMap oldTriggersMap

    let updateSchemas (schemas : Map<SchemaName, SourceTriggersDatabase>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateTriggersDatabase schema existingSchema
            with
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateTriggers (db : SystemContext) (triggers : SourceTriggers) (cancellationToken : CancellationToken) : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetTriggersObjects ()

            let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
            // We don't touch in any way schemas not in layout.
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name triggers.Schemas)
                |> Map.ofSeq

            let updater = TriggersUpdater(db, allSchemas)
            ignore <| updater.UpdateSchemas triggers.Schemas schemasMap
            return updater
        }

let private findBrokenTriggersEntity (schemaName : SchemaName) (trigEntityRef : ResolvedEntityRef) (entity : PreparedTriggersEntity) : TriggerRef seq =
    seq {
        for KeyValue(triggerName, maybeTrigger) in entity.Triggers do
            match maybeTrigger with
            | Error e when not e.AllowBroken -> yield { Schema = schemaName; Entity = trigEntityRef; Name = triggerName }
            | _ -> ()
    }

let private findBrokenTriggersSchema (schemaName : SchemaName) (trigSchemaName : SchemaName) (schema : PreparedTriggersSchema) : TriggerRef seq =
    seq {
        for KeyValue(trigEntityName, entity) in schema.Entities do
            yield! findBrokenTriggersEntity schemaName { Schema = trigSchemaName; Name = trigEntityName } entity
    }

let private findBrokenTriggersDatabase (schemaName : SchemaName) (db : PreparedTriggersDatabase) : TriggerRef seq =
    seq {
        for KeyValue(trigSchemaName, schema) in db.Schemas do
            yield! findBrokenTriggersSchema schemaName trigSchemaName schema
    }

let private findBrokenTriggers (triggers : PreparedTriggers) : TriggerRef seq =
    seq {
        for KeyValue(schemaName, schema) in triggers.Schemas do
            yield! findBrokenTriggersDatabase schemaName schema
    }

let private checkTriggerName (ref : TriggerRef) : Expr<Trigger -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let checkSchema = checkSchemaName ref.Schema
    let triggerName = string ref.Name
    <@ fun triggers -> (%checkSchema) triggers.Schema && (%checkEntity) triggers.TriggerEntity && triggers.Name = triggerName @>

let markBrokenTriggers (db : SystemContext) (triggers : PreparedTriggers) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenTriggers triggers |> Seq.map checkTriggerName
    genericMarkBroken db.Triggers checks cancellationToken
