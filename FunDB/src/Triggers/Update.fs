module FunWithFlags.FunDB.Triggers.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Quotations
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type UpdateTriggersException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UpdateTriggersException (message, null)

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
            let entityId =
                match Map.tryFind entityRef allEntitiesMap with
                | Some id -> id
                | None -> raisef UpdateTriggersException "Unknown entity %O for trigger %O" entityRef triggerName
            let newTrigger =
                Trigger (
                    TriggerEntityId = entityId,
                    Name = triggerName
                )
            existingSchema.Triggers.Add(newTrigger)
            newTrigger
        ignore <| updateDifference db updateFunc createFunc newTriggersMap oldTriggersMap

    let updateSchemas (schemas : Map<SchemaName, SourceTriggersDatabase>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateTriggersDatabase schema existingSchema
            with
            | :? UpdateTriggersException as e -> raisefWithInner UpdateTriggersException e.InnerException "In schema %O: %s" name e.Message
        let createFunc name = raisef UpdateTriggersException "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateTriggers (db : SystemContext) (triggers : SourceTriggers) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    task {
        let! _ = serializedSaveChangesAsync db cancellationToken

        let currentSchemas = db.GetTriggersObjects ()

        let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
        // We don't touch in any way schemas not in layout.
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name triggers.Schemas)
            |> Map.ofSeq

        // See `Layout.Update` for explanation on why is this in a lambda.
        return fun () ->
            task {
                let updater = TriggersUpdater(db, allSchemas)
                updater.UpdateSchemas triggers.Schemas schemasMap
                let! changedEntries = serializedSaveChangesAsync db cancellationToken
                return changedEntries
            }
    }

let private findBrokenTriggersEntity (schemaName : SchemaName) (trigEntityRef : ResolvedEntityRef) (entity : ErroredTriggersEntity) : TriggerRef seq =
    seq {
        for KeyValue(triggerName, trigger) in entity do
            yield { Schema = schemaName; Entity = trigEntityRef; Name = triggerName }
    }

let private findBrokenTriggersSchema (schemaName : SchemaName) (trigSchemaName : SchemaName) (schema : ErroredTriggersSchema) : TriggerRef seq =
    seq {
        for KeyValue(trigEntityName, entity) in schema do
            yield! findBrokenTriggersEntity schemaName { schema = trigSchemaName; name = trigEntityName } entity
    }

let private findBrokenTriggersDatabase (schemaName : SchemaName) (db : ErroredTriggersDatabase) : TriggerRef seq =
    seq {
        for KeyValue(trigSchemaName, schema) in db do
            yield! findBrokenTriggersSchema schemaName trigSchemaName schema
    }

let private findBrokenTriggers (triggers : ErroredTriggers) : TriggerRef seq =
    seq {
        for KeyValue(schemaName, schema) in triggers do
            yield! findBrokenTriggersDatabase schemaName schema
    }

let private checkTriggerName (ref : TriggerRef) : Expr<Trigger -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let checkSchema = checkSchemaName ref.Schema
    let triggerName = string ref.Name
    <@ fun triggers -> (%checkSchema) triggers.Schema && (%checkEntity) triggers.TriggerEntity && triggers.Name = triggerName @>

let markBrokenTriggers (db : SystemContext) (triggers : ErroredTriggers) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenTriggers triggers |> Seq.map checkTriggerName
    genericMarkBroken db.Triggers checks <@ fun x -> Trigger(AllowBroken = true) @> cancellationToken
