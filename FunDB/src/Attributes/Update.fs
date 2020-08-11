module FunWithFlags.FunDB.Attributes.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private AttributesUpdater (db : SystemContext, allSchemas : Schema seq) =
    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAttributesField (attrs : SourceAttributesField) (existingAttrs : FieldAttributes) : unit =
        existingAttrs.AllowBroken <- attrs.AllowBroken
        existingAttrs.Priority <- attrs.Priority
        existingAttrs.Attributes <- attrs.Attributes

    let updateAttributesDatabase (schema : SourceAttributesDatabase) (existingSchema : Schema) : unit =
        let addOldAttrsKey (attrs : FieldAttributes) =
            (({ schema = FunQLName attrs.FieldEntity.Schema.Name; name = FunQLName attrs.FieldEntity.Name }, FunQLName attrs.FieldName), attrs)
        let oldAttrsMap =
            existingSchema.FieldsAttributes |> Seq.map addOldAttrsKey |> Map.ofSeq

        let addNewAttrsFieldsKey schemaName entityName (fieldName, attrs : SourceAttributesField) =
            (({ schema = schemaName; name = entityName }, fieldName), attrs)
        let addNewAttrsEntitiesKey schemaName (entityName, entity : SourceAttributesEntity) =
            entity.Fields |> Map.toSeq |> Seq.map (addNewAttrsFieldsKey schemaName entityName)
        let addNewAttrsKey (schemaName, schema : SourceAttributesSchema) =
            schema.Entities |> Map.toSeq |> Seq.collect (addNewAttrsEntitiesKey schemaName)
        let newAttrsMap = schema.Schemas |> Map.toSeq |> Seq.collect addNewAttrsKey |> Map.ofSeq

        let updateFunc _ = updateAttributesField
        let createFunc (entityRef, FunQLName fieldName) =
            let entityId = Map.find entityRef allEntitiesMap
            let newAttrs =
                FieldAttributes (
                    FieldEntityId = entityId,
                    FieldName = fieldName
                )
            existingSchema.FieldsAttributes.Add(newAttrs)
            newAttrs
        ignore <| updateDifference db updateFunc createFunc newAttrsMap oldAttrsMap

    let updateSchemas (schemas : Map<SchemaName, SourceAttributesDatabase>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateAttributesDatabase
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateAttributes (db : SystemContext) (attrs : SourceDefaultAttributes) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync(cancellationToken)

        let currentSchemas = db.GetAttributesObjects ()

        let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
        // We don't touch in any way schemas not in layout.
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name attrs.Schemas)
            |> Map.ofSeq

        let updater = AttributesUpdater(db, allSchemas)
        updater.UpdateSchemas attrs.Schemas schemasMap
        let! changedEntries = db.SaveChangesAsync(cancellationToken)
        return changedEntries > 0
    }

let markBrokenAttributes (db : SystemContext) (attrs : ErroredDefaultAttributes) (cancellationToken : CancellationToken) : Task<unit> =
    task {
        let currentSchemas = db.GetAttributesObjects ()

        let! schemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)

        for schema in schemas do
            match Map.tryFind (FunQLName schema.Name) attrs with
            | None -> ()
            | Some dbErrors ->
                for attrs in schema.FieldsAttributes do
                    match Map.tryFind (FunQLName attrs.FieldEntity.Schema.Name) dbErrors with
                    | None -> ()
                    | Some schemaErrors ->
                        match Map.tryFind (FunQLName attrs.FieldEntity.Name) schemaErrors with
                        | None -> ()
                        | Some entityErrors ->
                            if Map.containsKey (FunQLName attrs.FieldName) entityErrors then
                                attrs.AllowBroken <- true

        let! _ = db.SaveChangesAsync(cancellationToken)
        return ()
    }