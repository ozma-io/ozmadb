module FunWithFlags.FunDB.Attributes.Update

open System.Linq
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

type private EntityKey = SchemaName * EntityName

type private AttributesUpdater (db : SystemContext, allSchemas : Schema seq) =
    let entityToInfo (entity : Entity) = entity.Id
    let makeEntity schemaName (entity : Entity) = ((schemaName, FunQLName entity.Name), entityToInfo entity)
    let makeSchema (schema : Schema) = Seq.map (makeEntity (FunQLName schema.Name)) schema.Entities

    let allEntitiesMap = allSchemas |> Seq.collect makeSchema |> Map.ofSeq

    let updateAttributesField (attrs : SourceAttributesField) (existingAttrs : FieldAttributes) : unit =
        existingAttrs.AllowBroken <- attrs.allowBroken
        existingAttrs.Attributes <- attrs.attributes

    let updateAttributesDatabase (schema : SourceAttributesDatabase) (existingSchema : Schema) : unit =
        let addOldAttrsKey (attrs : FieldAttributes) =
            ((FunQLName attrs.FieldEntity.Schema.Name, FunQLName attrs.FieldEntity.Name, FunQLName attrs.FieldName), attrs)
        let oldAttrsMap =
            existingSchema.FieldsAttributes |> Seq.map addOldAttrsKey |> Map.ofSeq

        let addNewAttrsFieldsKey schemaName entityName (fieldName, attrs : SourceAttributesField) =
            ((schemaName, entityName, fieldName), attrs)
        let addNewAttrsEntitiesKey schemaName (entityName, entity : SourceAttributesEntity) =
            entity.fields |> Map.toSeq |> Seq.map (addNewAttrsFieldsKey schemaName entityName)
        let addNewAttrsKey (schemaName, schema : SourceAttributesSchema) =
            schema.entities |> Map.toSeq |> Seq.collect (addNewAttrsEntitiesKey schemaName)
        let newAttrsMap = schema.schemas |> Map.toSeq |> Seq.collect addNewAttrsKey |> Map.ofSeq

        let updateFunc _ = updateAttributesField
        let createFunc (schemaName, entityName, FunQLName fieldName) =
            let entityId = Map.find (schemaName, entityName) allEntitiesMap
            let newAttrs =
                FieldAttributes (
                    FieldEntityId = entityId,
                    FieldName = fieldName
                )
            existingSchema.FieldsAttributes.Add(newAttrs)
            newAttrs
        updateDifference db updateFunc createFunc newAttrsMap oldAttrsMap

    let updateSchemas : Map<SchemaName, SourceAttributesDatabase> -> Map<SchemaName, Schema> -> unit =
        let updateFunc _ = updateAttributesDatabase
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        updateDifference db updateFunc createFunc

    member this.UpdateSchemas = updateSchemas

let updateAttributes (db : SystemContext) (attrs : SourceDefaultAttributes) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync()

        let currentSchemas = db.Schemas |> getFieldsObjects |> getAttributesObjects

        // We don't touch in any way schemas not in layout.
        let wantedSchemas = attrs.schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
        let! schemas = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync()

        let schemasMap = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, schema)) |> Map.ofSeq

        let updater = AttributesUpdater(db, schemas)
        updater.UpdateSchemas attrs.schemas schemasMap
        let! changedEntries = db.SaveChangesAsync()
        return changedEntries > 0
    }

let markBrokenAttributes (db : SystemContext) (attrs : ErroredDefaultAttributes) : Task<unit> =
    task {
        let currentSchemas = db.Schemas |> getFieldsObjects |> getAttributesObjects

        let! schemas = currentSchemas.AsTracking().ToListAsync()

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

        let! _ = db.SaveChangesAsync()
        return ()
    }