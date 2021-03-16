module FunWithFlags.FunDB.Attributes.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type UpdateAttributesException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UpdateAttributesException (message, null)

type private AttributesUpdater (db : SystemContext, allSchemas : Schema seq) =
    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAttributesField (attrs : SourceAttributesField) (existingAttrs : FieldAttributes) : unit =
        existingAttrs.AllowBroken <- attrs.AllowBroken
        existingAttrs.Priority <- attrs.Priority
        existingAttrs.Attributes <- attrs.Attributes

    let updateAttributesDatabase (schema : SourceAttributesDatabase) (existingSchema : Schema) : unit =
        let addOldAttrsKey (attrs : FieldAttributes) =
            ({ entity = { schema = FunQLName attrs.FieldEntity.Schema.Name; name = FunQLName attrs.FieldEntity.Name }; name = FunQLName attrs.FieldName }, attrs)
        let oldAttrsMap =
            existingSchema.FieldsAttributes |> Seq.map addOldAttrsKey |> Map.ofSeq

        let addNewAttrsFieldKey schemaName entityName (fieldName, attrs : SourceAttributesField) =
            ({ entity = { schema = schemaName; name = entityName }; name = fieldName }, attrs)
        let addNewAttrsEntityKey schemaName (entityName, entity : SourceAttributesEntity) =
            entity.Fields |> Map.toSeq |> Seq.map (addNewAttrsFieldKey schemaName entityName)
        let addNewAttrsSchemaKey (schemaName, schema : SourceAttributesSchema) =
            schema.Entities |> Map.toSeq |> Seq.collect (addNewAttrsEntityKey schemaName)
        let newAttrsMap = schema.Schemas |> Map.toSeq |> Seq.collect addNewAttrsSchemaKey |> Map.ofSeq

        let updateFunc _ = updateAttributesField
        let createFunc fieldRef =
            let entityId =
                match Map.tryFind fieldRef.entity allEntitiesMap with
                | Some id -> id
                | None -> raisef UpdateAttributesException "Unknown entity %O" fieldRef.entity
            let newAttrs =
                FieldAttributes (
                    FieldEntityId = entityId,
                    FieldName = string fieldRef.name
                )
            existingSchema.FieldsAttributes.Add(newAttrs)
            newAttrs
        ignore <| updateDifference db updateFunc createFunc newAttrsMap oldAttrsMap

    let updateSchemas (schemas : Map<SchemaName, SourceAttributesDatabase>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateAttributesDatabase schema existingSchema
            with
            | :? UpdateAttributesException as e -> raisefWithInner UpdateAttributesException e.InnerException "In schema %O: %s" name e.Message
        let createFunc name = raisef UpdateAttributesException "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateAttributes (db : SystemContext) (attrs : SourceDefaultAttributes) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    task {
        let! _ = serializedSaveChangesAsync db cancellationToken

        let currentSchemas = db.GetAttributesObjects ()

        let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
        // We don't touch in any way schemas not in layout.
        let schemasMap =
            allSchemas
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Seq.filter (fun (name, schema) -> Map.containsKey name attrs.Schemas)
            |> Map.ofSeq

        // See `Layout.Update` for explanation on why is this in a lambda.
        return fun () ->
            task {
                let updater = AttributesUpdater(db, allSchemas)
                updater.UpdateSchemas attrs.Schemas schemasMap
                let! changedEntries = serializedSaveChangesAsync db cancellationToken
                return changedEntries
            }
    }

let private findBrokenAttributesEntity (schemaName : SchemaName) (attrEntityRef : ResolvedEntityRef) (entity : ErroredAttributesEntity) : DefaultAttributeRef seq =
    seq {
        for KeyValue(attrFieldName, field) in entity do
            yield { Schema = schemaName; Field = { entity = attrEntityRef; name = attrFieldName } }
    }

let private findBrokenAttributesSchema (schemaName : SchemaName) (attrSchemaName : SchemaName) (schema : ErroredAttributesSchema) : DefaultAttributeRef seq =
    seq {
        for KeyValue(attrEntityName, entity) in schema do
            yield! findBrokenAttributesEntity schemaName { schema = attrSchemaName; name = attrEntityName } entity
    }

let private findBrokenAttributesDatabase (schemaName : SchemaName) (db : ErroredAttributesDatabase) : DefaultAttributeRef seq =
    seq {
        for KeyValue(attrSchemaName, schema) in db do
            yield! findBrokenAttributesSchema schemaName attrSchemaName schema
    }

let private findBrokenAttributes (attrs : ErroredDefaultAttributes) : DefaultAttributeRef seq =
    seq {
        for KeyValue(schemaName, schema) in attrs do
            yield! findBrokenAttributesDatabase schemaName schema
    }

let private checkAttributeName (ref : DefaultAttributeRef) : Expr<FieldAttributes -> bool> =
    let checkEntity = checkEntityName ref.Field.entity
    let checkSchema = checkSchemaName ref.Schema
    let fieldName = string ref.Field.name
    <@ fun attrs -> (%checkSchema) attrs.Schema && (%checkEntity) attrs.FieldEntity && attrs.FieldName = fieldName @>

let markBrokenAttributes (db : SystemContext) (attrs : ErroredDefaultAttributes) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenAttributes attrs |> Seq.map checkAttributeName
    genericMarkBroken db.FieldsAttributes checks <@ fun x -> FieldAttributes(AllowBroken = true) @> cancellationToken
