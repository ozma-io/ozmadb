module FunWithFlags.FunDB.Attributes.Update

open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private AttributesUpdater (db : SystemContext, allSchemas : Schema seq) as this =
    inherit SystemUpdater(db)

    let allEntitiesMap = makeAllEntitiesMap allSchemas

    let updateAttributesField (attrs : SourceAttributesField) (existingAttrs : FieldAttributes) : unit =
        existingAttrs.AllowBroken <- attrs.AllowBroken
        existingAttrs.Priority <- attrs.Priority
        existingAttrs.Attributes <- attrs.Attributes

    let updateAttributesDatabase (schema : SourceAttributesDatabase) (existingSchema : Schema) : unit =
        let addOldAttrsKey (attrs : FieldAttributes) =
            ({ Entity = { Schema = FunQLName attrs.FieldEntity.Schema.Name; Name = FunQLName attrs.FieldEntity.Name }; Name = FunQLName attrs.FieldName }, attrs)
        let oldAttrsMap =
            existingSchema.FieldsAttributes |> Seq.map addOldAttrsKey |> Map.ofSeq

        let addNewAttrsFieldKey schemaName entityName (fieldName, attrs : SourceAttributesField) =
            ({ Entity = { Schema = schemaName; Name = entityName }; Name = fieldName }, attrs)
        let addNewAttrsEntityKey schemaName (entityName, entity : SourceAttributesEntity) =
            entity.Fields |> Map.toSeq |> Seq.map (addNewAttrsFieldKey schemaName entityName)
        let addNewAttrsSchemaKey (schemaName, schema : SourceAttributesSchema) =
            schema.Entities |> Map.toSeq |> Seq.collect (addNewAttrsEntityKey schemaName)
        let newAttrsMap = schema.Schemas |> Map.toSeq |> Seq.collect addNewAttrsSchemaKey |> Map.ofSeq

        let updateFunc _ = updateAttributesField
        let createFunc fieldRef =
            let entity =
                match Map.tryFind fieldRef.Entity allEntitiesMap with
                | Some id -> id
                | None -> raisef SystemUpdaterException "Unknown entity %O" fieldRef.Entity
            FieldAttributes (
                FieldEntity = entity,
                FieldName = string fieldRef.Name,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc newAttrsMap oldAttrsMap

    let updateSchemas (schemas : Map<SchemaName, SourceAttributesDatabase>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateAttributesDatabase schema existingSchema
            with
            | e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateAttributes (db : SystemContext) (attrs : SourceDefaultAttributes) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetAttributesObjects ()

            let! allSchemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)
            // We don't touch in any way schemas not in layout.
            let schemasMap =
                allSchemas
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Seq.filter (fun (name, schema) -> Map.containsKey name attrs.Schemas)
                |> Map.ofSeq

            let updater = AttributesUpdater(db, allSchemas)
            ignore <| updater.UpdateSchemas attrs.Schemas schemasMap
            return updater
        }

let private findBrokenAttributesEntity (schemaName : SchemaName) (attrEntityRef : ResolvedEntityRef) (entity : ErroredAttributesEntity) : DefaultAttributeRef seq =
    seq {
        for KeyValue(attrFieldName, field) in entity do
            yield { Schema = schemaName; Field = { Entity = attrEntityRef; Name = attrFieldName } }
    }

let private findBrokenAttributesSchema (schemaName : SchemaName) (attrSchemaName : SchemaName) (schema : ErroredAttributesSchema) : DefaultAttributeRef seq =
    seq {
        for KeyValue(attrEntityName, entity) in schema do
            yield! findBrokenAttributesEntity schemaName { Schema = attrSchemaName; Name = attrEntityName } entity
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
    let checkEntity = checkEntityName ref.Field.Entity
    let checkSchema = checkSchemaName ref.Schema
    let fieldName = string ref.Field.Name
    <@ fun attrs -> (%checkSchema) attrs.Schema && (%checkEntity) attrs.FieldEntity && attrs.FieldName = fieldName @>

let markBrokenAttributes (db : SystemContext) (attrs : ErroredDefaultAttributes) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenAttributes attrs |> Seq.map checkAttributeName
    genericMarkBroken db.FieldsAttributes checks <@ fun x -> FieldAttributes(AllowBroken = true) @> cancellationToken
