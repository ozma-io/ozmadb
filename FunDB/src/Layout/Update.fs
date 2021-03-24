module FunWithFlags.FunDB.Layout.Update

open System
open System.Linq
open Microsoft.FSharp.Quotations
open Z.EntityFramework.Plus
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDBSchema.System

let private makeEntity schemaName (entity : Entity) = ({ schema = schemaName; name = FunQLName entity.Name }, entity)
let private makeSchema (schema : Schema) = schema.Entities |> Seq.ofObj |> Seq.map (makeEntity (FunQLName schema.Name))

let makeAllEntitiesMap (allSchemas : Schema seq) : Map<ResolvedEntityRef, Entity> = allSchemas |> Seq.collect makeSchema |> Map.ofSeq

type private LayoutUpdater (db : SystemContext, allSchemas : Schema seq) as this =
    inherit SystemUpdater(db)

    let updateColumnFields (entity : Entity) : Map<FieldName, SourceColumnField> -> Map<FieldName, ColumnField> -> Map<FieldName, ColumnField> =
        let updateColumnFunc _ (newColumn : SourceColumnField) (oldColumn : ColumnField) =
            let def =
                match newColumn.DefaultValue with
                | None -> null
                | Some def -> def
            oldColumn.IsNullable <- newColumn.IsNullable
            oldColumn.IsImmutable <- newColumn.IsImmutable
            oldColumn.Default <- def
            oldColumn.Type <- newColumn.Type
        let createColumnFunc (FunQLName name) =
            ColumnField (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateColumnFunc createColumnFunc

    let updateComputedFields (entity : Entity) : Map<FieldName, SourceComputedField> -> Map<FieldName, ComputedField> -> Map<FieldName, ComputedField> =
        let updateComputedFunc _ (newComputed : SourceComputedField) (oldComputed : ComputedField) =
            oldComputed.Expression <- newComputed.Expression
            oldComputed.AllowBroken <- newComputed.AllowBroken
            oldComputed.IsVirtual <- newComputed.IsVirtual
        let createComputedFunc (FunQLName name) =
            ComputedField (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateComputedFunc createComputedFunc

    let updateUniqueConstraints (entity : Entity) : Map<FieldName, SourceUniqueConstraint> -> Map<FieldName, UniqueConstraint> -> Map<FieldName, UniqueConstraint> =
        let updateUniqueFunc _ (newUnique : SourceUniqueConstraint) (oldUnique : UniqueConstraint) =
            let columnNames = Array.map (fun x -> x.ToString()) newUnique.Columns
            oldUnique.Columns <- columnNames
        let createUniqueFunc (FunQLName name) =
            UniqueConstraint (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateUniqueFunc createUniqueFunc

    let updateCheckConstraints (entity : Entity) : Map<FieldName, SourceCheckConstraint> -> Map<FieldName, CheckConstraint> -> Map<FieldName, CheckConstraint> =
        let updateCheckFunc _ (newCheck : SourceCheckConstraint) (oldCheck : CheckConstraint) =
            oldCheck.Expression <- newCheck.Expression
        let createCheckFunc (FunQLName name) =
            CheckConstraint (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateCheckFunc createCheckFunc

    let updateIndexes (entity : Entity) : Map<FieldName, SourceIndex> -> Map<FieldName, Index> -> Map<FieldName, Index> =
        let updateIndexFunc _ (newIndex : SourceIndex) (oldIndex : Index) =
            oldIndex.Expressions <- newIndex.Expressions
            oldIndex.IsUnique <- newIndex.IsUnique
        let createIndexFunc (FunQLName name) =
            Index (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateIndexFunc createIndexFunc

    let updateEntity (entity : SourceEntity) (existingEntity : Entity) : unit =
        let columnFieldsMap = existingEntity.ColumnFields |> Seq.ofObj |> Seq.map (fun col -> (FunQLName col.Name, col)) |> Map.ofSeq
        let computedFieldsMap = existingEntity.ComputedFields |> Seq.ofObj |> Seq.map (fun comp -> (FunQLName comp.Name, comp)) |> Map.ofSeq
        let uniqueConstraintsMap = existingEntity.UniqueConstraints |> Seq.ofObj |> Seq.map (fun unique -> (FunQLName unique.Name, unique)) |> Map.ofSeq
        let checkConstraintsMap = existingEntity.CheckConstraints |> Seq.ofObj |> Seq.map (fun check -> (FunQLName check.Name, check)) |> Map.ofSeq
        let indexesMap = existingEntity.Indexes |> Seq.ofObj |> Seq.map (fun index -> (FunQLName index.Name, index)) |> Map.ofSeq

        ignore <| updateColumnFields existingEntity entity.ColumnFields columnFieldsMap
        ignore <| updateComputedFields existingEntity entity.ComputedFields computedFieldsMap
        ignore <| updateUniqueConstraints existingEntity entity.UniqueConstraints uniqueConstraintsMap
        ignore <| updateCheckConstraints existingEntity entity.CheckConstraints checkConstraintsMap
        ignore <| updateIndexes existingEntity entity.Indexes indexesMap

        if entity.MainField = funId then
            existingEntity.MainField <- null
        else
            existingEntity.MainField <-entity.MainField.ToString()
        existingEntity.ForbidExternalReferences <- entity.ForbidExternalReferences
        existingEntity.IsAbstract <- entity.IsAbstract
        existingEntity.IsFrozen <- entity.IsFrozen

    let updateSchema (schema : SourceSchema) (existingSchema : Schema) =
        let entitiesMap = existingSchema.Entities |> Seq.ofObj |> Seq.map (fun entity -> (FunQLName entity.Name, entity)) |> Map.ofSeq

        let updateFunc _ = updateEntity
        let createFunc (FunQLName name) =
            Entity (
                Name = name,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc (Map.filter (fun name entity -> not entity.IsHidden) schema.Entities) entitiesMap

    let updateParents (schemas : Map<SchemaName, SourceSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let parents = existingSchemas |> Map.values |> makeAllEntitiesMap

        for KeyValue(schemaName, schema) in schemas do
            for KeyValue(entityName, entity) in schema.Entities do
                match entity.Parent with
                | None -> ()
                | Some parent ->
                    let existingEntity = parents.[{ schema = schemaName; name = entityName }]
                    let parentEntity = parents.[parent]
                    existingEntity.Parent <- parentEntity

    let updateSchemas (schemas : Map<SchemaName, SourceSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateSchema schema existingSchema
            with
            | :? SystemUpdaterException as e -> raisefWithInner SystemUpdaterException e.InnerException "In schema %O: %s" name e.Message
        let createFunc (FunQLName name) =
            Schema (
                Name = name
            )
        let existingSchemas = this.UpdateDifference updateFunc createFunc schemas existingSchemas
        updateParents schemas existingSchemas
        existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateLayout (db : SystemContext) (layout : SourceLayout) (cancellationToken : CancellationToken) : Task<unit -> Task<bool>> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetLayoutObjects ()
            let! schemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)

            // We don't touch in any way schemas not in layout.
            let schemasMap =
                schemas
                |> Seq.filter (fun schema -> Map.containsKey (FunQLName schema.Name) layout.Schemas)
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
                |> Map.ofSeq

            let updater = LayoutUpdater (db, schemas)
            ignore <| updater.UpdateSchemas layout.Schemas schemasMap
            return updater
        }

let private findBrokenComputedFieldsEntity (entityRef : ResolvedEntityRef) (entity : ErroredEntity) : ResolvedFieldRef seq =
    seq {
        for KeyValue(fieldName, field) in entity.ComputedFields do
            yield { entity = entityRef; name = fieldName }
    }

let private findBrokenComputedFieldsSchema (schemaName : SchemaName) (schema : ErroredSchema) : ResolvedFieldRef seq =
    seq {
        for KeyValue(entityName, entity) in schema do
            yield! findBrokenComputedFieldsEntity { schema = schemaName; name = entityName } entity
    }

let private findBrokenComputedFields (layout : ErroredLayout) : ResolvedFieldRef seq =
    seq {
        for KeyValue(schemaName, schema) in layout do
            yield! findBrokenComputedFieldsSchema schemaName schema
    }

let checkSchemaName (name : SchemaName) : Expr<Schema -> bool> =
    let schemaName = string name
    <@ fun schema -> schema.Name = schemaName @>

let checkEntityName (ref : ResolvedEntityRef) : Expr<Entity -> bool> =
    let checkSchema = checkSchemaName ref.schema
    let entityName = string ref.name
    <@ fun entity -> entity.Name = entityName && (%checkSchema) entity.Schema @>

let checkColumnFieldName (ref : ResolvedFieldRef) : Expr<ColumnField -> bool> =
    let checkEntity = checkEntityName ref.entity
    let compName = string ref.name
    <@ fun field -> field.Name = compName && (%checkEntity) field.Entity @>

let checkComputedFieldName (ref : ResolvedFieldRef) : Expr<ComputedField -> bool> =
    let checkEntity = checkEntityName ref.entity
    let compName = string ref.name
    <@ fun field -> field.Name = compName && (%checkEntity) field.Entity @>

let genericMarkBroken (queryable : IQueryable<'a>) (checks : Expr<'a -> bool> seq) (setBroken : Expr<'a -> 'a>) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let errors = Array.ofSeq checks
        if not <| Array.isEmpty errors then
            let check = errors |> Seq.fold1 (fun a b -> <@ fun field -> (%a) field || (%b) field @>)
            let! _ = queryable.Where(Expr.toExpressionFunc check).UpdateAsync(Expr.toMemberInit setBroken, cancellationToken)
            ()
    }

let markBrokenLayout (db : SystemContext) (layout : ErroredLayout) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenComputedFields layout |> Seq.map checkComputedFieldName
    genericMarkBroken db.ComputedFields checks <@ fun x -> ComputedField(AllowBroken = true) @> cancellationToken
