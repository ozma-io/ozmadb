module FunWithFlags.FunDB.Layout.Update

open FSharpPlus
open Microsoft.FSharp.Quotations
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types

type private LayoutUpdater (db : SystemContext) as this =
    inherit SystemUpdater(db)

    let updateColumnFields (entity : Entity) : Map<FieldName, SourceColumnField> -> Map<FieldName, ColumnField> -> Map<FieldName, ColumnField> =
        let updateColumnFunc _ (newColumn : SourceColumnField) (oldColumn : ColumnField) =
            oldColumn.IsNullable <- newColumn.IsNullable
            oldColumn.IsImmutable <- newColumn.IsImmutable
            oldColumn.Default <- Option.toObj newColumn.DefaultValue
            oldColumn.Type <- newColumn.Type
            oldColumn.Description <- newColumn.Description
            oldColumn.Metadata <- newColumn.Metadata.ToString(Formatting.None)
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
            oldComputed.IsMaterialized <- newComputed.IsMaterialized
            oldComputed.Description <- newComputed.Description
            oldComputed.Metadata <- newComputed.Metadata.ToString(Formatting.None)
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
            oldUnique.IsAlternateKey <- newUnique.IsAlternateKey
            oldUnique.Description <- newUnique.Description
            oldUnique.Metadata <- newUnique.Metadata.ToString(Formatting.None)
        let createUniqueFunc (FunQLName name) =
            UniqueConstraint (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateUniqueFunc createUniqueFunc

    let updateCheckConstraints (entity : Entity) : Map<FieldName, SourceCheckConstraint> -> Map<FieldName, CheckConstraint> -> Map<FieldName, CheckConstraint> =
        let updateCheckFunc _ (newCheck : SourceCheckConstraint) (oldCheck : CheckConstraint) =
            oldCheck.Expression <- newCheck.Expression
            oldCheck.Description <- newCheck.Description
            oldCheck.Metadata <- newCheck.Metadata.ToString(Formatting.None)
        let createCheckFunc (FunQLName name) =
            CheckConstraint (
                Name = name,
                Entity = entity
            )
        this.UpdateDifference updateCheckFunc createCheckFunc

    let updateIndexes (entity : Entity) : Map<FieldName, SourceIndex> -> Map<FieldName, Index> -> Map<FieldName, Index> =
        let updateIndexFunc _ (newIndex : SourceIndex) (oldIndex : Index) =
            oldIndex.Expressions <- newIndex.Expressions
            oldIndex.IncludedExpressions <- newIndex.IncludedExpressions
            oldIndex.IsUnique <- newIndex.IsUnique
            oldIndex.Predicate <- Option.toObj newIndex.Predicate
            oldIndex.Type <- string newIndex.Type
            oldIndex.Description <- newIndex.Description
            oldIndex.Metadata <- newIndex.Metadata.ToString(Formatting.None)
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

        existingEntity.MainField <- Option.map string entity.MainField |> Option.toObj
        existingEntity.IsAbstract <- entity.IsAbstract
        existingEntity.IsFrozen <- entity.IsFrozen
        match entity.SaveRestoreKey with
        | None ->
            existingEntity.SaveRestoreKey <- null
        | Some key ->
            existingEntity.SaveRestoreKey <- string key
        existingEntity.Description <- entity.Description
        existingEntity.Metadata <- entity.Metadata.ToString(Formatting.None)

    let updateSchema (schema : SourceSchema) (existingSchema : Schema) =
        let entitiesMap = existingSchema.Entities |> Seq.ofObj |> Seq.map (fun entity -> (FunQLName entity.Name, entity)) |> Map.ofSeq

        let updateFunc _ = updateEntity
        let createFunc (FunQLName name) =
            Entity (
                Name = name,
                Schema = existingSchema
            )
        ignore <| this.UpdateDifference updateFunc createFunc (Map.filter (fun name entity -> not entity.IsHidden) schema.Entities) entitiesMap
        existingSchema.Description <- schema.Description
        existingSchema.Metadata <- schema.Metadata.ToString(Formatting.None)

    let updateParents (schemas : Map<SchemaName, SourceSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let parents = existingSchemas |> Map.values |> makeAllEntitiesMap

        for KeyValue(schemaName, schema) in schemas do
            for KeyValue(entityName, entity) in schema.Entities do
                match entity.Parent with
                | None -> ()
                | Some parent ->
                    let existingEntity = parents.[{ Schema = schemaName; Name = entityName }]
                    let parentEntity = parents.[parent]
                    existingEntity.Parent <- parentEntity

    let updateSchemas (schemas : Map<SchemaName, SourceSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc name schema existingSchema =
            try
                updateSchema schema existingSchema
            with
            | e -> raisefWithInner SystemUpdaterException e "In schema %O" name
        let createFunc (FunQLName name) =
            Schema (
                Name = name
            )
        let existingSchemas = this.UpdateDifference updateFunc createFunc schemas existingSchemas
        updateParents schemas existingSchemas
        existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateLayout (db : SystemContext) (layout : SourceLayout) (cancellationToken : CancellationToken) : Task<UpdateResult> =
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

            let updater = LayoutUpdater(db)
            ignore <| updater.UpdateSchemas layout.Schemas schemasMap
            return updater
        }

let private findBrokenComputedFieldsEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : ResolvedFieldRef seq =
    seq {
        for KeyValue(fieldName, maybeField) in entity.ComputedFields do
            match maybeField with
            | Error e when not e.AllowBroken -> yield { Entity = entityRef; Name = fieldName }
            | _ -> ()
    }

let private findBrokenComputedFieldsSchema (schemaName : SchemaName) (schema : ResolvedSchema) : ResolvedFieldRef seq =
    seq {
        for KeyValue(entityName, entity) in schema.Entities do
            yield! findBrokenComputedFieldsEntity { Schema = schemaName; Name = entityName } entity
    }

let private findBrokenComputedFields (layout : Layout) : ResolvedFieldRef seq =
    seq {
        for KeyValue(schemaName, schema) in layout.Schemas do
            yield! findBrokenComputedFieldsSchema schemaName schema
    }

let checkColumnFieldName (ref : ResolvedFieldRef) : Expr<ColumnField -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let compName = string ref.Name
    <@ fun field -> field.Name = compName && (%checkEntity) field.Entity @>

let checkComputedFieldName (ref : ResolvedFieldRef) : Expr<ComputedField -> bool> =
    let checkEntity = checkEntityName ref.Entity
    let compName = string ref.Name
    <@ fun field -> field.Name = compName && (%checkEntity) field.Entity @>

let markBrokenLayout (db : SystemContext) (layout : Layout) (cancellationToken : CancellationToken) : Task =
    let checks = findBrokenComputedFields layout |> Seq.map checkComputedFieldName
    genericMarkBroken db.ComputedFields checks cancellationToken
