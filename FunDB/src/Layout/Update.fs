module FunWithFlags.FunDB.Layout.Update

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDBSchema.System

let private entityToInfo (entity : Entity) = entity.Id
let private makeEntity schemaName (entity : Entity) = ({ schema = schemaName; name = FunQLName entity.Name }, entityToInfo entity)
let private makeSchema (schema : Schema) = Seq.map (makeEntity (FunQLName schema.Name)) schema.Entities

let makeAllEntitiesMap (allSchemas : Schema seq) : Map<ResolvedEntityRef, int> = allSchemas |> Seq.collect makeSchema |> Map.ofSeq

type private LayoutUpdater (db : SystemContext, allSchemas : Schema seq) =
    let allEntitiesMap = makeAllEntitiesMap allSchemas
    let mutable needsParentPass = false

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
            let newColumn =
                ColumnField (
                    Name = name
                )
            entity.ColumnFields.Add(newColumn)
            newColumn
        updateDifference db updateColumnFunc createColumnFunc

    let updateComputedFields (entity : Entity) : Map<FieldName, SourceComputedField> -> Map<FieldName, ComputedField> -> Map<FieldName, ComputedField> =
        let updateComputedFunc _ (newComputed : SourceComputedField) (oldComputed : ComputedField) =
            oldComputed.Expression <- newComputed.Expression
            oldComputed.AllowBroken <- newComputed.AllowBroken
            oldComputed.IsVirtual <- newComputed.IsVirtual
        let createComputedFunc (FunQLName name) =
            let newComputed =
                ComputedField (
                    Name = name
                )
            entity.ComputedFields.Add(newComputed)
            newComputed
        updateDifference db updateComputedFunc createComputedFunc

    let updateUniqueConstraints (entity : Entity) : Map<FieldName, SourceUniqueConstraint> -> Map<FieldName, UniqueConstraint> -> Map<FieldName, UniqueConstraint> =
        let updateUniqueFunc _ (newUnique : SourceUniqueConstraint) (oldUnique : UniqueConstraint) =
            let columnNames = Array.map (fun x -> x.ToString()) newUnique.Columns
            oldUnique.Columns <- columnNames
        let createUniqueFunc (FunQLName name) =
            let newUnique =
                UniqueConstraint (
                    Name = name
                )
            entity.UniqueConstraints.Add(newUnique)
            newUnique
        updateDifference db updateUniqueFunc createUniqueFunc

    let updateCheckConstraints (entity : Entity) : Map<FieldName, SourceCheckConstraint> -> Map<FieldName, CheckConstraint> -> Map<FieldName, CheckConstraint> =
        let updateCheckFunc _ (newCheck : SourceCheckConstraint) (oldCheck : CheckConstraint) =
            oldCheck.Expression <- newCheck.Expression
        let createCheckFunc (FunQLName name) =
            let newCheck =
                CheckConstraint (
                    Name = name
                )
            entity.CheckConstraints.Add(newCheck)
            newCheck
        updateDifference db updateCheckFunc createCheckFunc

    let setParent (existingEntity : Entity) (ref : ResolvedEntityRef) =
        match Map.tryFind ref allEntitiesMap with
        | None ->
            existingEntity.ParentId <- Nullable()
            needsParentPass <- true
        | Some id ->
            existingEntity.ParentId <- Nullable(id)

    let updateEntity (entity : SourceEntity) (existingEntity : Entity) : unit =
        let columnFieldsMap = existingEntity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, col)) |> Map.ofSeq
        let computedFieldsMap = existingEntity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, comp)) |> Map.ofSeq
        let uniqueConstraintsMap = existingEntity.UniqueConstraints |> Seq.map (fun unique -> (FunQLName unique.Name, unique)) |> Map.ofSeq
        let checkConstraintsMap = existingEntity.CheckConstraints |> Seq.map (fun check -> (FunQLName check.Name, check)) |> Map.ofSeq

        ignore <| updateColumnFields existingEntity entity.ColumnFields columnFieldsMap
        ignore <| updateComputedFields existingEntity entity.ComputedFields computedFieldsMap
        ignore <| updateUniqueConstraints existingEntity entity.UniqueConstraints uniqueConstraintsMap
        ignore <| updateCheckConstraints existingEntity entity.CheckConstraints checkConstraintsMap

        if entity.MainField = funId then
            existingEntity.MainField <- null
        else
            existingEntity.MainField <-entity.MainField.ToString()
        existingEntity.ForbidExternalReferences <- entity.ForbidExternalReferences
        existingEntity.ForbidTriggers <- entity.ForbidTriggers
        existingEntity.IsHidden <- entity.IsHidden
        existingEntity.IsAbstract <- entity.IsAbstract
        match entity.Parent with
        | None ->
            existingEntity.ParentId <- Nullable()
        | Some ref ->
            if not <| isNull existingEntity.Parent then
                if not (FunQLName existingEntity.Parent.Schema.Name = ref.schema && FunQLName existingEntity.Parent.Name = ref.name) then
                    setParent existingEntity ref
            else
                setParent existingEntity ref

    let updateSchema (schema : SourceSchema) (existingSchema : Schema) : unit =
        let entitiesMap = existingSchema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, entity)) |> Map.ofSeq

        let updateFunc _ = updateEntity
        let createFunc (FunQLName name) =
            let newEntity =
                Entity (
                    Name = name,
                    ColumnFields = List(),
                    ComputedFields = List(),
                    UniqueConstraints = List(),
                    CheckConstraints = List()
                )
            existingSchema.Entities.Add(newEntity)
            newEntity
        ignore <| updateDifference db updateFunc createFunc schema.Entities entitiesMap

    let updateSchemas schemas existingSchemas =
        let updateFunc _ = updateSchema
        let createFunc (FunQLName name) =
            let newSchema =
                Schema (
                    Name = name,
                    Entities = List()
                )
            ignore <| db.Schemas.Add(newSchema)
            newSchema
        ignore <| updateDifference db updateFunc createFunc schemas existingSchemas

    member this.UpdateSchemas = updateSchemas
    member this.NeedsParentPass = needsParentPass

let private updateLayoutParents (db : SystemContext) (layout : SourceLayout) : Task<unit> =
    task {
        let currentSchemas = db.GetLayoutObjects ()
        let! schemas = currentSchemas.AsTracking().ToListAsync()

        let allEntitiesMap = makeAllEntitiesMap schemas
        let neededSchemas =
            schemas |> Seq.filter (fun schema -> Map.containsKey (FunQLName schema.Name) layout.Schemas)

        for schema in neededSchemas do
            for entity in schema.Entities do
                if not entity.ParentId.HasValue then
                    let newEntity = layout.FindEntity { schema = FunQLName schema.Name; name = FunQLName entity.Name } |> Option.get
                    match newEntity.Parent with
                    | None -> ()
                    | Some ref ->
                        let id = Map.find ref allEntitiesMap
                        entity.ParentId <- Nullable(id)

        let! changedEntries = db.SaveChangesAsync()
        return ()
    }

let updateLayout (db : SystemContext) (layout : SourceLayout) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync(cancellationToken)

        let currentSchemas = db.GetLayoutObjects ()
        let! schemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)

        // We don't touch in any way schemas not in layout.
        let schemasMap =
            schemas
            |> Seq.filter (fun schema -> Map.containsKey (FunQLName schema.Name) layout.Schemas)
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Map.ofSeq

        let updater = LayoutUpdater (db, schemas)
        updater.UpdateSchemas layout.Schemas schemasMap
        let! changedEntries = db.SaveChangesAsync(cancellationToken)

        if updater.NeedsParentPass then
            do! updateLayoutParents db layout

        return changedEntries > 0
    }

let markBrokenLayout (db : SystemContext) (layout : ErroredLayout) (cancellationToken : CancellationToken) : Task<unit> =
    task {
        let currentSchemas = db.GetLayoutObjects ()

        let! schemas = currentSchemas.AsTracking().ToListAsync(cancellationToken)

        for schema in schemas do
            match Map.tryFind (FunQLName schema.Name) layout with
            | None -> ()
            | Some schemaErrors ->
                for entity in schema.Entities do
                    match Map.tryFind (FunQLName entity.Name) schemaErrors with
                    | None -> ()
                    | Some errors ->
                        for comp in entity.ComputedFields do
                            if Map.containsKey (FunQLName comp.Name) errors.computedFields then
                                comp.AllowBroken <- true

        let! _ = db.SaveChangesAsync(cancellationToken)
        return ()
    }