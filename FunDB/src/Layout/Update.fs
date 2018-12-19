module FunWithFlags.FunDB.Layout.Update

open System
open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source

let private updateDifference (db : SystemContext) (updateFunc : 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) =
    for KeyValue (name, newObject) in newObjects do
        match Map.tryFind name existingObjects with
            | Some existingObject -> updateFunc newObject existingObject
            | None ->
                let newExistingObject = createFunc name
                updateFunc newObject newExistingObject
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            ignore <| db.Remove(existingObject)

let private updateColumnFields (db : SystemContext) (entity : Entity) : Map<FieldName, SourceColumnField> -> Map<FieldName, ColumnField> -> unit =
    let updateColumnFunc (newColumn : SourceColumnField) (oldColumn : ColumnField) =
        let def =
            match newColumn.defaultValue with
                | None -> null
                | Some def -> def
        if oldColumn.Nullable <> newColumn.isNullable then
            oldColumn.Nullable <- newColumn.isNullable
        if oldColumn.Default <> def then
            oldColumn.Default <- def
        if oldColumn.Type <> newColumn.fieldType then
            oldColumn.Type <- newColumn.fieldType
    let createColumnFunc name =
        let newColumn =
            new ColumnField (
                Name = name.ToString()
            )
        entity.ColumnFields.Add(newColumn)
        newColumn
    updateDifference db updateColumnFunc createColumnFunc

let private updateComputedFields (db : SystemContext) (entity : Entity) : Map<FieldName, SourceComputedField> -> Map<FieldName, ComputedField> -> unit =
    let updateComputedFunc (newComputed : SourceComputedField) (oldComputed : ComputedField) =
        if oldComputed.Expression <> newComputed.expression then
            oldComputed.Expression <- newComputed.expression
    let createComputedFunc name =
        let newComputed =
            new ComputedField (
                Name = name.ToString()
            )
        entity.ComputedFields.Add(newComputed)
        newComputed
    updateDifference db updateComputedFunc createComputedFunc

let private updateUniqueConstraints (db : SystemContext) (entity : Entity) : Map<FieldName, SourceUniqueConstraint> -> Map<FieldName, UniqueConstraint> -> unit =
    let updateUniqueFunc (newUnique : SourceUniqueConstraint) (oldUnique : UniqueConstraint) =
        let columnNames = Array.map (fun x -> x.ToString()) newUnique.columns
        if oldUnique.Columns <> columnNames then
            oldUnique.Columns <- columnNames
    let createUniqueFunc name =
        let newUnique =
            new UniqueConstraint (
                Name = name.ToString()
            )
        entity.UniqueConstraints.Add(newUnique)
        newUnique
    updateDifference db updateUniqueFunc createUniqueFunc

let private updateCheckConstraints (db : SystemContext) (entity : Entity) : Map<FieldName, SourceCheckConstraint> -> Map<FieldName, CheckConstraint> -> unit =
    let updateCheckFunc (newCheck : SourceCheckConstraint) (oldCheck : CheckConstraint) =
        if oldCheck.Expression <> newCheck.expression then
            oldCheck.Expression <- newCheck.expression
    let createCheckFunc name =
        let newCheck =
            new CheckConstraint (
                Name = name.ToString()
            )
        entity.CheckConstraints.Add(newCheck)
        newCheck
    updateDifference db updateCheckFunc createCheckFunc

let private updateEntity (db : SystemContext) (entity : SourceEntity) (existingEntity : Entity) : unit =
    let columnFieldsMap = existingEntity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, col)) |> Map.ofSeq
    let computedFieldsMap = existingEntity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, comp)) |> Map.ofSeq
    let uniqueConstraintsMap = existingEntity.UniqueConstraints |> Seq.map (fun unique -> (FunQLName unique.Name, unique)) |> Map.ofSeq
    let checkConstraintsMap = existingEntity.CheckConstraints |> Seq.map (fun check -> (FunQLName check.Name, check)) |> Map.ofSeq

    updateColumnFields db existingEntity entity.columnFields columnFieldsMap
    updateComputedFields db existingEntity entity.computedFields computedFieldsMap
    updateUniqueConstraints db existingEntity entity.uniqueConstraints uniqueConstraintsMap
    updateCheckConstraints db existingEntity entity.checkConstraints checkConstraintsMap

    if entity.mainField = funId then
        existingEntity.MainField <- null
    else
        let mainField = entity.mainField.ToString()
        if existingEntity.MainField <> mainField then
            existingEntity.MainField <- mainField

let private updateEntities (db : SystemContext) (schema : Schema option) : Map<EntityName, SourceEntity> -> Map<EntityName, Entity> -> unit =
    let updateFunc = updateEntity db
    let createFunc name =
        let newEntity =
            new Entity (
                Name = name.ToString()
            )
        match schema with
            | Some dbSchema -> dbSchema.Entities.Add(newEntity)
            | None -> ignore <| db.Entities.Add(newEntity)
        newEntity
    updateDifference db updateFunc createFunc

let private updateSchema (db : SystemContext) (name : SchemaName) (schema : SourceSchema) (existingSchema : Schema option) : unit =
    let newSchema =
        match existingSchema with
            | None ->
                let newSchema =
                    new Schema (
                        Name = name.ToString()
                    )
                ignore <| db.Schemas.Add(newSchema)
                newSchema
            | Some currentSchema -> currentSchema
    let entitiesMap = newSchema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, entity)) |> Map.ofSeq
    updateEntities db (Some newSchema) schema.entities entitiesMap

let updateLayout (db : SystemContext) (layout : SourceLayout) : bool =
    let schemas = getLayoutObjects db

    // We don't touch in any way schemas not in layout.
    let wantedSchemas = layout.schemas |> Map.toSeq |> Seq.map (fun (name, schema) -> name.ToString()) |> Seq.toArray
    let schemasMap =
        schemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name))
        |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
        |> Map.ofSeq
   
    for KeyValue (name, schema) in layout.schemas do
        updateSchema db name schema (Map.tryFind name schemasMap)
    let changedEntries = db.SaveChanges()
    changedEntries > 0