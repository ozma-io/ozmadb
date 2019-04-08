module FunWithFlags.FunDB.Layout.Update

open System.Threading.Tasks
open System.Linq
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source

type private LayoutUpdater (db : SystemContext) =
    let updateColumnFields (entity : Entity) : Map<FieldName, SourceColumnField> -> Map<FieldName, ColumnField> -> unit =
        let updateColumnFunc _ (newColumn : SourceColumnField) (oldColumn : ColumnField) =
            let def =
                match newColumn.defaultValue with
                | None -> null
                | Some def -> def
            oldColumn.Nullable <- newColumn.isNullable
            oldColumn.Default <- def
            oldColumn.Type <- newColumn.fieldType
        let createColumnFunc (FunQLName name) =
            let newColumn =
                ColumnField (
                    Name = name
                )
            entity.ColumnFields.Add(newColumn)
            newColumn
        updateDifference db updateColumnFunc createColumnFunc

    let updateComputedFields (entity : Entity) : Map<FieldName, SourceComputedField> -> Map<FieldName, ComputedField> -> unit =
        let updateComputedFunc _ (newComputed : SourceComputedField) (oldComputed : ComputedField) =
            oldComputed.Expression <- newComputed.expression
        let createComputedFunc (FunQLName name) =
            let newComputed =
                ComputedField (
                    Name = name
                )
            entity.ComputedFields.Add(newComputed)
            newComputed
        updateDifference db updateComputedFunc createComputedFunc

    let updateUniqueConstraints (entity : Entity) : Map<FieldName, SourceUniqueConstraint> -> Map<FieldName, UniqueConstraint> -> unit =
        let updateUniqueFunc _ (newUnique : SourceUniqueConstraint) (oldUnique : UniqueConstraint) =
            let columnNames = Array.map (fun x -> x.ToString()) newUnique.columns
            oldUnique.Columns <- columnNames
        let createUniqueFunc (FunQLName name) =
            let newUnique =
                UniqueConstraint (
                    Name = name
                )
            entity.UniqueConstraints.Add(newUnique)
            newUnique
        updateDifference db updateUniqueFunc createUniqueFunc

    let updateCheckConstraints (entity : Entity) : Map<FieldName, SourceCheckConstraint> -> Map<FieldName, CheckConstraint> -> unit =
        let updateCheckFunc _ (newCheck : SourceCheckConstraint) (oldCheck : CheckConstraint) =
            oldCheck.Expression <- newCheck.expression
        let createCheckFunc (FunQLName name) =
            let newCheck =
                CheckConstraint (
                    Name = name
                )
            entity.CheckConstraints.Add(newCheck)
            newCheck
        updateDifference db updateCheckFunc createCheckFunc

    let updateEntity (entity : SourceEntity) (existingEntity : Entity) : unit =
        let columnFieldsMap = existingEntity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, col)) |> Map.ofSeq
        let computedFieldsMap = existingEntity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, comp)) |> Map.ofSeq
        let uniqueConstraintsMap = existingEntity.UniqueConstraints |> Seq.map (fun unique -> (FunQLName unique.Name, unique)) |> Map.ofSeq
        let checkConstraintsMap = existingEntity.CheckConstraints |> Seq.map (fun check -> (FunQLName check.Name, check)) |> Map.ofSeq

        updateColumnFields existingEntity entity.columnFields columnFieldsMap
        updateComputedFields existingEntity entity.computedFields computedFieldsMap
        updateUniqueConstraints existingEntity entity.uniqueConstraints uniqueConstraintsMap
        updateCheckConstraints existingEntity entity.checkConstraints checkConstraintsMap

        if entity.mainField = funId then
            existingEntity.MainField <- null
        else
            existingEntity.MainField <- entity.mainField.ToString()
        existingEntity.ForbidExternalReferences <- entity.forbidExternalReferences

    let updateSchema (schema : SourceSchema) (existingSchema : Schema) : unit =
        let entitiesMap = existingSchema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, entity)) |> Map.ofSeq

        let updateFunc _ = updateEntity
        let createFunc (FunQLName name) =
            let newEntity =
                Entity (
                    Name = name
                )
            existingSchema.Entities.Add(newEntity)
            newEntity
        updateDifference db updateFunc createFunc schema.entities entitiesMap

    let updateSchemas : Map<SchemaName, SourceSchema> -> Map<SchemaName, Schema> -> unit =
        let updateFunc _ = updateSchema
        let createFunc (FunQLName name) =
            let newSchema =
                Schema (
                    Name = name
                )
            ignore <| db.Schemas.Add(newSchema)
            newSchema
        updateDifference db updateFunc createFunc

    member this.UpdateSchemas = updateSchemas

let updateLayout (db : SystemContext) (layout : SourceLayout) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync()

        let schemas = getLayoutObjects db.Schemas

        // We don't touch in any way schemas not in layout.
        let wantedSchemas = layout.schemas |> Map.toSeq |> Seq.map (fun (name, schema) -> name.ToString()) |> Seq.toArray
        let! schemasList = schemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync()
        let schemasMap =
            schemasList
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema))
            |> Map.ofSeq

        let updater = LayoutUpdater db
        updater.UpdateSchemas layout.schemas schemasMap
        let! changedEntries = db.SaveChangesAsync()
        return changedEntries > 0
    }