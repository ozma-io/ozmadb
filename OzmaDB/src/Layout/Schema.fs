module OzmaDB.Layout.Schema

open FSharpPlus
open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json.Linq

open OzmaDB.OzmaUtils
open OzmaDB.Layout.Source
open OzmaDB.OzmaQL.AST
open OzmaDBSchema.System

type SchemaLayoutException(message: string, innerException: Exception) =
    inherit Exception(message, innerException)

    new(message: string) = SchemaLayoutException(message, null)

let private makeSourceColumnField (field: ColumnField) : SourceColumnField =
    { Type = field.Type
      DefaultValue = if field.Default = null then None else Some field.Default
      IsNullable = field.IsNullable
      IsImmutable = field.IsImmutable
      Description = field.Description
      Metadata = JsonMap.parse field.Metadata }

let private makeSourceComputedField (field: ComputedField) : SourceComputedField =
    { Expression = field.Expression
      AllowBroken = field.AllowBroken
      IsVirtual = field.IsVirtual
      IsMaterialized = field.IsMaterialized
      Description = field.Description
      Metadata = JsonMap.parse field.Metadata }

let private makeSourceUniqueConstraint (constr: UniqueConstraint) : SourceUniqueConstraint =
    { Columns = Array.map OzmaQLName constr.Columns
      IsAlternateKey = constr.IsAlternateKey
      Description = constr.Description
      Metadata = JsonMap.parse constr.Metadata }

let private makeSourceCheckConstraint (constr: CheckConstraint) : SourceCheckConstraint =
    { Expression = constr.Expression
      Description = constr.Description
      Metadata = JsonMap.parse constr.Metadata }

let private makeSourceIndex (index: Index) : SourceIndex =
    { Expressions = index.Expressions
      IncludedExpressions = index.IncludedExpressions
      IsUnique = index.IsUnique
      Predicate = Option.ofObj index.Predicate
      Type = indexTypesMap.[index.Type]
      Description = index.Description
      Metadata = JsonMap.parse index.Metadata }

let private makeSourceEntity (entity: Entity) : SourceEntity =
    { ColumnFields =
        entity.ColumnFields
        |> Seq.map (fun col -> (OzmaQLName col.Name, makeSourceColumnField col))
        |> Map.ofSeqUnique
      ComputedFields =
        entity.ComputedFields
        |> Seq.map (fun comp -> (OzmaQLName comp.Name, makeSourceComputedField comp))
        |> Map.ofSeqUnique
      UniqueConstraints =
        entity.UniqueConstraints
        |> Seq.map (fun constr -> (OzmaQLName constr.Name, makeSourceUniqueConstraint constr))
        |> Map.ofSeqUnique
      CheckConstraints =
        entity.CheckConstraints
        |> Seq.map (fun constr -> (OzmaQLName constr.Name, makeSourceCheckConstraint constr))
        |> Map.ofSeqUnique
      Indexes =
        entity.Indexes
        |> Seq.map (fun index -> (OzmaQLName index.Name, makeSourceIndex index))
        |> Map.ofSeqUnique
      MainField = Option.ofObj entity.MainField |> Option.map OzmaQLName
      InsertedInternally = false
      UpdatedInternally = false
      DeletedInternally = false
      TriggersMigration = false
      SaveRestoreKey =
        if isNull entity.SaveRestoreKey then
            None
        else
            Some <| OzmaQLName entity.SaveRestoreKey
      IsHidden = false
      IsFrozen = entity.IsFrozen
      Parent =
        if entity.Parent = null then
            None
        else
            Some
                { Schema = OzmaQLName entity.Parent.Schema.Name
                  Name = OzmaQLName entity.Parent.Name }
      IsAbstract = entity.IsAbstract
      Description = entity.Description
      Metadata = JsonMap.parse entity.Metadata }

let private makeSourceSchema (schema: Schema) : SourceSchema =
    { Entities =
        schema.Entities
        |> Seq.map (fun entity -> (OzmaQLName entity.Name, makeSourceEntity entity))
        |> Map.ofSeqUnique
      Description = schema.Description
      Metadata = JsonMap.parse schema.Metadata }

let buildSchemaLayout
    (db: SystemContext)
    (filter: Expression<Func<Schema, bool>> option)
    (cancellationToken: CancellationToken)
    : Task<SourceLayout> =
    task {
        let currentSchemas = db.GetLayoutObjects()

        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)

        let! schemas = currentSchemas.ToListAsync(cancellationToken)

        let sourceSchemas =
            schemas
            |> Seq.map (fun schema -> (OzmaQLName schema.Name, makeSourceSchema schema))
            |> Map.ofSeqUnique

        return { Schemas = sourceSchemas }
    }

let private applyHiddenLayoutSchemaData (sourceSchema: SourceSchema) (systemSchema: SourceSchema) : SourceSchema =
    let mergeOne entity systemEntity =
        { entity with
            InsertedInternally = systemEntity.InsertedInternally
            UpdatedInternally = systemEntity.UpdatedInternally
            DeletedInternally = systemEntity.DeletedInternally
            TriggersMigration = systemEntity.TriggersMigration
            IsHidden = systemEntity.IsHidden }

    { Entities = Map.unionWith mergeOne sourceSchema.Entities systemSchema.Entities
      Description = sourceSchema.Description
      Metadata = sourceSchema.Metadata }

let applyHiddenLayoutData (sourceLayout: SourceLayout) (systemLayout: SourceLayout) : SourceLayout =
    let mergeOne name schema =
        match Map.tryFind name systemLayout.Schemas with
        | None -> schema
        | Some systemSchema -> applyHiddenLayoutSchemaData schema systemSchema

    { Schemas = Map.map mergeOne sourceLayout.Schemas }
