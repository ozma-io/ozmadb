module FunWithFlags.FunDB.Layout.Schema

open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

type SchemaLayoutException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = SchemaLayoutException (message, null)

let private makeSourceColumnField (field : ColumnField) : SourceColumnField =
    { Type = field.Type
      DefaultValue =
          if field.Default = null
          then None
          else Some field.Default
      IsNullable = field.IsNullable
      IsImmutable = field.IsImmutable
    }

let private makeSourceComputedField (field : ComputedField) : SourceComputedField =
    { Expression = field.Expression
      AllowBroken = field.AllowBroken
      IsVirtual = field.IsVirtual
      IsMaterialized = field.IsMaterialized
    }

let private makeSourceUniqueConstraint (constr : UniqueConstraint) : SourceUniqueConstraint =
    { Columns = Array.map FunQLName constr.Columns
    }

let private makeSourceCheckConstraint (constr : CheckConstraint) : SourceCheckConstraint =
    { Expression = constr.Expression
    }

let private makeSourceIndex (index : Index) : SourceIndex =
    { Expressions = index.Expressions
      IncludedExpressions = index.IncludedExpressions
      IsUnique = index.IsUnique
      Predicate = Option.ofObj index.Predicate
      Type = Map.find index.Type indexTypesMap
    }

let private makeSourceEntity (entity : Entity) : SourceEntity =
    { ColumnFields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceColumnField col)) |> Map.ofSeqUnique
      ComputedFields = entity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, makeSourceComputedField comp)) |> Map.ofSeqUnique
      UniqueConstraints = entity.UniqueConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceUniqueConstraint constr)) |> Map.ofSeqUnique
      CheckConstraints = entity.CheckConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceCheckConstraint constr)) |> Map.ofSeqUnique
      Indexes = entity.Indexes |> Seq.map (fun index -> (FunQLName index.Name, makeSourceIndex index)) |> Map.ofSeqUnique
      MainField =
        if isNull entity.MainField
        then funId
        else FunQLName entity.MainField
      ForbidExternalReferences = entity.ForbidExternalReferences
      ForbidTriggers = false
      TriggersMigration = false
      IsHidden = false
      IsFrozen = entity.IsFrozen
      Parent =
        if entity.Parent = null
        then None
        else Some { Schema = FunQLName entity.Parent.Schema.Name; Name = FunQLName entity.Parent.Name }
      IsAbstract = entity.IsAbstract
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { Entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> Map.ofSeqUnique
    }

let buildSchemaLayout (db : SystemContext) (withoutSchemas : SchemaName seq) (cancellationToken : CancellationToken) : Task<SourceLayout> =
    task {
        let withoutSchemasArr = withoutSchemas |> Seq.map string |> Array.ofSeq
        let currentSchemas = db.GetLayoutObjects().Where(fun schema -> not (withoutSchemasArr.Contains(schema.Name)))
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }

let private applyHiddenLayoutSchemaData (sourceSchema : SourceSchema) (systemSchema : SourceSchema) : SourceSchema =
    let mergeOne entity systemEntity =
        { entity with
              ForbidTriggers = systemEntity.ForbidTriggers
              TriggersMigration = systemEntity.TriggersMigration
              IsHidden = systemEntity.IsHidden
        }
    { Entities = Map.unionWith (fun name -> mergeOne) sourceSchema.Entities systemSchema.Entities
    }

let applyHiddenLayoutData (sourceLayout : SourceLayout) (systemLayout : SourceLayout) : SourceLayout =
    let mergeOne name schema =
        match Map.tryFind name systemLayout.Schemas with
        | None -> schema
        | Some systemSchema -> applyHiddenLayoutSchemaData schema systemSchema
    { Schemas = Map.map mergeOne sourceLayout.Schemas
    }
