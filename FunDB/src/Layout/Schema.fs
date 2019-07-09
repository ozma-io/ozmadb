module FunWithFlags.FunDB.Layout.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST

let private makeSourceColumnField (field : ColumnField) : SourceColumnField =
    { fieldType = field.Type
      defaultValue =
          if field.Default = null
          then None
          else Some field.Default
      isNullable = field.Nullable
      isImmutable = field.Immutable
    }

let private makeSourceComputedField (field : ComputedField) : SourceComputedField =
    { expression = field.Expression
    }

let private makeSourceUniqueConstraint (constr : UniqueConstraint) : SourceUniqueConstraint =
    { columns = Array.map FunQLName constr.Columns
    }

let private makeSourceCheckConstraint (constr : CheckConstraint) : SourceCheckConstraint =
    { expression = constr.Expression
    }

let private makeSourceEntity (entity : Entity) : SourceEntity =
    { columnFields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceColumnField col)) |> Map.ofSeqUnique
      computedFields = entity.ComputedFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceComputedField col)) |> Map.ofSeqUnique
      uniqueConstraints = entity.UniqueConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceUniqueConstraint constr)) |> Map.ofSeqUnique
      checkConstraints = entity.CheckConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceCheckConstraint constr)) |> Map.ofSeqUnique
      mainField =
        if entity.MainField = null
        then funId
        else FunQLName entity.MainField
      forbidExternalReferences = entity.ForbidExternalReferences
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> Map.ofSeqUnique
    }

let buildSchemaLayout (db : SystemContext) : Task<SourceLayout> =
    task {
        let currentSchemas = getLayoutObjects db.Schemas
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }