module FunWithFlags.FunDB.Layout.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

type SchemaLayoutException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = SchemaLayoutException (message, null)

let private makeSourceColumnField (field : ColumnField) : SourceColumnField =
    { fieldType = field.Type
      defaultValue =
          if field.Default = null
          then None
          else Some field.Default
      isNullable = field.IsNullable
      isImmutable = field.IsImmutable
    }

let private makeSourceComputedField (field : ComputedField) : SourceComputedField =
    { expression = field.Expression
      allowBroken = field.AllowBroken
      isVirtual = field.IsVirtual
    }

let private makeSourceUniqueConstraint (constr : UniqueConstraint) : SourceUniqueConstraint =
    { columns = Array.map FunQLName constr.Columns
    }

let private makeSourceCheckConstraint (constr : CheckConstraint) : SourceCheckConstraint =
    { expression = constr.Expression
    }

let private makeSourceEntity (entity : Entity) : SourceEntity =
    { columnFields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceColumnField col)) |> Map.ofSeqUnique
      computedFields = entity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, makeSourceComputedField comp)) |> Map.ofSeqUnique
      uniqueConstraints = entity.UniqueConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceUniqueConstraint constr)) |> Map.ofSeqUnique
      checkConstraints = entity.CheckConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceCheckConstraint constr)) |> Map.ofSeqUnique
      mainField =
        if isNull entity.MainField
        then funId
        else FunQLName entity.MainField
      forbidExternalReferences = entity.ForbidExternalReferences
      isHidden = entity.IsHidden
      parent =
        if entity.Parent = null
        then None
        else Some { schema = FunQLName entity.Parent.Schema.Name; name = FunQLName entity.Parent.Name }
      isAbstract = entity.IsAbstract
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> Map.ofSeqUnique
    }

let buildSchemaLayout (db : SystemContext) : Task<SourceLayout> =
    task {
        let currentSchemas = db.GetLayoutObjects ()
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }