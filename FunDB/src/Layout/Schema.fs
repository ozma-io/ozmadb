module FunWithFlags.FunDB.Layout.Schema

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
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
    }

let private makeSourceUniqueConstraint (constr : UniqueConstraint) : SourceUniqueConstraint =
    { Columns = Array.map FunQLName constr.Columns
    }

let private makeSourceCheckConstraint (constr : CheckConstraint) : SourceCheckConstraint =
    { Expression = constr.Expression
    }

let private makeSourceEntity (entity : Entity) : SourceEntity =
    { ColumnFields = entity.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceColumnField col)) |> Map.ofSeqUnique
      ComputedFields = entity.ComputedFields |> Seq.map (fun comp -> (FunQLName comp.Name, makeSourceComputedField comp)) |> Map.ofSeqUnique
      UniqueConstraints = entity.UniqueConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceUniqueConstraint constr)) |> Map.ofSeqUnique
      CheckConstraints = entity.CheckConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceCheckConstraint constr)) |> Map.ofSeqUnique
      MainField =
        if isNull entity.MainField
        then funId
        else FunQLName entity.MainField
      ForbidExternalReferences = entity.ForbidExternalReferences
      ForbidTriggers = entity.ForbidTriggers
      IsHidden = entity.IsHidden
      Parent =
        if entity.Parent = null
        then None
        else Some { schema = FunQLName entity.Parent.Schema.Name; name = FunQLName entity.Parent.Name }
      IsAbstract = entity.IsAbstract
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { Entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> Map.ofSeqUnique
    }

let buildSchemaLayout (db : SystemContext) (cancellationToken : CancellationToken) : Task<SourceLayout> =
    task {
        let currentSchemas = db.GetLayoutObjects ()
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }