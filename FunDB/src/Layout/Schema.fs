module FunWithFlags.FunDB.Layout.Schema

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST

let private makeSourceColumnField (field : ColumnField) : SourceColumnField =
    { fieldType = field.Type
      defaultExpr =
          if field.Default = null
          then None
          else Some field.Default
      isNullable = field.Nullable
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
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> Map.ofSeqUnique }

let buildSchemaLayout (db : SystemContext) : SourceLayout =
    let (schemas, systemEntities) = getLayoutObjects db

    { schemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique
      systemEntities = systemEntities |> Seq.map (fun ent -> (FunQLName ent.Name, makeSourceEntity ent)) |> Map.ofSeqUnique
    }