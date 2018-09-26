module FunWithFlags.FunDB.Layout.Schema

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Source

let private makeSourceColumnField (field : ColumnField) : SourceColumnField =
    { fieldType = field.Type
      defaultExpr =
          if field.Default = null
          then None
          else Some field.Default
      isNullable = field.Nullable
    }

let private makeSourceComputedField (field : ColumnField) : SourceColumnField =
    { expression = field.Expression
    }

let private makeSourceUniqueConstraint (constr : UniqueConstraint) : SourceUniqueConstraint =
    { columns = Array.map FunQLName constr.Columns
    }

let private makeSourceCheckConstraint (constr : CheckConstraint) : SourceCheckConstraint =
    { expression = constr.Expression
    }

let private makeSourceEntity (entity : Entity) : SourceEntity =
    { isAbstract = entity.IsAbstract
      ancestor =
          if entity.Ancestor = null
          then None
          else Some (FunQLName entity.Ancestor)
      columnFields = schema.ColumnFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceColumnField col)) |> mapOfSeqUnique
      computedFields = schema.ComputedFields |> Seq.map (fun col -> (FunQLName col.Name, makeSourceComputedField col)) |> mapOfSeqUnique
      uniqueConstraints = schema.UniqueConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceUniqueConstraint constr)) |> mapOfSeqUnique
      checkConstraints = schema.CheckConstraints |> Seq.map (fun constr -> (FunQLName constr.Name, makeSourceCheckConstraint constr)) |> mapOfSeqUnique
    }

let private makeSourceSchema (schema : Schema) : SourceSchema =
    { entities = schema.Entities |> Seq.map (fun entity -> (FunQLName entity.Name, makeSourceEntity entity)) |> mapOfSeqUnique }

let buildSchemaLayout (db : DatabaseContext) : SourceLayout =
    let schemas =
        db.Schemas
            .Include(fun sch -> sch.Entities)
                .ThenInclude(fun ent -> ent.Fields)
            .Include(fun sch -> sch.Entities)
                .ThenInclude(fun ent -> ent.Ancestor)
            .Include(fun sch -> sch.Entities)
                .ThenInclude(fun ent -> ent.UniqueConstraints)
            .Include(fun sch -> sch.Entities)
                .ThenInclude(fun ent -> ent.CheckConstraints)
    let systemEntities =
        db.Entities.Where(fun ent -> ent.SchemaId == null)
            .Include(fun ent -> ent.Fields)
            .Include(fun ent -> ent.Ancestor)
            .Include(fun ent -> ent.UniqueConstraints)
            .Include(fun ent -> ent.CheckConstraints)

    { schemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> mapOfSeqUnique
      systemEntities = systemEntities |> Seq.map (fun ent -> (FunQLName ent.Name, makeSourceEntity ent)) |> mapOfSeqUnique
    }
