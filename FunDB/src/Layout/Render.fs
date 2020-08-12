module FunWithFlags.FunDB.Layout.Render

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types

let renderCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { Expression = constr.expression.ToFunQLString()
    }

let renderUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { Columns = constr.columns
    }

let renderComputedField (comp : ResolvedComputedField) : SourceComputedField =
    { Expression = comp.expression.ToFunQLString()
      AllowBroken = comp.allowBroken
      IsVirtual = Option.isSome comp.virtualCases
    }

let renderColumnField (column : ResolvedColumnField) : SourceColumnField =
    { Type = column.fieldType.ToFunQLString()
      IsNullable = column.isNullable
      IsImmutable = column.isImmutable
      DefaultValue = Option.map (fun (x : FieldValue) -> x.ToFunQLString()) column.defaultValue
    }

let renderEntity (entity : ResolvedEntity) : SourceEntity =
    let mapComputed name : Result<ResolvedComputedField, ComputedFieldError> -> SourceComputedField option = function
        | Error { inheritedFrom = None; source = source } -> Some source
        | Ok ({ inheritedFrom = None } as comp) -> Some (renderComputedField comp)
        | _ -> None
    { ColumnFields = Map.mapMaybe (fun name (col : ResolvedColumnField) -> if Option.isNone col.inheritedFrom then Some (renderColumnField col) else None) entity.columnFields
      ComputedFields = Map.mapMaybe mapComputed entity.computedFields
      UniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      CheckConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      MainField = entity.mainField
      ForbidExternalReferences = entity.forbidExternalReferences
      ForbidTriggers = entity.forbidTriggers
      IsHidden = entity.isHidden
      Parent = Option.map (fun inher -> inher.parent) entity.inheritance
      IsAbstract = entity.isAbstract
    }

let renderSchema (schema : ResolvedSchema) : SourceSchema =
    { Entities = Map.map (fun name entity -> renderEntity entity) schema.entities
    }

let renderLayout (layout : Layout) : SourceLayout =
    { Schemas = Map.map (fun name schema -> renderSchema schema) layout.schemas
    }