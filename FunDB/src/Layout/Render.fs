module FunWithFlags.FunDB.Layout.Render

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types

let renderCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { expression = constr.expression.ToFunQLString()
    }

let renderUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { columns = constr.columns
    }

let renderComputedField (comp : ResolvedComputedField) : SourceComputedField =
    { expression = comp.expression.ToFunQLString()
      allowBroken = comp.allowBroken
      isVirtual = Option.isSome comp.virtualCases
    }

let renderColumnField (column : ResolvedColumnField) : SourceColumnField =
    { fieldType = column.fieldType.ToFunQLString()
      isNullable = column.isNullable
      isImmutable = column.isImmutable
      defaultValue = Option.map (fun (x : FieldValue) -> x.ToFunQLString()) column.defaultValue
    }

let renderEntity (entity : ResolvedEntity) : SourceEntity =
    let mapComputed name : Result<ResolvedComputedField, ComputedFieldError> -> SourceComputedField option = function
        | Error { inheritedFrom = None; source = source } -> Some source
        | Ok ({ inheritedFrom = None } as comp) -> Some (renderComputedField comp)
        | _ -> None
    { columnFields = Map.mapMaybe (fun name (col : ResolvedColumnField) -> if Option.isNone col.inheritedFrom then Some (renderColumnField col) else None) entity.columnFields
      computedFields = Map.mapMaybe mapComputed entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      isHidden = entity.isHidden
      parent = Option.map (fun inher -> inher.parent) entity.inheritance
      isAbstract = entity.isAbstract
    }

let renderSchema (schema : ResolvedSchema) : SourceSchema =
    { entities = Map.map (fun name entity -> renderEntity entity) schema.entities
    }

let renderLayout (layout : Layout) : SourceLayout =
    { schemas = Map.map (fun name schema -> renderSchema schema) layout.schemas
    }