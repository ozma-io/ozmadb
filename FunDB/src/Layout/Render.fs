module FunWithFlags.FunDB.Layout.Render

open FunWithFlags.FunDB.Utils
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
    }

let renderColumnField (column : ResolvedColumnField) : SourceColumnField =
    { fieldType = column.fieldType.ToFunQLString()
      isNullable = column.isNullable
      isImmutable = column.isImmutable
      defaultValue = Option.map (fun (x : FieldValue) -> x.ToFunQLString()) column.defaultValue
    }

let renderEntity (entity : ResolvedEntity) : SourceEntity =
    { columnFields = Map.mapMaybe (fun name (col : ResolvedColumnField) -> if Option.isNone col.inheritedFrom then Some (renderColumnField col) else None) entity.columnFields
      computedFields = Map.mapMaybe (fun name comp -> if Option.isNone comp.inheritedFrom then Some (renderComputedField comp) else None) entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      hidden = entity.hidden
      parent = Option.map (fun inher -> inher.parent) entity.inheritance
      isAbstract = entity.isAbstract
    }

let renderSchema (schema : ResolvedSchema) : SourceSchema =
    { entities = Map.map (fun name entity -> renderEntity entity) schema.entities
      forbidExternalInheritance = schema.forbidExternalInheritance
    }

let renderLayout (layout : Layout) : SourceLayout =
    { schemas = Map.map (fun name schema -> renderSchema schema) layout.schemas
    }