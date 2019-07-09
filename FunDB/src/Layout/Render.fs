module FunWithFlags.FunDB.Layout.Render

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types

let private renderCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { expression = constr.expression.ToFunQLString()
    }

let private renderUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { columns = constr.columns
    }

let private renderComputedField (comp : ResolvedComputedField) : SourceComputedField =
    { expression = comp.expression.ToFunQLString()
    }

let private renderColumnField (column : ResolvedColumnField) : SourceColumnField =
    { fieldType = column.fieldType.ToFunQLString()
      isNullable = column.isNullable
      isImmutable = column.isImmutable
      defaultValue = Option.map (fun (x : FieldValue) -> x.ToFunQLString()) column.defaultValue
    }

let private renderEntity (entity : ResolvedEntity) : SourceEntity =
    { columnFields = Map.map (fun name col -> renderColumnField col) entity.columnFields
      computedFields = Map.map (fun name comp -> renderComputedField comp) entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
    }

let renderSchema (schema : ResolvedSchema) : SourceSchema =
    { entities = Map.map (fun name entity -> renderEntity entity) schema.entities
    }

let renderLayout (layout : Layout) : SourceLayout =
    { schemas = Map.map (fun name schema -> renderSchema schema) layout.schemas
    }