module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Render
module SQL = FunWithFlags.FunDB.SQL.AST

[<NoComparison>]
type SerializedColumnField =
    { fieldType : ResolvedFieldType
      valueType : SQL.SimpleValueType
      defaultValue : FieldValue option
      isNullable : bool
      isImmutable : bool
      inheritedFrom : ResolvedEntityRef option
    }

[<NoComparison>]
type SerializedComputedField =
    { expression : string
      // Set when there's no dereferences in the expression
      isLocal : bool
      // Set when computed field uses Id
      hasId : bool
      usedSchemas : UsedSchemas
      inheritedFrom : ResolvedEntityRef option
    }

[<NoComparison>]
type SerializedEntity =
    { columnFields : Map<FieldName, SerializedColumnField>
      computedFields : Map<FieldName, SerializedComputedField>
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
      hidden : bool
      parent : ResolvedEntityRef option
      children : Set<ResolvedEntityRef>
      isAbstract : bool
      root : ResolvedEntityRef
    }

[<NoComparison>]
type SerializedSchema =
    { entities : Map<EntityName, SerializedEntity>
      roots : Set<EntityName>
    }

[<NoComparison>]
type SerializedLayout =
    { schemas : Map<SchemaName, SerializedSchema>
    }

let serializeComputedField (comp : ResolvedComputedField) : SerializedComputedField =
    { expression = comp.expression.ToFunQLString()
      isLocal = comp.isLocal
      hasId = comp.hasId
      usedSchemas = comp.usedSchemas
      inheritedFrom = comp.inheritedFrom
    }

let serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { fieldType = column.fieldType
      valueType = column.valueType
      isNullable = column.isNullable
      isImmutable = column.isImmutable
      defaultValue = column.defaultValue
      inheritedFrom = column.inheritedFrom
    }

let serializeEntity (entity : ResolvedEntity) : SerializedEntity =
    { columnFields = Map.map (fun name col -> serializeColumnField col) entity.columnFields
      computedFields = Map.map (fun name comp -> serializeComputedField comp) entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      hidden = entity.hidden
      parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      isAbstract = entity.isAbstract
      children = entity.children
      root = entity.root
    }

let serializeSchema (schema : ResolvedSchema) : SerializedSchema =
    { entities = Map.map (fun name entity -> serializeEntity entity) schema.entities
      roots = schema.roots
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { schemas = Map.map (fun name schema -> serializeSchema schema) layout.schemas
    }
