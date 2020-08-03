module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Render
module SQL = FunWithFlags.FunDB.SQL.AST

[<NoEquality; NoComparison>]
type SerializedColumnField =
    { fieldType : ResolvedFieldType
      valueType : SQL.SimpleValueType
      defaultValue : FieldValue option
      isNullable : bool
      isImmutable : bool
      inheritedFrom : ResolvedEntityRef option
    }

[<NoEquality; NoComparison>]
type SerializedComputedField =
    { expression : string
      allowBroken : bool
      isBroken : bool
      inheritedFrom : ResolvedEntityRef option
      isVirtual : bool
    }

[<NoEquality; NoComparison>]
type SerializedChildEntity =
    { ref : ResolvedEntityRef
      direct : bool
    }

[<NoEquality; NoComparison>]
type SerializedEntity =
    { columnFields : Map<FieldName, SerializedColumnField>
      computedFields : Map<FieldName, SerializedComputedField>
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
      isHidden : bool
      parent : ResolvedEntityRef option
      children : SerializedChildEntity seq
      isAbstract : bool
      root : ResolvedEntityRef
    }

[<NoEquality; NoComparison>]
type SerializedSchema =
    { entities : Map<EntityName, SerializedEntity>
      roots : Set<EntityName>
    }

[<NoEquality; NoComparison>]
type SerializedLayout =
    { schemas : Map<SchemaName, SerializedSchema>
    }

let serializeComputedField (comp : ResolvedComputedField) : SerializedComputedField =
    { expression = comp.expression.ToFunQLString()
      inheritedFrom = comp.inheritedFrom
      allowBroken = comp.allowBroken
      isBroken = false
      isVirtual = Option.isSome comp.virtualCases
    }

let serializeComputedFieldError (comp : ComputedFieldError) : SerializedComputedField =
    { expression = comp.source.expression
      inheritedFrom = comp.inheritedFrom
      allowBroken = comp.source.allowBroken
      isBroken = true
      isVirtual = comp.source.isVirtual
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
    let mapComputed name = function
        | Ok f -> serializeComputedField f
        | Error e -> serializeComputedFieldError e
    { columnFields = Map.map (fun name col -> serializeColumnField col) entity.columnFields
      computedFields = Map.map mapComputed entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      isHidden = entity.isHidden
      parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      isAbstract = entity.isAbstract
      children = entity.children |> Map.toSeq |> Seq.map (fun (ref, info) -> { ref = ref; direct = info.direct })
      root = entity.root
    }

let serializeSchema (schema : ResolvedSchema) : SerializedSchema =
    { entities = Map.map (fun name entity -> serializeEntity entity) schema.entities
      roots = schema.roots
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { schemas = Map.map (fun name schema -> serializeSchema schema) layout.schemas
    }
