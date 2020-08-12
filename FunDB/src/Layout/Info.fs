module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Render
module SQL = FunWithFlags.FunDB.SQL.AST

[<NoEquality; NoComparison>]
type SerializedColumnField =
    { FieldType : ResolvedFieldType
      ValueType : SQL.SimpleValueType
      DefaultValue : FieldValue option
      IsNullable : bool
      IsImmutable : bool
      InheritedFrom : ResolvedEntityRef option
    }

[<NoEquality; NoComparison>]
type SerializedComputedField =
    { Expression : string
      AllowBroken : bool
      IsBroken : bool
      InheritedFrom : ResolvedEntityRef option
      IsVirtual : bool
    }

[<NoEquality; NoComparison>]
type SerializedChildEntity =
    { Ref : ResolvedEntityRef
      Direct : bool
    }

[<NoEquality; NoComparison>]
type SerializedEntity =
    { ColumnFields : Map<FieldName, SerializedColumnField>
      ComputedFields : Map<FieldName, SerializedComputedField>
      UniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints : Map<ConstraintName, SourceCheckConstraint>
      MainField : FieldName
      ForbidExternalReferences : bool
      ForbidTriggers : bool
      IsHidden : bool
      Parent : ResolvedEntityRef option
      Children : SerializedChildEntity seq
      IsAbstract : bool
      Root : ResolvedEntityRef
    }

[<NoEquality; NoComparison>]
type SerializedSchema =
    { Entities : Map<EntityName, SerializedEntity>
      Roots : Set<EntityName>
    }

[<NoEquality; NoComparison>]
type SerializedLayout =
    { Schemas : Map<SchemaName, SerializedSchema>
    }

let serializeComputedField (comp : ResolvedComputedField) : SerializedComputedField =
    { Expression = comp.expression.ToFunQLString()
      InheritedFrom = comp.inheritedFrom
      AllowBroken = comp.allowBroken
      IsBroken = false
      IsVirtual = Option.isSome comp.virtualCases
    }

let serializeComputedFieldError (comp : ComputedFieldError) : SerializedComputedField =
    { Expression = comp.source.Expression
      InheritedFrom = comp.inheritedFrom
      AllowBroken = comp.source.AllowBroken
      IsBroken = true
      IsVirtual = comp.source.IsVirtual
    }

let serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { FieldType = column.fieldType
      ValueType = column.valueType
      IsNullable = column.isNullable
      IsImmutable = column.isImmutable
      DefaultValue = column.defaultValue
      InheritedFrom = column.inheritedFrom
    }

let serializeEntity (entity : ResolvedEntity) : SerializedEntity =
    let mapComputed name = function
        | Ok f -> serializeComputedField f
        | Error e -> serializeComputedFieldError e
    { ColumnFields = Map.map (fun name col -> serializeColumnField col) entity.columnFields
      ComputedFields = Map.map mapComputed entity.computedFields
      UniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      CheckConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      MainField = entity.mainField
      ForbidExternalReferences = entity.forbidExternalReferences
      ForbidTriggers = entity.forbidTriggers
      IsHidden = entity.isHidden
      Parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      IsAbstract = entity.isAbstract
      Children = entity.children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.direct })
      Root = entity.root
    }

let serializeSchema (schema : ResolvedSchema) : SerializedSchema =
    { Entities = Map.map (fun name entity -> serializeEntity entity) schema.entities
      Roots = schema.roots
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { Schemas = Map.map (fun name schema -> serializeSchema schema) layout.schemas
    }
