module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
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
      Parent : ResolvedEntityRef option
      Children : SerializedChildEntity seq
      IsAbstract : bool
      IsFrozen : bool
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

let serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { FieldType = column.fieldType
      ValueType = column.valueType
      IsNullable = column.isNullable
      IsImmutable = column.isImmutable
      DefaultValue = column.defaultValue
      InheritedFrom = column.inheritedFrom
    }

let serializeCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { Expression = constr.expression.ToFunQLString()
    }

let serializeUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { Columns = constr.columns
    }

let serializeEntity (entity : ResolvedEntity) : SerializedEntity =
    { ColumnFields = Map.map (fun name col -> serializeColumnField col) entity.columnFields
      ComputedFields =  Map.mapMaybe (fun name col -> col |> Result.getOption |> Option.map serializeComputedField) entity.computedFields
      UniqueConstraints = Map.map (fun name constr -> serializeUniqueConstraint constr) entity.uniqueConstraints
      CheckConstraints = Map.map (fun name constr -> serializeCheckConstraint constr) entity.checkConstraints
      MainField = entity.mainField
      ForbidExternalReferences = entity.forbidExternalReferences
      Parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      IsAbstract = entity.isAbstract
      IsFrozen = entity.isFrozen
      Children = entity.children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.direct })
      Root = entity.root
    }

let serializeSchema (schema : ResolvedSchema) : SerializedSchema =
    { Entities = Map.mapMaybe (fun name entity -> if entity.isHidden then None else Some <| serializeEntity entity) schema.entities
      Roots = schema.roots
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { Schemas = Map.map (fun name schema -> serializeSchema schema) layout.schemas
    }
