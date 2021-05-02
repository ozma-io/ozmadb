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
      Indexes : Map<IndexName, SourceIndex>
      MainField : FieldName
      ForbidExternalReferences : bool
      Parents : ResolvedEntityRef array
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
    { Expression = comp.Expression.ToFunQLString()
      InheritedFrom = comp.InheritedFrom
      AllowBroken = comp.AllowBroken
      IsBroken = false
      IsVirtual = Option.isSome comp.VirtualCases
    }

let serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { FieldType = column.FieldType
      ValueType = column.ValueType
      IsNullable = column.IsNullable
      IsImmutable = column.IsImmutable
      DefaultValue = column.DefaultValue
      InheritedFrom = column.InheritedFrom
    }

let serializeUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { Columns = constr.Columns
    }


let serializeCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { Expression = constr.Expression.ToFunQLString()
    }

let serializeIndex (index : ResolvedIndex) : SourceIndex =
    { Expressions = index.Expressions |> Array.map (fun x -> x.ToFunQLString())
      IsUnique = index.IsUnique
    }

let rec private inheritanceChain (layout : Layout) (entity : ResolvedEntity) : ResolvedEntityRef seq =
    seq {
        match entity.Parent with
        | None -> ()
        | Some parent ->
            yield parent
            let parentEntity = layout.FindEntity parent |> Option.get
            yield! inheritanceChain layout parentEntity
    }

let serializeEntity (layout : Layout) (entity : ResolvedEntity) : SerializedEntity =
    { ColumnFields = Map.map (fun name col -> serializeColumnField col) entity.ColumnFields
      ComputedFields =  Map.mapMaybe (fun name col -> col |> Result.getOption |> Option.map serializeComputedField) entity.ComputedFields
      UniqueConstraints = Map.map (fun name constr -> serializeUniqueConstraint constr) entity.UniqueConstraints
      CheckConstraints = Map.map (fun name constr -> serializeCheckConstraint constr) entity.CheckConstraints
      Indexes = Map.map (fun name index -> serializeIndex index) entity.Indexes
      MainField = entity.MainField
      ForbidExternalReferences = entity.ForbidExternalReferences
      Parents = inheritanceChain layout entity |> Seq.toArray
      IsAbstract = entity.IsAbstract
      IsFrozen = entity.IsFrozen
      Children = entity.Children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.Direct })
      Root = entity.Root
    }

let serializeSchema (layout : Layout) (schema : ResolvedSchema) : SerializedSchema =
    { Entities = Map.mapMaybe (fun name entity -> if entity.IsHidden then None else Some <| serializeEntity layout entity) schema.Entities
      Roots = schema.Roots
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { Schemas = Map.map (fun name schema -> serializeSchema layout schema) layout.Schemas
    }
