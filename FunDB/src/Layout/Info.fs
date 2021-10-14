module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.AST

[<NoEquality; NoComparison>]
type SerializedFieldAccess =
    { Select : bool
      Insert : bool
      Update : bool
    }

let fullSerializedFieldAccess : SerializedFieldAccess =
    { Select = true
      Insert = true
      Update = true
    }

[<NoEquality; NoComparison>]
type SerializedColumnField =
    { FieldType : ResolvedFieldType
      ValueType : SQL.SimpleValueType
      DefaultValue : FieldValue option
      IsNullable : bool
      IsImmutable : bool
      InheritedFrom : ResolvedEntityRef option
      Access : SerializedFieldAccess
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
type SerializedIndexColumn =
    { Expr : string
      OpClass : FunQLName option
      Order : SortOrder option
      Nulls : NullsOrder option
    }

[<NoEquality; NoComparison>]
type SerializedIndex =
    { Expressions : SerializedIndexColumn[]
      IsUnique : bool
      Predicate : string option
      Type : IndexType
    }

[<NoEquality; NoComparison>]
type SerializedEntityAccess =
    { Select : bool
      Delete : bool
      Insert : bool
    }

let fullSerializedEntityAccess : SerializedEntityAccess =
    { Select = true
      Delete = true
      Insert = true
    }

[<NoEquality; NoComparison>]
type SerializedEntity =
    { ColumnFields : Map<FieldName, SerializedColumnField>
      ComputedFields : Map<FieldName, SerializedComputedField>
      MainField : FieldName
      ForbidExternalReferences : bool
      Parents : ResolvedEntityRef[]
      Children : SerializedChildEntity[]
      IsAbstract : bool
      IsFrozen : bool
      Root : ResolvedEntityRef
      Access : SerializedEntityAccess
    }

[<NoEquality; NoComparison>]
type SerializedSchema =
    { Entities : Map<EntityName, SerializedEntity>
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
      IsVirtual = Option.isSome comp.Virtual
    }

let serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { FieldType = column.FieldType
      ValueType = column.ValueType
      IsNullable = column.IsNullable
      IsImmutable = column.IsImmutable
      DefaultValue = column.DefaultValue
      InheritedFrom = column.InheritedFrom
      Access = fullSerializedFieldAccess
    }

let serializeUniqueConstraint (constr : ResolvedUniqueConstraint) : SourceUniqueConstraint =
    { Columns = constr.Columns
    }

let serializeCheckConstraint (constr : ResolvedCheckConstraint) : SourceCheckConstraint =
    { Expression = constr.Expression.ToFunQLString()
    }

let serializeIndexColumn (col : ResolvedIndexColumn) : SerializedIndexColumn =
    { Expr = col.Expr.ToFunQLString()
      OpClass = col.OpClass
      Order = col.Order
      Nulls = col.Nulls
    }

let serializeIndex (index : ResolvedIndex) : SerializedIndex =
    { Expressions = index.Expressions |> Array.map serializeIndexColumn
      IsUnique = index.IsUnique
      Predicate = index.Predicate |> Option.map (fun x -> x.ToFunQLString())
      Type = index.Type
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
      MainField = entity.MainField
      ForbidExternalReferences = entity.ForbidExternalReferences
      Parents = inheritanceChain layout entity |> Seq.toArray
      IsAbstract = entity.IsAbstract
      IsFrozen = entity.IsFrozen
      Children = entity.Children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.Direct }) |> Seq.toArray
      Root = entity.Root
      Access = fullSerializedEntityAccess
    }

let serializeSchema (layout : Layout) (schema : ResolvedSchema) : SerializedSchema =
    { Entities = Map.mapMaybe (fun name entity -> if entity.IsHidden then None else Some <| serializeEntity layout entity) schema.Entities
    }

let serializeLayout (layout : Layout) : SerializedLayout =
    { Schemas = Map.map (fun name schema -> serializeSchema layout schema) layout.Schemas
    }
