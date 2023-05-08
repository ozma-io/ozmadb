module FunWithFlags.FunDB.Layout.Info

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Triggers.Merge
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
      HasUpdateTriggers : bool
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
type SerializedUniqueConstraint =
    { Columns : FieldName[]
      IsAlternateKey : bool
      InheritedFrom : ResolvedEntityRef option
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
      UniqueConstraints : Map<ConstraintName, SerializedUniqueConstraint>
      MainField : FieldName
      InsertedInternally : bool
      UpdatedInternally : bool
      DeletedInternally : bool
      Parents : ResolvedEntityRef[]
      Children : SerializedChildEntity[]
      IsAbstract : bool
      IsFrozen : bool
      Root : ResolvedEntityRef
      Access : SerializedEntityAccess
      ReferencingFields : Map<ResolvedFieldRef, ReferenceDeleteAction>
      CascadeDeleted : bool
      HasInsertTriggers : bool
      HasDeleteTriggers : bool
    } with
    member this.ShouldLog = false

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog


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

let private checkUpdateTriggers (name : FieldName) (triggers : MergedTriggersTime) =
    Map.containsKey MUFAll triggers.OnUpdateFields || Map.containsKey (MUFField name) triggers.OnUpdateFields

let serializeColumnField (triggers : MergedTriggersEntity) (name : FieldName) (column : ResolvedColumnField) : SerializedColumnField =
    { FieldType = column.FieldType
      ValueType = column.ValueType
      IsNullable = column.IsNullable
      IsImmutable = column.IsImmutable
      DefaultValue = column.DefaultValue
      InheritedFrom = column.InheritedFrom
      Access = fullSerializedFieldAccess
      HasUpdateTriggers = checkUpdateTriggers name triggers.Before || checkUpdateTriggers name triggers.After
    }

let serializeUniqueConstraint (constr : ResolvedUniqueConstraint) : SerializedUniqueConstraint =
    { Columns = constr.Columns
      IsAlternateKey = constr.IsAlternateKey
      InheritedFrom = constr.InheritedFrom
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

let serializeEntity (layout : Layout) (triggers : MergedTriggersEntity) (entity : ResolvedEntity) : SerializedEntity =
    { ColumnFields = Map.map (serializeColumnField triggers) entity.ColumnFields
      ComputedFields =  Map.mapMaybe (fun name col -> col |> Result.getOption |> Option.map serializeComputedField) entity.ComputedFields
      UniqueConstraints = Map.map (fun name constr -> serializeUniqueConstraint constr) entity.UniqueConstraints
      MainField = entity.MainField
      InsertedInternally = entity.InsertedInternally
      UpdatedInternally = entity.UpdatedInternally
      DeletedInternally = entity.DeletedInternally
      Parents = inheritanceChain layout entity |> Seq.toArray
      IsAbstract = entity.IsAbstract
      IsFrozen = entity.IsFrozen
      Children = entity.Children |> Map.toSeq |> Seq.map (fun (ref, info) -> { Ref = ref; Direct = info.Direct }) |> Seq.toArray
      Root = entity.Root
      Access = fullSerializedEntityAccess
      ReferencingFields = entity.ReferencingFields
      CascadeDeleted = entity.CascadeDeleted
      HasInsertTriggers = not (Array.isEmpty triggers.Before.OnInsert && Array.isEmpty triggers.After.OnInsert)
      HasDeleteTriggers = not (Array.isEmpty triggers.Before.OnDelete && Array.isEmpty triggers.After.OnDelete)
    }

let serializeSchema (layout : Layout) (triggersSchema : MergedTriggersSchema) (schema : ResolvedSchema) : SerializedSchema =
    let mapOne name entity =
        if entity.IsHidden then
            None
        else
            let triggers = Map.findWithDefault name emptyMergedTriggersEntity triggersSchema.Entities
            Some <| serializeEntity layout triggers entity
    { Entities = Map.mapMaybe mapOne schema.Entities
    }

let serializeLayout (triggers : MergedTriggers) (layout : Layout) : SerializedLayout =
    let mapOne name schema =
        let triggersSchema = Map.findWithDefault name emptyMergedTriggersSchema triggers.Schemas
        serializeSchema layout triggersSchema schema
    { Schemas = Map.map mapOne layout.Schemas
    }
