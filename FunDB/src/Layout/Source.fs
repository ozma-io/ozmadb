module FunWithFlags.FunDB.Layout.Source

open Microsoft.FSharp.Reflection
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { Columns : FieldName[]
    }

type SourceCheckConstraint =
    { Expression : string
    }

type IndexType =
    | [<CaseName("btree")>] [<DefaultCase>] ITBTree
    | [<CaseName("gist")>] ITGIST
    | [<CaseName("gin")>] ITGIN
    with
        static member private Fields = unionNames (unionCases typeof<IndexType>) |> Map.mapWithKeys (fun name case -> (case.Info.Name, Option.get name))

        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let (case, _) = FSharpValue.GetUnionFields(this, typeof<IndexType>)
            Map.find case.Name IndexType.Fields

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

let indexTypesMap = unionNames (unionCases typeof<IndexType>) |> Map.mapWithKeys (fun name case -> (Option.get name, FSharpValue.MakeUnion(case.Info, [||]) :?> IndexType))

type SourceIndex =
    { Expressions : string[]
      IncludedExpressions : string[]
      IsUnique : bool
      Type : IndexType
      Predicate : string option
    }

type SourceColumnField =
    { Type : string
      DefaultValue : string option
      IsNullable : bool
      IsImmutable : bool
    }

type SourceComputedField =
    { Expression : string
      AllowBroken : bool
      IsVirtual : bool
      IsMaterialized : bool
    }

type SourceEntity =
    { ColumnFields : Map<FieldName, SourceColumnField>
      ComputedFields : Map<FieldName, SourceComputedField>
      UniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints : Map<ConstraintName, SourceCheckConstraint>
      Indexes : Map<IndexName, SourceIndex>
      MainField : FieldName
      [<JsonIgnore>]
      InsertedInternally : bool
      [<JsonIgnore>]
      UpdatedInternally : bool
      [<JsonIgnore>]
      DeletedInternally : bool
      [<JsonIgnore>]
      TriggersMigration : bool
      [<JsonIgnore>]
      IsHidden : bool
      IsAbstract : bool
      IsFrozen : bool
      Parent : ResolvedEntityRef option
    }

type SourceSchema =
    { Entities : Map<EntityName, SourceEntity>
    }

let emptySourceSchema : SourceSchema =
    { Entities = Map.empty
    }

let unionSourceSchema (a : SourceSchema) (b : SourceSchema) : SourceSchema =
    { Entities = Map.unionUnique a.Entities b.Entities
    }

type IEntitiesSet =
    abstract member HasVisibleEntity : ResolvedEntityRef -> bool

type SourceLayout =
    { Schemas : Map<SchemaName, SourceSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.Name schema.Entities

        interface IEntitiesSet with
            member this.HasVisibleEntity ref =
                match this.FindEntity ref with
                | Some entity when not entity.IsHidden -> true
                | _ -> false

let emptySourceLayout : SourceLayout =
    { Schemas = Map.empty
    }

let unionSourceLayout (a : SourceLayout) (b : SourceLayout) : SourceLayout =
    { Schemas = Map.unionWith (fun name -> unionSourceSchema) a.Schemas b.Schemas
    }