module FunWithFlags.FunDB.Layout.Source

open FSharpPlus
open System.Runtime.Serialization
open System.ComponentModel
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { Columns : FieldName[]
      IsAlternateKey : bool
    }

type SourceCheckConstraint =
    { Expression : string
    }

type IndexType =
    | [<CaseKey("btree")>] [<DefaultCase>] ITBTree
    | [<CaseKey("gist")>] ITGIST
    | [<CaseKey("gin")>] ITGIN
    with
        static member private LookupKey = prepareLookupCaseKey<IndexType>

        override this.ToString () = this.ToFunQLString()
        member this.ToFunQLString () = IndexType.LookupKey this |> Option.get

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

let indexTypesMap =
    enumCases<IndexType>
    |> Seq.map (fun (case, value) -> (Option.get (caseKey case.Info), value))
    |> dict

type SourceIndex =
    { Expressions : string[]
      IncludedExpressions : string[]
      IsUnique : bool
      Type : IndexType
      Predicate : string option
      [<DefaultValue("")>]
      Description : string
      [<DefaultValue("{}")>]
      Metadata : string
    }

type SourceColumnField =
    { Type : string
      DefaultValue : string option
      IsNullable : bool
      IsImmutable : bool
      [<DefaultValue("")>]
      Description : string
      [<DefaultValue("{}")>]
      Metadata : string
    }

type SourceComputedField =
    { Expression : string
      AllowBroken : bool
      IsVirtual : bool
      IsMaterialized : bool
      [<DefaultValue("")>]
      Description : string
      [<DefaultValue("{}")>]
      Metadata : string
    }

type SourceEntity =
    { ColumnFields : Map<FieldName, SourceColumnField>
      ComputedFields : Map<FieldName, SourceComputedField>
      UniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints : Map<ConstraintName, SourceCheckConstraint>
      Indexes : Map<IndexName, SourceIndex>
      MainField : FieldName option
      SaveRestoreKey : ConstraintName option
      [<IgnoreDataMember>]
      InsertedInternally : bool
      [<IgnoreDataMember>]
      UpdatedInternally : bool
      [<IgnoreDataMember>]
      DeletedInternally : bool
      [<IgnoreDataMember>]
      TriggersMigration : bool
      [<IgnoreDataMember>]
      IsHidden : bool
      IsAbstract : bool
      IsFrozen : bool
      Parent : ResolvedEntityRef option
      [<DefaultValue("")>]
      Description : string
      [<DefaultValue("{}")>]
      Metadata : string
    }

type SourceSchema =
    { Entities : Map<EntityName, SourceEntity>
      [<DefaultValue("")>]
      Description : string
      [<DefaultValue("{}")>]
      Metadata : string
    }

let emptySourceSchema : SourceSchema =
    { Entities = Map.empty
      Description = ""
      Metadata = "{}"
    }

let unionDescription (a : string) (b : string) : string =
    match (a, b) with
    | ("", b) -> b
    | (a, "") -> a
    | _ -> failwith "Cannot union non-empty descriptions"

let unionMetadata (a : JObject) (b : JObject) : JObject =
    match (a.HasValues, b.HasValues) with
    | (_, false) -> a
    | (false, _) -> b
    | _ -> failwith "Cannot union non-empty metadata"

let unionRawMetadata (a : string) (b : string) : string =
    match (a, b) with
    | ("{}", b) -> b
    | (a, "{}") -> a
    | _ -> failwith "Cannot union non-empty metadata"

let unionSourceSchema (a : SourceSchema) (b : SourceSchema) : SourceSchema =
    { Entities = Map.unionUnique a.Entities b.Entities
      Description = unionDescription a.Description b.Description
      Metadata = unionRawMetadata a.Metadata b.Metadata
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
    { Schemas = Map.unionWith unionSourceSchema a.Schemas b.Schemas
    }