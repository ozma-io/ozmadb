module FunWithFlags.FunDB.Triggers.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Objects.Types
module SQL = FunWithFlags.FunDB.SQL.AST

type TriggerUpdateFields =
    | TUFAll
    | TUFSet of Set<FieldName>

type TriggerRef =
    { Schema : SchemaName
      Entity : ResolvedEntityRef
      Name : TriggerName
    } with
        override this.ToString () = sprintf "%s.%s.%s" (this.Schema.ToFunQLString()) (this.Entity.ToFunQLString()) (this.Name.ToFunQLString())

[<NoEquality; NoComparison>]
type ResolvedTrigger =
    { Priority : int
      Time : TriggerTime
      OnInsert : bool
      OnUpdateFields : TriggerUpdateFields
      OnDelete : bool
      Procedure : string
      AllowBroken : bool
    }

[<NoEquality; NoComparison>]
type TriggersEntity =
    { Triggers : Map<TriggerName, PossiblyBroken<ResolvedTrigger>>
    }

[<NoEquality; NoComparison>]
type TriggersSchema =
    { Entities : Map<EntityName, TriggersEntity>
    }

[<NoEquality; NoComparison>]
type TriggersDatabase =
    { Schemas : Map<SchemaName, TriggersSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.Name schema.Entities

[<NoEquality; NoComparison>]
type ResolvedTriggers =
    { Schemas : Map<SchemaName, TriggersDatabase>
    }