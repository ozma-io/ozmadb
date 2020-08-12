module FunWithFlags.FunDB.Triggers.Types

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type TriggerUpdateFields =
    | TUFAll
    | TUFSet of Set<FieldName>

[<NoEquality; NoComparison>]
type ResolvedTrigger =
    { AllowBroken : bool
      Priority : int
      Time : TriggerTime
      OnInsert : bool
      OnUpdateFields : TriggerUpdateFields
      OnDelete : bool
      Procedure : string
    }

[<NoEquality; NoComparison>]
type TriggerError =
    { Source : SourceTrigger
      Error : exn
    }

[<NoEquality; NoComparison>]
type TriggersEntity =
    { Triggers : Map<TriggerName, Result<ResolvedTrigger, TriggerError>>
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
            match Map.tryFind entity.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.Entities

[<NoEquality; NoComparison>]
type ResolvedTriggers =
    { Schemas : Map<SchemaName, TriggersDatabase>
    }

type ErroredTriggersEntity = Map<TriggerName, exn>
type ErroredTriggersSchema = Map<EntityName, ErroredTriggersEntity>
type ErroredTriggersDatabase = Map<SchemaName, ErroredTriggersSchema>
type ErroredTriggers = Map<SchemaName, ErroredTriggersDatabase>