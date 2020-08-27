module FunWithFlags.FunDB.Triggers.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
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

let private unionErroredTriggersSchema (a : ErroredTriggersSchema) (b : ErroredTriggersSchema) =
    Map.unionWith (fun name -> Map.unionUnique) a b

let private unionErroredTriggersDatabase (a : ErroredTriggersDatabase) (b : ErroredTriggersDatabase) =
    Map.unionWith (fun name -> unionErroredTriggersSchema) a b

let unionErroredTriggers (a : ErroredTriggers) (b : ErroredTriggers) =
    Map.unionWith (fun name -> unionErroredTriggersDatabase) a b