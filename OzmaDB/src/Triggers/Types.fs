module OzmaDB.Triggers.Types

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.Triggers.Source
open OzmaDB.Objects.Types

module SQL = OzmaDB.SQL.AST

type TriggerUpdateFields =
    | TUFAll
    | TUFSet of Set<FieldName>

type TriggerRef =
    { Schema: SchemaName
      Entity: ResolvedEntityRef
      Name: TriggerName }

    override this.ToString() =
        sprintf "%s.%s.%s" (this.Schema.ToOzmaQLString()) (this.Entity.ToOzmaQLString()) (this.Name.ToOzmaQLString())

[<NoEquality; NoComparison>]
type ResolvedTrigger =
    { Priority: int
      Time: TriggerTime
      OnInsert: bool
      OnUpdateFields: TriggerUpdateFields
      OnDelete: bool
      Procedure: string
      AllowBroken: bool }

[<NoEquality; NoComparison>]
type TriggersEntity =
    { Triggers: Map<TriggerName, PossiblyBroken<ResolvedTrigger>> }

[<NoEquality; NoComparison>]
type TriggersSchema =
    { Entities: Map<EntityName, TriggersEntity> }

[<NoEquality; NoComparison>]
type TriggersDatabase =
    { Schemas: Map<SchemaName, TriggersSchema> }

    member this.FindEntity(entity: ResolvedEntityRef) =
        match Map.tryFind entity.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind entity.Name schema.Entities

[<NoEquality; NoComparison>]
type ResolvedTriggers =
    { Schemas: Map<SchemaName, TriggersDatabase> }
