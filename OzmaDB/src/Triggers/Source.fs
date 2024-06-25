module OzmaDB.Triggers.Source

open FSharpPlus
open System.ComponentModel

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.OzmaQL.Utils
open OzmaDB.OzmaQL.AST

type TriggerTime =
    | [<CaseKey("BEFORE")>] TTBefore
    | [<CaseKey("AFTER")>] TTAfter

    static member private LookupKey = prepareLookupCaseKey<TriggerTime>

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        TriggerTime.LookupKey this |> Option.get

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

type SourceTrigger =
    { AllowBroken: bool
      [<DefaultValue(0)>]
      Priority: int
      Time: TriggerTime
      OnInsert: bool
      OnUpdateFields: FieldName[]
      OnDelete: bool
      Procedure: string }

type SourceTriggersEntity =
    { Triggers: Map<TriggerName, SourceTrigger> }

let emptySourceTriggersEntity: SourceTriggersEntity = { Triggers = Map.empty }

let mergeSourceTriggersEntity (a: SourceTriggersEntity) (b: SourceTriggersEntity) : SourceTriggersEntity =
    { Triggers = Map.unionUnique a.Triggers b.Triggers }

type SourceTriggersSchema =
    { Entities: Map<EntityName, SourceTriggersEntity> }

let mergeSourceTriggersSchema (a: SourceTriggersSchema) (b: SourceTriggersSchema) : SourceTriggersSchema =
    { Entities = Map.unionWith mergeSourceTriggersEntity a.Entities b.Entities }

type SourceTriggersDatabase =
    { Schemas: Map<SchemaName, SourceTriggersSchema> }

    member this.FindEntity(entity: ResolvedEntityRef) =
        match Map.tryFind entity.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind entity.Name schema.Entities

let emptySourceTriggersDatabase: SourceTriggersDatabase = { Schemas = Map.empty }

let mergeSourceTriggerDatabase (a: SourceTriggersDatabase) (b: SourceTriggersDatabase) : SourceTriggersDatabase =
    { Schemas = Map.unionWith mergeSourceTriggersSchema a.Schemas b.Schemas }

type SourceTriggers =
    { Schemas: Map<SchemaName, SourceTriggersDatabase> }

let emptySourceTriggers: SourceTriggers = { Schemas = Map.empty }

let updateFieldsAll = OzmaQLName "*"
