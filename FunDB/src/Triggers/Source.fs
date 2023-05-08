module FunWithFlags.FunDB.Triggers.Source

open FSharpPlus
open Microsoft.FSharp.Reflection
open System.ComponentModel

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

type TriggerTime =
    | [<CaseKey("BEFORE")>] TTBefore
    | [<CaseKey("AFTER")>] TTAfter
    with
        static member private Fields = caseNames typeof<TriggerTime> |> Map.mapWithKeys (fun name case -> (case.Info.Name, Option.get name))

        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let (case, _) = FSharpValue.GetUnionFields(this, typeof<TriggerTime>)
            Map.find case.Name TriggerTime.Fields

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type SourceTrigger =
    { AllowBroken : bool
      [<DefaultValue(0)>]
      Priority : int
      Time : TriggerTime
      OnInsert : bool
      OnUpdateFields : FieldName[]
      OnDelete : bool
      Procedure : string
    }

type SourceTriggersEntity =
    { Triggers : Map<TriggerName, SourceTrigger>
    }

let emptySourceTriggersEntity : SourceTriggersEntity =
    { Triggers = Map.empty }

let mergeSourceTriggersEntity (a : SourceTriggersEntity) (b : SourceTriggersEntity) : SourceTriggersEntity =
    { Triggers = Map.unionUnique a.Triggers b.Triggers
    }

type SourceTriggersSchema =
    { Entities : Map<EntityName, SourceTriggersEntity>
    }

let mergeSourceTriggersSchema (a : SourceTriggersSchema) (b : SourceTriggersSchema) : SourceTriggersSchema =
    { Entities = Map.unionWith mergeSourceTriggersEntity a.Entities b.Entities
    }

type SourceTriggersDatabase =
    { Schemas : Map<SchemaName, SourceTriggersSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.Schema this.Schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.Name schema.Entities

let emptySourceTriggersDatabase : SourceTriggersDatabase =
    { Schemas = Map.empty }

let mergeSourceTriggerDatabase (a : SourceTriggersDatabase) (b : SourceTriggersDatabase) : SourceTriggersDatabase =
    { Schemas = Map.unionWith mergeSourceTriggersSchema a.Schemas b.Schemas
    }

type SourceTriggers =
    { Schemas : Map<SchemaName, SourceTriggersDatabase>
    }

let emptySourceTriggers : SourceTriggers =
    { Schemas = Map.empty }

let updateFieldsAll = FunQLName "*"