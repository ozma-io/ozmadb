module FunWithFlags.FunDB.Triggers.Render

open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types

let private renderTrigger = function
    | Ok trigger ->
        { AllowBroken = trigger.AllowBroken
          Priority = trigger.Priority
          Time = trigger.Time
          OnInsert = trigger.OnInsert
          OnUpdateFields =
            match trigger.OnUpdateFields with
            | TUFAll -> [|updateFieldsAll|]
            | TUFSet fs -> Array.ofSeq fs
          OnDelete = trigger.OnDelete
          Procedure = trigger.Procedure
        } : SourceTrigger
    | Error (e : TriggerError) -> e.Source

let private renderTriggersEntity (entity : TriggersEntity) : SourceTriggersEntity =
    { Triggers = Map.map (fun name -> renderTrigger) entity.Triggers
    }

let private renderTriggersSchema (schema : TriggersSchema) : SourceTriggersSchema =
    { Entities = Map.map (fun name -> renderTriggersEntity) schema.Entities
    }

let renderTriggersDatabase (db : TriggersDatabase) : SourceTriggersDatabase =
    { Schemas = Map.map (fun name -> renderTriggersSchema) db.Schemas
    }

let renderDefaultTriggers (attrs : ResolvedTriggers) : SourceTriggers =
    { Schemas = Map.map (fun name -> renderTriggersDatabase) attrs.Schemas
    }