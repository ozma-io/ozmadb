module FunWithFlags.FunDB.Triggers.Resolve

open NetJs
open NetJs.Value

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types

type ResolveTriggersException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveTriggersException (message, null)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolveTriggersException "Invalid role name"

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let resolveTrigger (entity : ResolvedEntity) (trigger : SourceTrigger) : ResolvedTrigger =
        if entity.forbidTriggers then
            raisef ResolveTriggersException "Triggers are disabled for this entity"

        let updateFields =
            match trigger.OnUpdateFields with
            | [|f|] when f = updateFieldsAll -> TUFAll
            | fields ->
                for field in fields do
                    if not (Map.containsKey field entity.columnFields) then
                        raisef ResolveTriggersException "Unknown update field name: %O" field
                try
                    TUFSet (Set.ofSeqUnique fields)
                with
                | Failure f -> raisef ResolveTriggersException "Repeated field: %s" f

        { AllowBroken = trigger.AllowBroken
          Priority = trigger.Priority
          Time = trigger.Time
          OnInsert = trigger.OnInsert
          OnUpdateFields = updateFields
          OnDelete = trigger.OnDelete
          Procedure = trigger.Procedure
        }

    let resolveTriggersEntity (entity : ResolvedEntity) (entityTriggers : SourceTriggersEntity) : ErroredTriggersEntity * TriggersEntity =
        let mutable errors = Map.empty

        let mapTrigger name (trigger : SourceTrigger) =
            try
                try
                    checkName name
                    Ok <| resolveTrigger entity trigger
                with
                | :? ResolveTriggersException as e when trigger.AllowBroken || forceAllowBroken ->
                    errors <- Map.add name (e :> exn) errors
                    Error { Source = trigger; Error = e }
            with
            | :? ResolveTriggersException as e -> raisefWithInner ResolveTriggersException e.InnerException "Error in trigger %O: %s" name e.Message

        let ret =
            { Triggers = entityTriggers.Triggers |> Map.map mapTrigger
            }
        (errors, ret)

    let resolveTriggersSchema (schema : ResolvedSchema) (schemaTriggers : SourceTriggersSchema) : ErroredTriggersSchema * TriggersSchema =
        let mutable errors = Map.empty

        let mapEntity name entityTriggers =
            try
                let entity =
                    match Map.tryFind name schema.entities with
                    | None -> raisef ResolveTriggersException "Unknown entity name"
                    | Some entity -> entity
                let (entityErrors, newEntity) = resolveTriggersEntity entity entityTriggers
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? ResolveTriggersException as e -> raisefWithInner ResolveTriggersException e.InnerException "Error in triggers entity %O: %s" name e.Message

        let ret =
            { Entities = schemaTriggers.Entities |> Map.map mapEntity
            }
        (errors, ret)

    let resolveTriggersDatabase (db : SourceTriggersDatabase) : ErroredTriggersDatabase * TriggersDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaTriggers =
            try
                let schema =
                    match Map.tryFind name layout.schemas with
                    | None -> raisef ResolveTriggersException "Unknown schema name"
                    | Some schema -> schema
                let (schemaErrors, newSchema) = resolveTriggersSchema schema schemaTriggers
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? ResolveTriggersException as e -> raisefWithInner ResolveTriggersException e.InnerException "Error in triggers schema %O: %s" name e.Message

        let ret =
            { Schemas = db.Schemas |> Map.map mapSchema
            } : TriggersDatabase
        (errors, ret)

    let resolveTriggers (triggers : SourceTriggers) : ErroredTriggers * ResolvedTriggers =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef ResolveTriggersException "Unknown schema name"
                let (dbErrors, newDb) = resolveTriggersDatabase db
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newDb
            with
            | :? ResolveTriggersException as e -> raisefWithInner ResolveTriggersException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { Schemas = triggers.Schemas |> Map.map mapDatabase
            }
        (errors, ret)

    member this.ResolveTriggers triggers = resolveTriggers triggers

let resolveTriggers (layout : Layout) (forceAllowBroken : bool) (source : SourceTriggers) : ErroredTriggers * ResolvedTriggers =
    let phase1 = Phase1Resolver (layout, forceAllowBroken)
    phase1.ResolveTriggers source