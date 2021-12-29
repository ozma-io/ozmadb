module FunWithFlags.FunDB.Triggers.Resolve

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types

type ResolveTriggersException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ResolveTriggersException (message, innerException, isUserException innerException)

    new (message : string) = ResolveTriggersException (message, null, true)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolveTriggersException "Invalid role name"

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let resolveTrigger (entity : ResolvedEntity) (trigger : SourceTrigger) : ResolvedTrigger =
        if entity.InsertedInternally && trigger.OnInsert then
            raisef ResolveTriggersException "INSERT triggers are disabled for this entity"
        if entity.UpdatedInternally && not (Array.isEmpty trigger.OnUpdateFields) then
            raisef ResolveTriggersException "UPDATE triggers are disabled for this entity"
        if entity.DeletedInternally && trigger.OnDelete then
            raisef ResolveTriggersException "DELETE triggers are disabled for this entity"
        if entity.CascadeDeleted && trigger.OnDelete then
            raisef ResolveTriggersException "ON DELETE triggers are not implemented for entities containing ON DELETE CASCADE reference fields"

        let (updateFields, updateFieldsSeq) =
            match trigger.OnUpdateFields with
            | [|f|] when f = updateFieldsAll -> (TUFAll, Map.keys entity.ColumnFields)
            | fields ->
                for fieldName in fields do
                    if not <| Map.containsKey fieldName entity.ColumnFields then
                        raisef ResolveTriggersException "Unknown update field name: %O" fieldName
                let fieldsSet =
                    try
                        Set.ofSeqUnique fields
                    with
                    | Failure f -> raisef ResolveTriggersException "Repeated field: %s" f
                (TUFSet fieldsSet, fieldsSet |> Set.toSeq)

        for fieldName in updateFieldsSeq do
            let field = Map.find fieldName entity.ColumnFields
            match field.FieldType with
            | FTScalar (SFTReference (refEntityRef, Some opt)) ->
                match opt with
                | RDASetDefault
                | RDASetNull -> raisef ResolveTriggersException "ON UPDATE triggers are not implemented for ON DELETE SET NULL/DEFAULT reference fields"
                | _ -> ()
            | _ -> ()

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
                    if not trigger.AllowBroken then
                        errors <- Map.add name (e :> exn) errors
                    Error (e :> exn)
            with
            | e -> raisefWithInner ResolveTriggersException e "In trigger %O" name

        let ret =
            { Triggers = entityTriggers.Triggers |> Map.map mapTrigger
            }
        (errors, ret)

    let resolveTriggersSchema (schema : ResolvedSchema) (schemaTriggers : SourceTriggersSchema) : ErroredTriggersSchema * TriggersSchema =
        let mutable errors = Map.empty

        let mapEntity name entityTriggers =
            try
                let entity =
                    match Map.tryFind name schema.Entities with
                    | Some entity when not entity.IsHidden -> entity
                    | _ -> raisef ResolveTriggersException "Unknown entity name"
                let (entityErrors, newEntity) = resolveTriggersEntity entity entityTriggers
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | e -> raisefWithInner ResolveTriggersException e "In triggers entity %O" name

        let ret =
            { Entities = schemaTriggers.Entities |> Map.map mapEntity
            }
        (errors, ret)

    let resolveTriggersDatabase (db : SourceTriggersDatabase) : ErroredTriggersDatabase * TriggersDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaTriggers =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolveTriggersException "Unknown schema name"
                    | Some schema -> schema
                let (schemaErrors, newSchema) = resolveTriggersSchema schema schemaTriggers
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | e -> raisefWithInner ResolveTriggersException e "In triggers schema %O" name

        let ret =
            { Schemas = db.Schemas |> Map.map mapSchema
            } : TriggersDatabase
        (errors, ret)

    let resolveTriggers (triggers : SourceTriggers) : ErroredTriggers * ResolvedTriggers =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveTriggersException "Unknown schema name"
                let (dbErrors, newDb) = resolveTriggersDatabase db
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newDb
            with
            | e -> raisefWithInner ResolveTriggersException e "In schema %O" name

        let ret =
            { Schemas = triggers.Schemas |> Map.map mapDatabase
            }
        (errors, ret)

    member this.ResolveTriggers triggers = resolveTriggers triggers

let resolveTriggers (layout : Layout) (forceAllowBroken : bool) (source : SourceTriggers) : ErroredTriggers * ResolvedTriggers =
    let phase1 = Phase1Resolver (layout, forceAllowBroken)
    phase1.ResolveTriggers source