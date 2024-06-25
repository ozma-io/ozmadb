module OzmaDB.Triggers.Resolve

open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Utils
open OzmaDB.Layout.Types
open OzmaDB.Triggers.Source
open OzmaDB.Triggers.Types
open OzmaDB.Objects.Types

type ResolveTriggersException(message: string, innerException: exn, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: exn) =
        ResolveTriggersException(message, innerException, isUserException innerException)

    new(message: string) = ResolveTriggersException(message, null, true)

let private checkName (OzmaQLName name) : unit =
    if not <| goodName name then
        raisef ResolveTriggersException "Invalid role name"

type private Phase1Resolver(layout: Layout, forceAllowBroken: bool) =
    let resolveTrigger (entity: ResolvedEntity) (trigger: SourceTrigger) : ResolvedTrigger =
        if entity.InsertedInternally && trigger.OnInsert then
            raisef ResolveTriggersException "INSERT triggers are disabled for this entity"

        if entity.UpdatedInternally && not (Array.isEmpty trigger.OnUpdateFields) then
            raisef ResolveTriggersException "UPDATE triggers are disabled for this entity"

        if entity.DeletedInternally && trigger.OnDelete then
            raisef ResolveTriggersException "DELETE triggers are disabled for this entity"

        if entity.CascadeDeleted && trigger.OnDelete then
            raisef
                ResolveTriggersException
                "ON DELETE triggers are not implemented for entities containing ON DELETE CASCADE reference fields"

        let (updateFields, updateFieldsSeq) =
            match trigger.OnUpdateFields with
            | [| f |] when f = updateFieldsAll -> (TUFAll, Map.keys entity.ColumnFields)
            | fields ->
                for fieldName in fields do
                    if not <| Map.containsKey fieldName entity.ColumnFields then
                        raisef ResolveTriggersException "Unknown update field name: %O" fieldName

                let fieldsSet =
                    try
                        Set.ofSeqUnique fields
                    with Failure f ->
                        raisef ResolveTriggersException "Repeated field: %s" f

                (TUFSet fieldsSet, fieldsSet |> Set.toSeq)

        for fieldName in updateFieldsSeq do
            let field = Map.find fieldName entity.ColumnFields

            match field.FieldType with
            | FTScalar(SFTReference(refEntityRef, Some opt)) ->
                match opt with
                | RDASetDefault
                | RDASetNull ->
                    raisef
                        ResolveTriggersException
                        "ON UPDATE triggers are not implemented for ON DELETE SET NULL/DEFAULT reference fields"
                | _ -> ()
            | _ -> ()

        { Priority = trigger.Priority
          Time = trigger.Time
          OnInsert = trigger.OnInsert
          OnUpdateFields = updateFields
          OnDelete = trigger.OnDelete
          Procedure = trigger.Procedure
          AllowBroken = trigger.AllowBroken }

    let resolveTriggersEntity (entity: ResolvedEntity) (entityTriggers: SourceTriggersEntity) : TriggersEntity =
        let mapTrigger name (trigger: SourceTrigger) =
            try
                try
                    checkName name
                    Ok <| resolveTrigger entity trigger
                with :? ResolveTriggersException as e when trigger.AllowBroken || forceAllowBroken ->
                    Error
                        { Error = e
                          AllowBroken = trigger.AllowBroken }
            with e ->
                raisefWithInner ResolveTriggersException e "In trigger %O" name

        { Triggers = entityTriggers.Triggers |> Map.map mapTrigger }

    let resolveTriggersSchema (schema: ResolvedSchema) (schemaTriggers: SourceTriggersSchema) : TriggersSchema =
        let mapEntity name entityTriggers =
            try
                let entity =
                    match Map.tryFind name schema.Entities with
                    | Some entity when not entity.IsHidden -> entity
                    | _ -> raisef ResolveTriggersException "Unknown entity name"

                resolveTriggersEntity entity entityTriggers
            with e ->
                raisefWithInner ResolveTriggersException e "In triggers entity %O" name

        { Entities = schemaTriggers.Entities |> Map.map mapEntity }

    let resolveTriggersDatabase (db: SourceTriggersDatabase) : TriggersDatabase =
        let mapSchema name schemaTriggers =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolveTriggersException "Unknown schema name"
                    | Some schema -> schema

                resolveTriggersSchema schema schemaTriggers
            with e ->
                raisefWithInner ResolveTriggersException e "In triggers schema %O" name

        { Schemas = db.Schemas |> Map.map mapSchema }

    let resolveTriggers (triggers: SourceTriggers) : ResolvedTriggers =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveTriggersException "Unknown schema name"

                resolveTriggersDatabase db
            with e ->
                raisefWithInner ResolveTriggersException e "In schema %O" name

        { Schemas = triggers.Schemas |> Map.map mapDatabase }

    member this.ResolveTriggers triggers = resolveTriggers triggers

let resolveTriggers (layout: Layout) (forceAllowBroken: bool) (source: SourceTriggers) : ResolvedTriggers =
    let phase1 = Phase1Resolver(layout, forceAllowBroken)
    phase1.ResolveTriggers source
