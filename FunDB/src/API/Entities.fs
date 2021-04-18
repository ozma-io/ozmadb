module FunWithFlags.FunDB.API.Entities

open System
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Triggers

let private convertEntityArguments (rawArgs : RawArguments) (entity : ResolvedEntity) : Result<LocalArgumentsMap, string> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind (fieldName.ToString()) rawArgs with
        | None -> Ok None
        | Some value ->
            match parseValueFromJson field.FieldType field.IsNullable value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.FieldType fieldName
            | Some arg -> Ok (Some (fieldName, arg))
    match entity.ColumnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

type private BeforeTriggerError =
    | BEError of EntityErrorInfo
    | BECancelled

type EntitiesAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<EntitiesAPI>()
    let query = ctx.Transaction.Connection.Query
    // When this is greater than zero, constraints should remain deferred.
    // Increased with nested requests for deferring.
    let mutable deferConstraintsDepth = 0

    let runArgsTrigger (run : ITriggerScript -> Task<ArgsTriggerResult>) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<LocalArgumentsMap, BeforeTriggerError>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let script = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                try
                    match! run script with
                    | ATCancelled -> return Error BECancelled
                    | ATUntouched -> return Ok args
                    | ATTouched rawArgs ->
                        match convertEntityArguments rawArgs entity with
                        | Error str ->
                            logger.LogError("Trigger {} returned invalid arguments", ref)
                            rctx.WriteEvent (fun event ->
                                event.Type <- "triggerError"
                                event.Error <- "arguments"
                                event.Details <- str
                            )
                            return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEArguments str))
                        | Ok newArgs -> return Ok newArgs
                with
                | :? TriggerRunException as ex ->
                        logger.LogError(ex, "Exception in trigger {}", ref)
                        let str = exceptionString ex
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- str
                        )
                        return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException str))
            }

    let runAfterTrigger (run : ITriggerScript -> Task) (entityRef : ResolvedEntityRef) (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let script = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                try
                    do! run script
                    return Ok ()
                with
                | :? TriggerRunException as ex ->
                        logger.LogError(ex, "Exception in trigger {}", ref)
                        let str = exceptionString ex
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- str
                        )
                        return Error <| EETrigger (ref.Schema, ref.Name, EEException str)
            }

    let applyInsertTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<LocalArgumentsMap, BeforeTriggerError>> =
        runArgsTrigger (fun script -> script.RunInsertTriggerBefore entityRef args ctx.CancellationToken) entityRef entity args trigger

    let applyUpdateTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : int) (args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<LocalArgumentsMap, BeforeTriggerError>> =
        runArgsTrigger (fun script -> script.RunUpdateTriggerBefore entityRef id args ctx.CancellationToken) entityRef entity args trigger

    let applyDeleteTriggerBefore (entityRef : ResolvedEntityRef) (id : int) () (trigger : MergedTrigger) : Task<Result<unit, BeforeTriggerError>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let script = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                try
                    let! maybeContinue = script.RunDeleteTriggerBefore entityRef id ctx.CancellationToken
                    if maybeContinue then
                        return Ok ()
                    else
                        return Error BECancelled
                with
                | :? TriggerRunException as ex ->
                        logger.LogError(ex, "Exception in trigger {}", ref)
                        let str = exceptionString ex
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- str
                        )
                        return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException str))
            }

    let applyInsertTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunInsertTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyUpdateTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunUpdateTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyDeleteTriggerAfter (entityRef : ResolvedEntityRef) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunDeleteTriggerAfter entityRef ctx.CancellationToken) entityRef trigger

    member this.GetEntityInfo (entityRef : ResolvedEntityRef) : Task<Result<SerializedEntity, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | Some entity ->
                try
                    let res = getEntityInfo ctx.Layout (getReadRole rctx.User.Type) entityRef entity
                    return Ok res
                with
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getEntityInfo"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.Error <- "access_denied"
                            event.Details <- exceptionString ex
                        )
                        return Error EEAccessDenied
            | _ -> return Error EENotFound
        }

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Task<Result<int option, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity when entity.IsFrozen -> return Error EEFrozen
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args ->
                    let beforeTriggers = findMergedTriggersInsert entityRef TTBefore ctx.Triggers
                    match! Seq.foldResultTask (applyInsertTriggerBefore entityRef entity) args beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error BECancelled -> return Ok None
                    | Ok args ->
                        try
                            let! newId = insertEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Type) entityRef args ctx.CancellationToken
                            rctx.WriteEventSync (fun event ->
                                event.Type <- "insertEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable newId
                                event.Details <- JsonConvert.SerializeObject args
                            )
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersInsert entityRef TTAfter ctx.Triggers
                            match! Seq.foldResultTask (applyInsertTriggerAfter entityRef newId args) () afterTriggers with
                            | Error e -> return Error e
                            | Ok () -> return Ok (Some newId)
                        with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to insert entry")
                            let str = exceptionString ex
                            rctx.WriteEvent (fun event ->
                                event.Type <- "insertEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.Error <- "execution"
                                event.Details <- str
                            )
                            return Error (EEExecution str)
                        | :? EntityDeniedException as ex ->
                            logger.LogError(ex, "Access denied")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "insertEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.Error <- "access_denied"
                                event.Details <- exceptionString ex
                            )
                            return Error EEAccessDenied
        }

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (id : int) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity when entity.IsFrozen -> return Error EEFrozen
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args when Map.isEmpty args -> return Ok ()
                | Ok args ->
                    let beforeTriggers = findMergedTriggersUpdate entityRef TTBefore (Map.keys args) ctx.Triggers
                    match! Seq.foldResultTask (applyUpdateTriggerBefore entityRef entity id) args beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error BECancelled -> return Ok ()
                    | Ok args ->
                        try
                            do! updateEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Type) entityRef id args ctx.CancellationToken
                            rctx.WriteEventSync (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Details <- JsonConvert.SerializeObject args
                            )
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersUpdate entityRef TTAfter (Map.keys args) ctx.Triggers
                            return! Seq.foldResultTask (applyUpdateTriggerAfter entityRef id args) () afterTriggers
                        with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to update entry")
                            let str = exceptionString ex
                            rctx.WriteEvent (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "execution"
                                event.Details <- str
                            )
                            return Error (EEExecution str)
                        | :? EntityNotFoundException as ex ->
                            logger.LogError(ex, "Not found")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "not_found"
                                event.Details <- exceptionString ex
                            )
                            return Error EENotFound
                        | :? EntityDeniedException as ex ->
                            logger.LogError(ex, "Access denied")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "access_denied"
                                event.Details <- exceptionString ex
                            )
                            return Error EEAccessDenied
        }

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (id : int) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity when entity.IsFrozen -> return Error EEFrozen
            | Some entity ->
                let beforeTriggers = findMergedTriggersDelete entityRef TTBefore ctx.Triggers
                match! Seq.foldResultTask (applyDeleteTriggerBefore entityRef id) () beforeTriggers with
                | Error (BEError e) -> return Error e
                | Error BECancelled -> return Ok ()
                | Ok () ->
                    try
                        do! deleteEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Type) entityRef id ctx.CancellationToken
                        rctx.WriteEventSync (fun event ->
                            event.Type <- "deleteEntity"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.EntityId <- Nullable id
                        )
                        if entity.TriggersMigration then
                            ctx.ScheduleMigration ()
                        let afterTriggers = findMergedTriggersDelete entityRef TTAfter ctx.Triggers
                        match! Seq.foldResultTask (applyDeleteTriggerAfter entityRef) () afterTriggers with
                        | Error e -> return Error e
                        | Ok () -> return Ok ()
                    with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to delete entry")
                            let str = exceptionString ex
                            rctx.WriteEvent (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "execution"
                                event.Details <- str
                            )
                            return Error (EEExecution str)
                        | :? EntityNotFoundException as ex ->
                            logger.LogError(ex, "Not found")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "not_found"
                                event.Details <- exceptionString ex
                            )
                            return Error EENotFound
                        | :? EntityDeniedException as ex ->
                            logger.LogError(ex, "Access denied")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "access_denied"
                                event.Details <- exceptionString ex
                            )
                            return Error EEAccessDenied
        }

    member this.RunTransaction (transaction : Transaction) : Task<Result<TransactionResult, TransactionError>> =
        task {
            let handleOne (i, op) =
                task {
                    let! res =
                        match op with
                        | TInsertEntity (ref, entries) ->
                            Task.map (Result.map TRInsertEntity) <| this.InsertEntity ref entries
                        | TUpdateEntity (ref, id, entries) ->
                            Task.map (Result.map (fun _ -> TRUpdateEntity)) <| this.UpdateEntity ref id entries
                        | TDeleteEntity (ref, id) ->
                            Task.map (Result.map (fun _ -> TRDeleteEntity)) <| this.DeleteEntity ref id
                    return Result.mapError (fun err -> { Operation = i; Details = err }) res
                }
            match! transaction.Operations |> Seq.indexed |> Seq.traverseResultTask handleOne with
            | Ok results ->
                return Ok { Results = Array.ofSeq results }
            | Error e ->
                return Error e
        }

    member this.DeferConstraints (func : unit -> Task<'a>) : Task<Result<'a, EntityErrorInfo>> =
        task {
            if deferConstraintsDepth = 0 then
                let! _ = query.ExecuteNonQuery "SET CONSTRAINTS ALL DEFERRED" Map.empty ctx.CancellationToken
                ()
            deferConstraintsDepth <- deferConstraintsDepth + 1
            let! ret =
                task {
                    try
                        return! func ()
                    with
                    | ex ->
                        logger.LogError(ex, "Error while having constraints deferred")
                        deferConstraintsDepth <- deferConstraintsDepth - 1
                        try
                            let! _ = query.ExecuteNonQuery "SET CONSTRAINTS ALL IMMEDIATE" Map.empty ctx.CancellationToken
                            ()
                        with
                        | ex  -> logger.LogError(ex, "Error while setting constraints immediate when throwing")
                        return reraise' ex
                }
            deferConstraintsDepth <- deferConstraintsDepth - 1
            if deferConstraintsDepth = 0 then
                try
                    let! _ = query.ExecuteNonQuery "SET CONSTRAINTS ALL IMMEDIATE" Map.empty ctx.CancellationToken
                    return Ok ret
                with
                | :? QueryException as ex ->
                    logger.LogError(ex, "Deferred error")
                    let str = exceptionString ex
                    rctx.WriteEvent (fun event ->
                        event.Type <- "deferConstraints"
                        event.Error <- "execution"
                        event.Details <- str
                    )
                    return Error (EEExecution str)
            else
                return Ok ret
        }

    interface IEntitiesAPI with
        member this.GetEntityInfo entityRef = this.GetEntityInfo entityRef
        member this.InsertEntity entityRef rawArgs = this.InsertEntity entityRef rawArgs
        member this.UpdateEntity entityRef id rawArgs = this.UpdateEntity entityRef id rawArgs
        member this.DeleteEntity entityRef id = this.DeleteEntity entityRef id
        member this.RunTransaction transaction = this.RunTransaction transaction
        member this.ConstraintsDeferred = deferConstraintsDepth > 0
        member this.DeferConstraints func = this.DeferConstraints func
