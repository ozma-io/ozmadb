module FunWithFlags.FunDB.API.Entities

open System
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Triggers

let private convertEntityArguments (rawArgs : RawArguments) (entity : ResolvedEntity) : Result<EntityArguments, string> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind (fieldName.ToString()) rawArgs with
        | None -> Ok None
        | Some value ->
            match parseValueFromJson field.fieldType field.isNullable value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.fieldType fieldName
            | Some arg -> Ok (Some (fieldName, arg))
    match entity.columnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

let private getRole = function
    | RTRoot -> None
    | RTRole role -> Some role

type private BeforeTriggerError =
    | BEError of EntityErrorInfo
    | BECancelled

type EntitiesAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<EntitiesAPI>()
    let query = ctx.Transaction.Connection.Query

    let runArgsTrigger (run : ITriggerScript -> Task<ArgsTriggerResult>) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : EntityArguments) (trigger : MergedTrigger) : Task<Result<EntityArguments, BeforeTriggerError>> =
        let ref =
            { Schema = trigger.Schema
              Entity = entityRef
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
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- exceptionString ex
                        )
                        return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException (exceptionString ex)))
            }

    let runAfterTrigger (run : ITriggerScript -> Task) (entityRef : ResolvedEntityRef) (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        let ref =
            { Schema = trigger.Schema
              Entity = entityRef
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
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- exceptionString ex
                        )
                        return Error <| EETrigger (ref.Schema, ref.Name, EEException (exceptionString ex))
            }

    let applyInsertTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : EntityArguments) (trigger : MergedTrigger) : Task<Result<EntityArguments, BeforeTriggerError>> =
        runArgsTrigger (fun script -> script.RunInsertTriggerBefore entityRef args ctx.CancellationToken) entityRef entity args trigger

    let applyUpdateTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : int) (args : EntityArguments) (trigger : MergedTrigger) : Task<Result<EntityArguments, BeforeTriggerError>> =
        runArgsTrigger (fun script -> script.RunUpdateTriggerBefore entityRef id args ctx.CancellationToken) entityRef entity args trigger

    let applyDeleteTriggerBefore (entityRef : ResolvedEntityRef) (id : int) () (trigger : MergedTrigger) : Task<Result<unit, BeforeTriggerError>> =
        let ref =
            { Schema = trigger.Schema
              Entity = entityRef
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
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- exceptionString ex
                        )
                        return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException (exceptionString ex)))
            }

    let applyInsertTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : EntityArguments) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunInsertTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyUpdateTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : EntityArguments) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunUpdateTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyDeleteTriggerAfter (entityRef : ResolvedEntityRef) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunDeleteTriggerAfter entityRef ctx.CancellationToken) entityRef trigger

    member this.GetEntityInfo (entityRef : ResolvedEntityRef) : Task<Result<SerializedEntity, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | Some entity ->
                try
                    let res = getEntityInfo ctx.Layout (getRole rctx.User.Type) entityRef entity
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
                            let! newId = insertEntity query rctx.GlobalArguments ctx.Layout (getRole rctx.User.Type) entityRef args ctx.CancellationToken
                            rctx.WriteEventSync (fun event ->
                                event.Type <- "insertEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable newId
                                event.Details <- JsonConvert.SerializeObject args
                            )
                            if entity.triggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersInsert entityRef TTAfter ctx.Triggers
                            match! Seq.foldResultTask (applyInsertTriggerAfter entityRef newId args) () afterTriggers with
                            | Error e -> return Error e
                            | Ok () -> return Ok (Some newId)
                        with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to insert entry")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "insertEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.Error <- "execution"
                                event.Details <- exceptionString ex
                            )
                            return Error (EEExecution <| exceptionString ex)
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
                            do! updateEntity query rctx.GlobalArguments ctx.Layout (getRole rctx.User.Type) entityRef id args ctx.CancellationToken
                            rctx.WriteEventSync (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Details <- JsonConvert.SerializeObject args
                            )
                            if entity.triggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersUpdate entityRef TTAfter (Map.keys args) ctx.Triggers
                            return! Seq.foldResultTask (applyUpdateTriggerAfter entityRef id args) () afterTriggers
                        with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to update entry")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "execution"
                                event.Details <- exceptionString ex
                            )
                            return Error (EEExecution <| exceptionString ex)
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
            | Some entity ->
                let beforeTriggers = findMergedTriggersDelete entityRef TTBefore ctx.Triggers
                match! Seq.foldResultTask (applyDeleteTriggerBefore entityRef id) () beforeTriggers with
                | Error (BEError e) -> return Error e
                | Error BECancelled -> return Ok ()
                | Ok () ->
                    try
                        do! deleteEntity query rctx.GlobalArguments ctx.Layout (getRole rctx.User.Type) entityRef id ctx.CancellationToken
                        rctx.WriteEventSync (fun event ->
                            event.Type <- "deleteEntity"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.EntityId <- Nullable id
                        )
                        if entity.triggersMigration then
                            ctx.ScheduleMigration ()
                        let afterTriggers = findMergedTriggersDelete entityRef TTAfter ctx.Triggers
                        match! Seq.foldResultTask (applyDeleteTriggerAfter entityRef) () afterTriggers with
                        | Error e -> return Error e
                        | Ok () -> return Ok ()
                    with
                        | :? EntityExecutionException as ex ->
                            logger.LogError(ex, "Failed to delete entry")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.schema.ToString()
                                event.EntityName <- entityRef.name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "execution"
                                event.Details <- exceptionString ex
                            )
                            return Error (EEExecution <| exceptionString ex)
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
                    return Result.mapError (fun err -> (i, err)) res
                }
            match! transaction.Operations |> Seq.indexed |> Seq.traverseResultTask handleOne with
            | Ok results ->
                return Ok { Results = Array.ofSeq results }
            | Error (i, err) ->
                return Error { Error = err
                               Operation = i
                             }
        }

    interface IEntitiesAPI with
        member this.GetEntityInfo entityRef = this.GetEntityInfo entityRef
        member this.InsertEntity entityRef rawArgs = this.InsertEntity entityRef rawArgs
        member this.UpdateEntity entityRef id rawArgs = this.UpdateEntity entityRef id rawArgs
        member this.DeleteEntity entityRef id = this.DeleteEntity entityRef id
        member this.RunTransaction transaction = this.RunTransaction transaction