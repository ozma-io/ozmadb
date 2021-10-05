module FunWithFlags.FunDB.API.Entities

open System
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.Operations.Command
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Triggers

let private insertEntityComments (ref : ResolvedEntityRef) (role : RoleType) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "insert into %O" ref
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private updateEntityComments (ref : ResolvedEntityRef) (role : RoleType) (id : int) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "update %O, id %i" ref id
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private deleteEntityComments (ref : ResolvedEntityRef) (role : RoleType) (id : int) =
    let refStr = sprintf "delete from %O, id %i" ref id
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; roleStr]

let private commandComments (source : string) (role : RoleType) (arguments : RawArguments) =
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role when role.CanRead -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [source; argumentsStr; roleStr]

type private BeforeTriggerError =
    | BEError of EntityErrorInfo
    | BECancelled

type EntitiesAPI (api : IFunDBAPI) =
    let rctx = api.Request
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
                        let newArgs = convertEntityArguments entity rawArgs
                        return Ok newArgs
                with
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError("Trigger {} returned invalid arguments", ref)
                    let str = exceptionString ex
                    rctx.WriteEvent (fun event ->
                        event.Type <- "triggerError"
                        event.Error <- "arguments"
                        event.Details <- str
                    )
                    return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEArguments str))
                | :? TriggerRunException as ex when ex.IsUserException ->
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
                | :? TriggerRunException as ex when ex.IsUserException ->
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
                | :? TriggerRunException as ex when ex.IsUserException ->
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
                    let res = getEntityInfo ctx.Layout (getReadRole rctx.User.Effective.Type) entityRef entity
                    return Ok res
                with
                    | :? EntityDeniedException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Access denied")
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getEntityInfo"
                            event.SchemaName <- entityRef.Schema.ToString()
                            event.EntityName <- entityRef.Name.ToString()
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
                try
                    let args = convertEntityArguments entity rawArgs
                    let beforeTriggers = findMergedTriggersInsert entityRef TTBefore ctx.Triggers
                    match! Seq.foldResultTask (applyInsertTriggerBefore entityRef entity) args beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error BECancelled -> return Ok None
                    | Ok args ->
                        let comments = insertEntityComments entityRef rctx.User.Effective.Type args
                        let! newId = insertEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) entityRef (Some comments) args ctx.CancellationToken
                        do! rctx.WriteEventSync (fun event ->
                            event.Type <- "insertEntity"
                            event.SchemaName <- entityRef.Schema.ToString()
                            event.EntityName <- entityRef.Name.ToString()
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
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity insert")
                    let str = exceptionString ex
                    return Error (EEArguments str)
                | :? EntityArgumentsException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity insert")
                    let str = exceptionString ex
                    return Error (EEArguments str)
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to insert entry")
                    let str = exceptionString ex
                    return Error (EEExecution str)
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "insertEntity"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
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
                try
                    let args = convertEntityArguments entity rawArgs
                    let beforeTriggers = findMergedTriggersUpdate entityRef TTBefore (Map.keys args) ctx.Triggers
                    match! Seq.foldResultTask (applyUpdateTriggerBefore entityRef entity id) args beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error BECancelled -> return Ok ()
                    | Ok args ->
                        let comments = updateEntityComments entityRef rctx.User.Effective.Type id args
                        do! updateEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) entityRef id (Some comments) args ctx.CancellationToken
                        do! rctx.WriteEventSync (fun event ->
                            event.Type <- "updateEntity"
                            event.SchemaName <- entityRef.Schema.ToString()
                            event.EntityName <- entityRef.Name.ToString()
                            event.EntityId <- Nullable id
                            event.Details <- JsonConvert.SerializeObject args
                        )
                        if entity.TriggersMigration then
                            ctx.ScheduleMigration ()
                        let afterTriggers = findMergedTriggersUpdate entityRef TTAfter (Map.keys args) ctx.Triggers
                        return! Seq.foldResultTask (applyUpdateTriggerAfter entityRef id args) () afterTriggers
                with
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity update")
                    let str = exceptionString ex
                    return Error (EEArguments str)
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to update entry")
                    let str = exceptionString ex
                    return Error (EEExecution str)
                | :? EntityNotFoundException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Not found")
                    return Error EENotFound
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "updateEntity"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
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
                        let comments = deleteEntityComments entityRef rctx.User.Effective.Type id
                        do! deleteEntity query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) entityRef id (Some comments) ctx.CancellationToken
                        do! rctx.WriteEventSync (fun event ->
                            event.Type <- "deleteEntity"
                            event.SchemaName <- entityRef.Schema.ToString()
                            event.EntityName <- entityRef.Name.ToString()
                            event.EntityId <- Nullable id
                        )
                        if entity.TriggersMigration then
                            ctx.ScheduleMigration ()
                        let afterTriggers = findMergedTriggersDelete entityRef TTAfter ctx.Triggers
                        match! Seq.foldResultTask (applyDeleteTriggerAfter entityRef) () afterTriggers with
                        | Error e -> return Error e
                        | Ok () -> return Ok ()
                    with
                        | :? EntityExecutionException as ex when ex.IsUserException ->
                            logger.LogError(ex, "Failed to delete entry")
                            let str = exceptionString ex
                            return Error (EEExecution str)
                        | :? EntityNotFoundException as ex when ex.IsUserException ->
                            logger.LogError(ex, "Not found")
                            return Error EENotFound
                        | :? EntityDeniedException as ex when ex.IsUserException ->
                            logger.LogError(ex, "Access denied")
                            rctx.WriteEvent (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.Schema.ToString()
                                event.EntityName <- entityRef.Name.ToString()
                                event.EntityId <- Nullable id
                                event.Error <- "access_denied"
                                event.Details <- exceptionString ex
                            )
                            return Error EEAccessDenied
        }

    member this.RunCommand (rawCommand : string) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            try
                let! cmd = ctx.GetAnonymousCommand rctx.IsPrivileged rawCommand
                let comments = commandComments rawCommand rctx.User.Effective.Type rawArgs
                do! executeCommand query ctx.Triggers rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) cmd (Some comments) rawArgs ctx.CancellationToken
                return Ok ()
            with
            | :? CommandArgumentsException as ex when ex.IsUserException ->
                logger.LogError(ex, "Invalid arguments for command")
                let str = exceptionString ex
                return Error (EEArguments str)
            | :? CommandExecutionException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to update entry")
                let str = exceptionString ex
                return Error (EEExecution str)
            | :? CommandResolveException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to resolve command")
                let str = exceptionString ex
                return Error (EECompilation str)
            | :? CommandDeniedException as ex when ex.IsUserException ->
                logger.LogError(ex, "Access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "runCommand"
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
                        | TCommand (rawCmd, rawArgs) ->
                            Task.map (Result.map (fun _ -> TRCommand)) <| this.RunCommand rawCmd rawArgs
                    return Result.mapError (fun err -> { Operation = i; Details = err }) res
                }
            match! transaction.Operations |> Seq.indexed |> Seq.traverseResultTask handleOne with
            | Ok results ->
                let resultsArr = Array.ofSeq results
                logger.LogInformation("Executed {} operations in a transaction", resultsArr.Length)
                return Ok { Results = resultsArr }
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
        member this.RunCommand rawCommand rawArgs = this.RunCommand rawCommand rawArgs
        member this.RunTransaction transaction = this.RunTransaction transaction
        member this.ConstraintsDeferred = deferConstraintsDepth > 0
        member this.DeferConstraints func = this.DeferConstraints func
