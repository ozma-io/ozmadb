module FunWithFlags.FunDB.API.Entities

open System
open System.Threading.Tasks
open System.Collections.Generic
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Triggers.Run
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.Operations.Command
open FunWithFlags.FunDB.API.Types

let private insertEntityComments (ref : ResolvedEntityRef) (role : RoleType) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "insert into %O" ref
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private massInsertEntityComments (ref : ResolvedEntityRef) (role : RoleType) =
    let refStr = sprintf "mass insert into %O" ref
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; roleStr]

let private updateEntityComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "update %O, id %O" ref key
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private deleteEntityComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) =
    let refStr = sprintf "delete from %O, id %O" ref key
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; roleStr]

let private getRelatedEntitiesComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) =
    let refStr = sprintf "getting related for %O, id %O" ref key
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

type private BeforeTriggerError<'c> =
    | BEError of EntityErrorInfo
    | BECancelled of 'c

type MassInsertRow =
    { Args : LocalArgumentsMap
      Id : int
    }

type PendingMassInsert =
    { Entity : ResolvedEntityRef
      Rows : List<RawArguments>
      StartIndex : int
    }

type private EarlyStopException<'a> (value : 'a) =
    inherit Exception()

    member this.Value = value

let convertRawRowKey (entity : ResolvedEntity) = function
    | RRKPrimary id -> RKPrimary id
    | RRKAlt (name, rawArgs) -> RKAlt (name, convertEntityArguments entity rawArgs)

let private usersEntityRef = { Schema = FunQLName "public"; Name = FunQLName "users" }

type EntitiesAPI (api : IFunDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<EntitiesAPI>()
    let query = ctx.Transaction.Connection.Query

    let checkUsersQuota layout =
        task {
            match rctx.Quota.MaxUsers with
            | None -> return Ok ()
            | Some maxUsers ->
                let! currUsers = ctx.Transaction.System.Users.CountAsync ctx.CancellationToken
                if currUsers > maxUsers then
                    return Error <| GEQuotaExceeded "Max number of users reached"
                else
                    return Ok ()
        }
    let scheduleCheckUsersQuota () =
        match rctx.Quota.MaxUsers with
        | None -> ()
        | Some maxUsers -> ctx.ScheduleBeforeCommit "check_max_users" checkUsersQuota

    let checkSizeQuota layout =
        task {
            match rctx.Quota.MaxSize with
            | None -> return Ok ()
            | Some maxSize ->
                let! currSize = getDatabaseSize ctx.Transaction.Connection.Query ctx.CancellationToken
                if currSize > int64 maxSize * 1024L * 1024L then
                    return Error <| GEQuotaExceeded "Max database size reached"
                else
                    return Ok ()
        }
    let scheduleCheckSizeQuota () =
        match rctx.Quota.MaxSize with
        | None -> ()
        | Some maxSize -> ctx.ScheduleBeforeCommit "check_max_size" checkSizeQuota

    let runArgsTrigger (run : TriggerScript -> Task<ArgsTriggerResult>) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<LocalArgumentsMap, BeforeTriggerError<unit>>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let preparedTrigger = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                try
                    match! run preparedTrigger.Script with
                    | ATCancelled -> return Error (BECancelled ())
                    | ATUntouched -> return Ok args
                    | ATTouched rawArgs ->
                        let newArgs = convertEntityArguments entity rawArgs
                        return Ok newArgs
                with
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Trigger {name} returned invalid arguments", ref)
                    let str = Exn.fullMessage ex
                    rctx.WriteEvent (fun event ->
                        event.Type <- "triggerError"
                        event.Error <- "arguments"
                        event.Details <- str
                    )
                    return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEArguments str))
                | :? TriggerRunException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Exception in trigger {name}", ref)
                    let str = Exn.fullMessage ex
                    rctx.WriteEvent (fun event ->
                        event.Type <- "triggerError"
                        event.Error <- "exception"
                        event.Details <- str
                    )
                    return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException str))
            }

    let runAfterTrigger (run : TriggerScript -> Task) (entityRef : ResolvedEntityRef) (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let preparedTrigger = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                try
                    do! run preparedTrigger.Script
                    return Ok ()
                with
                | :? TriggerRunException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Exception in trigger {name}", ref)
                        let str = Exn.fullMessage ex
                        rctx.WriteEvent (fun event ->
                            event.Type <- "triggerError"
                            event.Error <- "exception"
                            event.Details <- str
                        )
                        return Error <| EETrigger (ref.Schema, ref.Name, EEException str)
            }

    let applyInsertTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<LocalArgumentsMap, BeforeTriggerError<unit>>> =
        runArgsTrigger (fun script -> script.RunInsertTriggerBefore entityRef args ctx.CancellationToken) entityRef entity args trigger

    let applyUpdateTriggerBefore (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (key : RowKey, args : LocalArgumentsMap) (trigger : MergedTrigger) : Task<Result<RowKey * LocalArgumentsMap, BeforeTriggerError<RowId>>> =
        task {
            let! id = resolveKey query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) entityRef None key ctx.CancellationToken
            match! runArgsTrigger (fun script -> script.RunUpdateTriggerBefore entityRef id args ctx.CancellationToken) entityRef entity args trigger with
            | Ok args -> return Ok (RKPrimary id, args)
            | Error (BECancelled ()) -> return Error (BECancelled id)
            | Error (BEError e) -> return Error (BEError e)
        }

    let applyDeleteTriggerBefore (entityRef : ResolvedEntityRef) (key : RowKey) (trigger : MergedTrigger) : Task<Result<RowKey, BeforeTriggerError<RowId>>> =
        let ref =
            { Schema = trigger.Schema
              Entity = Option.defaultValue entityRef trigger.Inherited
              Name = trigger.Name
            }
        let preparedTrigger = ctx.FindTrigger ref |> Option.get
        rctx.RunWithSource (ESTrigger ref) <| fun () ->
            task {
                let! id = resolveKey query rctx.GlobalArguments ctx.Layout (getWriteRole rctx.User.Effective.Type) entityRef None key ctx.CancellationToken
                try
                    let! maybeContinue = preparedTrigger.Script.RunDeleteTriggerBefore entityRef id ctx.CancellationToken
                    if maybeContinue then
                        return Ok (RKPrimary id)
                    else
                        return Error (BECancelled id)
                with
                | :? TriggerRunException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Exception in trigger {name}", ref)
                        let str = Exn.fullMessage ex
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
            match ctx.Layout.FindEntity entityRef with
            | Some entity ->
                try
                    let res = getEntityInfo ctx.Layout ctx.Triggers (getReadRole rctx.User.Effective.Type) entityRef
                    return Ok res
                with
                    | :? EntityDeniedException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Access denied")
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getEntityInfo"
                            event.SchemaName <- entityRef.Schema.ToString()
                            event.EntityName <- entityRef.Name.ToString()
                            event.Error <- "access_denied"
                            event.Details <- Exn.fullMessage ex
                        )
                        return Error EEAccessDenied
            | _ -> return Error EENotFound
        }

    member this.InsertEntities (entityRef : ResolvedEntityRef) (rawRowsArgs : RawArguments seq) : Task<Result<(int option)[], TransactionError>> =
        task {
            match ctx.Layout.FindEntity entityRef with
            | None -> return Error { Details = EENotFound; Operation = 0 }
            | Some entity when entity.IsHidden -> return Error { Details = EENotFound; Operation = 0 }
            | Some entity when entity.IsFrozen -> return Error { Details = EEFrozen; Operation = 0 }
            | Some entity ->
                try
                    let beforeTriggers = findMergedTriggersInsert entityRef TTBefore ctx.Triggers
                    let afterTriggers = findMergedTriggersInsert entityRef TTAfter ctx.Triggers

                    let runSingleId (i, rowArgs : LocalArgumentsMap) =
                        task {
                            match! Seq.foldResultTask (applyInsertTriggerBefore entityRef entity) rowArgs beforeTriggers with
                            | Error (BEError e) -> return Error { Details = e; Operation = i }
                            | Error (BECancelled ()) -> return Ok None
                            | Ok args ->
                                let comments = insertEntityComments entityRef rctx.User.Effective.Type args
                                let! newIds =
                                    insertEntities
                                        query
                                        rctx.GlobalArguments
                                        ctx.Layout
                                        (getWriteRole rctx.User.Effective.Type)
                                        entityRef
                                        (Some comments)
                                        [args]
                                        ctx.CancellationToken
                                let newId = newIds.[0]
                                do! rctx.WriteEventSync (fun event ->
                                    event.Type <- "insertEntity"
                                    event.SchemaName <- entityRef.Schema.ToString()
                                    event.EntityName <- entityRef.Name.ToString()
                                    event.RowId <- Nullable newId
                                    event.Details <- JsonConvert.SerializeObject args
                                )
                                match! Seq.foldResultTask (applyInsertTriggerAfter entityRef newId args) () afterTriggers with
                                | Error e -> return Error { Details = e; Operation = i }
                                | Ok () -> return Ok (Some newId)
                        }

                    let convertOne (i, rowArgs) =
                        try
                            Ok <| convertEntityArguments entity rowArgs
                        with
                        | :? ArgumentCheckException as ex when ex.IsUserException ->
                            logger.LogError(ex, "Invalid arguments for entity insert")
                            let str = Exn.fullMessage ex
                            Error { Details = EEArguments str; Operation = i }

                    match Seq.traverseResult convertOne (Seq.indexed rawRowsArgs) with
                    | Error e -> return Error e
                    | Ok rowsArgs ->
                        let! ret =
                            task {
                                if not (Seq.isEmpty beforeTriggers && Seq.isEmpty afterTriggers) then
                                    let! ret = Seq.traverseResultTask runSingleId (Seq.indexed rowsArgs)
                                    if Result.isOk ret && entity.TriggersMigration then
                                        ctx.ScheduleMigration ()
                                    return Result.map Seq.toArray ret
                                else
                                    let comments = massInsertEntityComments entityRef rctx.User.Effective.Type
                                    let cachedArgs = Seq.cache rowsArgs
                                    let! newIds =
                                        insertEntities
                                            query
                                            rctx.GlobalArguments
                                            ctx.Layout
                                            (getWriteRole rctx.User.Effective.Type)
                                            entityRef
                                            (Some comments)
                                            cachedArgs
                                            ctx.CancellationToken
                                    let details = Seq.map2 (fun args id -> { Args = args; Id = id }) cachedArgs newIds
                                    do! rctx.WriteEventSync (fun event ->
                                        event.Type <- "insertEntities"
                                        event.SchemaName <- entityRef.Schema.ToString()
                                        event.EntityName <- entityRef.Name.ToString()
                                        event.Details <- JsonConvert.SerializeObject details
                                    )
                                    return Ok (Array.map Some newIds)
                            }
                        match ret with
                        | Error _ as e -> return e
                        | Ok info ->
                            if not <| Array.isEmpty info then
                                if entity.TriggersMigration then
                                    ctx.ScheduleMigration ()
                                if entityRef = usersEntityRef then
                                    scheduleCheckUsersQuota ()
                                scheduleCheckSizeQuota ()
                            return Ok info
                with
                | :? EntityArgumentsException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity insert")
                    let str = Exn.fullMessage ex
                    return Error { Details = EEArguments str; Operation = 0 }
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to insert entry")
                    let str = Exn.fullMessage ex
                    return Error { Details = EEExecution str; Operation = 0 }
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "insertEntity"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
                        event.Error <- "access_denied"
                        event.Details <- Exn.fullMessage ex
                    )
                    return Error { Details = EEAccessDenied; Operation = 0 }
        }

    member this.UpdateEntity (entityRef : ResolvedEntityRef) (rawKey : RawRowKey) (rawArgs : RawArguments) : Task<Result<RowId, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity entityRef with
            | None -> return Error EENotFound
            | Some entity when entity.IsHidden -> return Error EENotFound
            | Some entity when entity.IsFrozen -> return Error EEFrozen
            | Some entity ->
                try
                    let args = convertEntityArguments entity rawArgs
                    let key = convertRawRowKey entity rawKey
                    if Map.isEmpty args then
                        let! id =
                            resolveKey
                                query
                                rctx.GlobalArguments
                                ctx.Layout
                                (getWriteRole rctx.User.Effective.Type)
                                entityRef
                                None
                                key
                                ctx.CancellationToken
                        return Ok id
                    else
                        let beforeTriggers = findMergedTriggersUpdate entityRef TTBefore (Map.keys args) ctx.Triggers
                        match! Seq.foldResultTask (applyUpdateTriggerBefore entityRef entity) (key, args) beforeTriggers with
                        | Error (BEError e) -> return Error e
                        | Error (BECancelled id) -> return Ok id
                        | Ok (key, args) ->
                            let comments = updateEntityComments entityRef rctx.User.Effective.Type key args
                            let! (id, subEntityRef) =
                                updateEntity
                                    query
                                    rctx.GlobalArguments
                                    ctx.Layout
                                    (getWriteRole rctx.User.Effective.Type)
                                    entityRef
                                    key
                                    (Some comments)
                                    args
                                    ctx.CancellationToken
                            do! rctx.WriteEventSync (fun event ->
                                event.Type <- "updateEntity"
                                event.SchemaName <- entityRef.Schema.ToString()
                                event.EntityName <- entityRef.Name.ToString()
                                event.RowId <- Nullable id
                                event.Details <- JsonConvert.SerializeObject args
                            )
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersUpdate entityRef TTAfter (Map.keys args) ctx.Triggers
                            match! Seq.foldResultTask (applyUpdateTriggerAfter entityRef id args) () afterTriggers with
                            | Ok () -> return Ok id
                            | Error e -> return Error e
                with
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity update")
                    let str = Exn.fullMessage ex
                    return Error (EEArguments str)
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to update entry")
                    let str = Exn.fullMessage ex
                    return Error (EEExecution str)
                | :? EntityNotFoundException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Not found")
                    return Error EENotFound
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    let (entityId, detailsPrefix) =
                        match rawKey with
                        | RRKPrimary id -> (Nullable id, "")
                        | RRKAlt (name, args) -> (Nullable(), sprintf "Alternate key %O, args: %O\n" name args)
                    rctx.WriteEvent (fun event ->
                        event.Type <- "updateEntity"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
                        event.RowId <- entityId
                        event.Error <- "access_denied"
                        event.Details <- detailsPrefix + Exn.fullMessage ex
                    )
                    return Error EEAccessDenied
        }

    member this.DeleteEntity (entityRef : ResolvedEntityRef) (rawKey : RawRowKey) : Task<Result<unit, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity entityRef with
            | None -> return Error EENotFound
            | Some entity when entity.IsHidden -> return Error EENotFound
            | Some entity when entity.IsFrozen -> return Error EEFrozen
            | Some entity ->
                try
                    let key = convertRawRowKey entity rawKey
                    let beforeTriggers = findMergedTriggersDelete entityRef TTBefore ctx.Triggers
                    match! Seq.foldResultTask (applyDeleteTriggerBefore entityRef) key beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error (BECancelled id) -> return Ok ()
                    | Ok key ->
                            let comments = deleteEntityComments entityRef rctx.User.Effective.Type key
                            let! id =
                                deleteEntity
                                    query
                                    rctx.GlobalArguments
                                    ctx.Layout
                                    (getWriteRole rctx.User.Effective.Type)
                                    entityRef
                                    key
                                    (Some comments)
                                    ctx.CancellationToken
                            do! rctx.WriteEventSync (fun event ->
                                event.Type <- "deleteEntity"
                                event.SchemaName <- entityRef.Schema.ToString()
                                event.EntityName <- entityRef.Name.ToString()
                                event.RowId <- Nullable id
                            )
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersDelete entityRef TTAfter ctx.Triggers
                            match! Seq.foldResultTask (applyDeleteTriggerAfter entityRef) () afterTriggers with
                            | Error e -> return Error e
                            | Ok () -> return Ok ()
                with
                | :? ArgumentCheckException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Invalid arguments for entity delete")
                    let str = Exn.fullMessage ex
                    return Error (EEArguments str)
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to delete entry")
                    let str = Exn.fullMessage ex
                    return Error (EEExecution str)
                | :? EntityNotFoundException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Not found")
                    return Error EENotFound
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    let (entityId, detailsPrefix) =
                        match rawKey with
                        | RRKPrimary id -> (Nullable id, "")
                        | RRKAlt (name, args) -> (Nullable(), sprintf "Alternate key %O, args: %O\n" name args)
                    rctx.WriteEvent (fun event ->
                        event.Type <- "deleteEntity"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
                        event.RowId <- entityId
                        event.Error <- "access_denied"
                        event.Details <- detailsPrefix + Exn.fullMessage ex
                    )
                    return Error EEAccessDenied
        }

    member this.GetRelatedEntities (entityRef : ResolvedEntityRef) (rawKey : RawRowKey) : Task<Result<ReferencesTree, EntityErrorInfo>> =
        task {
            match ctx.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity when entity.IsHidden -> return Error EENotFound
            | Some entity ->
                try
                    let key = convertRawRowKey entity rawKey
                    let comments = getRelatedEntitiesComments entityRef rctx.User.Effective.Type key
                    let! ret =
                        getRelatedEntities
                            query
                            rctx.GlobalArguments
                            ctx.Layout
                            (getReadRole rctx.User.Effective.Type)
                            (fun _ _ _ -> true)
                            entityRef
                            key
                            (Some comments)
                            ctx.CancellationToken
                    return Ok ret
                with
                | :? EntityExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to get related entities")
                    let str = Exn.fullMessage ex
                    return Error (EEExecution str)
                | :? EntityNotFoundException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Not found")
                    return Error EENotFound
                | :? EntityDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    let (entityId, detailsPrefix) =
                        match rawKey with
                        | RRKPrimary id -> (Nullable id, "")
                        | RRKAlt (name, args) -> (Nullable(), sprintf "Alternate key %O, args: %O\n" name args)
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getRelatedEntities"
                        event.SchemaName <- entityRef.Schema.ToString()
                        event.EntityName <- entityRef.Name.ToString()
                        event.RowId <- entityId
                        event.Error <- "access_denied"
                        event.Details <- detailsPrefix + Exn.fullMessage ex
                    )
                    return Error EEAccessDenied
        }

    member this.RecursiveDeleteEntity (entityRef : ResolvedEntityRef) (rawKey : RawRowKey) : Task<Result<ReferencesTree, EntityErrorInfo>> =
        task {
            match! this.GetRelatedEntities entityRef rawKey with
            | Error e -> return Error e
            | Ok tree ->
                let deleteOne entityRef id =
                    unitTask {
                        match! this.DeleteEntity entityRef (RRKPrimary id) with
                        | Error e -> return raise <| EarlyStopException(e)
                        | Ok () -> ()
                    }
                try
                    let! deleted = iterDeleteReferences deleteOne tree
                    return Ok tree
                with
                | :? EarlyStopException<EntityErrorInfo> as e -> return Error e.Value
        }

    member this.RunCommand (rawCommand : string) (rawArgs : RawArguments) : Task<Result<unit, EntityErrorInfo>> =
        task {
            try
                let! cmd = ctx.GetAnonymousCommand rctx.IsPrivileged rawCommand
                let comments = commandComments rawCommand rctx.User.Effective.Type rawArgs
                do!
                    executeCommand
                        query
                        ctx.Triggers
                        rctx.GlobalArguments
                        ctx.Layout
                        (getWriteRole rctx.User.Effective.Type)
                        cmd
                        (Some comments)
                        rawArgs
                        ctx.CancellationToken
                for KeyValue(entityRef, usedEntity) in cmd.UsedDatabase do
                    let anyChildren (f : UsedEntity -> bool) =
                        usedEntity.Children |> Map.values |> Seq.exists f
                    let entity = ctx.Layout.FindEntity entityRef |> Option.get
                    if entity.TriggersMigration && anyChildren (fun ent -> ent.Insert || ent.Update || ent.Delete) then
                        ctx.ScheduleMigration ()
                    if anyChildren (fun ent -> ent.Insert) then
                        if entityRef = usersEntityRef then
                            scheduleCheckUsersQuota ()
                        scheduleCheckSizeQuota ()
                return Ok ()
            with
            | :? CommandArgumentsException as ex when ex.IsUserException ->
                logger.LogError(ex, "Invalid arguments for command")
                let str = Exn.fullMessage ex
                return Error (EEArguments str)
            | :? CommandExecutionException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to update entry")
                let str = Exn.fullMessage ex
                return Error (EEExecution str)
            | :? CommandResolveException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to resolve command")
                let str = Exn.fullMessage ex
                return Error (EECompilation str)
            | :? CommandDeniedException as ex when ex.IsUserException ->
                logger.LogError(ex, "Access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "runCommand"
                    event.Error <- "access_denied"
                    event.Details <- Exn.fullMessage ex
                )
                return Error EEAccessDenied
        }

    member this.RunTransaction (transaction : Transaction) : Task<Result<TransactionResult, TransactionError>> =
        task {
            let mutable pendingInsert = None

            let inline runPendingInsert (pending : PendingMassInsert) =
                task {
                    match! this.InsertEntities pending.Entity pending.Rows with
                    | Error e -> return Error { e with Operation = pending.StartIndex + e.Operation }
                    | Ok ids -> return ids |> Seq.map TRInsertEntity |> Ok
                }

            let inline checkPendingInsert () =
                task {
                    match pendingInsert with
                    | Some pending ->
                        let! ret = runPendingInsert pending
                        pendingInsert <- None
                        return ret
                    | None -> return Ok Seq.empty
                }

            let handleOne (i, op) =
                let inline checkAndRun (resultFn : 'a -> TransactionOpResult) (opFn : unit -> Task<Result<'a, EntityErrorInfo>>) =
                    task {
                        match! checkPendingInsert () with
                        | Error e -> return Error e
                        | Ok prevRet ->
                            match! opFn () with
                            | Ok ret -> return Ok (Seq.append prevRet (Seq.singleton (resultFn ret)))
                            | Error e -> return Error { Operation = i; Details = e }
                    }

                let inline checkAndRunConst result = checkAndRun (fun _ -> result)

                task {
                    match op with
                    | TInsertEntity (ref, entries) ->
                        match pendingInsert with
                        | None ->
                            pendingInsert <- Some { Entity = ref; Rows = List(seq { entries }); StartIndex = i }
                            return Ok Seq.empty
                        | Some pending when pending.Entity = ref ->
                            pending.Rows.Add(entries)
                            return Ok Seq.empty
                        | Some pending ->
                            match! runPendingInsert pending with
                            | Ok rets ->
                                pendingInsert <- Some { Entity = ref; Rows = List(seq { entries }); StartIndex = i }
                                return Ok rets
                            | Error e -> return Error e
                    | TUpdateEntity (ref, id, entries) -> return! checkAndRun TRUpdateEntity (fun () -> this.UpdateEntity ref id entries)
                    | TDeleteEntity (ref, id) -> return! checkAndRunConst TRDeleteEntity (fun () -> this.DeleteEntity ref id)
                    | TRecursiveDeleteEntity (ref, id) -> return! checkAndRun TRRecursiveDeleteEntity (fun () -> this.RecursiveDeleteEntity ref id)
                    | TCommand (rawCmd, rawArgs) -> return! checkAndRunConst TRCommand (fun () -> this.RunCommand rawCmd rawArgs)
                }

            match! transaction.Operations |> Seq.indexed |> Seq.traverseResultTask handleOne with
            | Ok resultLists ->
                match! checkPendingInsert () with
                | Ok newResults ->
                    let resultsArr = Array.ofSeq (Seq.append (Seq.concat resultLists) newResults)
                    logger.LogInformation("Executed {count} operations in a transaction", resultsArr.Length)
                    return Ok { Results = resultsArr }
                | Error e -> return Error e
            | Error e -> return Error e
        }

    member this.DeferConstraints (func : unit -> Task<'a>) : Task<Result<'a, EntityErrorInfo>> =
        task {
            try
                let! ret = ctx.Transaction.DeferConstraints ctx.CancellationToken func
                return Ok ret
            with
            | :? DeferredConstraintsException as ex ->
                logger.LogError(ex, "Deferred error")
                let str = Exn.fullMessage ex
                rctx.WriteEvent (fun event ->
                    event.Type <- "deferConstraints"
                    event.Error <- "execution"
                    event.Details <- str
                )
                return Error (EEExecution str)
        }

    interface IEntitiesAPI with
        member this.GetEntityInfo entityRef = this.GetEntityInfo entityRef
        member this.InsertEntities entityRef entityRowArgs = this.InsertEntities entityRef entityRowArgs
        member this.UpdateEntity entityRef id rawArgs = this.UpdateEntity entityRef id rawArgs
        member this.DeleteEntity entityRef id = this.DeleteEntity entityRef id
        member this.GetRelatedEntities entityRef id = this.GetRelatedEntities entityRef id
        member this.RecursiveDeleteEntity entityRef id = this.RecursiveDeleteEntity entityRef id
        member this.RunCommand rawCommand rawArgs = this.RunCommand rawCommand rawArgs
        member this.RunTransaction transaction = this.RunTransaction transaction
        member this.DeferConstraints func = this.DeferConstraints func
