module OzmaDB.API.Entities

open System
open System.Threading.Tasks
open System.Collections.Generic
open System.Linq
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Connection
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Query
open OzmaDB.Layout.Types
open OzmaDB.Layout.Info
open OzmaDB.Triggers.Types
open OzmaDB.Triggers.Source
open OzmaDB.Triggers.Merge
open OzmaDB.Triggers.Run
open OzmaDB.Operations.Entity
open OzmaDB.Operations.Command
open OzmaDB.API.Types

let private insertEntryComments (ref : ResolvedEntityRef) (role : RoleType) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "insert into %O" ref
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private massInsertEntryComments (ref : ResolvedEntityRef) (role : RoleType) =
    let refStr = sprintf "mass insert into %O" ref
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; roleStr]

let private updateEntryComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) (arguments : LocalArgumentsMap) =
    let refStr = sprintf "update %O, id %O" ref key
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; argumentsStr; roleStr]

let private deleteEntryComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) =
    let refStr = sprintf "delete from %O, id %O" ref key
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; roleStr]

let private getRelatedEntriesComments (ref : ResolvedEntityRef) (role : RoleType) (key : RowKey) =
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
      Entries : List<RawArguments>
      StartIndex : int
    }

type private EarlyStopException<'a> (value : 'a) =
    inherit Exception()

    member this.Value = value

let convertRawRowKey (entity : ResolvedEntity) = function
    | RRKPrimary id -> RKPrimary id
    | RRKAlt (name, rawArgs) -> RKAlt (name, convertEntityArguments entity rawArgs)

let private usersEntityRef = { Schema = OzmaQLName "public"; Name = OzmaQLName "users" }

type EntitiesAPI (api : IOzmaDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger: ILogger<EntitiesAPI> = ctx.LoggerFactory.CreateLogger<EntitiesAPI>()
    let query = ctx.Transaction.Connection.Query

    let checkUsersQuota layout =
        task {
            match rctx.Quota.MaxUsers with
            | None -> return Ok ()
            | Some maxUsers ->
                let! currUsers = ctx.Transaction.System.Users.Where(fun user -> user.IsEnabled).CountAsync(ctx.CancellationToken)
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
                | :? ArgumentCheckException as e when e.IsUserException ->
                    logger.LogError(e, "Trigger {name} returned invalid arguments", ref)
                    return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEOperation (EOEExecution (UVEArgument e.Details))))
                | :? TriggerRunException as e when e.IsUserException ->
                    logger.LogError(e, "Exception in trigger {name}", ref)
                    return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException (fullUserMessage e, e.UserData)))
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
                        let str = fullUserMessage ex
                        return Error <| EETrigger (ref.Schema, ref.Name, EEException (str, ex.UserData))
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
                        let str = fullUserMessage ex
                        return Error <| BEError (EETrigger (trigger.Schema, trigger.Name, EEException (str, ex.UserData)))
            }

    let applyInsertTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunInsertTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyUpdateTriggerAfter (entityRef : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunUpdateTriggerAfter entityRef id args ctx.CancellationToken) entityRef trigger

    let applyDeleteTriggerAfter (entityRef : ResolvedEntityRef) () (trigger : MergedTrigger) : Task<Result<unit, EntityErrorInfo>> =
        runAfterTrigger (fun script -> script.RunDeleteTriggerAfter entityRef ctx.CancellationToken) entityRef trigger

    member this.GetEntityInfo (req : GetEntityInfoRequest) : Task<Result<SerializedEntity, EntityErrorInfo>> =
        wrapAPIResult rctx logger "getEntityInfo" req <| fun () -> task {
            match ctx.Layout.FindEntity req.Entity with
            | Some entity ->
                try
                    let res = getEntityInfo ctx.Layout ctx.Triggers (getReadRole rctx.User.Effective.Type) req.Entity
                    return Ok res
                with
                    | :? EntityOperationException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Getting entity info failed")
                        return Error (EEOperation ex.Details)
            | _ ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
        }

    member this.InsertEntries (req : InsertEntriesRequest) : Task<Result<InsertEntriesResponse, TransactionErrorInfo>> =
        wrapAPIError rctx logger "insertEntries" req <| fun () -> task {
            match ctx.Layout.FindEntity req.Entity with
            | None ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error { Inner = EEOperation (EOERequest err); Operation = 0 }
            | Some entity when entity.IsHidden ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error { Inner = EEOperation (EOERequest err); Operation = 0 }
            | Some entity when entity.IsFrozen ->
                let err = sprintf "Entity %O is frozen" req.Entity
                return Error { Inner = EEOperation (EOERequest err); Operation = 0 }
            | Some entity ->
                try
                    let beforeTriggers = findMergedTriggersInsert req.Entity TTBefore ctx.Triggers
                    let afterTriggers = findMergedTriggersInsert req.Entity TTAfter ctx.Triggers

                    let runSingleId (i, (rawArgs : RawArguments, rowArgs : LocalArgumentsMap)) : Task<Result<InsertEntryResponse, TransactionErrorInfo>> =
                        task {
                            match! Seq.foldResultTask (applyInsertTriggerBefore req.Entity entity) rowArgs beforeTriggers with
                            | Error (BEError e) -> return Error { Inner = e; Operation = i }
                            | Error (BECancelled ()) -> return Ok { Id = None }
                            | Ok args ->
                                let comments = insertEntryComments req.Entity rctx.User.Effective.Type args
                                let! newIds =
                                    insertEntries
                                        query
                                        rctx.GlobalArguments
                                        ctx.Layout
                                        (getWriteRole rctx.User.Effective.Type)
                                        req.Entity
                                        (Some comments)
                                        [args]
                                        ctx.CancellationToken
                                let newId = newIds.[0]
                                let req = { Entity = req.Entity; Fields = rawArgs } : InsertEntryRequest
                                let resp = { Id = Some newId } : InsertEntryResponse
                                do! logAPIResponse rctx "insertEntry" req resp
                                match! Seq.foldResultTask (applyInsertTriggerAfter req.Entity newId args) () afterTriggers with
                                | Error e -> return Error { Inner = e; Operation = i }
                                | Ok () -> return Ok resp
                        }

                    let convertOne (i, rowArgs) =
                        try
                            Ok <| convertEntityArguments entity rowArgs
                        with
                        | :? ArgumentCheckException as e when e.IsUserException ->
                            logger.LogError(e, "Invalid arguments for entity insert")
                            Error { Inner = EEOperation (EOEExecution (UVEArgument e.Details)); Operation = i }

                    match Seq.traverseResult convertOne (Seq.indexed req.Entries) with
                    | Error e -> return Error e
                    | Ok rowsArgs ->
                        let! ret =
                            task {
                                if not (Seq.isEmpty beforeTriggers && Seq.isEmpty afterTriggers) then
                                    let! ret = Seq.traverseResultTask runSingleId (Seq.indexed (Seq.zip req.Entries rowsArgs))
                                    if Result.isOk ret && entity.TriggersMigration then
                                        ctx.ScheduleMigration ()
                                    return Result.map Seq.toArray ret
                                else
                                    let comments = massInsertEntryComments req.Entity rctx.User.Effective.Type
                                    let cachedArgs = Seq.cache rowsArgs
                                    let! newIds =
                                        insertEntries
                                            query
                                            rctx.GlobalArguments
                                            ctx.Layout
                                            (getWriteRole rctx.User.Effective.Type)
                                            req.Entity
                                            (Some comments)
                                            cachedArgs
                                            ctx.CancellationToken
                                    let responses = Array.map (fun id -> { Id = Some id } : InsertEntryResponse) newIds
                                    for (reqArgs, resp) in Seq.zip req.Entries responses do
                                        let singleReq = { Entity = req.Entity; Fields = reqArgs } : InsertEntryRequest
                                        do! logAPIResponse rctx "insertEntry" singleReq resp
                                    return Ok responses
                            }
                        match ret with
                        | Error err -> return Error err
                        | Ok responses ->
                            if not <| Array.isEmpty responses then
                                if entity.TriggersMigration then
                                    ctx.ScheduleMigration ()
                                if req.Entity = usersEntityRef then
                                    scheduleCheckUsersQuota ()
                                scheduleCheckSizeQuota ()
                            return Ok { Entries = responses }
                with
                | :? EntityOperationException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to insert entry")
                    return Error { Inner = EEOperation e.Details; Operation = 0 }
        }

    member this.UpdateEntry (req : UpdateEntryRequest) : Task<Result<UpdateEntryResponse, EntityErrorInfo>> =
        wrapAPIError rctx logger "updateEntry" req <| fun () -> task {
            match ctx.Layout.FindEntity req.Entity with
            | None ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity when entity.IsHidden ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity when entity.IsFrozen ->
                let err = sprintf "Entity %O is frozen" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity ->
                try
                    let args = convertEntityArguments entity req.Fields
                    let key = convertRawRowKey entity req.Id
                    if Map.isEmpty args then
                        let! id =
                            resolveKey
                                query
                                rctx.GlobalArguments
                                ctx.Layout
                                (getWriteRole rctx.User.Effective.Type)
                                req.Entity
                                None
                                key
                                ctx.CancellationToken
                        return Ok { Id = id }
                    else
                        let beforeTriggers = findMergedTriggersUpdate req.Entity TTBefore (Map.keys args) ctx.Triggers
                        match! Seq.foldResultTask (applyUpdateTriggerBefore req.Entity entity) (key, args) beforeTriggers with
                        | Error (BEError e) -> return Error e
                        | Error (BECancelled id) ->
                            return Ok { Id = id }
                        | Ok (key, args) ->
                            let comments = updateEntryComments req.Entity rctx.User.Effective.Type key args
                            let! (id, subEntityRef) =
                                updateEntry
                                    query
                                    rctx.GlobalArguments
                                    ctx.Layout
                                    (getWriteRole rctx.User.Effective.Type)
                                    req.Entity
                                    key
                                    (Some comments)
                                    args
                                    ctx.CancellationToken
                            let resp = { Id = id } : UpdateEntryResponse
                            do! logAPIResponse rctx "updateEntry" req resp
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            if req.Entity = usersEntityRef && Map.containsKey "is_enabled" req.Fields then
                                scheduleCheckUsersQuota ()
                            let afterTriggers = findMergedTriggersUpdate req.Entity TTAfter (Map.keys args) ctx.Triggers
                            match! Seq.foldResultTask (applyUpdateTriggerAfter req.Entity id args) () afterTriggers with
                            | Ok () -> return Ok resp
                            | Error e -> return Error e
                with
                | :? ArgumentCheckException as e when e.IsUserException ->
                    logger.LogError(e, "Invalid arguments for entity update")
                    return Error (EEOperation (EOEExecution (UVEArgument e.Details)))
                | :? EntityOperationException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to update entry")
                    return Error (EEOperation e.Details)
        }

    member this.DeleteEntry (req : DeleteEntryRequest) : Task<Result<unit, EntityErrorInfo>> =
        wrapAPIError rctx logger "deleteEntry" req <| fun () -> task {
            match ctx.Layout.FindEntity req.Entity with
            | None ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity when entity.IsHidden ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity when entity.IsFrozen ->
                let err = sprintf "Entity %O is frozen" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity ->
                try
                    let key = convertRawRowKey entity req.Id
                    let beforeTriggers = findMergedTriggersDelete req.Entity TTBefore ctx.Triggers
                    match! Seq.foldResultTask (applyDeleteTriggerBefore req.Entity) key beforeTriggers with
                    | Error (BEError e) -> return Error e
                    | Error (BECancelled id) -> return Ok ()
                    | Ok key ->
                            let comments = deleteEntryComments req.Entity rctx.User.Effective.Type key
                            let! id =
                                deleteEntry
                                    query
                                    rctx.GlobalArguments
                                    ctx.Layout
                                    (getWriteRole rctx.User.Effective.Type)
                                    req.Entity
                                    key
                                    (Some comments)
                                    ctx.CancellationToken
                            let resp = { Id = id } : DeleteEntryResponse
                            do! logAPIResponse rctx "deleteEntry" req resp
                            if entity.TriggersMigration then
                                ctx.ScheduleMigration ()
                            let afterTriggers = findMergedTriggersDelete req.Entity TTAfter ctx.Triggers
                            match! Seq.foldResultTask (applyDeleteTriggerAfter req.Entity) () afterTriggers with
                            | Error e -> return Error e
                            | Ok () -> return Ok ()
                with
                | :? ArgumentCheckException as e when e.IsUserException ->
                    logger.LogError(e, "Invalid arguments for entity delete")
                    return Error (EEOperation (EOEExecution (UVEArgument e.Details)))
                | :? EntityOperationException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to delete entry")
                    return Error (EEOperation e.Details)
        }

    member this.GetRelatedEntries (req : GetRelatedEntriesRequest) : Task<Result<ReferencesTree, EntityErrorInfo>> =
        wrapAPIResult rctx logger "getRelatedEntries" req <| fun () -> task {
            match ctx.Layout.FindEntity(req.Entity) with
            | None ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity when entity.IsHidden ->
                let err = sprintf "Entity %O not found" req.Entity
                return Error (EEOperation (EOERequest err))
            | Some entity ->
                try
                    let key = convertRawRowKey entity req.Id
                    let comments = getRelatedEntriesComments req.Entity rctx.User.Effective.Type key
                    let! ret =
                        getRelatedEntries
                            query
                            rctx.GlobalArguments
                            ctx.Layout
                            (getReadRole rctx.User.Effective.Type)
                            (fun _ _ _ -> true)
                            req.Entity
                            key
                            (Some comments)
                            ctx.CancellationToken
                    return Ok ret
                with
                | :? EntityOperationException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to get related entities")
                    return Error (EEOperation e.Details)
        }

    member this.RecursiveDeleteEntry (req : DeleteEntryRequest) : Task<Result<ReferencesTree, EntityErrorInfo>> =
        task {
            match! this.GetRelatedEntries { Entity = req.Entity; Id = req.Id } with
            | Error e -> return Error e
            | Ok tree ->
                let deleteOne entityRef id =
                    unitTask {
                        match! this.DeleteEntry { Entity = entityRef; Id = RRKPrimary id } with
                        | Error e -> return raise <| EarlyStopException(e)
                        | Ok () -> ()
                    }
                try
                    let! deleted = iterDeleteReferences deleteOne tree
                    return Ok tree
                with
                | :? EarlyStopException<EntityErrorInfo> as e -> return Error e.Value
        }

    member this.RunCommand (req : CommandRequest) : Task<Result<unit, EntityErrorInfo>> =
        wrapUnitAPIResult rctx logger "runCommand" req <| fun () -> task {
            try
                let! cmd = ctx.GetAnonymousCommand rctx.IsPrivileged req.Command
                let comments = commandComments req.Command rctx.User.Effective.Type req.Args
                do!
                    executeCommand
                        query
                        ctx.Triggers
                        rctx.GlobalArguments
                        ctx.Layout
                        (getWriteRole rctx.User.Effective.Type)
                        cmd
                        (Some comments)
                        req.Args
                        ctx.CancellationToken
                for KeyValue(entityRef, usedEntity) in cmd.UsedDatabase do
                    let anyChildren (f : UsedEntity -> bool) =
                        usedEntity.Children |> Map.values |> Seq.exists f
                    let entity = ctx.Layout.FindEntity entityRef |> Option.get
                    if entity.TriggersMigration && anyChildren (fun ent -> ent.Insert || ent.Update || ent.Delete) then
                        ctx.ScheduleMigration ()
                    if entityRef = usersEntityRef && anyChildren (fun ent -> ent.Insert || ent.Update) then
                        scheduleCheckUsersQuota ()
                    if anyChildren (fun ent -> ent.Insert) then
                        scheduleCheckSizeQuota ()
                return Ok ()
            with
            | :? CommandException as e when e.IsUserException ->
                logger.LogError(e, "Command failed to execute")
                return Error (EECommand e.Details)
        }

    member this.RunTransaction (req : TransactionRequest) : Task<Result<TransactionResponse, TransactionErrorInfo>> =
        task {
            logAPIRequest rctx logger "runTransaction" req

            let inline runPendingInsert (pending : PendingMassInsert) =
                task {
                    match! this.InsertEntries { Entity = pending.Entity; Entries = pending.Entries } with
                    | Error e -> return Error { e with Operation = pending.StartIndex + e.Operation }
                    | Ok resp -> return resp.Entries |> Seq.map TRInsertEntity |> Ok
                }

            let mutable pendingInsert = None

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
                let inline checkAndRun (resultFn : 'a -> TransactionOpResponse) (opFn : unit -> Task<Result<'a, EntityErrorInfo>>) =
                    task {
                        match! checkPendingInsert () with
                        | Error e -> return Error e
                        | Ok prevRet ->
                            match! opFn () with
                            | Ok ret -> return Ok (Seq.append prevRet (Seq.singleton (resultFn ret)))
                            | Error e -> return Error { Operation = i; Inner = e }
                    }

                let inline checkAndRunConst result = checkAndRun (fun _ -> result)

                task {
                    match op with
                    | TInsertEntity insert ->
                        match pendingInsert with
                        | None ->
                            pendingInsert <- Some { Entity = insert.Entity; Entries = List(seq { insert.Fields }); StartIndex = i }
                            return Ok Seq.empty
                        | Some pending when pending.Entity = insert.Entity ->
                            pending.Entries.Add(insert.Fields)
                            return Ok Seq.empty
                        | Some pending ->
                            match! runPendingInsert pending with
                            | Ok rets ->
                                pendingInsert <- Some { Entity = insert.Entity; Entries = List(seq { insert.Fields }); StartIndex = i }
                                return Ok rets
                            | Error e -> return Error e
                    | TUpdateEntity update -> return! checkAndRun TRUpdateEntity (fun () -> this.UpdateEntry update)
                    | TDeleteEntity delete -> return! checkAndRunConst TRDeleteEntity (fun () -> this.DeleteEntry delete)
                    | TRecursiveDeleteEntity delete -> return! checkAndRun TRRecursiveDeleteEntity (fun () -> this.RecursiveDeleteEntry delete)
                    | TCommand cmd -> return! checkAndRunConst TRCommand (fun () -> this.RunCommand cmd)
                }

            match! req.Operations |> Seq.indexed |> Seq.traverseResultTask handleOne with
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
            logAPIRequest rctx logger "deferConstraints" emptyRequest
            try
                let! ret = ctx.Transaction.DeferConstraints ctx.CancellationToken func
                return Ok ret
            with
            | :? DeferredConstraintsException as e when e.IsUserException ->
                logger.LogError(e, "Deferred error")
                let details = convertQueryExecutionException ctx.Layout e.QueryException
                let resp = EEOperation (EOEExecution details)
                logAPIError rctx "deferConstraints" emptyRequest resp
                return Error resp
        }

    interface IEntitiesAPI with
        member this.GetEntityInfo req = this.GetEntityInfo req
        member this.InsertEntries req = this.InsertEntries req
        member this.UpdateEntry req = this.UpdateEntry req
        member this.DeleteEntry req = this.DeleteEntry req
        member this.GetRelatedEntries req = this.GetRelatedEntries req
        member this.RecursiveDeleteEntry req = this.RecursiveDeleteEntry req
        member this.RunCommand req = this.RunCommand req
        member this.RunTransaction req = this.RunTransaction req
        member this.DeferConstraints req = this.DeferConstraints req
