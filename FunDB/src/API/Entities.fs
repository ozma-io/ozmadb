module FunWithFlags.FunDB.API.Entities

open System
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.API.Types

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

type EntitiesAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<EntitiesAPI>()
    let query = ctx.Transaction.Connection.Query

    member this.GetEntityInfo (entityRef : ResolvedEntityRef) : Task<Result<SerializedEntity, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                try
                    let res = getEntityInfo ctx.State.Layout (getRole rctx.User.Type) entityRef entity
                    return Ok res
                with
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        do! rctx.WriteEvent (fun event ->
                            event.Type <- "getEntityInfo"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.Error <- "access_denied"
                            event.Details <- exceptionString ex
                        )
                        return Error EEAccessDenied
        }

    member this.InsertEntity (entityRef : ResolvedEntityRef) (rawArgs : RawArguments) : Task<Result<int, EntityErrorInfo>> =
        task {
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args ->
                    try
                        let! newId = insertEntity query rctx.GlobalArguments ctx.State.Layout (getRole rctx.User.Type) entityRef args ctx.CancellationToken
                        rctx.WriteEventSync (fun event ->
                            event.Type <- "insertEntity"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.EntityId <- Nullable newId
                            event.Details <- args.ToString()
                        )
                        return Ok newId
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to insert entry")
                        do! rctx.WriteEvent (fun event ->
                            event.Type <- "insertEntity"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.Error <- "execution"
                            event.Details <- exceptionString ex
                        )
                        return Error (EEExecution <| exceptionString ex)
                    | :? EntityDeniedException as ex ->
                        logger.LogError(ex, "Access denied")
                        do! rctx.WriteEvent (fun event ->
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
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                match convertEntityArguments rawArgs entity with
                | Error str -> return Error <| EEArguments str
                | Ok args when Map.isEmpty args -> return Ok ()
                | Ok args ->
                    try
                        do! updateEntity query rctx.GlobalArguments ctx.State.Layout (getRole rctx.User.Type) entityRef id args ctx.CancellationToken
                        rctx.WriteEventSync (fun event ->
                            event.Type <- "updateEntity"
                            event.SchemaName <- entityRef.schema.ToString()
                            event.EntityName <- entityRef.name.ToString()
                            event.EntityId <- Nullable id
                            event.Details <- args.ToString()
                        )
                        return Ok ()
                    with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to update entry")
                        do! rctx.WriteEvent (fun event ->
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
                        do! rctx.WriteEvent (fun event ->
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
                        do! rctx.WriteEvent (fun event ->
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
            match ctx.State.Layout.FindEntity(entityRef) with
            | None -> return Error EENotFound
            | Some entity ->
                try
                    do! deleteEntity query rctx.GlobalArguments ctx.State.Layout (getRole rctx.User.Type) entityRef id ctx.CancellationToken
                    rctx.WriteEventSync (fun event ->
                        event.Type <- "deleteEntity"
                        event.SchemaName <- entityRef.schema.ToString()
                        event.EntityName <- entityRef.name.ToString()
                        event.EntityId <- Nullable id
                    )
                    return Ok ()
                with
                    | :? EntityExecutionException as ex ->
                        logger.LogError(ex, "Failed to delete entry")
                        do! rctx.WriteEvent (fun event ->
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
                        do! rctx.WriteEvent (fun event ->
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
                        do! rctx.WriteEvent (fun event ->
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