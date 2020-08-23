module FunWithFlags.FunDB.API.SaveRestore

open System.IO
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.Operations.SaveRestore
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

let private isRootRole : RoleType -> bool = function
    | RTRoot -> true
    | RTRole _ -> false

type SaveRestoreAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<SaveRestoreAPI>()

    member this.SaveSchema (name : SchemaName) : Task<Result<SchemaDump, SaveErrorInfo>> =
        task {
            if not (isRootRole rctx.User.Type) then
                logger.LogError("Dump access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "saveSchema"
                    event.Error <- "access_denied"
                )
                return Error RSEAccessDenied
            else
                try
                    let! schema = saveSchema ctx.Transaction.System name ctx.CancellationToken
                    return Ok schema
                with
                | :? SaveSchemaException as ex ->
                    match ex.Info with
                    | SENotFound -> return Error RSENotFound
        }

    member this.SaveZipSchema (name : SchemaName) : Task<Result<Stream, SaveErrorInfo>> =
        task {
            match! this.SaveSchema name with
            | Error e -> return Error e
            | Ok dump ->
                let stream = new MemoryStream()
                schemasToZipFile (Map.singleton name dump) stream
                ignore <| stream.Seek(0L, SeekOrigin.Begin)
                return Ok (stream :> Stream)
        }

    member this.RestoreSchema (name : SchemaName) (dump : SchemaDump) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            if not (isRootRole rctx.User.Type) then
                logger.LogError("Restore access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "restoreSchema"
                    event.Error <- "access_denied"
                )
                return Error RREAccessDenied
            else if Map.containsKey name ctx.Preload.Schemas then
                logger.LogError("Cannot restore preloaded schemas")
                rctx.WriteEvent (fun event ->
                    event.Type <- "restoreSchema"
                    event.Error <- "preloaded"
                )
                return Error RREPreloaded
            else
                let! modified = restoreSchema ctx.Transaction.System name dump ctx.CancellationToken
                if modified then
                    ctx.ScheduleMigration ()
                rctx.WriteEventSync (fun event ->
                    event.Type <- "restoreSchema"
                    event.Details <- dump.ToString()
                )
                return Ok ()
        }

    member this.RestoreZipSchema (name : SchemaName) (stream : Stream) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            let maybeDumps =
                try
                    Ok <| schemasFromZipFile stream
                with
                | :? RestoreSchemaException as e -> Error (RREInvalidFormat <| exceptionString e)
            match Result.map (Map.toList) maybeDumps with
            | Error e -> return Error e
            | Ok [(dumpName, dump)] when name = dumpName -> return! this.RestoreSchema name dump
            | Ok _ -> return Error (RREInvalidFormat <| sprintf "Archive should only contain directory %O" name)
        }

    interface ISaveRestoreAPI with
        member this.SaveSchema name = this.SaveSchema name
        member this.SaveZipSchema name = this.SaveZipSchema name
        member this.RestoreSchema name dump = this.RestoreSchema name dump
        member this.RestoreZipSchema name stream = this.RestoreZipSchema name stream