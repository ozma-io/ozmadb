module FunWithFlags.FunDB.API.SaveRestore

open System.IO
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json

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

    member this.SaveSchemas (names : SchemaName seq) : Task<Result<Map<SchemaName, SchemaDump>, SaveErrorInfo>> =
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
                    let! schemasSeq = names |> Seq.mapTask (fun name -> saveSchema ctx.Transaction.System name ctx.CancellationToken |> Task.map (fun x -> (name, x)))
                    return Ok (Map.ofSeq schemasSeq)
                with
                | :? SaveSchemaException as ex ->
                    match ex.Info with
                    | SENotFound -> return Error RSENotFound
        }

    member this.SaveZipSchemas (names : SchemaName seq) : Task<Result<Stream, SaveErrorInfo>> =
        task {
            match! this.SaveSchemas names with
            | Error e -> return Error e
            | Ok dump ->
                let stream = new MemoryStream()
                schemasToZipFile dump stream
                ignore <| stream.Seek(0L, SeekOrigin.Begin)
                return Ok (stream :> Stream)
        }

    member this.RestoreSchemas (dumps : Map<SchemaName, SchemaDump>) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            if not (isRootRole rctx.User.Type) then
                logger.LogError("Restore access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "restoreSchema"
                    event.Error <- "access_denied"
                )
                return Error RREAccessDenied
            else if not <| Set.isEmpty (Set.intersect (Map.keysSet dumps) (Map.keysSet ctx.Preload.Schemas)) then
                logger.LogError("Cannot restore preloaded schemas")
                rctx.WriteEvent (fun event ->
                    event.Type <- "restoreSchema"
                    event.Error <- "preloaded"
                )
                return Error RREPreloaded
            else
                try
                    let! modified = restoreSchemas ctx.Transaction.System dumps ctx.CancellationToken
                    if modified then
                        ctx.ScheduleMigration ()
                    rctx.WriteEventSync (fun event ->
                        event.Type <- "restoreSchemas"
                        event.Details <- JsonConvert.SerializeObject dumps
                    )
                    return Ok ()
                with
                | :? RestoreSchemaException as e -> return Error <| RREConsistency (exceptionString e)
        }

    member this.RestoreZipSchemas (stream : Stream) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            let maybeDumps =
                try
                    Ok <| schemasFromZipFile stream
                with
                | :? RestoreSchemaException as e -> Error (RREInvalidFormat <| exceptionString e)
            match maybeDumps with
            | Error e -> return Error e
            | Ok dumps ->
                match! this.RestoreSchemas dumps with
                | Ok () -> return Ok ()
                | Error e -> return Error e
        }

    interface ISaveRestoreAPI with
        member this.SaveSchemas names = this.SaveSchemas names
        member this.SaveZipSchemas names = this.SaveZipSchemas names
        member this.RestoreSchemas dumps = this.RestoreSchemas dumps
        member this.RestoreZipSchemas stream = this.RestoreZipSchemas stream