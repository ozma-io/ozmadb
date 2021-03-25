module FunWithFlags.FunDB.API.SaveRestore

open System.IO
open System.Linq
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
            match parseValueFromJson field.FieldType field.IsNullable value with
            | None -> Error <| sprintf "Cannot convert field to expected type %O: %O" field.FieldType fieldName
            | Some arg -> Ok (Some (fieldName, arg))
    match entity.ColumnFields |> Map.toSeq |> Seq.traverseResult getValue with
    | Ok res -> res |> Seq.mapMaybe id |> Map.ofSeq |> Ok
    | Error err -> Error err

let private canSave : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

let private canRestore : RoleType -> bool = function
    | RTRoot -> true
    | RTRole _ -> false

type SaveRestoreAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<SaveRestoreAPI>()

    member this.SaveSchemas (schemas : SaveSchemas) : Task<Result<Map<SchemaName, SchemaDump>, SaveErrorInfo>> =
        task {
            let names =
                match schemas with
                | SSNames names -> Array.toSeq names
                | SSAll -> Map.keys ctx.Layout.Schemas
                | SSNonPreloaded ->
                    let preloadSchemas = Map.keysSet ctx.Preload.Schemas
                    let allSchemas = Map.keysSet ctx.Layout.Schemas
                    Set.toSeq (Set.difference allSchemas preloadSchemas)
            if not (canSave rctx.User.Type) then
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

    member this.SaveZipSchemas (schemas : SaveSchemas) : Task<Result<Stream, SaveErrorInfo>> =
        task {
            match! this.SaveSchemas schemas with
            | Error e -> return Error e
            | Ok dump ->
                let stream = new MemoryStream()
                schemasToZipFile dump stream
                ignore <| stream.Seek(0L, SeekOrigin.Begin)
                return Ok (stream :> Stream)
        }

    // Be careful when introducing this API to JavaScript! `RestoreSchemas` uses `DEFER CONSTRAINTS`,
    // but Entities API has its own `DEFER CONSTRAINTS` management.
    member this.RestoreSchemas (dumps : Map<SchemaName, SchemaDump>) (dropOthers : bool) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            if not (canRestore rctx.User.Type) then
                logger.LogError("Restore access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "restoreSchema"
                    event.Error <- "access_denied"
                )
                return Error RREAccessDenied
            else
                let restoredSchemas = Map.keysSet dumps
                let preloadSchemas = Map.keysSet ctx.Preload.Schemas
                if not <| Set.isEmpty (Set.intersect restoredSchemas preloadSchemas) then
                    logger.LogError("Cannot restore preloaded schemas")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "restoreSchema"
                        event.Error <- "preloaded"
                    )
                    return Error RREPreloaded
                else
                    try
                        let droppedSchemas =
                            if not dropOthers then
                                Set.empty
                            else
                                let emptySchemas = Set.difference (Map.keysSet ctx.Layout.Schemas) preloadSchemas
                                Set.difference emptySchemas restoredSchemas
                        let dumps =
                            let emptyDumps = droppedSchemas |> Seq.map (fun name -> (name, emptySchemaDump)) |> Map.ofSeq
                            Map.union emptyDumps dumps
                        let! modified = restoreSchemas ctx.Transaction.System dumps ctx.CancellationToken
                        if not (Set.isEmpty droppedSchemas) then
                            let droppedSchemasArr = droppedSchemas |> Seq.map string |> Array.ofSeq
                            let! _ = ctx.Transaction.System.Schemas.AsQueryable().Where(fun entity -> droppedSchemasArr.Contains(entity.Name)).DeleteFromQueryAsync()
                            ()
                        if modified || not (Set.isEmpty droppedSchemas) then
                            ctx.ScheduleMigration ()
                        rctx.WriteEventSync (fun event ->
                            event.Type <- "restoreSchemas"
                            event.Details <- sprintf "{\"dropOthers\":%b,\"dumps\":%s}" dropOthers (JsonConvert.SerializeObject dumps)
                        )
                        return Ok ()
                    with
                    | :? RestoreSchemaException as e -> return Error <| RREConsistency (exceptionString e)
        }

    member this.RestoreZipSchemas (stream : Stream) (dropOthers : bool) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            let maybeDumps =
                try
                    Ok <| schemasFromZipFile stream
                with
                | :? RestoreSchemaException as e -> Error (RREInvalidFormat <| exceptionString e)
            match maybeDumps with
            | Error e -> return Error e
            | Ok dumps ->
                match! this.RestoreSchemas dumps dropOthers with
                | Ok () -> return Ok ()
                | Error e -> return Error e
        }

    interface ISaveRestoreAPI with
        member this.SaveSchemas names = this.SaveSchemas names
        member this.SaveZipSchemas names = this.SaveZipSchemas names
        member this.RestoreSchemas dumps dropOthers = this.RestoreSchemas dumps dropOthers
        member this.RestoreZipSchemas stream dropOthers = this.RestoreZipSchemas stream dropOthers