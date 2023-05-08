module FunWithFlags.FunDB.API.SaveRestore

open System.IO
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.Operations.SaveRestore
open FunWithFlags.FunDB.API.Types

let private canSave : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

let private canRestore : RoleType -> bool = function
    | RTRoot -> true
    | RTRole _ -> false

type SaveRestoreAPI (api : IFunDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<SaveRestoreAPI>()

    let mutable newCustomEntities = None
    let updateCustomEntities (layout : Layout) =
        task {
            match newCustomEntities with
            | None -> return Ok ()
            | Some customEntities ->
                try
                    do! restoreCustomEntities layout ctx.Transaction customEntities ctx.CancellationToken
                    return Ok ()
                with
                | :? RestoreSchemaException as e ->
                    let msg = sprintf "Error during restoring custom entities: %s" (fullUserMessage e)
                    return Error <| GECommit (RRERequest msg)
        }

    let updateCustomEntities newEnts =
        newCustomEntities <- Some newEnts
        ctx.ScheduleBeforeCommit "update_custom_entities" updateCustomEntities

    member this.SaveSchemas (req : SaveSchemasRequest) : Task<Result<SaveSchemasResponse, SaveErrorInfo>> =
        wrapAPIResult rctx "saveSchemas" req <| task {
            let names =
                match req.Schemas with
                | SSNames names -> Array.toSeq names
                | SSAll -> Map.keys ctx.Layout.Schemas
                | SSNonPreloaded ->
                    let preloadSchemas = Map.keysSet ctx.Preload.Schemas
                    let allSchemas = Map.keysSet ctx.Layout.Schemas
                    Set.toSeq (Set.difference allSchemas preloadSchemas)
            if not (canSave rctx.User.Effective.Type) then
                return Error RSEAccessDenied
            else
                let runOne results name =
                    task {
                        if not <| Map.containsKey name ctx.Layout.Schemas then
                            let msg = sprintf "Schema %O is not found" name
                            return Error (RSERequest msg)
                        else
                            let! schema = saveSchema ctx.Transaction ctx.Layout name ctx.CancellationToken
                            return Ok <| Map.add name schema results
                    }
                let! result = names |> Seq.foldResultTask runOne Map.empty
                return Result.map (fun schemas -> { Schemas = schemas }) result
        }

    member this.SaveZipSchemas (req : SaveSchemasRequest) : Task<Result<Stream, SaveErrorInfo>> =
        task {
            match! this.SaveSchemas req with
            | Error e -> return Error e
            | Ok dump ->
                // Used as a buffer because the ZipArchive API is synchronous.
                let stream = new MemoryStream()
                schemasToZipFile dump.Schemas stream
                ignore <| stream.Seek(0L, SeekOrigin.Begin)
                return Ok (stream :> Stream)
        }

    member this.RestoreSchemas (req : RestoreSchemasRequest) : Task<Result<unit, RestoreErrorInfo>> =
        wrapUnitAPIResult rctx "restoreSchemas" req <| task {
            let flags = Option.defaultValue emptyRestoreSchemasFlags req.Flags
            if not (canRestore rctx.User.Effective.Type) then
                return Error RREAccessDenied
            else
                let restoredSchemas = Map.keysSet req.Schemas
                let preloadSchemas = Map.keysSet ctx.Preload.Schemas
                if not <| Set.isEmpty (Set.intersect restoredSchemas preloadSchemas) then
                    return Error (RRERequest "Preloaded schemas cannot be restored")
                else
                    try
                        let droppedSchemas =
                            if not flags.DropOthers then
                                Set.empty
                            else
                                let emptySchemas = Set.difference (Map.keysSet ctx.Layout.Schemas) preloadSchemas
                                Set.difference emptySchemas restoredSchemas
                        let! modified = restoreSchemas ctx.Transaction ctx.Layout req.Schemas ctx.CancellationToken
                        do! deleteSchemas ctx.Layout ctx.Transaction droppedSchemas ctx.CancellationToken
                        if modified || not (Set.isEmpty droppedSchemas) then
                            ctx.ScheduleMigration ()
                        let newCustomEntities = req.Schemas |> Map.map (fun name dump -> dump.CustomEntities)
                        updateCustomEntities newCustomEntities
                        return Ok ()
                    with
                    | :? RestoreSchemaException as e when e.IsUserException ->
                        logger.LogError(e, "Failed to restore schemas")
                        return Error <| RRERequest (fullUserMessage e)
        }

    member this.RestoreZipSchemas (stream : Stream) (req : RestoreStreamSchemasRequest) : Task<Result<unit, RestoreErrorInfo>> =
        task {
            let maybeDumps =
                try
                    Ok <| schemasFromZipFile stream
                with
                | :? RestoreSchemaException as e when e.IsUserException ->
                    Error (RRERequest <| fullUserMessage e)
            match maybeDumps with
            | Error e ->
                logAPIError rctx "restoreZipSchemas" req e
                return Error e
            | Ok dumps ->
                let newReq = { Schemas = dumps; Flags = req.Flags }
                return! this.RestoreSchemas newReq
        }

    interface ISaveRestoreAPI with
        member this.SaveSchemas req = this.SaveSchemas req
        member this.SaveZipSchemas req = this.SaveZipSchemas req
        member this.RestoreSchemas req = this.RestoreSchemas req
        member this.RestoreZipSchemas stream req = this.RestoreZipSchemas stream req