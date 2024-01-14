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

let trySchemasFromZipFile (stream : Stream) =
    try
        Ok <| schemasFromZipFile stream
    with
    | :? RestoreSchemaException as e when e.IsUserException ->
        Error (RRERequest <| fullUserMessage e)

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
        wrapAPIResult rctx logger "saveSchemas" req <| task {
            let schemas =
                match req with
                | SRSpecified schemas ->
                    Map.toSeq schemas
                | SRAll settings ->
                    ctx.Layout.Schemas
                        |> Map.keys
                        |> Seq.map (fun name -> (name, settings))
                | SRNonPreloaded settings ->
                    let preloadSchemas = Map.keysSet ctx.Preload.Schemas
                    let allSchemas = Map.keysSet ctx.Layout.Schemas
                    let schemasSet = Set.difference allSchemas preloadSchemas
                    Seq.map (fun name -> (name, settings)) schemasSet
            if not (canSave rctx.User.Effective.Type) then
                return Error RSEAccessDenied
            else
                let runOne results (name, settings : SaveSchemaSettings) =
                    task {
                        if not <| Map.containsKey name ctx.Layout.Schemas then
                            let msg = sprintf "Schema %O is not found" name
                            return Error (RSERequest msg)
                        else
                            let onlyCustomEnttieies =
                                match settings.OnlyCustomEntities with
                                | None | Some OCFalse -> false
                                | Some OCTrue -> true
                                | Some OCPreloaded -> Map.containsKey name ctx.Preload.Schemas
                            let! schema =
                                if onlyCustomEnttieies then
                                    task {
                                        let! customEntities = saveCustomEntities ctx.Transaction ctx.Layout name ctx.CancellationToken
                                        return SSCustomEntities customEntities
                                    }
                                else
                                    task {
                                        let! schemaDump = saveSchema ctx.Transaction ctx.Layout name ctx.CancellationToken
                                        return SSFull schemaDump
                                    }
                            return Ok <| Map.add name schema results
                    }
                let! result = schemas |> Seq.foldResultTask runOne Map.empty
                return Result.map (fun schemas -> { Schemas = schemas }) result
        }

    member this.RestoreSchemas (req : RestoreSchemasRequest) : Task<Result<unit, RestoreErrorInfo>> =
        wrapUnitAPIResult rctx logger "restoreSchemas" req <| task {
            let flags = Option.defaultValue emptyRestoreSchemasFlags req.Flags
            if not (canRestore rctx.User.Effective.Type) then
                return Error RREAccessDenied
            else
                let getFullSchema name (data : SavedSchemaData) =
                    match data with
                    | SSFull dump -> Some dump
                    | _ -> None

                let fullSchemas = Map.mapMaybe getFullSchema req.Schemas
                let fullRestoredSchemas = Map.keysSet fullSchemas
                let preloadedSchemas = Map.keysSet ctx.Preload.Schemas
                
                if not <| Set.isEmpty (Set.intersect fullRestoredSchemas preloadedSchemas) then
                    return Error (RRERequest "Only custom entities can be restored from preloaded schemas")
                else
                    try
                        let droppedSchemas =
                            if not flags.DropOthers then
                                Set.empty
                            else
                                let emptySchemas = Set.difference (Map.keysSet ctx.Layout.Schemas) preloadedSchemas
                                Set.difference emptySchemas (Map.keysSet req.Schemas)
                        let! modifiedMetadata = restoreSchemas ctx.Transaction ctx.Layout fullSchemas ctx.CancellationToken
                        do! deleteSchemas ctx.Layout ctx.Transaction droppedSchemas ctx.CancellationToken
                        if modifiedMetadata || not (Set.isEmpty droppedSchemas) then
                            ctx.ScheduleMigration ()

                        let getCustomEntities name (data : SavedSchemaData) =
                            match data with
                            | SSFull dump -> dump.CustomEntities
                            | SSCustomEntities customEntities -> customEntities

                        let newCustomEntities = req.Schemas |> Map.map getCustomEntities
                        updateCustomEntities newCustomEntities
                        return Ok ()
                    with
                    | :? RestoreSchemaException as e when e.IsUserException ->
                        logger.LogError(e, "Failed to restore schemas")
                        return Error <| RRERequest (fullUserMessage e)
        }

    interface ISaveRestoreAPI with
        member this.SaveSchemas req = this.SaveSchemas req
        member this.RestoreSchemas req = this.RestoreSchemas req
