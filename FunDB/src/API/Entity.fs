module FunWithFlags.FunDB.API.Entity

open Suave
open Suave.Filters
open Suave.Operators

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.ContextCache
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.FunQL.AST

let entitiesApi (rctx : RequestContext) : WebPart =
    let returnError = function
        | EEArguments msg -> RequestErrors.BAD_REQUEST <| sprintf "Invalid arguments: %s" msg
        | EEAccessDenied -> RequestErrors.FORBIDDEN ""
        | EENotFound -> RequestErrors.NOT_FOUND ""
        | EEExecute msg -> RequestErrors.BAD_REQUEST <| sprintf "Execution error: %s" msg

    let getEntityRef (schema : string, name : string) =
        { schema = FunQLName schema
          name = FunQLName name
        }

    let getRecordRef (schema : string, name : string, id : int) =
        (getEntityRef (schema, name), id)
    
    let insertEntity (entityRef : ResolvedEntityRef) =
        request <| fun req ->
            let rawArgs = req.form |> Seq.mapMaybe (fun (name, maybeArg) -> Option.map (fun arg -> (name, arg)) maybeArg) |> Map.ofSeq
            match rctx.InsertEntity entityRef rawArgs with
                | Ok () -> Successful.OK ""
                | Result.Error err -> returnError err

    let updateEntity (entityRef : ResolvedEntityRef, id : int) =
        request <| fun req ->
            let rawArgs = req.form |> Seq.mapMaybe (fun (name, maybeArg) -> Option.map (fun arg -> (name, arg)) maybeArg) |> Map.ofSeq
            match rctx.UpdateEntity entityRef id rawArgs with
                | Ok () -> Successful.OK ""
                | Result.Error err -> returnError err

    let deleteEntity (entityRef : ResolvedEntityRef, id : int) =
        match rctx.DeleteEntity entityRef id with
            | Ok () -> Successful.OK ""
            | Result.Error err -> returnError err

    choose
        [ POST >=> pathScan "/entity/%s/%s" (getEntityRef >> insertEntity)
          PUT >=> pathScan "/entity/%s/%s/%i" (getRecordRef >> updateEntity)
          DELETE >=> pathScan "/entity/%s/%s/%i" (getRecordRef >> deleteEntity)
        ]