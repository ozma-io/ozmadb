module FunWithFlags.FunDB.API.Entity

open Microsoft.AspNetCore.Http
open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.ContextCache
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.FunQL.AST

let entitiesApi (cacheStore : ContextCacheStore) : HttpHandler =
    let guarded = withContext cacheStore

    let returnError = function
        | EEArguments msg -> RequestErrors.BAD_REQUEST <| sprintf "Invalid arguments: %s" msg
        | EEAccessDenied -> RequestErrors.FORBIDDEN ""
        | EENotFound -> RequestErrors.NOT_FOUND ""
        | EEExecute msg -> RequestErrors.BAD_REQUEST <| sprintf "Execution error: %s" msg

    let insertEntity (entityRef : ResolvedEntityRef) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let rawArgs = queryArgs ctx
        match rctx.InsertEntity entityRef rawArgs with
        | Ok () -> text "" next ctx
        | Result.Error err -> returnError err next ctx

    let updateEntity (entityRef : ResolvedEntityRef) (id : int) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let rawArgs = queryArgs ctx
        match rctx.UpdateEntity entityRef id rawArgs with
        | Ok () -> Successful.OK "" next ctx
        | Result.Error err -> returnError err next ctx

    let deleteEntity (entityRef : ResolvedEntityRef) (id : int) (rctx : RequestContext) =
        match rctx.DeleteEntity entityRef id with
        | Ok () -> Successful.OK ""
        | Result.Error err -> returnError err

    let recordApi (entityRef : ResolvedEntityRef) (id : int) =
        choose
            [ PUT >=> guarded (updateEntity entityRef id)
              DELETE >=> guarded (deleteEntity entityRef id)
            ]

    let entityApi (schema : string, name : string) =
        let entityRef =
            { schema = FunQLName schema
              name = FunQLName name
            }
        choose
            [ route "/" >=> POST >=> guarded (insertEntity entityRef)
              routef "/%i" (recordApi entityRef)
            ]

    choose
        [ subRoutef "/entity/%s/%s" entityApi
        ]