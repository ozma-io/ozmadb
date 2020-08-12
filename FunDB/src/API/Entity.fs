module FunWithFlags.FunDB.API.Entity

open Newtonsoft.Json
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.Context

let private errorHandler = function
    | EENotFound -> RequestErrors.badRequest
    | EEAccessDenied -> RequestErrors.forbidden
    | EEArguments _ -> RequestErrors.notFound
    | EEExecution _ -> RequestErrors.unprocessableEntity

let entitiesApi : HttpHandler =
    let getEntityInfo (entityRef : ResolvedEntityRef) (rctx : RequestContext) : HttpHandler =
        fun next ctx -> task {
            match! rctx.GetEntityInfo entityRef with
            | Ok info ->
                return! Successful.ok (json info) next ctx
            | Result.Error err -> return! errorHandler err (json err) next ctx
        }

    let entityApi (schema : string, name : string) =
        let entityRef =
            { schema = FunQLName schema
              name = FunQLName name
            }
        choose
            [ GET >=> withContext (getEntityInfo entityRef)
            ]

    let runTransaction (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let! transaction = ctx.BindModelAsync<Transaction>()
            match! rctx.RunTransaction transaction with
            | Ok ret ->
                return! commitAndReturn (json ret) rctx next ctx
            | Error err ->
                return! RequestErrors.badRequest (json err) next ctx
        }

    let transactionApi =
        POST >=> withContext runTransaction

    choose
        [ subRoutef "/entity/%s/%s" entityApi
          subRoute "/transaction" transactionApi
        ]