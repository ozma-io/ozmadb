module FunWithFlags.FunDB.HTTP.Entity

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types

let private errorHandler = function
    | EENotFound -> RequestErrors.badRequest
    | EEFrozen -> RequestErrors.forbidden
    | EEAccessDenied -> RequestErrors.forbidden
    | EEArguments _ -> RequestErrors.notFound
    | EEExecution _ -> RequestErrors.unprocessableEntity
    | EEException _ -> ServerErrors.internalError
    | EETrigger _ -> ServerErrors.internalError

let entitiesApi : HttpHandler =
    let getEntityInfo (entityRef : ResolvedEntityRef) (api : IFunDBAPI) : HttpHandler =
        fun next ctx -> task {
            match! api.Entities.GetEntityInfo entityRef with
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

    let runTransaction (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let! transaction = ctx.BindModelAsync<Transaction>()
            match! api.Entities.RunTransaction transaction with
            | Ok ret ->
                return! commitAndReturn (json ret) api next ctx
            | Error err ->
                return! RequestErrors.badRequest (json err) next ctx
        }

    let transactionApi =
        POST >=> withContext runTransaction

    choose
        [ subRoutef "/entity/%s/%s" entityApi
          subRoute "/transaction" transactionApi
        ]