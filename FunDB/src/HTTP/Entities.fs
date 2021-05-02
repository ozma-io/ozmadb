module FunWithFlags.FunDB.HTTP.Entities

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

[<NoEquality; NoComparison>]
type TopLevelTransaction =
    { Operations : TransactionOp[]
      DeferConstraints : bool
    }

let entitiesApi : HttpHandler =
    let getEntityInfo (entityRef : ResolvedEntityRef) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            match! api.Entities.GetEntityInfo entityRef with
            | Ok info ->
                return! Successful.ok (json info) next ctx
            | Error err -> return! errorHandler err (json err) next ctx
        }

    let entityApi (schema : string, name : string) =
        let entityRef =
            { Schema = FunQLName schema
              Name = FunQLName name
            }
        choose
            [ GET >=> withContext (getEntityInfo entityRef)
            ]

    let runTransaction (api : IFunDBAPI) =
        safeBindJson <| fun transaction (next : HttpFunc) (ctx : HttpContext) ->
            task {
                if not transaction.DeferConstraints then
                    match! api.Entities.RunTransaction { Operations = transaction.Operations } with
                    | Ok ret ->
                        return! commitAndReturn (json ret) api next ctx
                    | Error err ->
                        return! RequestErrors.badRequest (json err) next ctx
                else
                    match! api.Entities.DeferConstraints (fun () -> api.Entities.RunTransaction { Operations = transaction.Operations }) with
                    | Ok (Ok ret) ->
                        return! commitAndReturn (json ret) api next ctx
                    | Ok (Error err) ->
                        return! RequestErrors.badRequest (json err) next ctx
                    | Error err ->
                        return! RequestErrors.badRequest (json err) next ctx
            }

    let transactionApi =
        POST >=> withContext runTransaction

    choose
        [ subRoutef "/entities/%s/%s" entityApi
          subRoute "/transaction" transactionApi
        ]