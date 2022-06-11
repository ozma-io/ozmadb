﻿module FunWithFlags.FunDB.HTTP.Entities

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
    | EECompilation _ -> RequestErrors.badRequest
    | EEExecution _ -> RequestErrors.unprocessableEntity
    | EEException _ -> ServerErrors.internalError
    | EETrigger _ -> ServerErrors.internalError

[<NoEquality; NoComparison>]
type TopLevelTransaction =
    { Operations : TransactionOp[]
      DeferConstraints : bool
      ForceAllowBroken : bool
    }

type RelatedEntitiesRequest =
    { Id : RawRowKey
    }

let entitiesApi : HttpHandler =
    let getEntityInfo (entityRef : ResolvedEntityRef) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            match! api.Entities.GetEntityInfo entityRef with
            | Ok info ->
                return! Successful.ok (json info) next ctx
            | Error err -> return! errorHandler err (json err) next ctx
        }

    let getRelatedEntities (entityRef : ResolvedEntityRef) (api : IFunDBAPI) (req : RelatedEntitiesRequest) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            match! api.Entities.GetRelatedEntities entityRef req.Id with
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
            [ route "/related" >=> POST >=> withContext (fun api -> safeBindJson (getRelatedEntities entityRef api))
              route "/info" >=> GET >=> withContext (getEntityInfo entityRef)
            ]

    let runTransactionInner (api : IFunDBAPI) (transaction : TopLevelTransaction) =
        task {
            if transaction.ForceAllowBroken then
                api.Request.Context.SetForceAllowBroken ()
            return! api.Entities.RunTransaction { Operations = transaction.Operations }
        }

    let runTransaction (api : IFunDBAPI) =
        safeBindJson <| fun (transaction : TopLevelTransaction) (next : HttpFunc) (ctx : HttpContext) ->
            task {
                if not transaction.DeferConstraints then
                    match! runTransactionInner api transaction with
                    | Ok ret ->
                        return! commitAndReturn (json ret) api next ctx
                    | Error err ->
                        return! RequestErrors.badRequest (json err) next ctx
                else
                    match! api.Entities.DeferConstraints (fun () -> runTransactionInner api transaction) with
                    | Ok (Ok ret) ->
                        return! commitAndReturn (json ret) api next ctx
                    | Ok (Error err) ->
                        return! RequestErrors.badRequest (json err) next ctx
                    | Error err ->
                        return! RequestErrors.badRequest (json err) next ctx
            }

    choose
        [ subRoutef "/entities/%s/%s" entityApi
          route "/transaction" >=> POST >=> withContext runTransaction
        ]