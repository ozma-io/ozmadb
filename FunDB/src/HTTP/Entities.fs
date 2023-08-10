module FunWithFlags.FunDB.HTTP.Entities

open System
open FSharp.Control.Tasks.Affine
open Giraffe
open Giraffe.EndpointRouting
open FSharpPlus
open Microsoft.Extensions.DependencyInjection

open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types

// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type HTTPTransaction =
    { Operations : TransactionOp[]
      DeferConstraints : bool
      ForceAllowBroken : bool
    }

// TODO: make it private after we fix JSON serialization for the private F# types.
type RelatedEntitiesHTTPRequest =
    { Id : RawRowKey
    }

let entitiesApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let getEntityInfo (entityRef : ResolvedEntityRef) : HttpHandler =
        let job (api : IFunDBAPI) =
            task {
                let! ret = api.Entities.GetEntityInfo { Entity = entityRef }
                return jobReply ret
            }
        utils.PerformReadJob job

    let getRelatedEntities (entityRef : ResolvedEntityRef) : HttpHandler =
         safeBindJson <| fun (httpReq : RelatedEntitiesHTTPRequest) ->
            let job (api : IFunDBAPI) =
                task {
                    let req =
                        { Entity = entityRef
                          Id = httpReq.Id
                        }
                    let! ret = api.Entities.GetRelatedEntities req
                    return jobReply ret
                }
            utils.PerformReadJob job

    let getRef next (schema, name) =
        let entityRef = { Schema = FunQLName schema; Name = FunQLName name }
        next entityRef

    let entityApi =
        [ POST [ routef "/%s/%s/related" <| getRef getRelatedEntities ]
          GET [ routef "/%s/%s/info" <| getRef getEntityInfo ]
        ]

    let runTransactionInner (api : IFunDBAPI) (transaction : HTTPTransaction) =
        task {
            if transaction.ForceAllowBroken then
                api.Request.Context.SetForceAllowBroken ()
            return! api.Entities.RunTransaction { Operations = transaction.Operations }
        }

    let runTransaction =
        safeBindJson <| fun (transaction : HTTPTransaction) ->
            let handler api =
                task {
                    if not transaction.DeferConstraints then
                        let! ret = runTransactionInner api transaction
                        return! jobReplyWithCommit api ret
                    else
                        let! ret = api.Entities.DeferConstraints (fun () -> runTransactionInner api transaction)
                        let flatRet =
                            ret
                            |> Result.map (Result.mapError id<IErrorDetails>)
                            |> Result.mapError id<IErrorDetails>
                            |> Result.flatten
                        return! jobReplyWithCommit api flatRet
                }
            utils.PerformWriteJob handler

    let transactionApi =
        [ POST [route "/" runTransaction]
        ]

    [ subRoute "/entities" entityApi
      subRoute "/transaction" transactionApi
    ]