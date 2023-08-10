module FunWithFlags.FunDB.HTTP.Actions

open System
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.API.Types

let actionsApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let runAction (ref : ActionRef) (args : JObject option) =
        let job (api : IFunDBAPI) =
            task {
                let req = { Action = ref; Args = args }
                let! ret = api.Actions.RunAction req
                return! jobReplyWithCommit api ret
            }
        utils.PerformWriteJob job

    let runActionRoute (schema, name) =
        let ref = { Schema = FunQLName schema; Name = FunQLName name }
        safeBindJson <| runAction ref

    let actionApi =
        [ POST [routef "/%s/%s" runActionRoute] // DEPRECATED
          POST [routef "/%s/%s/run" runActionRoute]
        ]

    [ subRoute "/actions" actionApi
    ]