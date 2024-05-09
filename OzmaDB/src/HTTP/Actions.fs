module OzmaDB.HTTP.Actions

open System
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection
open Newtonsoft.Json.Linq

open OzmaDB.HTTP.Utils
open OzmaDB.OzmaQL.AST
open OzmaDB.Actions.Types
open OzmaDB.API.Types

let actionsApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let runAction (ref : ActionRef) (args : JObject option) =
        let job (api : IOzmaDBAPI) =
            task {
                let req = { Action = ref; Args = args }
                let! ret = api.Actions.RunAction req
                return! jobReplyWithCommit api ret
            }
        utils.PerformWriteJob job

    let runActionRoute (schema, name) =
        let ref = { Schema = OzmaQLName schema; Name = OzmaQLName name }
        safeBindJson <| runAction ref

    let actionApi =
        [ POST [routef "/%s/%s" runActionRoute] // DEPRECATED
          POST [routef "/%s/%s/run" runActionRoute]
        ]

    [ subRoute "/actions" actionApi
    ]