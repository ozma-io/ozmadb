module FunWithFlags.FunDB.HTTP.Actions

open Giraffe.EndpointRouting

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.API.Types

let actionsApi : Endpoint list =
    let runAction (ref : ActionRef) (api : IFunDBAPI) =
        safeBindJson <| fun args ->
            let req = { Action = ref; Args = args }
            handleRequestWithCommit api (api.Actions.RunAction req)

    let runActionRoute (schema, name) =
        let ref = { Schema = FunQLName schema; Name = FunQLName name }
        withContextWrite (runAction ref)

    let actionApi =
        [ POST [routef "/%s/%s" runActionRoute] // DEPRECATED
          POST [routef "/%s/%s/run" runActionRoute]
        ]

    [ subRoute "/actions" actionApi
    ]