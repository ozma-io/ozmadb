module FunWithFlags.FunDB.HTTP.Actions

open Newtonsoft.Json.Linq
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.API.Types

let private actionError e =
    let handler =
        match e with
        | AENotFound -> RequestErrors.notFound
        | AECompilation _ -> ServerErrors.internalError
        | AEException _ -> ServerErrors.internalError
    handler (json e)

let actionsApi : HttpHandler =
    let runAction (ref : ActionRef) (api : IFunDBAPI) =
        safeBindJson <| fun args (next : HttpFunc) (ctx : HttpContext) ->
            task {
                match! api.Actions.RunAction ref args with
                | Ok ret ->
                    return! commitAndReturn (json ret) api next ctx
                | Error err ->
                    return! actionError err next ctx
            }

    let actionApi (schema, name) =
        let ref = { Schema = FunQLName schema; Name = FunQLName name }
        POST >=> withContext (runAction ref)

    choose
        [ subRoutef "/actions/%s/%s" actionApi
        ]