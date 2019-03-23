module FunWithFlags.FunDB.API.Permissions

open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.ContextCache

let permissionsApi (settings : APISettings) : HttpHandler =
    let guarded = withContext settings

    let permissions (rctx : RequestContext) =
        json rctx.Cache.allowedDatabase

    choose
        [ route "/permissions" >=> GET >=> guarded permissions
        ]