module FunWithFlags.FunDB.API.Permissions

open Giraffe

open FunWithFlags.FunDB.API.Utils

let permissionsApi (settings : APISettings) : HttpHandler =
    let guarded = withContext settings

    (*let permissions (rctx : RequestContext) =
        json rctx.Cache.allowedDatabase*)

    choose
        [// route "/permissions" >=> GET >=> guarded permissions
        ]