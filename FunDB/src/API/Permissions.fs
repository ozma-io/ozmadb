module FunWithFlags.FunDB.API.Permissions

open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.ContextCache

let permissionsApi (cacheStore : ContextCacheStore) : HttpHandler =
    let guarded = withContext cacheStore

    let permissions (rctx : RequestContext) =
        json rctx.Cache.allowedDatabase

    choose
        [ route "/permissions" >=> GET >=> guarded permissions
        ]