module FunWithFlags.FunDB.API.Permissions

open Suave
open Suave.Filters
open Suave.Operators

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Context

let permissionsApi (rctx : RequestContext) : WebPart =
    choose
        [ path "/permissions" >=> GET >=> jsonResponse rctx.Cache.allowedDatabase
        ]