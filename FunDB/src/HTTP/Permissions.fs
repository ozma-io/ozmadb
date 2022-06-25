module FunWithFlags.FunDB.HTTP.Permissions

open Giraffe

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.API.Types

let permissionsApi : HttpHandler =
    let getPermissions (api : IFunDBAPI) : HttpHandler =
        Successful.ok (json api.Permissions.UserPermissions)

    choose
        [ route "/permissions" >=> GET >=> withContextRead getPermissions
        ]