module FunWithFlags.FunDB.HTTP.Permissions

open Giraffe
open Giraffe.EndpointRouting

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.API.Types

let permissionsApi : Endpoint list =
    let getPermissions (api : IFunDBAPI) : HttpHandler =
        Successful.ok (json api.Permissions.UserPermissions)

    [ GET [route "/permissions" <| withContextRead getPermissions]
    ]