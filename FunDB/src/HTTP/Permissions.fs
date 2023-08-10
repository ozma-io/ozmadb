module FunWithFlags.FunDB.HTTP.Permissions

open System
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.API.Types

let permissionsApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let getPermissions (api : IFunDBAPI) =
        Task.result (jobJson api.Permissions.UserPermissions)

    [ GET [route "/permissions" <| utils.PerformReadJob getPermissions]
    ]