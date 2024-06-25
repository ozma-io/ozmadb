module OzmaDB.HTTP.Permissions

open System
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open OzmaDB.OzmaUtils
open OzmaDB.HTTP.Utils
open OzmaDB.API.Types

let permissionsApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let getPermissions (api: IOzmaDBAPI) =
        Task.result (jobJson api.Permissions.UserPermissions)

    [ GET [ route "/permissions" <| utils.PerformReadJob getPermissions ] ]
