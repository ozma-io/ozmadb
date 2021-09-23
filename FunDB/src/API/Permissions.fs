module FunWithFlags.FunDB.API.Permissions

open Microsoft.Extensions.Logging

open FunWithFlags.FunDB.API.Types

type PermissionsAPI (api : IFunDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<PermissionsAPI>()

    member this.UserPermissions =
        { IsRoot = rctx.User.Effective.Type.IsRoot
        }

    interface IPermissionsAPI with
        member this.UserPermissions = this.UserPermissions