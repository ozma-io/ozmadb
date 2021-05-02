module FunWithFlags.FunDB.API.Permissions

open Microsoft.Extensions.Logging

open FunWithFlags.FunDB.API.Types

type PermissionsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<PermissionsAPI>()

    member this.UserPermissions =
        { IsRoot = rctx.User.Type.IsRoot
        }

    interface IPermissionsAPI with
        member this.UserPermissions = this.UserPermissions