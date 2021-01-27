module FunWithFlags.FunDB.API.Permissions

open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.Actions.Run
open FunWithFlags.FunDB.API.Types

type PermissionsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<PermissionsAPI>()

    member this.UserPermissions =
        { IsRoot = rctx.User.Type.IsRoot
        }

    interface IPermissionsAPI with
        member this.UserPermissions = this.UserPermissions