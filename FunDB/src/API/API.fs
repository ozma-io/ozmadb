module FunWithFlags.FunDB.API.API

open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.UserViews
open FunWithFlags.FunDB.API.Entities
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.API.Actions
open FunWithFlags.FunDB.API.Permissions
open FunWithFlags.FunDB.API.Domains

type FunDBAPI (rctx : IRequestContext) as this =
    let uv = UserViewsAPI this
    let entities = EntitiesAPI this
    let saveRestore = SaveRestoreAPI this
    let actions = ActionsAPI this
    let permissions = PermissionsAPI this
    let domains = DomainsAPI this

    do
        rctx.Context.SetAPI this

    member this.UserViews = uv
    member this.Entities = entities
    member this.SaveRestore = saveRestore
    member this.Actions = actions
    member this.Permissions = permissions
    member this.Domains = domains

    interface IFunDBAPI with
        member this.Request = rctx
        member this.UserViews = uv :> IUserViewsAPI
        member this.Entities = entities :> IEntitiesAPI
        member this.SaveRestore = saveRestore :> ISaveRestoreAPI
        member this.Actions = actions :> IActionsAPI
        member this.Permissions = permissions :> IPermissionsAPI
        member this.Domains = domains :> IDomainsAPI