module FunWithFlags.FunDB.API.API

open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.UserViews
open FunWithFlags.FunDB.API.Entities
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.API.Actions
open FunWithFlags.FunDB.API.Permissions
open FunWithFlags.FunDB.API.Domains

type FunDBAPI (rctx : IRequestContext) as this =
    let uv = lazy (UserViewsAPI this)
    let entities = lazy (EntitiesAPI this)
    let saveRestore = lazy (SaveRestoreAPI this)
    let actions = lazy (ActionsAPI this)
    let permissions = lazy (PermissionsAPI this)
    let domains = lazy (DomainsAPI this)

    do
        rctx.Context.SetAPI this

    member this.UserViews = uv.Value
    member this.Entities = entities.Value
    member this.SaveRestore = saveRestore.Value
    member this.Actions = actions.Value
    member this.Permissions = permissions.Value
    member this.Domains = domains.Value

    interface IFunDBAPI with
        member this.Request = rctx
        member this.UserViews = uv.Value :> IUserViewsAPI
        member this.Entities = entities.Value :> IEntitiesAPI
        member this.SaveRestore = saveRestore.Value :> ISaveRestoreAPI
        member this.Actions = actions.Value :> IActionsAPI
        member this.Permissions = permissions.Value :> IPermissionsAPI
        member this.Domains = domains.Value :> IDomainsAPI