module OzmaDB.API.API

open OzmaDB.API.Types
open OzmaDB.API.UserViews
open OzmaDB.API.Entities
open OzmaDB.API.SaveRestore
open OzmaDB.API.Actions
open OzmaDB.API.Permissions
open OzmaDB.API.Domains

type OzmaDBAPI (rctx : IRequestContext) as this =
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

    interface IOzmaDBAPI with
        member this.Request = rctx
        member this.UserViews = uv.Value :> IUserViewsAPI
        member this.Entities = entities.Value :> IEntitiesAPI
        member this.SaveRestore = saveRestore.Value :> ISaveRestoreAPI
        member this.Actions = actions.Value :> IActionsAPI
        member this.Permissions = permissions.Value :> IPermissionsAPI
        member this.Domains = domains.Value :> IDomainsAPI