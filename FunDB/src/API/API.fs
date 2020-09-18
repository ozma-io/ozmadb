module FunWithFlags.FunDB.API.API

open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Request
open FunWithFlags.FunDB.API.UserViews
open FunWithFlags.FunDB.API.Entities
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.API.Actions

type FunDBAPI (rctx : IRequestContext) as this =
    let uv = UserViewsAPI rctx
    let entities = EntitiesAPI rctx
    let saveRestore = SaveRestoreAPI rctx
    let actions = ActionsAPI rctx

    do
        rctx.Context.SetAPI this

    member this.UserViews = uv
    member this.Entities = entities
    member this.SaveRestore = saveRestore
    member this.Actions = actions

    interface IFunDBAPI with
        member this.Request = rctx
        member this.UserViews = uv :> IUserViewsAPI
        member this.Entities = entities :> IEntitiesAPI
        member this.SaveRestore = saveRestore :> ISaveRestoreAPI
        member this.Actions = actions :> IActionsAPI