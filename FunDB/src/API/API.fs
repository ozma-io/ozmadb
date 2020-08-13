module FunWithFlags.FunDB.API.API

open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Request
open FunWithFlags.FunDB.API.UserViews
open FunWithFlags.FunDB.API.Entities
open FunWithFlags.FunDB.API.SaveRestore

type FunDBAPI (rctx : RequestContext) =
    let uv = UserViewsAPI rctx
    let entities = EntitiesAPI rctx
    let saveRestore = SaveRestoreAPI rctx

    member this.UserViews = uv
    member this.Entities = entities
    member this.SaveRestore = saveRestore

    interface IFunDBAPI with
        member this.Request = rctx :> IRequestContext
        member this.UserViews = uv :> IUserViewsAPI
        member this.Entities = entities :> IEntitiesAPI
        member this.SaveRestore = saveRestore :> ISaveRestoreAPI