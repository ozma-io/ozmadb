module FunWithFlags.FunDB.API.Utils

open Giraffe
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open Newtonsoft.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.ContextCache

let queryArgs (ctx : HttpContext) =
    ctx.Request.Query |> Seq.mapMaybe (function KeyValue(name, par) -> par |> Seq.first |> Option.map (fun x -> (name, x))) |> Map.ofSeq

let formArgs (ctx : HttpContext) =
    ctx.Request.Form |> Seq.mapMaybe (function KeyValue(name, par) -> par |> Seq.first |> Option.map (fun x -> (name, x))) |> Map.ofSeq

let authorize =
    requiresAuthentication (challenge JwtBearerDefaults.AuthenticationScheme)

type RealmAccess =
    { roles : string[]
    }

let withContext (cacheStore : ContextCacheStore) (f : RequestContext -> HttpHandler) : HttpHandler =
    let protectedApi (next : HttpFunc) (ctx : HttpContext) =
        let userClaim = ctx.User.FindFirst "preferred_username"
        let userName = userClaim.Value
        let userRoles = ctx.User.FindFirst "realm_access"
        let isRoot =
            if not <| isNull userRoles then
                let roles = JsonConvert.DeserializeObject<RealmAccess> userRoles.Value
                roles.roles |> Seq.contains "fundb_admin"
            else
                false

        let specifiedLang =
            ctx.Request.GetTypedHeaders().AcceptLanguage
            |> Seq.sortByDescending (fun lang -> if lang.Quality.HasValue then lang.Quality.Value else 1.0)
            |> Seq.first
        let lang =
            match specifiedLang with
            | Some l -> l.Value.Value
            | None -> "en-US"
        try
            use rctx =
                new RequestContext {
                    cacheStore = cacheStore
                    userName = userName
                    isRoot = isRoot
                    language = lang
            }
            f rctx next ctx
        with
        | RequestException REUserNotFound -> RequestErrors.FORBIDDEN "" next ctx
    authorize >=> protectedApi