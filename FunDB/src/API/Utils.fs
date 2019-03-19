module FunWithFlags.FunDB.API.Utils

open Giraffe
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.ContextCache

let queryArgs (ctx : HttpContext) =
    ctx.Request.Query |> Seq.mapMaybe (function KeyValue(name, par) -> par |> Seq.first |> Option.map (fun x -> (name, x))) |> Map.ofSeq

let authorize =
    requiresAuthentication (challenge JwtBearerDefaults.AuthenticationScheme)

let withContext (cacheStore : ContextCacheStore) (f : RequestContext -> HttpHandler) : HttpHandler =
    let protectedApi (next : HttpFunc) (ctx : HttpContext) =
        let userClaim = ctx.User.FindFirst "preferred_username"
        let userName = userClaim.Value
        let specifiedLang =
            ctx.Request.GetTypedHeaders().AcceptLanguage
            |> Seq.sortByDescending (fun lang -> if lang.Quality.HasValue then lang.Quality.Value else 1.0)
            |> Seq.first
        let lang =
            match specifiedLang with
            | Some l -> l.Value.Value
            | None -> "en-US"
        use rctx = new RequestContext(cacheStore, userName, lang)
        f rctx next ctx
    authorize >=> protectedApi