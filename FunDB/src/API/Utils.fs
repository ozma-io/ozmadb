module FunWithFlags.FunDB.API.Utils

open System.Collections.Generic
open Microsoft.Extensions.Primitives
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.V2.ContextInsensitive
open Newtonsoft.Json
open Giraffe

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Operations.Context
open FunWithFlags.FunDB.Operations.ContextCache

let private processArgs (f : Map<string, JToken> -> HttpHandler) (rawArgs : KeyValuePair<string, StringValues> seq) : HttpHandler =
    let args1 = rawArgs |> Seq.mapMaybe (function KeyValue(name, par) -> par |> Seq.first |> Option.map (fun x -> (name, x)))
    let tryArg (name, arg) =
        match tryJson arg with
        | None -> Error name
        | Some r -> Ok (name, r)
    match Seq.traverseResult tryArg args1 with
    | Error name -> sprintf "Invalid JSON value in argument %s" name |> text |> RequestErrors.badRequest
    | Ok args2 -> f <| Map.ofSeq args2

let queryArgs (f : Map<string, JToken> -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    processArgs f ctx.Request.Query next ctx

let formArgs (f : Map<string, JToken> -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    task {
        let! form = ctx.Request.ReadFormAsync ()
        return! processArgs f form next ctx
    }

let authorize =
    requiresAuthentication (challenge JwtBearerDefaults.AuthenticationScheme)

let commitAndReturn (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        match! rctx.Commit () with
        | Ok () -> return! Successful.OK "" next ctx
        | Error msg -> return! RequestErrors.badRequest (text msg) next ctx
    }

type RealmAccess =
    { roles : string[]
    }

[<NoComparison>]
type APISettings =
    { cacheStore : ContextCacheStore
      disableSecurity : bool
    }

let withContext (settings : APISettings) (f : RequestContext -> HttpHandler) : HttpHandler =
    let makeContext (userName : string) (isRoot : bool) (next : HttpFunc) (ctx : HttpContext) = task {
        let acceptLanguage = ctx.Request.GetTypedHeaders().AcceptLanguage
        let specifiedLang =
            if isNull acceptLanguage then
                None
            else
                acceptLanguage
                |> Seq.sortByDescending (fun lang -> if lang.Quality.HasValue then lang.Quality.Value else 1.0)
                |> Seq.first
        let lang =
            match specifiedLang with
            | Some l -> l.Value.Value
            | None -> "en-US"
        try
            use! rctx =
                RequestContext.Create
                    { cacheStore = settings.cacheStore
                      userName = userName
                      isRoot = isRoot
                      language = lang
                    }
            return! f rctx next ctx
        with
        | :? RequestException as e ->
            match e.Info with
            | REUserNotFound -> return! RequestErrors.forbidden (text "") next ctx
    }

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
        makeContext userName isRoot next ctx

    let unprotectedApi = makeContext "anonymous@example.com" true

    if settings.disableSecurity then
        unprotectedApi
    else
        authorize >=> protectedApi