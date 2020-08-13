module FunWithFlags.FunDB.HTTP.Utils

open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open System.Security.Claims
open Microsoft.Extensions.Primitives
open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.V2.ContextInsensitive
open Newtonsoft.Json
open Giraffe

open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.ContextCache
open FunWithFlags.FunDB.API.Request
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.API

type APIError =
    { Message : string
    } with
        [<DataMember>]
        member this.Error = "generic"

let errorJson str = json { Message = str }

let errorHandler (ex : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
    logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> setStatusCode 500 >=> errorJson ex.Message

let notFoundHandler : HttpFunc -> HttpContext -> HttpFuncResult = setStatusCode 404 >=> errorJson "Not Found"

let private processArgs (f : Map<string, JToken> -> HttpHandler) (rawArgs : KeyValuePair<string, StringValues> seq) : HttpHandler =
    let getArg (KeyValue(name : string, par)) =
        if name.StartsWith("__") then
            None
        else
            par |> Seq.first |> Option.map (fun x -> (name, x))
    let args1 = rawArgs |> Seq.mapMaybe getArg

    let tryArg (name, arg) =
        match tryJson arg with
        | None -> Error name
        | Some r -> Ok (name, r)

    match Seq.traverseResult tryArg args1 with
    | Error name -> sprintf "Invalid JSON value in argument %s" name |> errorJson |> RequestErrors.badRequest
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

let commitAndReturn (handler : HttpHandler) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        try
            do! api.Request.Context.Commit ()
            return! Successful.ok handler next ctx
        with
        | :? ContextException as e ->
            return! RequestErrors.badRequest (errorJson (exceptionString e)) next ctx
    }

let commitAndOk : IFunDBAPI -> HttpFunc -> HttpContext -> HttpFuncResult = commitAndReturn (json Map.empty)

type RealmAccess =
    { Roles : string[]
    }

type IInstancesSource =
    abstract member GetInstance : string -> CancellationToken -> Task<Instance option>

let anonymousUsername = "anonymous@example.com"

let withContext (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    let makeContext (instance : Instance) (userName : string) (isRoot : bool) (next : HttpFunc) (ctx : HttpContext) = task {
        let logger = ctx.GetLogger("withContext")
        logger.LogInformation("Creating context for instance {}, user {} (is_root: {})", instance.Name, userName, isRoot)
        let connectionString = sprintf "Host=%s; Port=%i; Database=%s; Username=%s; Password=%s" instance.Host instance.Port instance.Database instance.Username instance.Password
        let contextCache = ctx.GetService<InstancesCacheStore>()
        let! cacheStore = contextCache.GetContextCache(connectionString)

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
            use! dbCtx = cacheStore.GetCache ctx.RequestAborted
            let! rctx =
                RequestContext.Create
                    { UserName = userName
                      IsRoot = (userName = instance.Owner || isRoot)
                      Language = lang
                      Context = dbCtx
                    }
            return! f (FunDBAPI rctx) next ctx
        with
        | :? RequestException as e ->
            match e.Info with
            | REUserNotFound
            | RENoRole -> return! RequestErrors.forbidden (errorJson "") next ctx
    }

    let protectedApi (instance : Instance) (next : HttpFunc) (ctx : HttpContext) =
        let userClaim = ctx.User.FindFirst ClaimTypes.Email
        if isNull userClaim then
            RequestErrors.badRequest (errorJson "No email claim in security token") next ctx
        else
            let userRoles = ctx.User.FindFirst "realm_access"
            let isRoot =
                if not <| isNull userRoles then
                    let roles = JsonConvert.DeserializeObject<RealmAccess> userRoles.Value
                    roles.Roles |> Seq.contains "fundb_admin"
                else
                    false
            makeContext instance userClaim.Value isRoot next ctx

    let unprotectedApi instance = makeContext instance anonymousUsername true

    let lookupInstance (next : HttpFunc) (ctx : HttpContext) = task {
        let instancesSource = ctx.GetService<IInstancesSource>()
        let xInstance = ctx.Request.Headers.["X-Instance"]
        let instanceName =
            if xInstance.Count = 0 then
                ctx.Request.Host.Host
            else
                xInstance.[0]
        match! instancesSource.GetInstance instanceName ctx.RequestAborted with
        | None -> return! RequestErrors.notFound (errorJson (sprintf "Instance %s not found" instanceName)) next ctx
        | Some instance ->
            if instance.DisableSecurity then
                return! unprotectedApi instance next ctx
            else
                return! (authorize >=> protectedApi instance) next ctx
    }

    lookupInstance
