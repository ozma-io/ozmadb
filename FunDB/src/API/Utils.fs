module FunWithFlags.FunDB.API.Utils

open System.Collections.Generic
open Microsoft.Extensions.Primitives
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.V2.ContextInsensitive
open Newtonsoft.Json
open Giraffe
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging

open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Operations.Context
open FunWithFlags.FunDB.Operations.InstancesCache

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

let commitAndReturn (handler : HttpHandler) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        match! rctx.Commit () with
        | Ok () -> return! Successful.ok handler next ctx
        | Error msg -> return! RequestErrors.badRequest (text msg) next ctx
    }

let commitAndOk : RequestContext -> HttpFunc -> HttpContext -> HttpFuncResult = commitAndReturn (json Map.empty)

type RealmAccess =
    { roles : string[]
    }

[<NoEquality; NoComparison>]
type APISettings =
    { instancesStore : InstancesCacheStore
      instancesConnectionString : string
      disableSecurity : bool
      disableACL : bool
      forceHost : string option
    }

let anonymousUsername = "anonymous@example.com"

let withContext (settings : APISettings) (f : RequestContext -> HttpHandler) : HttpHandler =
    let makeContext (userName : string) (isRoot : bool) (next : HttpFunc) (ctx : HttpContext) = task {
        let host = Option.defaultValue ctx.Request.Host.Host settings.forceHost
        use instances =
            let loggerFactory = ctx.GetService<ILoggerFactory>()
            let systemOptions =
                (DbContextOptionsBuilder<InstancesContext> ())
                    .UseLoggerFactory(loggerFactory)
                    .UseNpgsql(settings.instancesConnectionString)
            new InstancesContext(systemOptions.Options)
        match! instances.Instances.FirstOrDefaultAsync(fun x -> x.Name = host) with
        | null -> return! RequestErrors.notFound (text (sprintf "Instance %s not found" host)) next ctx
        | instance->
            let! cacheStore = settings.instancesStore.GetContextCache(instance.ConnectionString)

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
                        { cacheStore = cacheStore
                          userName = userName
                          isRoot = isRoot
                          language = lang
                          disableACL = settings.disableACL
                        }
                return! f rctx next ctx
            with
            | :? RequestException as e ->
                match e.Info with
                | REUserNotFound
                | RENoRole -> return! RequestErrors.forbidden (text "") next ctx
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

    let unprotectedApi = makeContext anonymousUsername true

    if settings.disableSecurity then
        unprotectedApi
    else
        authorize >=> protectedApi