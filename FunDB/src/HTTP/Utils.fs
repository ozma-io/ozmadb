module FunWithFlags.FunDB.HTTP.Utils

open System
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open System.Security.Claims
open Microsoft.Extensions.Primitives
open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open Giraffe
open Npgsql

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.ContextCache
open FunWithFlags.FunDB.API.Request
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.API

[<SerializeAsObject("error")>]
type RequestErrorInfo =
    | [<CaseName("internal")>] REInternal of Message : string
    | [<CaseName("request")>] RERequest of Message : string
    | [<CaseName("no_endpoint")>] RENoEndpoint
    | [<CaseName("no_instance")>] RENoInstance
    | [<CaseName("access_denied")>] REAccessDenied
    with
        [<DataMember>]
        member this.Message =
            match this with
            | REInternal msg -> msg
            | RERequest msg -> msg
            | RENoEndpoint -> "API endpoint doesn't exist"
            | RENoInstance -> "Instance not found"
            | REAccessDenied -> "Database access denied"

let requestError e =
    let handler =
        match e with
        | REInternal _ -> ServerErrors.internalError
        | RERequest _ -> RequestErrors.badRequest
        | RENoEndpoint -> RequestErrors.notFound
        | RENoInstance -> RequestErrors.notFound
        | REAccessDenied _ -> RequestErrors.forbidden
    handler (json e)

let errorHandler (ex : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
    logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> requestError (REInternal ex.Message)

let notFoundHandler : HttpFunc -> HttpContext -> HttpFuncResult = requestError RENoEndpoint

let boolRequestArg (name : string) (ctx: HttpContext) : bool =
    match ctx.Request.Query.TryGetValue name with
    | (true, values) when values.Count = 0 -> true
    | (true, values) -> Option.defaultValue false (Parsing.tryBool values.[0])
    | (false, _) -> false

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
    | Error name -> sprintf "Invalid JSON value in argument %s" name |> RERequest |> requestError
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
            return! requestError (RERequest (exceptionString e)) next ctx
    }

let commitAndOk : IFunDBAPI -> HttpFunc -> HttpContext -> HttpFuncResult = commitAndReturn (json Map.empty)

type RealmAccess =
    { Roles : string[]
    }

type IInstance =
    inherit IDisposable
    inherit IAsyncDisposable

    abstract member Name : string
    abstract member Owner : string
    abstract member Host : string
    abstract member Port : int
    abstract member Username : string
    abstract member Password : string
    abstract member Database : string
    abstract member DisableSecurity : bool
    abstract member IsTemplate : bool
    abstract member AccessedAt : DateTime option

    abstract member UpdateAccessedAt : DateTime -> Task

type IInstancesSource =
    abstract member GetInstance : string -> CancellationToken -> Task<IInstance option>

let private anonymousUsername = "anonymous@example.com"
let private serviceDomain = "service"

type InstanceContext =
    { Instance : IInstance
      UserName : string
      IsRoot : bool
      CanRead : bool
    }

let instanceConnectionString (instance : IInstance) =
    let builder = NpgsqlConnectionStringBuilder ()
    builder.Host <- instance.Host
    builder.Port <- instance.Port
    builder.Database <- instance.Database
    builder.Username <- instance.Username
    builder.Password <- instance.Password
#if DEBUG
    builder.IncludeErrorDetails <- true
#endif
    builder.ConnectionString

type UserTokenInfo =
    { Client : string
      Email : string option
      IsRoot : bool
    }

let resolveUser (f : UserTokenInfo -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    let clientClaim = ctx.User.FindFirst "azp"
    if isNull clientClaim then
        requestError (RERequest "No azp claim in security token") next ctx
    else
        let client = clientClaim.Value
        let emailClaim = ctx.User.FindFirst ClaimTypes.Email
        let email =
            if isNull emailClaim then None else Some emailClaim.Value
        let userRoles = ctx.User.FindFirst "realm_access"
        let isRoot =
            if not <| isNull userRoles then
                let roles = JsonConvert.DeserializeObject<RealmAccess> userRoles.Value
                roles.Roles |> Seq.contains "fundb_admin"
            else
                false
        let info =
            { Client = client
              Email = email
              IsRoot = isRoot
            }
        f info next ctx

let lookupInstance (f : InstanceContext -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    task {
        let instancesSource = ctx.GetService<IInstancesSource>()
        let xInstance = ctx.Request.Headers.["X-Instance"]
        let instanceName =
            if xInstance.Count = 0 then
                ctx.Request.Host.Host
            else
                xInstance.[0]
        match! instancesSource.GetInstance instanceName ctx.RequestAborted with
        | None ->
            return! requestError RENoInstance next ctx
        | Some instance ->
            try
                if instance.DisableSecurity then
                    let ictx =
                        { Instance = instance
                          UserName = anonymousUsername
                          IsRoot = true
                          CanRead = true
                        }
                    return! f ictx next ctx
                else
                    let f' info =
                        let userName =
                            match info.Email with
                            | Some e -> e
                            | None -> sprintf "%s@%s" info.Client serviceDomain
                        let ictx =
                            { Instance = instance
                              UserName = userName
                              IsRoot = info.IsRoot
                              CanRead = instance.IsTemplate
                            }
                        f ictx
                    return! (authorize >=> resolveUser f') next ctx
            finally
                instance.Dispose ()
    }

let private randomAccessedAtGen = new ThreadLocal<Random>(fun () -> Random())

let private randomAccessedAtLaxSpan () =
    // A minute +/- ~5 seconds.
    TimeSpan.FromMinutes(1.0 + (2.0 * randomAccessedAtGen.Value.NextDouble() - 0.5) * 0.09)

let withContext (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    let makeContext (inst : InstanceContext) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let logger = ctx.GetLogger("withContext")
            use _ = logger.BeginScope("Creating context for instance {}, user {} (is root: {})", inst.Instance.Name, inst.UserName, inst.IsRoot)
            let connectionString = instanceConnectionString inst.Instance
            let instancesCache = ctx.GetService<InstancesCacheStore>()
            let! cacheStore = instancesCache.GetContextCache(connectionString)

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
                        { UserName = inst.UserName
                          IsRoot = (inst.UserName = inst.Instance.Owner || inst.IsRoot)
                          CanRead = inst.CanRead
                          Language = lang
                          Context = dbCtx
                          Source = ESAPI
                        }
                let currTime = DateTime.UtcNow
                match inst.Instance.AccessedAt with
                | Some prevTime when currTime - prevTime < randomAccessedAtLaxSpan () -> ()
                | _ ->  do! inst.Instance.UpdateAccessedAt currTime
                do! inst.Instance.DisposeAsync ()
                return! f (FunDBAPI rctx) next ctx
            with
            | :? RequestException as e ->
                match e.Info with
                | REUserNotFound
                | RENoRole -> return! requestError REAccessDenied next ctx
        }

    lookupInstance makeContext