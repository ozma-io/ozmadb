module FunWithFlags.FunDB.HTTP.Utils

open System
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open System.Security.Claims
open Microsoft.Extensions.Primitives
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open Giraffe
open NodaTime
open Npgsql

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.Request
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.API
open FunWithFlags.FunDB.HTTP.RateLimit

[<SerializeAsObject("error")>]
type RequestErrorInfo =
    | [<CaseName("internal")>] RIInternal of Message : string
    | [<CaseName("request")>] RIRequest of Message : string
    | [<CaseName("quotaExceeded")>] RIQuotaExceeded of Message : string
    | [<CaseName("rateExceeded")>] RIRateExceeded of Message : string
    | [<CaseName("noEndpoint")>] RINoEndpoint
    | [<CaseName("noInstance")>] RINoInstance
    | [<CaseName("accessDenied")>] RIAccessDenied
    | [<CaseName("concurrentUpdate")>] RIConcurrentUpdate
    | [<CaseName("stackOverflow")>] RIStackOverflow of Trace : EventSource list
    with
        [<DataMember>]
        member this.Message =
            match this with
            | RIInternal msg -> msg
            | RIRequest msg -> msg
            | RIQuotaExceeded msg -> msg
            | RIRateExceeded msg -> msg
            | RINoEndpoint -> "API endpoint doesn't exist"
            | RINoInstance -> "Instance not found"
            | RIAccessDenied -> "Database access denied"
            | RIConcurrentUpdate -> "Concurrent update detected; try again"
            | RIStackOverflow sources ->
                sources
                    |> Seq.map (sprintf "in %O")
                    |> String.concat "\n"
                    |> sprintf "Stack depth exceeded:\n%s"

let requestError e =
    let handler =
        match e with
        | RIInternal _ -> ServerErrors.internalError
        | RIQuotaExceeded _ -> RequestErrors.forbidden
        | RIRateExceeded _ -> RequestErrors.tooManyRequests
        | RIRequest _ -> RequestErrors.badRequest
        | RINoEndpoint -> RequestErrors.notFound
        | RINoInstance -> RequestErrors.notFound
        | RIAccessDenied _ -> RequestErrors.forbidden
        | RIConcurrentUpdate _ -> ServerErrors.serviceUnavailable
        | RIStackOverflow _ -> ServerErrors.internalError
    handler (json e)

let errorHandler (ex : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
    logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> requestError (RIInternal "Internal error")

let notFoundHandler : HttpFunc -> HttpContext -> HttpFuncResult = requestError RINoEndpoint

let tryBoolRequestArg (name : string) (ctx: HttpContext) : bool option =
    match ctx.Request.Query.TryGetValue name with
    | (true, values) when values.Count = 0 -> Some true
    | (true, values) -> Some <| Option.defaultValue false (Parsing.tryBool values.[0])
    | (false, _) -> None

let boolRequestArg (name : string) (ctx: HttpContext) : bool =
    Option.defaultValue false (tryBoolRequestArg name ctx)

let intRequestArg (name : string) (ctx: HttpContext) : int option =
    ctx.TryGetQueryStringValue name |> Option.bind Parsing.tryIntInvariant

let flagIfDebug (flag : bool) : bool =
#if DEBUG
    flag
#else
    false
#endif

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
    | Error name -> sprintf "Invalid JSON value in argument %s" name |> RIRequest |> requestError
    | Ok args2 -> f <| Map.ofSeq args2

let queryArgs (f : Map<string, JToken> -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    processArgs f ctx.Request.Query next ctx

let formArgs (f : Map<string, JToken> -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        let! form = ctx.Request.ReadFormAsync ()
        return! processArgs f form next ctx
    }

let bindJsonToken (token : JToken) (f : 'a -> HttpHandler) : HttpHandler =
    let mobj =
        try
            Ok <| token.ToObject()
        with
        | :? JsonException as e -> Error <| Exn.fullMessage e
    match mobj with
    | Ok o -> f o
    | Error e -> RequestErrors.BAD_REQUEST e

let safeBindJson (f : 'a -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        let! model =
            task {
                try
                    let! ret = ctx.BindJsonAsync<'a>()
                    return Ok ret
                with
                | :? JsonException as e -> return Error e
            }
        match model with
        | Error e -> return! requestError (RIRequest <| Exn.fullMessage e) next ctx
        | Ok ret when isRefNull ret -> return! requestError (RIRequest "Invalid JSON value") next ctx
        | Ok ret -> return! f ret next ctx
    }

let private generalToRequestError = function
    | GECommit msg -> RIRequest msg
    | GEQuotaExceeded msg -> RIQuotaExceeded msg

let commitAndReturn (handler : HttpHandler) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        match! api.Request.Context.Commit () with
        | Error e -> return! requestError (generalToRequestError e) next ctx
        | Ok () -> return! Successful.ok handler next ctx
    }

let commitAndOk : IFunDBAPI -> HttpFunc -> HttpContext -> HttpFuncResult = commitAndReturn (json Map.empty)

let inline private setPretendRole (api : IFunDBAPI) (pretendRole : ResolvedEntityRef option) (func : unit -> Task<'a>) : Task<'a> =
    match pretendRole with
    | None -> func ()
    | Some roleType -> api.Request.PretendRole roleType func

let inline setPretends (api : IFunDBAPI) (pretendUser : UserName option) (pretendRole : ResolvedEntityRef option) (func : unit -> Task<'a>) : Task<'a> =
    match pretendUser with
    | None -> setPretendRole api pretendRole func
    | Some userName -> api.Request.PretendUser userName (fun () -> setPretendRole api pretendRole func)

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
    abstract member AnyoneCanRead : bool
    abstract member AccessedAt : Instant option
    abstract member Published : bool
    abstract member MaxSize : int option // MiB
    abstract member MaxUsers : int option
    abstract member MaxRequestTime : Duration option
    abstract member ReadRateLimitsPerUser : RateLimit seq
    abstract member WriteRateLimitsPerUser : RateLimit seq

    abstract member UpdateAccessedAt : Instant -> Task

type IInstancesSource =
    abstract member GetInstance : string -> CancellationToken -> Task<IInstance option>
    abstract member SetExtraConnectionOptions : NpgsqlConnectionStringBuilder -> unit

let private anonymousUsername = "anonymous@example.com"
let private serviceDomain = "service"

type InstanceContext =
    { Source : IInstancesSource
      Instance : IInstance
      UserName : string
      IsRoot : bool
      CanRead : bool
    }

let instanceConnectionString (instance : IInstance) (modify : NpgsqlConnectionStringBuilder -> unit) =
    let builder = NpgsqlConnectionStringBuilder ()
    builder.Host <- instance.Host
    builder.Port <- instance.Port
    builder.Database <- instance.Database
    builder.Username <- instance.Username
    builder.Password <- instance.Password
#if DEBUG
    builder.IncludeErrorDetail <- true
#endif
    builder.Enlist <- false
    modify builder
    builder.ConnectionString

type UserTokenInfo =
    { Client : string
      Email : string option
      IsRoot : bool
    }

let resolveUser (f : UserTokenInfo -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    let client = ctx.User.FindFirstValue "azp"
    if isNull client then
        requestError (RIRequest "No azp claim in security token") next ctx
    else
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
            return! requestError RINoInstance next ctx
        | Some instance ->
            let anonymousCtx =
                { Source = instancesSource
                  Instance = instance
                  UserName = anonymousUsername
                  IsRoot = instance.DisableSecurity
                  CanRead = instance.AnyoneCanRead
                }
            try
                if instance.DisableSecurity then
                    return! f anonymousCtx next ctx
                else
                    let getCtx info =
                        let userName =
                            match info.Email with
                            | Some e -> e
                            | None -> sprintf "%s@%s" info.Client serviceDomain
                        let ictx =
                            { Source = instancesSource
                              Instance = instance
                              UserName = userName
                              IsRoot = userName.ToLowerInvariant() = instance.Owner.ToLowerInvariant() || info.IsRoot
                              CanRead = instance.AnyoneCanRead
                            }
                        f ictx
                    let failHandler =
                        if instance.AnyoneCanRead then
                            f anonymousCtx
                        else
                            challenge JwtBearerDefaults.AuthenticationScheme
                    return! (requiresAuthentication failHandler >=> resolveUser getCtx) next ctx
            finally
                instance.Dispose ()
    }

let setupRequestTimeout (f : HttpHandler) (inst : InstanceContext) (dbCtx : IContext) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match inst.Instance.MaxRequestTime with
        | None -> return! f next ctx
        | Some maxTime ->
            let oldToken = dbCtx.CancellationToken
            use newToken = new CancellationTokenSource()
            ignore <| oldToken.Register(fun () -> newToken.Cancel())
            newToken.CancelAfter(int maxTime.TotalMilliseconds)
            dbCtx.CancellationToken <- newToken.Token
            try
                return! f next ctx
            with
            | Exn.Innermost (:? OperationCanceledException as e) when not oldToken.IsCancellationRequested ->
                return! requestError (RIQuotaExceeded "Max request time reached") next ctx
    }

let private randomAccessedAtGen = new ThreadLocal<Random>(fun () -> Random())

let private randomAccessedAtLaxSpan () =
    // A minute +/- ~5 seconds.
    let minutes = 1.0 + (2.0 * randomAccessedAtGen.Value.NextDouble() - 0.5) * 0.09
    Duration.FromMilliseconds(int64 (minutes * 60.0 * 1000.0))

let private runWithApi (touchAccessedAt : bool) (f : IFunDBAPI -> HttpHandler) : InstanceContext -> HttpHandler =
    let runRequest (inst : InstanceContext) (dbCtx : IContext) (next : HttpFunc) (ctx : HttpContext) =
        task {
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
            let! rctx =
                RequestContext.Create
                    { UserName = inst.UserName
                      IsRoot = inst.IsRoot
                      CanRead = inst.CanRead
                      Language = lang
                      Context = dbCtx
                      Quota =
                        { MaxUsers = inst.Instance.MaxUsers
                          MaxSize = inst.Instance.MaxSize
                        }
                    }
            if touchAccessedAt then
                let currTime = SystemClock.Instance.GetCurrentInstant()
                match inst.Instance.AccessedAt with
                | Some prevTime when currTime - prevTime < randomAccessedAtLaxSpan () -> ()
                | _ ->  do! inst.Instance.UpdateAccessedAt currTime
            do! inst.Instance.DisposeAsync ()
            return! f (FunDBAPI rctx) next ctx
        }

    fun  (inst : InstanceContext) (next : HttpFunc) (ctx : HttpContext) ->
        task {
            if not inst.Instance.Published && not inst.IsRoot then
                return! requestError RIAccessDenied next ctx
            else
                let logger = ctx.GetLogger("runWithApi")
                use _ = logger.BeginScope("Creating context for instance {}, user {} (is root: {})", inst.Instance.Name, inst.UserName, inst.IsRoot)
                let connectionString = instanceConnectionString inst.Instance inst.Source.SetExtraConnectionOptions
                let instancesCache = ctx.GetService<InstancesCacheStore>()
                let! cacheStore = instancesCache.GetContextCache(connectionString)

                let longRunning =
                    if inst.IsRoot then
                        tryBoolRequestArg "X-LongRunning" ctx |> Option.defaultValue false
                    else
                        false
                let initialCancellationToken =
                    if longRunning then
                        let lifetime = ctx.GetService<IHostApplicationLifetime>()
                        lifetime.ApplicationStopping
                    else
                        ctx.RequestAborted

                try
                    use! dbCtx = cacheStore.GetCache initialCancellationToken
                    return! setupRequestTimeout (runRequest inst dbCtx) inst dbCtx next ctx
                with
                | :? ConcurrentUpdateException as e ->
                    logger.LogError(e, "Concurrent update exception")
                    // We may want to retry here in future.
                    return! requestError RIConcurrentUpdate next ctx
                | :? RequestException as e ->
                    logger.LogError(e, "Request error")
                    match e.Info with
                    | REUserNotFound
                    | RENoRole -> return! requestError RIAccessDenied next ctx
                    | REStackOverflow firstSource ->
                        let rec findAllSources (inner : exn) =
                            match inner with
                            | :? RequestException as e ->
                                match e.Info with
                                | REStackOverflow source -> source :: findAllSources e.InnerException
                                | _ -> []
                            | _ -> []

                        let allSources = firstSource :: findAllSources e.InnerException
                        return! requestError (RIStackOverflow allSources) next ctx
        }

let withContextHidden (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    lookupInstance (runWithApi false f)

let private rateExceeded msg = requestError (RIRateExceeded msg)

let private getIpAddress (ctx : HttpContext) =
    match ctx.Request.Headers.TryGetValue("X-Real-IP") with
    | (true, ip) -> Seq.last ip
    | (false, _) -> string ctx.Connection.RemoteIpAddress

let private withContextLimited (prefix : string) (getLimits : IInstance -> RateLimit seq) (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    let run = runWithApi true f
    lookupInstance <| fun inst next ctx ->
        let userId =
            match ctx.User.FindFirstValue ClaimTypes.NameIdentifier with
            | null -> getIpAddress ctx
            | userId -> userId
        checkRateLimit (run inst) rateExceeded (prefix + userId) (getLimits inst.Instance) next ctx

let withContextRead (f : IFunDBAPI -> HttpHandler) = withContextLimited "read." (fun inst -> inst.ReadRateLimitsPerUser) f
let withContextWrite (f : IFunDBAPI -> HttpHandler) = withContextLimited "write." (fun inst -> inst.WriteRateLimitsPerUser) f

let deprecated (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    let logger = ctx.GetLogger("deprecated")
    logger.LogWarning("Deprecated route used: {}", ctx.Request.Path)
    next ctx