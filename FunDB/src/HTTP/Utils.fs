module FunWithFlags.FunDB.HTTP.Utils

open System
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading
open System.IO
open System.Text
open System.Threading.Tasks
open System.Security.Claims
open Microsoft.IO
open Microsoft.Extensions.Primitives
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
open StackExchange
open Newtonsoft.Json
open Serilog
open Serilog.Context
open Microsoft.Extensions.Logging
open Giraffe
open NodaTime
open Npgsql

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
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
open FunWithFlags.FunDB.HTTP.LongRunning

[<SerializeAsObject("error")>]
type RequestErrorInfo =
    | [<CaseKey("internal")>] RIInternal
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] RIRequest of Details : string
    | [<CaseKey("rateExceeded", IgnoreFields=[|"Details"|])>] RIRateExceeded of Details : string
    | [<CaseKey("unsupportedMediaType")>] RIUnsupportedMediaType
    | [<CaseKey("unacceptable")>] RIUnacceptable
    | [<CaseKey("noEndpoint")>] RINoEndpoint
    | [<CaseKey("canceled")>] RICanceled
    | [<CaseKey("noInstance")>] RINoInstance
    | [<CaseKey("invalidRegion")>] RIWrongRegion of Region : string
    | [<CaseKey("unauthorized")>] RIUnauthorized
    | [<CaseKey("accessDenied", IgnoreFields=[|"Details"|])>] RIAccessDenied of Details : string
    | [<CaseKey("concurrentUpdate")>] RIConcurrentUpdate
    | [<CaseKey("notFinished")>] RINotFinished of Id : JobId
    | [<CaseKey("other", IgnoreFields=[|"Details"|])>] RIOther of Details : string
    with
        member this.LogMessage =
            match this with
            | RIInternal -> "Internal server error"
            | RIRequest msg -> msg
            | RIRateExceeded msg -> msg
            | RIUnsupportedMediaType -> "Unsupported media type"
            | RIUnacceptable -> "Unacceptable"
            | RINoEndpoint -> "API endpoint doesn't exist"
            | RICanceled -> "The request has been canceled"
            | RINoInstance -> "Instance not found"
            | RIWrongRegion region -> sprintf "Wrong instance region, correct region: %s" region
            | RIUnauthorized -> "Failed to authorize using the access token"
            | RIAccessDenied msg -> msg
            | RIConcurrentUpdate -> "Concurrent update detected; try again"
            | RINotFinished id -> "The background job has not yet finished"
            | RIOther msg -> msg

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | RIInternal _ -> 500
            | RIRateExceeded _ -> 429
            | RIRequest _ -> 400
            | RIUnsupportedMediaType -> 415
            | RIUnacceptable -> 405
            | RINoEndpoint -> 404
            | RICanceled -> 400
            | RINoInstance -> 404
            | RIWrongRegion _ -> 422
            | RIUnauthorized -> 401
            | RIAccessDenied _ -> 403
            | RIConcurrentUpdate _ -> 503
            | RINotFinished _ -> 500
            | RIOther _ -> 500

        static member private LookupKey = prepareLookupCaseKey<RequestErrorInfo>
        member this.Error =
            RequestErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = false

        interface IErrorDetails with
            member this.LogMessage = this.LogMessage
            member this.Message = this.Message
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

let requestError<'Error when 'Error :> IErrorDetails> (e : 'Error) =
    setHttpHeader "X-FunDB-Error" e.Error >=> setStatusCode e.HTTPResponseCode >=> json e

let errorHandler (e : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
    match e with
    | :? OperationCanceledException ->
        logger.LogInformation(e, "The request has been canceled.")
        clearResponse >=> requestError RICanceled
    | _ ->
        logger.LogError(e, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> requestError RIInternal

let notFoundHandler : HttpFunc -> HttpContext -> HttpFuncResult = requestError RINoEndpoint

let tryBoolRequestArg (name : string) (ctx: HttpContext) : bool option =
    match ctx.Request.Query.TryGetValue name with
    | (true, values) when values.Count = 0 -> Some true
    | (true, values) -> Parsing.tryBool (Seq.last values)
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

let private processArgs (rawArgs : KeyValuePair<string, StringValues> seq) (f : Map<string, JToken> -> HttpHandler) : HttpHandler =
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
    processArgs ctx.Request.Query f next ctx

let formArgs (f : Map<string, JToken> -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        let! form = ctx.Request.ReadFormAsync ()
        return! processArgs form f next ctx
    }

let bindJsonToken (token : JToken) (f : 'a -> HttpHandler) : HttpHandler =
    let mobj =
        try
            Ok <| token.ToObject()
        with
        | :? JsonException as e -> Error <| fullUserMessage e
    match mobj with
    | Ok o -> f o
    | Error e -> requestError (RIRequest e)

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
        | Error e -> return! requestError (RIRequest <| fullUserMessage e) next ctx
        | Ok ret when isRefNull ret -> return! requestError (RIRequest "Invalid JSON value") next ctx
        | Ok ret -> return! f ret next ctx
    }

type HttpJobResponseHeader =
    { ResponseCode : int
      Headers : IDictionary<string, string>
    }

type HttpJobResponse = HttpJobResponseHeader * JobDataWriter

let private utf8EncodingWithoutBom = UTF8Encoding(false)

// Convert that to async when we move to System.Text.Json
let jsonJobDataWriter (response : 'a) : JobDataWriter =
    let writer (stream : Stream) (cancellationToken : CancellationToken) =
        use streamWriter = new StreamWriter(stream, utf8EncodingWithoutBom, -1, true)
        use jsonTextWriter = new JsonTextWriter(streamWriter)
        let jsonSerializer = JsonSerializer.CreateDefault()
        jsonSerializer.Serialize(jsonTextWriter, response)
    JOSync writer

let jobError<'Error when 'Error :> IErrorDetails> (e : 'Error) : HttpJobResponse =
    let header =
        { ResponseCode = e.HTTPResponseCode
          Headers =
            dict <| seq {
                ("X-FunDB-Error", e.Error)
                ("Content-Type", "application/json")
            }
        }
    (header, jsonJobDataWriter e)

let jobJson<'Response> (r : 'Response) : HttpJobResponse =
    let header =
        { ResponseCode = 200
          Headers =
            dict <| seq {
                ("Content-Type", "application/json")
            }
        }
    (header, jsonJobDataWriter r)

let inline commitAndReturn (api : IFunDBAPI) ([<InlineIfLambda>] getResult : unit -> HttpJobResponse) : Task<HttpJobResponse> =
    task {
        match! api.Request.Context.Commit () with
        | Error e -> return jobError e
        | Ok () -> return getResult ()
    }

let inline commitAndOk (api : IFunDBAPI) : Task<HttpJobResponse> = commitAndReturn api (fun () -> jobJson Map.empty)

let inline jobReplyWithCommit<'Response, 'Error when 'Error :> IErrorDetails> (api : IFunDBAPI) (ret : Result<'Response, 'Error>) =
    match ret with
    | Ok ret -> commitAndReturn api (fun () -> jobJson ret)
    | Error e -> Task.result (jobError e)

let inline jobReply<'Response, 'Error when 'Error :> IErrorDetails> (ret : Result<'Response, 'Error>) =
    match ret with
    | Ok ret -> jobJson ret
    | Error e -> jobError e

type RealmAccess =
    { Roles : string[]
    }

type IInstance =
    inherit IDisposable

    abstract member Name : string
    abstract member Region : string option
    abstract member Host : string
    abstract member Port : int
    abstract member Username : string
    abstract member Password : string
    abstract member Database : string

    abstract member Owner : string
    abstract member DisableSecurity : bool
    abstract member AnyoneCanRead : bool
    abstract member Published : bool
    abstract member ShadowAdmins : string seq

    abstract member MaxSize : int option // MiB
    abstract member MaxUsers : int option
    abstract member MaxRequestTime : Duration option
    abstract member ReadRateLimitsPerUser : RateLimit seq
    abstract member WriteRateLimitsPerUser : RateLimit seq

    abstract member AccessedAt : Instant option

    abstract member UpdateAccessedAtAndDispose : Instant -> unit

type IInstancesSource =
    abstract member GetInstance : string -> CancellationToken -> Task<IInstance option>
    abstract member SetExtraConnectionOptions : NpgsqlConnectionStringBuilder -> unit
    abstract member Region : string option

let private anonymousUsername = "anonymous@example.com"
let private serviceDomain = "service"

type InstanceContext =
    { Source : IInstancesSource
      Instance : IInstance
      UserName : string
      IsRoot : bool
      IsGlobalAdmin : bool
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
      IsGlobalAdmin : bool
    }

type private LongRunningType =
    | LRNo
    | LRYes
    | LRHybrid

let private getUserTokenInfo
        (client : string)
        (email : string option)
        (ctx : HttpContext)
        : UserTokenInfo =
    let userRoles = ctx.User.FindFirst "realm_access"
    let isGlobalAdmin =
        if not <| isNull userRoles then
            let roles = JsonConvert.DeserializeObject<RealmAccess> userRoles.Value
            roles.Roles |> Seq.contains "fundb-admin"
        else
            false
    { Client = client
      Email = email
      IsGlobalAdmin = isGlobalAdmin
    }

let inline setPretendRole
        (api : IFunDBAPI)
        (pretendRole : ResolvedEntityRef option)
        ([<InlineIfLambda>] f : unit -> Task<HttpJobResponse>)
        : Task<HttpJobResponse> =
    match pretendRole with
    | None -> f ()
    | Some role ->
        task {
            match! api.Request.PretendRole { AsRole = PRRole role } f with
            | Ok r -> return r
            | Error e -> return jobError e
        }

let inline setPretendUser
        (api : IFunDBAPI)
        (pretendUser : UserName option)
        ([<InlineIfLambda>] f : unit -> Task<HttpJobResponse>)
        : Task<HttpJobResponse> =
    match pretendUser with
    | None -> f ()
    | Some userName ->
        task {
            match! api.Request.PretendUser { AsUser = userName } f with
            | Ok r -> return r
            | Error e -> return jobError e
        }

let inline private limitRequestTime (instance : IInstance) ([<InlineIfLambda>] f : CancellationToken -> Task<HttpJobResponse>) (cancellationToken : CancellationToken) =
    task {
        match instance.MaxRequestTime with
        | None -> return! f cancellationToken
        | Some maxTime ->
            use newToken = new CancellationTokenSource()
            ignore <| cancellationToken.UnsafeRegister(
                (fun source -> (source :?> CancellationTokenSource).Cancel()),
                newToken
            )
            newToken.CancelAfter(int maxTime.TotalMilliseconds)
            try
                return! f newToken.Token
            with
            | Exn.Innermost (:? OperationCanceledException as e) when newToken.IsCancellationRequested && not cancellationToken.IsCancellationRequested ->
                return jobError (GEQuotaExceeded "Maximum request time reached")
    }

let private randomAccessedAtLaxSpan () =
    // A minute +/- ~5 seconds.
    let minutes = 1.0 + (2.0 * Random.Shared.NextDouble() - 0.5) * 0.09
    Duration.FromMilliseconds(int64 (minutes * 60.0 * 1000.0))

let private getLanguage (ctx : HttpContext) =
    let acceptLanguage = ctx.Request.GetTypedHeaders().AcceptLanguage
    let specifiedLang =
        if isNull acceptLanguage then
            None
        else
            acceptLanguage
            |> Seq.sortByDescending (fun lang -> if lang.Quality.HasValue then lang.Quality.Value else 1.0)
            |> Seq.first
    match specifiedLang with
    | Some l -> l.Value.Value
    | None -> "en-US"

let private rateExceeded msg = requestError (RIRateExceeded msg)

let private getIpAddress (ctx : HttpContext) =
    match ctx.Request.Headers.TryGetValue("X-Real-IP") with
    | (true, ip) -> Seq.last ip
    | (false, _) -> string ctx.Connection.RemoteIpAddress

type HttpJobSettings () =
    member val RemoteIdleTimeout = TimeSpan(0, 0, 10) with get, set
    member val HybridLocalTimeout = TimeSpan(0, 0, 25) with get, set

type HttpJobUtils (
            serviceProvider : IServiceProvider,
            diagnostic : IDiagnosticContext,
            instancesSource : IInstancesSource,
            instancesCache : InstancesCacheStore,
            rmsManager : RecyclableMemoryStreamManager,
            options : IOptions<HttpJobSettings>,
            lifetime : IHostApplicationLifetime,
            logger : ILogger<HttpJobUtils>,
            jobResponseLogger : ILogger<LongRunningJob<HttpJobResponseHeader>>,
            rateLimiter : RateLimiter
        ) =
    let redisMultiplexer = serviceProvider.GetService<Redis.IConnectionMultiplexer>()

    let addContext (name : string) (value : string) =
        // The documentation says to dispose of the bookmark
        // once the context is exited from. We _don't_ do this,
        // allowing us to keep the context in the long-running jobs. 
        ignore <| LogContext.PushProperty(name, value)
        diagnostic.Set(name, value)

    let withUser
            (f : UserTokenInfo -> HttpHandler)
            (next : HttpFunc)
            (ctx : HttpContext)
            =
        let client = ctx.User.FindFirstValue "azp"
        if isNull client then
            requestError (RIRequest "No azp claim in security token") next ctx
        else
            addContext "Client" client

            let emailClaim = ctx.User.FindFirst ClaimTypes.Email
            let email =
                if isNull emailClaim then None else Some emailClaim.Value

            Option.iter (addContext "Email") email
            let info = getUserTokenInfo client email ctx
            f info next ctx

    let getInstanceContext (instance : IInstance) (info : UserTokenInfo) = 
        let userName =
            match info.Email with
            | Some e -> e
            | None -> sprintf "%s@%s" info.Client serviceDomain
        let lowerUserName = userName.ToLowerInvariant()
        let isOwner = lowerUserName = instance.Owner.ToLowerInvariant()
        let isShadowAdmin =
            instance.ShadowAdmins
            |> Seq.exists (fun admin -> lowerUserName = admin.ToLowerInvariant())
        { Source = instancesSource
          Instance = instance
          UserName = userName
          IsRoot = info.IsGlobalAdmin || isOwner || isShadowAdmin
          IsGlobalAdmin = info.IsGlobalAdmin
          CanRead = instance.AnyoneCanRead
        }

    let getAnonymousInstanceContext (instance : IInstance) = 
        { Source = instancesSource
          Instance = instance
          UserName = anonymousUsername
          IsRoot = instance.DisableSecurity
          IsGlobalAdmin = false
          CanRead = instance.AnyoneCanRead
        }

    let lookupInstance (ctx : HttpContext) =
        let instanceName =
            match ctx.Request.Headers.TryGetValue("X-FunDB-Instance") with
            | (true, xInstance) when not <| Seq.isEmpty xInstance -> Seq.last xInstance
            | _ -> ctx.Request.Host.Host

        addContext "Instance" instanceName
        instancesSource.GetInstance instanceName ctx.RequestAborted

    let withInstanceContext (f : InstanceContext -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
        task {
            match! lookupInstance ctx with
            | None ->
                return! requestError RINoInstance next ctx
            | Some instance ->
                use _ = instance
                match instance.Region with
                | Some instanceRegion -> ctx.SetHttpHeader("X-FunDB-Region", instanceRegion)
                | _ -> ()
                match (instancesSource.Region, instance.Region) with
                | (Some homeRegion, Some instanceRegion) when homeRegion <> instanceRegion ->
                    return! requestError (RIWrongRegion instanceRegion) next ctx
                | _ ->
                    let nextCheck (ictx : InstanceContext) : HttpHandler =
                        if not ictx.Instance.Published && not ictx.IsRoot then
                            requestError (RIAccessDenied "Access denied")
                        else
                            f ictx

                    if instance.DisableSecurity then
                        let ictx = getAnonymousInstanceContext instance
                        return! nextCheck ictx next ctx
                    else
                        let failHandler =
                            if instance.AnyoneCanRead then
                                let ictx = getAnonymousInstanceContext instance
                                nextCheck ictx
                            else
                                challenge JwtBearerDefaults.AuthenticationScheme

                        let proceed (info : UserTokenInfo) =
                            let ictx = getInstanceContext instance info
                            nextCheck ictx

                        return! (requiresAuthentication failHandler >=> withUser proceed) next ctx
        }

    let sendHttpJobResponse
            ((result, resultWriter) : HttpJobResponse)
            (next : HttpFunc)
            (ctx : HttpContext)
            : HttpFuncResult =
        task {
            ctx.Response.StatusCode <- result.ResponseCode
            for KeyValue(headerName,  headerValue) in result.Headers do
                ctx.Response.Headers.[headerName] <- headerValue
            let setSize = function
            | None -> ()
            | Some size ->
                ctx.Response.Headers.ContentLength <- Nullable(size)
            do! streamJobData rmsManager resultWriter ctx.Response.Body setSize ctx.RequestAborted
            return Some ctx
        }

    let withRemoteJob
            (f : RemoteJobSettings -> HttpHandler)
            (next : HttpFunc)
            (ctx : HttpContext)
            =
        match ctx.User.FindFirstValue ClaimTypes.NameIdentifier with
        | _ when isNull redisMultiplexer -> requestError (RIOther "Long-running jobs are disabled") next ctx
        | null -> requestError (RIAccessDenied "Long-running jobs are forbidden for anonymous users") next ctx
        | userId ->
            let remoteSettings =
                { Multiplexer = redisMultiplexer
                  ClientId = userId
                  IdleTimeout = options.Value.RemoteIdleTimeout
                } : RemoteJobSettings
            f remoteSettings next ctx

    let startRemoteJob
            (f : RemoteJobSettings -> Task<RemoteJobRef>)
            =
        let handler remoteSettings next ctx =
            task {
                let! remote = f remoteSettings
                return! requestError (RINotFinished remote.Id) next ctx
            }
        withRemoteJob handler

    let wrapAsJob
            (f : CancellationToken -> Task<HttpJobResponse>)
            (next : HttpFunc)
            (ctx : HttpContext)
            : HttpFuncResult =
        task {
            let longRunning =
                match ctx.Request.Headers.TryGetValue("X-FunDB-LongRunning") with
                | (true, values) ->
                    let lastValue = Seq.last values
                    match Parsing.tryBool lastValue with
                    | Some false -> Some LRNo
                    | Some true -> Some LRYes
                    | None when lastValue.ToLower() = "hybrid" -> Some LRHybrid
                    | None -> None
                | (false, _) -> Some LRNo

            match longRunning with
            | None -> return! requestError (RIRequest "Invalid X-FunDB-LongRunning value") next ctx
            | Some LRNo ->
                try
                    let! ret = f ctx.RequestAborted
                    return! sendHttpJobResponse ret next ctx
                with
                | ex ->
                    return! errorHandler ex logger next ctx
            | Some LRYes ->
                let job remoteSettings =
                    task {
                        use longJob = new LongRunningJob<HttpJobResponseHeader>(
                            rmsManager,
                            jobResponseLogger,
                            lifetime.ApplicationStopping
                        )
                        longJob.Start(f)
                        return! longJob.ConvertToRemote(remoteSettings)
                    }
                return! startRemoteJob job next ctx
            | Some LRHybrid ->
                use longJob = new LongRunningJob<HttpJobResponseHeader>(
                    rmsManager,
                    jobResponseLogger,
                    lifetime.ApplicationStopping
                )
                use registration = ctx.RequestAborted.Register(fun () -> longJob.Cancel())
                longJob.Start(f)
                match! longJob.LocalWait(options.Value.HybridLocalTimeout) with
                | Some res ->
                    return! sendHttpJobResponse res next ctx
                | None ->
                    return! startRemoteJob (longJob.ConvertToRemote) next ctx
        }

    let getDbContext (ictx : InstanceContext) : Task<IContext> =
        task {
            let connectionString = instanceConnectionString ictx.Instance ictx.Source.SetExtraConnectionOptions
            let! cacheStore = instancesCache.GetContextCache(connectionString)
            // We allow `GetCache` to work even if the request is interrupted;
            // most likely the cache will be needed soon.
            return! cacheStore.GetCache lifetime.ApplicationStopping
        }

    let runWithApi
            (touchAccessedAt : bool)
            (ictx : InstanceContext)
            (language : string)
            (f : IFunDBAPI -> Task<HttpJobResponse>)
            (cancellationToken : CancellationToken) : Task<HttpJobResponse> =
        task {
            logger.LogDebug("Creating context for instance {instance}, user {user} (is root: {isRoot})", ictx.Instance.Name, ictx.UserName, ictx.IsRoot)
            try
                use! dbCtx = getDbContext ictx
                let proceed (cancellationToken : CancellationToken) =
                    dbCtx.CancellationToken <- cancellationToken
                    task {
                        let! rctx =
                            RequestContext.Create
                                { UserName = ictx.UserName
                                  IsRoot = ictx.IsRoot
                                  CanRead = ictx.CanRead
                                  Language = language
                                  Context = dbCtx
                                  Quota =
                                    { MaxUsers = ictx.Instance.MaxUsers
                                      MaxSize = ictx.Instance.MaxSize
                                    }
                                }
                        if touchAccessedAt then
                            let currTime = SystemClock.Instance.GetCurrentInstant()
                            match ictx.Instance.AccessedAt with
                            | Some prevTime when currTime - prevTime < randomAccessedAtLaxSpan () ->
                                ictx.Instance.Dispose ()
                            | _ ->
                                ictx.Instance.UpdateAccessedAtAndDispose currTime
                        return! f (FunDBAPI rctx)
                    }
                return! limitRequestTime ictx.Instance proceed cancellationToken
            with
            | :? ConcurrentUpdateException as e ->
                logger.LogError(e, "Concurrent update exception")
                // We may want to retry here in future.
                return jobError RIConcurrentUpdate
            | :? DatabaseAccessDeniedException as e ->
                logger.LogError(e, "Database access denied")
                return jobError <| RIAccessDenied "Access denied"
            | :? RequestStackOverflowException as topE ->
                let rec findAllSources (e : RequestStackOverflowException) =
                    seq {
                        yield e.Source
                        match e.InnerException with
                        | :? RequestStackOverflowException as e -> yield! findAllSources e
                        | _ -> ()
                    }
                let allSources = findAllSources topE
                let msg =
                    sources
                    |> Seq.map (sprintf "in %O")
                    |> String.concat "\n"
                    |> sprintf "Stack depth exceeded:\n%s"
                return jobError <| RIOther msg
        }

    let runJobWithApi (touchAccessedAt : bool) (ictx : InstanceContext) (f : IFunDBAPI -> Task<HttpJobResponse>) (next : HttpFunc) (ctx : HttpContext) =
        let lang = getLanguage ctx
        wrapAsJob (runWithApi touchAccessedAt ictx lang f) next ctx

    let checkRateLimit (prefix : string) (getLimits : IInstance -> RateLimit seq) (inst : IInstance) (next : HttpFunc) (ctx : HttpContext) =
        let userId =
            match ctx.User.FindFirstValue ClaimTypes.NameIdentifier with
            | null -> getIpAddress ctx
            | userId -> userId
        let limits =
            { ClientId = prefix + userId
              Limits = getLimits inst
            } : RateLimits
        ctx.Features.Set(limits)
        rateLimiter.CheckRateLimit rateExceeded next ctx

    let withRateLimitCheck (prefix : string) (getLimits : IInstance -> RateLimit seq) (f : IFunDBAPI -> Task<HttpJobResponse>) =
        withInstanceContext (fun ictx next ->
            checkRateLimit prefix getLimits ictx.Instance (runJobWithApi true ictx f next)
        )

    member this.HybridLocalTimeout = options.Value.HybridLocalTimeout

    member this.WithUser (f : UserTokenInfo -> HttpHandler) : HttpHandler = withUser f
    member this.WithInstanceContext (f : InstanceContext -> HttpHandler) : HttpHandler = withInstanceContext f
    member this.WithRemoteJob (f : RemoteJobSettings -> HttpHandler) : HttpHandler = withRemoteJob f

    member this.WithHiddenContext (f : IFunDBAPI -> Task<HttpJobResponse>) : HttpHandler =
        withInstanceContext (fun inst -> runJobWithApi false inst f)

    member this.PerformReadJob (f : IFunDBAPI -> Task<HttpJobResponse>) = withRateLimitCheck "rate_limit:read:" (fun inst -> inst.ReadRateLimitsPerUser) f
    member this.PerformWriteJob (f : IFunDBAPI -> Task<HttpJobResponse>) = withRateLimitCheck "rate_limit:write:" (fun inst -> inst.WriteRateLimitsPerUser) f
