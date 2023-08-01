module FunWithFlags.FunDB.HTTP.Utils

open System
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open System.Security.Claims
open Microsoft.Extensions.Primitives
open Microsoft.Extensions.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
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

[<SerializeAsObject("error")>]
type RequestErrorInfo =
    | [<CaseKey("internal")>] RIInternal
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] RIRequest of Details : string
    | [<CaseKey("rateExceeded", IgnoreFields=[|"Details"|])>] RIRateExceeded of Details : string
    | [<CaseKey("noEndpoint")>] RINoEndpoint
    | [<CaseKey("noInstance")>] RINoInstance
    | [<CaseKey("invalidRegion")>] RIWrongRegion of Region : string
    | [<CaseKey("unauthorized")>] RIUnauthorized
    | [<CaseKey("accessDenied")>] RIAccessDenied
    | [<CaseKey("concurrentUpdate")>] RIConcurrentUpdate
    | [<CaseKey("stackOverflow")>] RIStackOverflow of Trace : EventSource[]
    with
        member this.LogMessage =
            match this with
            | RIInternal -> "Internal server error"
            | RIRequest msg -> msg
            | RIRateExceeded msg -> msg
            | RINoEndpoint -> "API endpoint doesn't exist"
            | RINoInstance -> "Instance not found"
            | RIWrongRegion region -> sprintf "Wrong instance region, correct region: %s" region
            | RIUnauthorized -> "Failed to authorize using the access token"
            | RIAccessDenied -> "Access denied"
            | RIConcurrentUpdate -> "Concurrent update detected; try again"
            | RIStackOverflow sources ->
                sources
                    |> Seq.map (sprintf "in %O")
                    |> String.concat "\n"
                    |> sprintf "Stack depth exceeded:\n%s"

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | RIInternal _ -> 500
            | RIRateExceeded _ -> 429
            | RIRequest _ -> 400
            | RINoEndpoint -> 404
            | RINoInstance -> 404
            | RIWrongRegion _ -> 422
            | RIUnauthorized -> 401
            | RIAccessDenied _ -> 403
            | RIConcurrentUpdate _ -> 503
            | RIStackOverflow _ -> 500

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
    let handler body =
        match e.HTTPResponseCode with
        | 401 -> RequestErrors.unauthorized JwtBearerDefaults.AuthenticationScheme "fundb" body
        | code -> setStatusCode code >=> body
    setHttpHeader "X-FunDB-Error" e.Error >=> handler (json e)

let errorHandler (e : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
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

let tryBoolHeaderArg (name : string) (ctx : HttpContext) : bool option =
    match ctx.Request.Headers.TryGetValue name with
    | (true, values) when values.Count = 0 -> None
    | (true, values) -> Parsing.tryBool (Seq.last values)
    | (false, _) -> None

let boolHeaderArg (name : string) (ctx: HttpContext) : bool =
    Option.defaultValue false (tryBoolHeaderArg name ctx)

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
        | Error e -> return! requestError (RIRequest <| fullUserMessage e) next ctx
        | Ok ret when isRefNull ret -> return! requestError (RIRequest "Invalid JSON value") next ctx
        | Ok ret -> return! f ret next ctx
    }

let commitAndReturn (handler : HttpHandler) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    task {
        match! api.Request.Context.Commit () with
        | Error e -> return! requestError e next ctx
        | Ok () -> return! Successful.ok handler next ctx
    }

let commitAndOk : IFunDBAPI -> HttpFunc -> HttpContext -> HttpFuncResult = commitAndReturn (json Map.empty)

let handleRequestWithCommit<'Response, 'Error when 'Error :> IErrorDetails> (api : IFunDBAPI) (ret : Task<Result<'Response, 'Error>>) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match! ret with
        | Ok ret -> return! commitAndReturn (json ret) api next ctx
        | Error e -> return! requestError e next ctx
    }

let handleRequest<'Response, 'Error when 'Error :> IErrorDetails> (ret : Task<Result<'Response, 'Error>>) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match! ret with
        | Ok ret -> return! Successful.ok (json ret) next ctx
        | Error e -> return! requestError e next ctx
    }

let handleVoidRequestWithCommit<'Error when 'Error :> IErrorDetails> (api : IFunDBAPI) (ret : Task<Result<unit, 'Error>>) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match! ret with
        | Ok ret -> return! commitAndOk api next ctx
        | Error e -> return! requestError e next ctx
    }

let handleVoidRequest<'Error when 'Error :> IErrorDetails> (ret : Task<Result<unit, 'Error>>) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match! ret with
        | Ok ret -> return! Successful.ok (json ret) next ctx
        | Error e -> return! requestError e next ctx
    }

let setPretendRole (api : IFunDBAPI) (pretendRole : ResolvedEntityRef option) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    match pretendRole with
    | None -> next ctx
    | Some role ->
        task {
            match! api.Request.PretendRole { AsRole = PRRole role } (fun () -> next ctx) with
            | Ok r -> return r
            | Error e -> return! requestError e earlyReturn ctx
        }

let setPretendUser (api : IFunDBAPI) (pretendUser : UserName option) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
    match pretendUser with
    | None -> next ctx
    | Some userName ->
        task {
            match! api.Request.PretendUser { AsUser = userName } (fun () -> next ctx) with
            | Ok r -> return r
            | Error e -> return! requestError e earlyReturn ctx
        }

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

let inline addContext (name : string) (value : string) ([<InlineIfLambda>] f : HttpHandler) (next : HttpFunc) (ctx: HttpContext) =
    task {
        let diagnostic = ctx.GetService<IDiagnosticContext>()
        use _ = LogContext.PushProperty(name, value)
        diagnostic.Set(name, value)
        try
            return! f next ctx
        with
        | ex ->
            let logger = ctx.GetLogger("runWithApi")
            return! errorHandler ex logger next ctx
    }

let private getUserTokenInfo (client : string) (email : string option) (ctx : HttpContext) : UserTokenInfo =
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

let resolveUser (f : UserTokenInfo -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    let client = ctx.User.FindFirstValue "azp"
    if isNull client then
        requestError (RIRequest "No azp claim in security token") next ctx
    else
        let resolveEmail (next : HttpFunc) (ctx : HttpContext) =
            let emailClaim = ctx.User.FindFirst ClaimTypes.Email
            let email =
                if isNull emailClaim then None else Some emailClaim.Value

            let setUserInfo (next : HttpFunc) (ctx : HttpContext) =
                let info = getUserTokenInfo client email ctx
                f info next ctx

            match email with
            | None -> setUserInfo next ctx
            | Some emailValue -> addContext "Email" emailValue setUserInfo next ctx

        addContext "Client" client resolveEmail next ctx

let private getInstanceContext (instancesSource : IInstancesSource) (instance : IInstance) (info : UserTokenInfo) = 
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

let private getAnonymousInstanceContext (instancesSource : IInstancesSource) (instance : IInstance) = 
    { Source = instancesSource
      Instance = instance
      UserName = anonymousUsername
      IsRoot = instance.DisableSecurity
      IsGlobalAdmin = false
      CanRead = instance.AnyoneCanRead
    }

let lookupInstance (f : InstanceContext -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    let instanceName =
        match ctx.Request.Headers.TryGetValue("X-FunDB-Instance") with
        | (true, xInstance) when not <| Seq.isEmpty xInstance -> Seq.last xInstance
        | _ -> ctx.Request.Host.Host

    let doLookup (next : HttpFunc) (ctx : HttpContext) =
        task {
            let instancesSource = ctx.GetService<IInstancesSource>()
            match! instancesSource.GetInstance instanceName ctx.RequestAborted with
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
                    if instance.DisableSecurity then
                        let ictx = getAnonymousInstanceContext instancesSource instance
                        return! f ictx next ctx
                    else
                        let failHandler =
                            if instance.AnyoneCanRead then
                                let ictx = getAnonymousInstanceContext instancesSource instance
                                f ictx
                            else
                                challenge JwtBearerDefaults.AuthenticationScheme

                        let proceed (info : UserTokenInfo) =
                            let ictx = getInstanceContext instancesSource instance info
                            f ictx

                        return! (requiresAuthentication failHandler >=> resolveUser proceed) next ctx
        }

    addContext "Instance" instanceName doLookup next ctx

let inline private setupRequestTimeout (inst : InstanceContext) (dbCtx : IContext) ([<InlineIfLambda>] f : HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    task {
        match inst.Instance.MaxRequestTime with
        | None -> return! f next ctx
        | Some maxTime ->
            let oldToken = dbCtx.CancellationToken
            use newToken = new CancellationTokenSource()
            ignore <| oldToken.Register(fun () -> newToken.Cancel())
            dbCtx.CancellationToken <- newToken.Token
            newToken.CancelAfter(int maxTime.TotalMilliseconds)
            try
                return! f next ctx
            with
            | Exn.Innermost (:? OperationCanceledException as e) when not oldToken.IsCancellationRequested ->
                return! requestError (GEQuotaExceeded "Max request time reached") next ctx
    }

let private randomAccessedAtLaxSpan () =
    // A minute +/- ~5 seconds.
    let minutes = 1.0 + (2.0 * Random.Shared.NextDouble() - 0.5) * 0.09
    Duration.FromMilliseconds(int64 (minutes * 60.0 * 1000.0))

let private getFunDBAPI (touchAccessedAt : bool) (inst : InstanceContext) (dbCtx : IContext) (ctx : HttpContext) =
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
            | Some prevTime when currTime - prevTime < randomAccessedAtLaxSpan () ->
                inst.Instance.Dispose ()
            | _ ->
                inst.Instance.UpdateAccessedAtAndDispose currTime
        return FunDBAPI rctx
    }

let private getDbContext (inst : InstanceContext) (ctx : HttpContext) =
    task {
        let logger = ctx.GetLogger("getDbContext")
        let connectionString = instanceConnectionString inst.Instance inst.Source.SetExtraConnectionOptions
        let instancesCache = ctx.GetService<InstancesCacheStore>()
        let! cacheStore = instancesCache.GetContextCache(connectionString)

        let longRunning =
            if inst.IsGlobalAdmin then
                boolHeaderArg "X-FunDB-LongRunning" ctx
            else
                false
        let initialCancellationToken =
            if longRunning then
                let lifetime = ctx.GetService<IHostApplicationLifetime>()
                lifetime.ApplicationStopping
            else
                ctx.RequestAborted

        return! cacheStore.GetCache initialCancellationToken
    }

let private runWithApi (touchAccessedAt : bool) (inst : InstanceContext) (f : IFunDBAPI -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    task {
        if not inst.Instance.Published && not inst.IsRoot then
            return! requestError RIAccessDenied next ctx
        else
            let logger = ctx.GetLogger("runWithApi")
            use _ = logger.BeginScope("Creating context for instance {instance}, user {user} (is root: {is_root})", inst.Instance.Name, inst.UserName, inst.IsRoot)
            try
                use! dbCtx = getDbContext inst ctx
                let proceed (next : HttpFunc) (ctx : HttpContext) =
                    task {
                        let! api = getFunDBAPI touchAccessedAt inst dbCtx ctx
                        return! f api next ctx
                    }
                return! setupRequestTimeout inst dbCtx proceed next ctx
            with
            | :? ConcurrentUpdateException as e ->
                logger.LogError(e, "Concurrent update exception")
                // We may want to retry here in future.
                return! requestError RIConcurrentUpdate next ctx
            | :? DatabaseAccessDeniedException as e ->
                logger.LogError(e, "Database access denied")
                return! requestError RIAccessDenied next ctx
            | :? RequestStackOverflowException as topE ->
                let rec findAllSources (e : RequestStackOverflowException) =
                    seq {
                        yield e.Source
                        match e.InnerException with
                        | :? RequestStackOverflowException as e -> yield! findAllSources e
                        | _ -> ()
                    }
                let allSources = findAllSources topE |> Seq.toArray
                return! requestError (RIStackOverflow allSources) next ctx
    }

let withContextHidden (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    lookupInstance (fun inst -> runWithApi false inst f)

let private rateExceeded msg = requestError (RIRateExceeded msg)

let private getIpAddress (ctx : HttpContext) =
    match ctx.Request.Headers.TryGetValue("X-Real-IP") with
    | (true, ip) -> Seq.last ip
    | (false, _) -> string ctx.Connection.RemoteIpAddress

let private withContextLimited (prefix : string) (getLimits : IInstance -> RateLimit seq) (f : IFunDBAPI -> HttpHandler) : HttpHandler =
    lookupInstance <| fun inst next ctx ->
        let userId =
            match ctx.User.FindFirstValue ClaimTypes.NameIdentifier with
            | null -> getIpAddress ctx
            | userId -> userId
        let run = runWithApi true inst f next
        checkRateLimit rateExceeded (prefix + userId) (getLimits inst.Instance) run ctx

let withContextRead (f : IFunDBAPI -> HttpHandler) = withContextLimited "rate_limit|read|" (fun inst -> inst.ReadRateLimitsPerUser) f
let withContextWrite (f : IFunDBAPI -> HttpHandler) = withContextLimited "rate_limit|write|" (fun inst -> inst.WriteRateLimitsPerUser) f
