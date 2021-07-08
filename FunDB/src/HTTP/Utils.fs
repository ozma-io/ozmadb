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
open Npgsql

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.SQL.Query
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
    | [<CaseName("concurrent_update")>] REConcurrentUpdate
    with
        [<DataMember>]
        member this.Message =
            match this with
            | REInternal msg -> msg
            | RERequest msg -> msg
            | RENoEndpoint -> "API endpoint doesn't exist"
            | RENoInstance -> "Instance not found"
            | REAccessDenied -> "Database access denied"
            | REConcurrentUpdate -> "Concurrent update detected; try again"

let requestError e =
    let handler =
        match e with
        | REInternal _ -> ServerErrors.internalError
        | RERequest _ -> RequestErrors.badRequest
        | RENoEndpoint -> RequestErrors.notFound
        | RENoInstance -> RequestErrors.notFound
        | REAccessDenied _ -> RequestErrors.forbidden
        | REConcurrentUpdate _ -> ServerErrors.serviceUnavailable
    handler (json e)

let errorHandler (ex : Exception) (logger : ILogger) : HttpFunc -> HttpContext -> HttpFuncResult =
    logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> requestError (REInternal ex.Message)

let notFoundHandler : HttpFunc -> HttpContext -> HttpFuncResult = requestError RENoEndpoint

let tryBoolRequestArg (name : string) (ctx: HttpContext) : bool option =
    match ctx.Request.Query.TryGetValue name with
    | (true, values) when values.Count = 0 -> Some true
    | (true, values) -> Some <| Option.defaultValue false (Parsing.tryBool values.[0])
    | (false, _) -> None

let boolRequestArg (name : string) (ctx: HttpContext) : bool =
    Option.defaultValue false (tryBoolRequestArg name ctx)

let intRequestArg (name : string) (ctx: HttpContext) : int option =
    ctx.TryGetQueryStringValue name |> Option.bind Parsing.tryIntInvariant

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
        | :? JsonException as e -> Error <| exceptionString e
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
        | Error e -> return! requestError (RERequest <| exceptionString e) next ctx
        | Ok ret when isRefNull ret -> return! requestError (RERequest "Invalid JSON value") next ctx
        | Ok ret -> return! f ret next ctx
    }

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
    abstract member IsTemplate : bool
    abstract member AccessedAt : DateTime option

    abstract member UpdateAccessedAt : DateTime -> Task

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
    builder.IncludeErrorDetails <- true
#endif
    modify builder
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
            let anonymousCtx =
                { Source = instancesSource
                  Instance = instance
                  UserName = anonymousUsername
                  IsRoot = instance.DisableSecurity
                  CanRead = instance.IsTemplate
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
                              IsRoot = info.IsRoot
                              CanRead = instance.IsTemplate
                            }
                        f ictx
                    let failHandler =
                        if instance.IsTemplate then
                            f anonymousCtx
                        else
                            challenge JwtBearerDefaults.AuthenticationScheme
                    return! (requiresAuthentication failHandler >=> resolveUser getCtx) next ctx
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
            let connectionString = instanceConnectionString inst.Instance inst.Source.SetExtraConnectionOptions
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
                let longRunning =
                    if inst.IsRoot then
                        let headers = ctx.Request.Headers.["X-LongRunning"]
                        if headers.Count > 0 then
                            headers.[headers.Count - 1].ToLowerInvariant() = "yes"
                        else
                            false
                    else
                        false
                let cancellationToken =
                    if longRunning then
                        let lifetime = ctx.GetService<IHostApplicationLifetime>()
                        lifetime.ApplicationStopping
                    else
                        ctx.RequestAborted
                use! dbCtx = cacheStore.GetCache cancellationToken
                let! rctx =
                    RequestContext.Create
                        { UserName = inst.UserName
                          IsRoot = (inst.UserName = inst.Instance.Owner || inst.IsRoot)
                          CanRead = inst.CanRead
                          Language = lang
                          Context = dbCtx
                        }
                let currTime = DateTime.UtcNow
                match inst.Instance.AccessedAt with
                | Some prevTime when currTime - prevTime < randomAccessedAtLaxSpan () -> ()
                | _ ->  do! inst.Instance.UpdateAccessedAt currTime
                do! inst.Instance.DisposeAsync ()
                return! f (FunDBAPI rctx) next ctx
            with
            | :? ConcurrentUpdateException ->
                // We may want to retry here in future.
                return! requestError REConcurrentUpdate next ctx
            | :? RequestException as e ->
                match e.Info with
                | REUserNotFound
                | RENoRole -> return! requestError REAccessDenied next ctx
        }

    lookupInstance makeContext