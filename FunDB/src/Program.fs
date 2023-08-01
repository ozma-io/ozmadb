open System
open System.IO
open System.Threading
open System.Linq
open System.Linq.Expressions
open System.Collections.Generic
open Newtonsoft.Json
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Routing
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
open Microsoft.IdentityModel.Tokens
open Microsoft.EntityFrameworkCore
open AspNetCoreRateLimit
open AspNetCoreRateLimit.Redis
open StackExchange
open Giraffe
open Giraffe.EndpointRouting
open NodaTime
open Npgsql
open NetJs.Json
open Serilog
open Serilog.Events
open Microsoft.Extensions.Logging
open Prometheus

open FunWithFlags.FunDB.FunQL.Json
open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.HTTP.Info
open FunWithFlags.FunDB.HTTP.Views
open FunWithFlags.FunDB.HTTP.Entities
open FunWithFlags.FunDB.HTTP.SaveRestore
open FunWithFlags.FunDB.HTTP.Actions
open FunWithFlags.FunDB.HTTP.Permissions
open FunWithFlags.FunDB.HTTP.Domains
open FunWithFlags.FunDB.HTTP.RateLimit
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.EventLogger

// Npgsql global type mapping is deprecated, but we are not sure how to make it better now.
#nowarn "44"

let private parseLimits : IList<FunWithFlags.FunDBSchema.Instances.RateLimit> -> RateLimit seq = function
    | null -> Seq.empty
    | limits -> limits |> Seq.map (fun limit -> { Period = limit.Period; Limit = limit.Limit })

let private deenlistConnectionString (str : string) =
    let builder = NpgsqlConnectionStringBuilder(str)
    builder.Enlist <- false
    string builder

type private DatabaseInstances (loggerFactory : ILoggerFactory, homeRegion: string option, connectionString : string, readOnlyConnectionString : string) =
    let connectionString = deenlistConnectionString connectionString
    let readOnlyConnectionString = deenlistConnectionString readOnlyConnectionString

    new (loggerFactory : ILoggerFactory, homeRegion: string option, connectionString : string) = DatabaseInstances (loggerFactory, homeRegion, connectionString, connectionString)

    interface IInstancesSource with
        member this.Region = homeRegion
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) =
            task {
                let readOnlyInstances =
                    let builder = DbContextOptionsBuilder<InstancesContext> ()
                    setupDbContextLogging loggerFactory builder
                    ignore <| builder.UseNpgsql(readOnlyConnectionString, fun opts -> ignore <| opts.UseNodaTime())
                    new InstancesContext(builder.Options)
                try
                    let! result = readOnlyInstances.Instances.AsNoTracking().FirstOrDefaultAsync((fun x -> x.Name = host && x.Enabled), cancellationToken)
                    do! readOnlyInstances.DisposeAsync ()
                    match result with
                    | null -> return None
                    | instance ->
                        let parsedRead = parseLimits instance.ReadRateLimitsPerUser
                        let parsedWrite = parseLimits instance.WriteRateLimitsPerUser
                        let obj =
                            { new IInstance with
                                member this.Name = instance.Name

                                member this.Region = Option.ofObj instance.Region
                                member this.Host = instance.Host
                                member this.Port = instance.Port
                                member this.Username = instance.Username
                                member this.Password = instance.Password
                                member this.Database = instance.Database

                                member this.Owner = instance.Owner
                                member this.DisableSecurity = instance.DisableSecurity
                                member this.AnyoneCanRead = instance.AnyoneCanRead
                                member this.Published = instance.Published
                                member this.ShadowAdmins = instance.ShadowAdmins

                                member this.MaxSize = Option.ofNullable instance.MaxSize
                                member this.MaxUsers = Option.ofNullable instance.MaxUsers
                                member this.MaxRequestTime = Option.ofNullable instance.MaxRequestTime
                                member this.ReadRateLimitsPerUser = parsedRead
                                member this.WriteRateLimitsPerUser = parsedWrite

                                member this.AccessedAt = Option.ofNullable instance.AccessedAt

                                member this.UpdateAccessedAtAndDispose newTime =
                                    ignore <|
                                        unitTask {
                                            let instances =
                                                let builder = DbContextOptionsBuilder<InstancesContext> ()
                                                setupDbContextLogging loggerFactory builder
                                                ignore <| builder.UseNpgsql(connectionString, fun opts -> ignore <| opts.UseNodaTime())
                                                new InstancesContext(builder.Options)
                                            let! _ =
                                                (instances.Instances :> IQueryable<Instance>)
                                                    .Where(fun inst -> inst.Id = instance.Id && (not inst.AccessedAt.HasValue || inst.AccessedAt.Value < newTime))
                                                    .UpdateFromQueryAsync(fun (inst : Instance) ->
                                                        inst.AccessedAt <- newTime
                                                        inst
                                                    )
                                            do! instances.DisposeAsync ()
                                            ()
                                        }

                                  member this.Dispose () = ()
                            }
                        return Some obj
                with
                | e ->
                    do! readOnlyInstances.DisposeAsync ()
                    return reraise' e
            }

        member this.SetExtraConnectionOptions (builder : NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            builder.ConnectionIdleLifetime <- 30
            builder.MaxAutoPrepare <- 50

type private StaticInstance (instance : Instance, homeRegion: string option) =
    interface IInstancesSource with
        member this.Region = homeRegion
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) =
            let parsedRead = parseLimits instance.ReadRateLimitsPerUser
            let parsedWrite = parseLimits instance.WriteRateLimitsPerUser
            let obj =
                { new IInstance with
                    member this.Name = instance.Name

                    member this.Region = Option.ofObj instance.Region
                    member this.Host = instance.Host
                    member this.Port = instance.Port
                    member this.Username = instance.Username
                    member this.Password = instance.Password
                    member this.Database = instance.Database

                    member this.Published = instance.Published
                    member this.Owner = instance.Owner
                    member this.DisableSecurity = instance.DisableSecurity
                    member this.AnyoneCanRead = instance.AnyoneCanRead
                    member this.AccessedAt = Some <| SystemClock.Instance.GetCurrentInstant()
                    member this.ShadowAdmins = Seq.empty

                    member this.MaxSize = Option.ofNullable instance.MaxSize
                    member this.MaxUsers = Option.ofNullable instance.MaxUsers
                    member this.MaxRequestTime = Option.ofNullable instance.MaxRequestTime
                    member this.ReadRateLimitsPerUser = parsedRead
                    member this.WriteRateLimitsPerUser = parsedWrite

                    member this.UpdateAccessedAtAndDispose newTime = ()

                    member this.Dispose () = ()
                }
            Task.result (Some obj)

        member this.SetExtraConnectionOptions (builder : NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            ()

let private appEndpoints : Endpoint list =
    List.concat
        [ viewsApi
          entitiesApi
          saveRestoreApi
          actionsApi
          infoApi
          permissionsApi
          domainsApi
        ]

let private isDebug =
#if DEBUG
    true
#else
    false
#endif

let private setupConfiguration (args : string[]) (webAppBuilder : WebApplicationBuilder) =
    // Configuration.
    let configPath = args.[0]
    ignore <| webAppBuilder.Configuration
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile(configPath, false, isDebug)
        .AddEnvironmentVariables()

let private setupLoggerConfiguration (configuration : LoggerConfiguration) =
    ignore <| configuration
        .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        // We really, really don't use DataProtection.
        .MinimumLevel.Override("Microsoft.AspNetCore.DataProtection", LogEventLevel.Error)
        .Enrich.FromLogContext()

let private setupLogging (webAppBuilder : WebApplicationBuilder) =
    let config = webAppBuilder.Configuration

    let configureSerilog (context : HostBuilderContext) (services : IServiceProvider) (configuration : LoggerConfiguration) =
        setupLoggerConfiguration configuration
        ignore <| configuration
            .ReadFrom.Configuration(config)
            .ReadFrom.Services(services)
        if not <| config.GetSection("Serilog").GetSection("WriteTo").Exists() then
            ignore <| configuration.WriteTo.Console()

    ignore <| webAppBuilder.Host.UseSerilog(configureSerilog)

let private setupAuthentication (webAppBuilder : WebApplicationBuilder) =
    let fundbSection = webAppBuilder.Configuration.GetSection("FunDB")
    let services = webAppBuilder.Services

    // Auth.
    let configureAuthentication (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let configureJwtBearer (cfg : JwtBearerOptions) =
        cfg.Authority <- fundbSection.["AuthAuthority"]
        cfg.TokenValidationParameters <- TokenValidationParameters (
            ValidateAudience = false
        )
        cfg.Events <- JwtBearerEvents()
        // https://stackoverflow.com/questions/48649717/addjwtbearer-onauthenticationfailed-return-custom-error
        cfg.Events.OnChallenge <- fun ctx ->
            ctx.HandleResponse ()
            ctx.Response.StatusCode <- StatusCodes.Status401Unauthorized
            ctx.Response.ContentType <- "application/json"
            ctx.Response.Headers.WWWAuthenticate <- JwtBearerDefaults.AuthenticationScheme
            ctx.Response.WriteAsync(JsonConvert.SerializeObject(RIUnauthorized))

    ignore <| services
        .AddGiraffe()
        .AddCors()
    if not <| fundbSection.GetValue("DisableSecurity", false) then
        ignore <| services
            .AddAuthentication(configureAuthentication)
            .AddJwtBearer(configureJwtBearer)

let private setupRateLimiting (webAppBuilder : WebApplicationBuilder) =
    let fundbSection = webAppBuilder.Configuration.GetSection("FunDB")
    let services = webAppBuilder.Services

    match fundbSection.["Redis"] with
    | null ->
        ignore <| services
            .AddMemoryCache()
            .AddInMemoryRateLimiting()
    | redisStr ->
        let redisOptions = Redis.ConfigurationOptions.Parse(redisStr)
        let getRedisMultiplexer (sp : IServiceProvider) =
            Redis.ConnectionMultiplexer.Connect(redisOptions) :> Redis.IConnectionMultiplexer
        ignore <| services
            .AddSingleton<Redis.IConnectionMultiplexer>(getRedisMultiplexer)
            .AddRedisRateLimiting()
    // Needed for the strategy, but we ignore it in RateLimit.fs. Painful...
    ignore <| services.AddSingleton<IRateLimitConfiguration, RateLimitConfiguration>();

let private setupJSON (webAppBuilder : WebApplicationBuilder) =
    ignore <| webAppBuilder.Services.AddSingleton<Json.ISerializer>(NewtonsoftJson.Serializer defaultJsonSettings)

let private setupEventLogger (webAppBuilder : WebApplicationBuilder) =
    let services = webAppBuilder.Services

    let getEventLogger (sp : IServiceProvider) =
        let logFactory = sp.GetRequiredService<ILoggerFactory>()
        new EventLogger(logFactory)
    // https://stackoverflow.com/a/59089881
    ignore <| services.AddSingleton<EventLogger>(getEventLogger)
    ignore <| services.AddHostedService(fun sp -> sp.GetRequiredService<EventLogger>())

let private setupInstancesCache (webAppBuilder : WebApplicationBuilder) =
    let fundbSection = webAppBuilder.Configuration.GetSection("FunDB")
    let services = webAppBuilder.Services

    let sourcePreload =
        match fundbSection.GetValue("Preloads") with
        | null -> emptySourcePreloadFile
        | path -> readSourcePreload path
    let preload = resolvePreload sourcePreload

    let makeInstancesStore (sp : IServiceProvider) =
        let cacheParams =
            { Preload = preload
              LoggerFactory = sp.GetRequiredService<ILoggerFactory>()
              EventLogger = sp.GetRequiredService<EventLogger>()
              AllowAutoMark = fundbSection.GetValue("AllowAutoMark", false)
            }
        InstancesCacheStore cacheParams
    ignore <| services.AddSingleton<InstancesCacheStore>(makeInstancesStore)

let private setupInstancesSource (webAppBuilder : WebApplicationBuilder) =
    let fundbSection = webAppBuilder.Configuration.GetSection("FunDB")
    let services = webAppBuilder.Services

    let getInstancesSource (sp : IServiceProvider) : IInstancesSource =
        let homeRegion = Option.ofObj <| fundbSection.GetValue("HomeRegion", null)
        match fundbSection.["InstancesSource"] with
        | "database" ->
            let instancesConnectionString = fundbSection.GetConnectionString("Instances")
            let instancesReadOnlyConnectionString =
                fundbSection.GetConnectionString("InstancesReadOnly")
                |> Option.ofObj
                |> Option.defaultValue instancesConnectionString
            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            DatabaseInstances(logFactory, homeRegion, instancesConnectionString, instancesReadOnlyConnectionString) :> IInstancesSource
        | "static" ->
            let instance = Instance(
                Owner = "owner@example.com"
            )
            fundbSection.GetSection("Instance").Bind(instance)
            if isNull instance.Database then
                instance.Database <- instance.Username
            StaticInstance(instance, homeRegion) :> IInstancesSource
        | _ -> failwith "Invalid InstancesSource"
    ignore <| services.AddSingleton<IInstancesSource>(getInstancesSource)

let private setupApp (app : IApplicationBuilder) =
    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    let configureEndpoints (endpoints : IEndpointRouteBuilder) =
        ignore <| endpoints.MapMetrics()
        ignore <| endpoints.MapGiraffeEndpoints(appEndpoints)

    ignore <| app
        .UseSerilogRequestLogging()
        .UseHttpMetrics()
        .UseHttpMethodOverride()
        .UseCors(configureCors)
        .UseAuthentication()
        // We hack into AspNetCoreRateLimit later, don't add middleware here!
        // .UseClientRateLimiting()
        .UseGiraffeErrorHandler(errorHandler)
        .UseRouting()
        .UseEndpoints(configureEndpoints)
        .UseGiraffe(notFoundHandler)

[<EntryPoint>]
let main (args : string[]) : int =
    let loggerConfig = LoggerConfiguration()
    setupLoggerConfiguration loggerConfig
    Log.Logger <-
        loggerConfig
            .WriteTo.Console()
            .CreateBootstrapLogger()

    try
        try
            // Register a global converter to have nicer native F# types JSON conversion.
            JsonConvert.DefaultSettings <- fun () -> defaultJsonSettings
            // Enable JSON and NodaTime for PostgreSQL.
            ignore <| NpgsqlConnection.GlobalTypeMapper.UseJsonNet()
            ignore <| NpgsqlConnection.GlobalTypeMapper.UseNodaTime()
            // Use JavaScript Date objects.
            V8SerializationSettings.Default.UseDate <- true

            let webAppBuilder = WebApplication.CreateBuilder()
            setupConfiguration args webAppBuilder
            setupLogging webAppBuilder
            setupAuthentication webAppBuilder
            setupRateLimiting webAppBuilder
            setupJSON webAppBuilder
            setupEventLogger webAppBuilder
            setupInstancesCache webAppBuilder
            setupInstancesSource webAppBuilder

            let app = webAppBuilder.Build()
            setupApp app

            app.Run()

            0
        with
        | e ->
            Log.Fatal(e, "Terminated unexpectedly")
            1
    finally
        Log.CloseAndFlush()
