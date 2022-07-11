open System
open System.IO
open System.Threading
open System.Collections.Generic
open Newtonsoft.Json
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Builder
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
open NodaTime
open Npgsql
open NetJs.Json
open Serilog
open Serilog.AspNetCore
open Serilog.Events
open Serilog.Formatting.Compact
open Microsoft.Extensions.Logging

open FunWithFlags.FunDB.FunQL.Json
open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunUtils
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

let private parseLimits : IList<FunWithFlags.FunDBSchema.Instances.RateLimit> -> RateLimit seq = function
    | null -> Seq.empty
    | limits -> limits |> Seq.map (fun limit -> { Period = limit.Period; Limit = limit.Limit })

type private DatabaseInstances (loggerFactory : ILoggerFactory, connectionString : string) =
    let connectionString =
        let builder = NpgsqlConnectionStringBuilder(connectionString)
        builder.Enlist <- false
        string builder

    interface IInstancesSource with
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) =
            task {
                let instances =
                    let systemOptions =
                        (DbContextOptionsBuilder<InstancesContext> ())
                            .UseLoggerFactory(loggerFactory)
                            .UseNpgsql(connectionString, fun opts -> ignore <| opts.UseNodaTime())
                    new InstancesContext(systemOptions.Options)
                try
                    match! instances.Instances.FirstOrDefaultAsync((fun x -> x.Name = host && x.Enabled), cancellationToken) with
                    | null ->
                        do! instances.DisposeAsync ()
                        return None
                    | instance ->
                        let parsedRead = parseLimits instance.ReadRateLimitsPerUser
                        let parsedWrite = parseLimits instance.WriteRateLimitsPerUser
                        let obj =
                            { new IInstance with
                                  member this.Name = instance.Name
                                  member this.Owner = instance.Owner
                                  member this.Host = instance.Host
                                  member this.Port = instance.Port
                                  member this.Username = instance.Username
                                  member this.Password = instance.Password
                                  member this.Database = instance.Database

                                  member this.DisableSecurity = instance.DisableSecurity
                                  member this.AnyoneCanRead = instance.AnyoneCanRead
                                  member this.Published = instance.Published
                                  member this.AccessedAt = Option.ofNullable instance.AccessedAt

                                  member this.MaxSize = Option.ofNullable instance.MaxSize
                                  member this.MaxUsers = Option.ofNullable instance.MaxUsers
                                  member this.MaxRequestTime = Option.ofNullable instance.MaxRequestTime
                                  member this.ReadRateLimitsPerUser = parsedRead
                                  member this.WriteRateLimitsPerUser = parsedWrite

                                  member this.UpdateAccessedAt newTime =
                                      unitTask {
                                          instance.AccessedAt <- newTime
                                          let! _ = instances.SaveChangesAsync ()
                                          ()
                                      }

                                  member this.Dispose () = instances.Dispose ()
                                  member this.DisposeAsync () = instances.DisposeAsync ()
                            }
                        return Some obj
                with
                | e ->
                    do! instances.DisposeAsync ()
                    return reraise' e
            }

        member this.SetExtraConnectionOptions (builder : NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            builder.ConnectionIdleLifetime <- 30
            builder.MaxAutoPrepare <- 50

type private StaticInstance (instance : Instance) =
    interface IInstancesSource with
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) =
            let parsedRead = parseLimits instance.ReadRateLimitsPerUser
            let parsedWrite = parseLimits instance.WriteRateLimitsPerUser
            let obj =
                { new IInstance with
                      member this.Name = instance.Name
                      member this.Owner = instance.Owner
                      member this.Host = instance.Host
                      member this.Port = instance.Port
                      member this.Username = instance.Username
                      member this.Password = instance.Password
                      member this.Database = instance.Database

                      member this.Published = instance.Published
                      member this.DisableSecurity = instance.DisableSecurity
                      member this.AnyoneCanRead = instance.AnyoneCanRead
                      member this.AccessedAt = Some <| SystemClock.Instance.GetCurrentInstant()

                      member this.MaxSize = Option.ofNullable instance.MaxSize
                      member this.MaxUsers = Option.ofNullable instance.MaxUsers
                      member this.MaxRequestTime = Option.ofNullable instance.MaxRequestTime
                      member this.ReadRateLimitsPerUser = parsedRead
                      member this.WriteRateLimitsPerUser = parsedWrite

                      member this.UpdateAccessedAt newTime = unitTask { () }

                      member this.Dispose () = ()
                      member this.DisposeAsync () = unitVtask { () }
                }
            Task.result (Some obj)

        member this.SetExtraConnectionOptions (builder : NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            ()

let private webApp : HttpHandler =
    choose
        [ viewsApi
          entitiesApi
          saveRestoreApi
          actionsApi
          infoApi
          permissionsApi
          domainsApi
          notFoundHandler
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

let private setupLogging (webAppBuilder : WebApplicationBuilder) =
    let config = webAppBuilder.Configuration

    let configureSerilog (context : HostBuilderContext) (services : IServiceProvider) (configuration : LoggerConfiguration) =
        ignore <| configuration
            .ReadFrom.Configuration(config)
            .ReadFrom.Services(services)
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
            .Enrich.FromLogContext()
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
            let ret = Map.ofSeq (seq {
                ("error", "unauthorized")
                ("message", "Failed to authorize using access token")
            })
            ctx.Response.WriteAsync(JsonConvert.SerializeObject(ret))

    ignore <| services
        .AddGiraffe()
        .AddCors()
    if not <| fundbSection.GetValue("DisableSecurity", false) then
        ignore <| services
            .AddAuthentication(configureAuthentication)
            .AddJwtBearer(configureJwtBearer)

let private setupRateLimiting (webAppBuilder : WebApplicationBuilder) =
    let services = webAppBuilder.Services

    match webAppBuilder.Configuration.GetValue("Redis") with
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
    let config = webAppBuilder.Configuration
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
    let config = webAppBuilder.Configuration
    let services = webAppBuilder.Services

    let getInstancesSource (sp : IServiceProvider) : IInstancesSource =
        match fundbSection.["InstancesSource"] with
        | "database" ->
            let instancesConnectionString = config.GetConnectionString("Instances")
            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            DatabaseInstances(logFactory, instancesConnectionString) :> IInstancesSource
        | "static" ->
            let instance = Instance(
                Owner = "owner@example.com"
            )
            config.GetSection("Instance").Bind(instance)
            if isNull instance.Database then
                instance.Database <- instance.Username
            StaticInstance(instance) :> IInstancesSource
        | _ -> failwith "Invalid InstancesSource"
    ignore <| services.AddSingleton<IInstancesSource>(getInstancesSource)

let private setupApp (app : IApplicationBuilder) =
    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    ignore <| app
        .UseSerilogRequestLogging()
        .UseHttpMethodOverride()
        .UseCors(configureCors)
        .UseAuthentication()
        // We hack into AspNetCoreRateLimit later, don't add middleware here!
        // .UseClientRateLimiting()
        .UseGiraffeErrorHandler(errorHandler)
        .UseGiraffe(webApp)

[<EntryPoint>]
let main (args : string[]) : int =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .Enrich.FromLogContext()
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