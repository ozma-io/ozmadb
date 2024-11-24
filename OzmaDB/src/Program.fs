open System
open System.IO
open System.Threading
open System.Threading.Tasks
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
open Microsoft.IdentityModel.Tokens
open Microsoft.EntityFrameworkCore
open AspNetCoreRateLimit
open AspNetCoreRateLimit.Redis
open StackExchange
open Giraffe
open Giraffe.EndpointRouting
open NodaTime
open Npgsql
open Serilog
open Serilog.Events
open Serilog.Settings.Configuration
open Microsoft.Extensions.Logging
open Prometheus
open Prometheus.HttpMetrics

open OzmaDB.OzmaQL.Json
open OzmaDBSchema
open OzmaDBSchema.Instances
open OzmaDB.OzmaUtils
open OzmaDB.Connection
open OzmaDB.HTTP.Info
open OzmaDB.HTTP.Views
open OzmaDB.HTTP.Entities
open OzmaDB.HTTP.SaveRestore
open OzmaDB.HTTP.Actions
open OzmaDB.HTTP.Permissions
open OzmaDB.HTTP.Domains
open OzmaDB.HTTP.RateLimit
open OzmaDB.HTTP.Utils
open OzmaDB.Operations.Preload
open OzmaDB.API.InstancesCache
open OzmaDB.EventLogger

// Npgsql global type mapping is deprecated, but we are not sure how to make it better now.
#nowarn "44"

let private parseLimits: IList<OzmaDBSchema.Instances.RateLimit> -> RateLimit seq =
    function
    | null -> Seq.empty
    | limits ->
        limits
        |> Seq.map (fun limit ->
            { Period = limit.Period
              Limit = limit.Limit })

let private deenlistConnectionString (str: string) =
    let builder = NpgsqlConnectionStringBuilder(str)
    builder.Enlist <- false
    string builder

type private DatabaseInstances
    (
        loggerFactory: ILoggerFactory,
        lifetime: IHostApplicationLifetime,
        homeRegion: string option,
        connectionString: string,
        readOnlyConnectionString: string
    ) =
    let connectionString = deenlistConnectionString connectionString
    let readOnlyConnectionString = deenlistConnectionString readOnlyConnectionString
    let logger = loggerFactory.CreateLogger<DatabaseInstances>()

    new
        (
            loggerFactory: ILoggerFactory,
            lifetime: IHostApplicationLifetime,
            homeRegion: string option,
            connectionString: string
        ) =
        DatabaseInstances(loggerFactory, lifetime, homeRegion, connectionString, connectionString)

    interface IInstancesSource with
        member this.Region = homeRegion

        member this.GetInstance (host: string) (cancellationToken: CancellationToken) =
            task {
                let readOnlyInstances =
                    let builder = DbContextOptionsBuilder<InstancesContext>()
                    setupDbContextLogging loggerFactory builder

                    ignore
                    <| builder.UseNpgsql(readOnlyConnectionString, (fun opts -> ignore <| opts.UseNodaTime()))

                    new InstancesContext(builder.Options)

                try
                    let! result =
                        readOnlyInstances.Instances
                            .AsNoTracking()
                            .FirstOrDefaultAsync((fun x -> x.Name = host && x.Enabled), cancellationToken)

                    do! readOnlyInstances.DisposeAsync()

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

                                member this.MaxJSHeapSize = Option.ofNullable instance.MaxJSHeapSize
                                member this.MaxJSStackSize = Option.ofNullable instance.MaxJSStackSize

                                member this.AccessedAt = Option.ofNullable instance.AccessedAt

                                member this.UpdateAccessedAtAndDispose(newTime: Instant) =
                                    let updateJob () : Task =
                                        task {
                                            try
                                                use instances =
                                                    let builder = DbContextOptionsBuilder<InstancesContext>()
                                                    setupDbContextLogging loggerFactory builder

                                                    ignore
                                                    <| builder.UseNpgsql(
                                                        connectionString,
                                                        fun opts -> ignore <| opts.UseNodaTime()
                                                    )

                                                    new InstancesContext(builder.Options)

                                                let! _ =
                                                    instances.Instances
                                                        // This doesn't work: https://github.com/dotnet/efcore/issues/14013
                                                        // .Where(fun inst -> inst.Id = instance.Id && (not inst.AccessedAt.HasValue || inst.AccessedAt.Value < newTime))
                                                        .FromSql(
                                                            $"""SELECT * FROM "instances" WHERE "id" = {instance.Id} AND ("accessed_at" IS NULL OR "accessed_at" < {newTime})"""
                                                        )
                                                        .ExecuteUpdateAsync(
                                                            (fun inst ->
                                                                inst.SetProperty(
                                                                    (fun inst -> inst.AccessedAt),
                                                                    (fun inst -> Nullable(newTime))
                                                                )),
                                                            lifetime.ApplicationStopping
                                                        )

                                                ()
                                            with e ->
                                                logger.LogError(e, "Failed to update AccessedAt")
                                        }

                                    ignore <| Threading.Tasks.Task.Run(updateJob)

                                member this.Dispose() = () }

                        return Some obj
                with e ->
                    do! readOnlyInstances.DisposeAsync()
                    return reraise' e
            }

        member this.SetExtraConnectionOptions(builder: NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            builder.ConnectionIdleLifetime <- 30
            builder.MaxAutoPrepare <- 50

type private StaticInstance
    (instances: Map<string, Instance>, defaultInstance: Instance option, homeRegion: string option) =
    let getInstance (instance: Instance) : IInstance =
        let parsedRead = parseLimits instance.ReadRateLimitsPerUser
        let parsedWrite = parseLimits instance.WriteRateLimitsPerUser

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

            member this.MaxJSHeapSize = Option.ofNullable instance.MaxJSHeapSize
            member this.MaxJSStackSize = Option.ofNullable instance.MaxJSStackSize

            member this.UpdateAccessedAtAndDispose newTime = ()

            member this.Dispose() = () }

    interface IInstancesSource with
        member this.Region = homeRegion

        member this.GetInstance (host: string) (cancellationToken: CancellationToken) =
            match Map.tryFind host instances with
            | Some instance -> Task.result (Some <| getInstance instance)
            | None -> Task.result (Option.map getInstance defaultInstance)

        member this.SetExtraConnectionOptions(builder: NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            ()

let private appEndpoints (serviceProvider: IServiceProvider) : Endpoint list =
    List.concat
        [ viewsApi serviceProvider
          entitiesApi serviceProvider
          saveRestoreApi serviceProvider
          actionsApi serviceProvider
          infoApi serviceProvider
          permissionsApi serviceProvider
          domainsApi serviceProvider ]

let private isDebug =
#if DEBUG
    true
#else
    false
#endif

let private setupConfiguration (args: string[]) (webAppBuilder: WebApplicationBuilder) =
    // Configuration.
    let cwd = Directory.GetCurrentDirectory()
    let configPath = POSIXPath.combine cwd args.[0]

    ignore
    <| webAppBuilder.Configuration
        .SetBasePath(cwd)
        .AddJsonFile(configPath, false, isDebug)
        .AddInMemoryCollection(seq { KeyValuePair("ConfigPath", configPath) })
        .AddEnvironmentVariables()

let private setupLoggerConfiguration (configuration: LoggerConfiguration) =
    ignore
    <| configuration.MinimumLevel
        .Override("Microsoft", LogEventLevel.Information)
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        // We really, really don't use DataProtection.
        .MinimumLevel.Override("Microsoft.AspNetCore.DataProtection", LogEventLevel.Error)
        .Enrich.WithThreadId()
        .Enrich.FromLogContext()

let private setupLogging (webAppBuilder: WebApplicationBuilder) =
    let config = webAppBuilder.Configuration

    let configureSerilog
        (context: HostBuilderContext)
        (services: IServiceProvider)
        (configuration: LoggerConfiguration)
        =
        setupLoggerConfiguration configuration

        let options =
            ConfigurationReaderOptions(typeof<ConsoleLoggerConfigurationExtensions>.Assembly)

        ignore
        <| configuration.ReadFrom
            .Configuration(config, options)
            .ReadFrom.Services(services)

        if not <| config.GetSection("Serilog").GetSection("WriteTo").Exists() then
            ignore
            <| configuration.WriteTo.Console(
                outputTemplate = "[{Timestamp:HH:mm:ss} {Level:u3}] <{ThreadId}> {Message:lj}{NewLine}{Exception}"
            )

    ignore <| webAppBuilder.Host.UseSerilog(configureSerilog)

let private setupAuthentication (webAppBuilder: WebApplicationBuilder) =
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    // Auth.
    let configureAuthentication (o: AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let configureJwtBearer (cfg: JwtBearerOptions) =
        cfg.Authority <- ozmadbSection.["AuthAuthority"]
        // Can be null, then `Authority` is used.
        cfg.MetadataAddress <- ozmadbSection.["AuthMetadataAddress"]
        cfg.RequireHttpsMetadata <- ozmadbSection.GetValue("AuthRequireHttpsMetadata", true)

        cfg.TokenValidationParameters <-
            TokenValidationParameters(ValidateIssuer = false, ValidateAudience = false, ValidateIssuerSigningKey = true)

        cfg.Events <- JwtBearerEvents()
        // https://stackoverflow.com/questions/48649717/addjwtbearer-onauthenticationfailed-return-custom-error
        cfg.Events.OnChallenge <-
            fun ctx ->
                ctx.HandleResponse()
                ctx.Response.StatusCode <- StatusCodes.Status401Unauthorized
                ctx.Response.ContentType <- "application/json"
                ctx.Response.Headers.WWWAuthenticate <- JwtBearerDefaults.AuthenticationScheme
                ctx.Response.WriteAsync(JsonConvert.SerializeObject(RIUnauthorized ctx.ErrorDescription))

    ignore <| services.AddGiraffe().AddCors()

    if not <| ozmadbSection.GetValue("DisableSecurity", false) then
        ignore
        <| services
            .AddAuthentication(configureAuthentication)
            .AddJwtBearer(configureJwtBearer)

let private setupRedis (webAppBuilder: WebApplicationBuilder) =
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    match ozmadbSection.["Redis"] with
    | null -> ()
    | redisStr ->
        let redisOptions = Redis.ConfigurationOptions.Parse(redisStr)

        let getRedisMultiplexer (sp: IServiceProvider) =
            let connection =
                Redis.ConnectionMultiplexer.Connect(redisOptions) :> Redis.IConnectionMultiplexer

            let db = connection.GetDatabase()
            connection

        ignore
        <| services.AddSingleton<Redis.IConnectionMultiplexer>(getRedisMultiplexer)

let private setupRateLimiting (webAppBuilder: WebApplicationBuilder) =
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    match ozmadbSection.["Redis"] with
    | null -> ignore <| services.AddMemoryCache().AddInMemoryRateLimiting()
    | redisStr -> ignore <| services.AddRedisRateLimiting()
    // Needed for the strategy, but we ignore it in RateLimit.fs. Painful...
    addRateLimiter services

let private setupJSON (webAppBuilder: WebApplicationBuilder) =
    ignore
    <| webAppBuilder.Services.AddSingleton<Json.ISerializer>(NewtonsoftJson.Serializer defaultJsonSettings)

let private setupEventLogger (webAppBuilder: WebApplicationBuilder) =
    let services = webAppBuilder.Services

    let getEventLogger (sp: IServiceProvider) =
        let logFactory = sp.GetRequiredService<ILoggerFactory>()
        new EventLogger(logFactory)
    // https://stackoverflow.com/a/59089881
    ignore <| services.AddSingleton<EventLogger>(getEventLogger)

    ignore
    <| services.AddHostedService(fun sp -> sp.GetRequiredService<EventLogger>())

let private setupInstancesCache (webAppBuilder: WebApplicationBuilder) =
    let configPath = webAppBuilder.Configuration.["ConfigPath"]
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    let sourcePreload =
        match ozmadbSection.["Preload"] with
        | null -> emptySourcePreloadFile
        | relPath ->
            let path = POSIXPath.combine (POSIXPath.dirName configPath) relPath
            readSourcePreload path

    let preload = resolvePreload sourcePreload

    let makeInstancesStore (sp: IServiceProvider) =
        let cacheParams =
            { Preload = preload
              LoggerFactory = sp.GetRequiredService<ILoggerFactory>()
              EventLogger = sp.GetRequiredService<EventLogger>()
              AllowAutoMark = ozmadbSection.GetValue("AllowAutoMark", false) }

        InstancesCacheStore cacheParams

    ignore <| services.AddSingleton<InstancesCacheStore>(makeInstancesStore)

let private setupInstancesSource (webAppBuilder: WebApplicationBuilder) =
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    let getInstancesSource (sp: IServiceProvider) : IInstancesSource =
        let homeRegion = Option.ofObj <| ozmadbSection.["HomeRegion"]

        match ozmadbSection.["InstancesSource"] with
        | "database" ->
            let instancesConnectionString = ozmadbSection.GetConnectionString("Instances")

            let instancesReadOnlyConnectionString =
                ozmadbSection.GetConnectionString("InstancesReadOnly")
                |> Option.ofObj
                |> Option.defaultValue instancesConnectionString

            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            let lifetime = sp.GetRequiredService<IHostApplicationLifetime>()

            DatabaseInstances(
                logFactory,
                lifetime,
                homeRegion,
                instancesConnectionString,
                instancesReadOnlyConnectionString
            )
            :> IInstancesSource
        | "static" ->
            let parseInstance (section: IConfigurationSection) =
                let instance = Instance(Owner = "owner@example.com")
                section.Bind(instance)

                if isNull instance.Database then
                    instance.Database <- instance.Username

                instance

            let defaultInstanceSection =
                try
                    Some <| ozmadbSection.GetRequiredSection("DefaultInstance")
                with :? InvalidOperationException ->
                    try
                        Some <| ozmadbSection.GetRequiredSection("Instance")
                    with :? InvalidOperationException ->
                        None

            let defaultInstance = Option.map parseInstance defaultInstanceSection

            let instancesSection = ozmadbSection.GetSection("Instances")

            let instances =
                instancesSection.GetChildren()
                |> Seq.map (fun s -> (s.Key, parseInstance s))
                |> Map.ofSeq

            StaticInstance(instances, defaultInstance, homeRegion) :> IInstancesSource
        | source -> failwithf "Invalid instancesSource: %s" source

    ignore <| services.AddSingleton<IInstancesSource>(getInstancesSource)

let private setupHttpUtils (webAppBuilder: WebApplicationBuilder) =
    let ozmadbSection = webAppBuilder.Configuration.GetSection("OzmaDB")
    let services = webAppBuilder.Services

    ignore <| services.Configure<HttpJobSettings>(ozmadbSection.GetSection("Jobs"))
    ignore <| services.AddSingleton<HttpJobUtils>()

let private setupApp (app: IApplicationBuilder) =
    let configureMetrics (options: HttpMiddlewareExporterOptions) =
        for name in
            seq {
                "Client"
                "Email"
                "Instance"
            } do
            ignore
            <| options.AddCustomLabel(
                name.ToLower(),
                fun context ->
                    match context.Items.TryGetValue(name :> obj) with
                    | (true, value) -> value :?> string
                    | (false, _) -> null
            )

    let configureCors (cfg: CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    let configureEndpoints (endpoints: IEndpointRouteBuilder) =
        ignore <| endpoints.MapMetrics()
        ignore <| endpoints.MapGiraffeEndpoints(appEndpoints endpoints.ServiceProvider)

    ignore
    <| app
        .UseSerilogRequestLogging()
        .UseHttpMetrics(configureMetrics)
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
let main (args: string[]) : int =
    let loggerConfig = LoggerConfiguration()
    setupLoggerConfiguration loggerConfig
    Log.Logger <- loggerConfig.WriteTo.Console().CreateBootstrapLogger()

    try
        try
            // Register a global converter to have nicer native F# types JSON conversion.
            JsonConvert.DefaultSettings <- fun () -> defaultJsonSettings
            // Enable JSON and NodaTime for PostgreSQL.
            ignore <| NpgsqlConnection.GlobalTypeMapper.UseJsonNet()
            ignore <| NpgsqlConnection.GlobalTypeMapper.UseNodaTime()

            ignore
            <| NpgsqlConnection.GlobalTypeMapper.AddTypeInfoResolverFactory(new ExtraTypeInfoResolverFactory())

            let webAppBuilder = WebApplication.CreateBuilder()
            setupConfiguration args webAppBuilder
            setupLogging webAppBuilder
            setupAuthentication webAppBuilder
            setupRedis webAppBuilder
            setupRateLimiting webAppBuilder
            setupJSON webAppBuilder
            setupEventLogger webAppBuilder
            setupInstancesCache webAppBuilder
            setupInstancesSource webAppBuilder
            setupHttpUtils webAppBuilder

            let app = webAppBuilder.Build()
            setupApp app

            app.Run()

            0
        with e ->
            Log.Fatal(e, "Terminated unexpectedly")
            1
    finally
        Log.CloseAndFlush()
