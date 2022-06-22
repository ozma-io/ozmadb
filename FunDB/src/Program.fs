open System
open System.IO
open System.Threading
open Newtonsoft.Json
open Microsoft.AspNetCore
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.Affine
open Microsoft.IdentityModel.Tokens
open Microsoft.EntityFrameworkCore
open Giraffe
open NodaTime
open Npgsql
open NetJs.Json

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
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.EventLogger

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
                                  member this.IsTemplate = instance.IsTemplate
                                  member this.AccessedAt = Option.ofNullable instance.AccessedAt
                                  member this.Published = instance.Published

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
                      member this.IsTemplate = instance.IsTemplate
                      member this.AccessedAt = Some <| SystemClock.Instance.GetCurrentInstant()

                      member this.UpdateAccessedAt newTime = unitTask { () }

                      member this.Dispose () = ()
                      member this.DisposeAsync () = unitVtask { () }
                }
            Task.result (Some obj)

        member this.SetExtraConnectionOptions (builder : NpgsqlConnectionStringBuilder) =
            builder.CommandTimeout <- 0
            ()

type Startup (config : IConfiguration) =
    let fundbSection = config.GetSection("FunDB")

    let sourcePreload =
        match fundbSection.GetValue("Preloads") with
        | null -> emptySourcePreloadFile
        | path -> readSourcePreload path
    let preload = resolvePreload sourcePreload

    let webApp =
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

    let authenticationOptions (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let jwtBearerOptions (cfg : JwtBearerOptions) =
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

    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    member this.Configure (app : IApplicationBuilder, env : IWebHostEnvironment) =
        ignore <|
            app
                .UseHttpMethodOverride()
                .UseCors(configureCors)
                .UseAuthentication()
                .UseGiraffeErrorHandler(errorHandler)
                .UseGiraffe(webApp)

    member this.ConfigureServices (services : IServiceCollection) =
        ignore <|
            services
                .AddGiraffe()
                .AddCors()
        if not <| fundbSection.GetValue("DisableSecurity", false) then
            ignore <|
                services
                    .AddAuthentication(authenticationOptions)
                    .AddJwtBearer(Action<JwtBearerOptions> jwtBearerOptions)

        ignore <| services.AddSingleton<Json.ISerializer>(NewtonsoftJson.Serializer defaultJsonSettings)
        let getEventLogger (sp : IServiceProvider) =
            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            new EventLogger(logFactory)
        // https://stackoverflow.com/a/59089881
        ignore <| services.AddSingleton<EventLogger>(getEventLogger)
        ignore <| services.AddHostedService(fun sp -> sp.GetRequiredService<EventLogger>())
        let allowAutoMark = fundbSection.GetValue("AllowAutoMark", false)
        let makeInstancesStore (sp : IServiceProvider) =
            let cacheParams =
                { Preload = preload
                  LoggerFactory = sp.GetRequiredService<ILoggerFactory>()
                  EventLogger = sp.GetRequiredService<EventLogger>()
                  AllowAutoMark = allowAutoMark
                }
            InstancesCacheStore cacheParams
        ignore <| services.AddSingleton<InstancesCacheStore>(makeInstancesStore)
        let getInstancesSource (sp : IServiceProvider) : IInstancesSource =
            match fundbSection.["InstancesSource"] with
            | "database" ->
                let instancesConnectionString = config.GetConnectionString("Instances")
                let logFactory = sp.GetRequiredService<ILoggerFactory>()
                DatabaseInstances(logFactory, instancesConnectionString) :> IInstancesSource
            | "static" ->
                let instanceSection = config.GetSection("Instance")
                let username = instanceSection.["Username"]
                let instance =
                    Instance(
                        Name = "static",
                        Host = instanceSection.["Host"],
                        Port = instanceSection.GetValue("Port", 5432),
                        Owner = "owner@example.com",
                        Database = instanceSection.GetValue("Database", username),
                        Username = username,
                        Password = instanceSection.["Password"],
                        DisableSecurity = instanceSection.GetValue("DisableSecurity", false),
                        IsTemplate = instanceSection.GetValue("IsTemplate",false),
                        CreatedAt = SystemClock.Instance.GetCurrentInstant()
                    )
                StaticInstance(instance) :> IInstancesSource
            | _ -> failwith "Invalid InstancesSource"
        ignore <| services.AddSingleton<IInstancesSource>(getInstancesSource)

let private isDebug =
#if DEBUG
    true
#else
    false
#endif

[<EntryPoint>]
let main (args : string[]) : int =
    // Register a global converter to have nicer native F# types JSON conversion.
    JsonConvert.DefaultSettings <- fun () -> defaultJsonSettings
    // Enable JSON and NodaTime for PostgreSQL.
    ignore <| NpgsqlConnection.GlobalTypeMapper.UseJsonNet()
    ignore <| NpgsqlConnection.GlobalTypeMapper.UseNodaTime()
    // Use JavaScript Date objects.
    V8SerializationSettings.Default.UseDate <- true

    let configPath = args.[0]

    let configuration =
        ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(configPath, false, isDebug)
            .AddEnvironmentVariables()
            .Build()

    let configureLogging (logging : ILoggingBuilder) =
        //ignore <| logging.ClearProviders()
        //ignore <| logging.AddConsole()
        ()

    // IdentityModelEventSource.ShowPII <- true

    WebHost
        .CreateDefaultBuilder()
        .UseConfiguration(configuration)
        .ConfigureLogging(configureLogging)
        .UseStartup<Startup>()
        .Build()
        .Run()

    0
