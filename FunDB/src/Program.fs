open System
open System.IO
open System.Threading
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
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
open Giraffe.Serialization.Json
open Npgsql
open NetJs.Json

open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.HTTP.Info
open FunWithFlags.FunDB.HTTP.Views
open FunWithFlags.FunDB.HTTP.Entities
open FunWithFlags.FunDB.HTTP.SaveRestore
open FunWithFlags.FunDB.HTTP.Actions
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.EventLogger
module FunQL = FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let httpJsonSettings =
    let converters : JsonConverter[] = [|
        FunQL.FieldValuePrettyConverter ()
        FunQL.FieldTypePrettyConverter ()
        SQL.ValuePrettyConverter ()
    |]
    let constructors = Array.map (fun conv -> fun _ -> Some conv) converters
    let jsonSettings = makeDefaultJsonSerializerSettings constructors
    let resolver = jsonSettings.ContractResolver :?> ConverterContractResolver
    resolver.NamingStrategy <- CamelCaseNamingStrategy(
        OverrideSpecifiedNames = false
    )
    jsonSettings.NullValueHandling <- NullValueHandling.Ignore
    jsonSettings

type DatabaseInstances (loggerFactory : ILoggerFactory, connectionString : string) =
    interface IInstancesSource with
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) = task {
            use instances =
                let systemOptions =
                    (DbContextOptionsBuilder<InstancesContext> ())
                        .UseLoggerFactory(loggerFactory)
                        .UseNpgsql(connectionString)
                new InstancesContext(systemOptions.Options)
            match! instances.Instances.FirstOrDefaultAsync((fun x -> x.Name = host && x.Enabled), cancellationToken) with
            | null -> return None
            | instance -> return Some instance
        }

type StaticInstance (instance : Instance) =
    interface IInstancesSource with
        member this.GetInstance (host : string) (cancellationToken : CancellationToken) = Task.result (Some instance)

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

        ignore <| services.AddSingleton<IJsonSerializer>(NewtonsoftJsonSerializer httpJsonSettings)
        let getEventLogger (sp : IServiceProvider) =
            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            new EventLogger(logFactory)
        // https://stackoverflow.com/a/59089881
        ignore <| services.AddSingleton<EventLogger>(getEventLogger)
        ignore <| services.AddHostedService(fun sp -> sp.GetRequiredService<EventLogger>())
        let makeInstancesStore (sp : IServiceProvider) =
            let eventLogger = sp.GetRequiredService<EventLogger>()
            let logFactory = sp.GetRequiredService<ILoggerFactory>()
            InstancesCacheStore(logFactory, preload, eventLogger)
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
                        DisableSecurity = instanceSection.GetValue("DisableSecurity", false)
                    )
                StaticInstance(instance) :> IInstancesSource
            | _ -> failwith "Invalid InstancesSource"
        ignore <| services.AddSingleton<IInstancesSource>(getInstancesSource)

[<EntryPoint>]
let main (args : string[]) : int =
    // Register a global converter to have nicer native F# types JSON conversion.
    JsonConvert.DefaultSettings <- fun () -> httpJsonSettings
    // Enable JSON for PostgreSQL.
    ignore <| NpgsqlConnection.GlobalTypeMapper.UseJsonNet()
    // Use JavaScript Date objects.
    V8SerializationSettings.Default.UseDate <- true

    let configPath = args.[0]

    let configuration =
        ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(configPath)
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
