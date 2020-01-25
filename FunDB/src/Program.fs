open System
open System.IO
open Newtonsoft.Json
open Microsoft.AspNetCore
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.IdentityModel.Tokens
open Microsoft.EntityFrameworkCore
open Giraffe
open Giraffe.Serialization.Json
open Npgsql

open FunWithFlags.FunDBSchema.Instances
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.API.Info
open FunWithFlags.FunDB.API.View
open FunWithFlags.FunDB.API.Entity
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Operations.InstancesCache
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.EventLogger
open FunWithFlags.FunDB.FunQL.Query
module FunQL = FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let httpJsonSettings =
    let converters : JsonConverter[] = [|
        FunQL.FieldValuePrettyConverter ()
        FunQL.ScalarFieldTypePrettyConverter ()
        FunQL.FieldTypePrettyConverter ()
        FunQL.FieldExprTypePrettyConverter ()
        SQL.ValuePrettyConverter ()
        SQL.SimpleTypePrettyConverter ()
        SQL.ValueTypePrettyConverter ()
        ExecutedValuePrettyConverter ()
        ExecutedRowPrettyConverter ()
    |]
    let constructors = Array.map (fun conv -> fun _ -> Some conv) converters
    let jsonSettings = makeDefaultJsonSerializerSettings constructors
    jsonSettings

type DatabaseInstances (loggerFactory : ILoggerFactory, connectionString : string) =
    interface IInstancesSource with
        member this.GetInstance (host : string) = task {
            use instances =
                let systemOptions =
                    (DbContextOptionsBuilder<InstancesContext> ())
                        .UseLoggerFactory(loggerFactory)
                        .UseNpgsql(connectionString)
                new InstancesContext(systemOptions.Options)
            match! instances.Instances.FirstOrDefaultAsync(fun x -> x.Name = host && x.Enabled) with
            | null -> return None
            | instance -> return Some instance
        }

type StaticInstance (instance : Instance) =
    interface IInstancesSource with
        member this.GetInstance (host : string) = Task.result (Some instance)

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
              infoApi
              (setStatusCode 404 >=> text "Not Found")
            ]

    let errorHandler (ex : Exception) (logger : ILogger) =
        logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> setStatusCode 500 >=> text ex.Message

    let authenticationOptions (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let jwtBearerOptions (cfg : JwtBearerOptions) =
        cfg.Authority <- fundbSection.["AuthAuthority"]
        cfg.TokenValidationParameters <- TokenValidationParameters (
            ValidateAudience = false
        )

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
        let makeInstancesStore (sp : IServiceProvider) =
            let eventLogger = sp.GetService<EventLogger>()
            let logFactory = sp.GetService<ILoggerFactory>()
            InstancesCacheStore(logFactory, preload, eventLogger)
        ignore <| services.AddSingleton<InstancesCacheStore>(makeInstancesStore)
        let getEventLogger (sp : IServiceProvider) =
            let logFactory = sp.GetService<ILoggerFactory>()
            new EventLogger(logFactory)
        ignore <| services.AddHostedService(Func<IServiceProvider, EventLogger> getEventLogger)
        let getInstancesSource (sp : IServiceProvider) : IInstancesSource =
            match fundbSection.["InstancesSource"] with
            | "database" ->
                let instancesConnectionString = config.GetConnectionString("Instances")
                let logFactory = sp.GetService<ILoggerFactory>()
                DatabaseInstances(logFactory, instancesConnectionString) :> IInstancesSource
            | "static" ->
                let instanceSection = config.GetSection("Instance")
                let username = instanceSection.["Username"]
                let instance =
                    Instance(
                        Name = "static",
                        Host = instanceSection.["Host"],
                        Port = instanceSection.GetValue("Port", 5432),
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
