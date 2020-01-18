open System
open System.IO
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
open Microsoft.IdentityModel.Tokens
open Giraffe
open Giraffe.Serialization.Json
open Npgsql

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Utils
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
        FunQL.FieldExprTypePrettyConverter ()
        SQL.ValuePrettyConverter ()
        SQL.SimpleTypePrettyConverter ()
        SQL.ValueTypePrettyConverter ()
        ExecutedValuePrettyConverter ()
        ExecutedRowPrettyConverter ()
    |]
    let constructors = Array.map (fun conv -> fun _ -> Some <| conv) converters
    let jsonSettings = makeDefaultJsonSerializerSettings constructors
    jsonSettings

type Startup (config : IConfiguration) =
    let fundbSection = config.GetSection("FunDB")

    let instancesConnectionString = config.GetConnectionString("Instances")
    let preloads = fundbSection.GetValue("Preloads") |> Option.ofNull
    let authAuthority = fundbSection.GetValue("AuthAuthority") |> Option.ofNull
    let disableSecurity = fundbSection.GetValue("DisableSecurity", false)
    let disableACL = fundbSection.GetValue("DisableACL", false)
    let forceHost = fundbSection.GetValue("ForceHost") |> Option.ofNull

    let sourcePreload =
        match preloads with
        | Some path -> readSourcePreload path
        | None -> emptySourcePreload
    let preload = resolvePreload sourcePreload

    let webApp (next : HttpFunc) (ctx : HttpContext) =
        let instancesStore = ctx.GetService<InstancesCacheStore>()
        let apiSettings =
            { instancesStore = instancesStore
              disableSecurity = disableSecurity
              disableACL = disableACL
              instancesConnectionString = instancesConnectionString
              forceHost = forceHost
            }

        choose
            [ viewsApi apiSettings
              entitiesApi apiSettings
              saveRestoreApi apiSettings
              (setStatusCode 404 >=> text "Not Found")
            ] next ctx

    let errorHandler (ex : Exception) (logger : ILogger) =
        logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> setStatusCode 500 >=> text ex.Message

    let authenticationOptions (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let jwtBearerOptions (cfg : JwtBearerOptions) =
        cfg.Authority <-
            match authAuthority with
            | None -> failwith "authAuthority is not set in configuration file"
            | Some aut -> aut
        cfg.TokenValidationParameters <- TokenValidationParameters (
            ValidateAudience = false
        )

    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    member this.Configure (app : IApplicationBuilder, env : IWebHostEnvironment) =
        if not disableSecurity then
            ignore <| app.UseAuthentication()
        ignore <|
            app
                .UseHttpMethodOverride()
                .UseCors(configureCors)
                .UseGiraffeErrorHandler(errorHandler)
                .UseGiraffe(webApp)

    member this.ConfigureServices (services : IServiceCollection) =
        ignore <| services.AddGiraffe()
        if not disableSecurity then
            ignore <|
                services
                    .AddAuthentication(authenticationOptions)
                    .AddJwtBearer(Action<JwtBearerOptions> jwtBearerOptions)
        ignore <| services.AddCors()

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
