open System
open System.IO
open Newtonsoft.Json
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging.Console
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open Microsoft.IdentityModel.Tokens
open Microsoft.IdentityModel.Logging
open Giraffe
open Giraffe.Serialization.Json
open Npgsql

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.API.View
open FunWithFlags.FunDB.API.Entity
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.Operations.ContextCache
open FunWithFlags.FunDB.Operations.Preload

type Config =
    { connectionString : string
      url : string
      preloads : string option
      authAuthority : string option
      // Disables auth completely
      disableSecurity : bool option
      // Disables permissions model, e.g. one should be in Users table but everyone is local root
      disableACL : bool option
    }

[<EntryPoint>]
let main (args : string[]) : int =
    // Register a global converter to have nicer native F# types JSON conversion
    JsonConvert.DefaultSettings <- fun () -> defaultJsonSerializerSettings
    // Enable JSON for PostgreSQL
    ignore <| NpgsqlConnection.GlobalTypeMapper.UseJsonNet()

    let configPath = args.[0]
    let rawConfig = File.ReadAllText(configPath)
    let config = JsonConvert.DeserializeObject<Config>(rawConfig)
    let disableSecurity = Option.defaultValue false config.disableSecurity

    let sourcePreload =
        match config.preloads with
        | Some path -> readSourcePreload path
        | None -> emptySourcePreload
    let preload = resolvePreload sourcePreload

    let webApp (next : HttpFunc) (ctx : HttpContext) =
        let cacheStore = ctx.GetService<ContextCacheStore>()
        let apiSettings =
            { cacheStore = cacheStore
              disableSecurity = disableSecurity
              disableACL = Option.defaultValue false config.disableACL
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
            match config.authAuthority with
            | None -> failwith "authAuthority is not set in configuration file"
            | Some aut -> aut
        cfg.TokenValidationParameters <- TokenValidationParameters (
            ValidateAudience = false
        )

    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    let configureApp (app : IApplicationBuilder) =
        if not disableSecurity then
            ignore <| app.UseAuthentication()
        ignore <|
            app
                .UseHttpMethodOverride()
                .UseCors(configureCors)
                .UseGiraffeErrorHandler(errorHandler)
                .UseGiraffe(webApp)

    let configureServices (services : IServiceCollection) =
        ignore <| services.AddGiraffe()
        if not disableSecurity then
            ignore <|
                services
                    .AddAuthentication(authenticationOptions)
                    .AddJwtBearer(Action<JwtBearerOptions> jwtBearerOptions)
        ignore <| services.AddCors()
        ignore <| services.AddSingleton<IJsonSerializer>(NewtonsoftJsonSerializer defaultJsonSerializerSettings)
        let makeCacheStore (sp : IServiceProvider) =
            let logFactory = sp.GetService<ILoggerFactory>()
            ContextCacheStore(logFactory, config.connectionString, preload)
        ignore <| services.AddSingleton<ContextCacheStore>(makeCacheStore)

    let configureLogging (builder : ILoggingBuilder) =
        ignore <| builder.AddConsole()

    // IdentityModelEventSource.ShowPII <- true

    WebHostBuilder()
        .UseKestrel()
        .Configure(Action<IApplicationBuilder> configureApp)
        .ConfigureServices(configureServices)
        .ConfigureLogging(configureLogging)
        .UseUrls(config.url)
        .Build()
        .Run()

    0
