open System
open System.IO
open Newtonsoft.Json
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open Microsoft.IdentityModel.Tokens
open Microsoft.IdentityModel.Logging
open Giraffe
open Giraffe.Serialization.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.API.View
open FunWithFlags.FunDB.API.Permissions
open FunWithFlags.FunDB.API.Entity
open FunWithFlags.FunDB.ContextCache
open FunWithFlags.FunDB.Layout.Source

type Config =
    { connectionString : string
      url : string
      preloadedLayout : string option
      migration : string option
      authAuthority : string
      authMetadata : string
    }

[<EntryPoint>]
let main (args : string[]) : int =
    // Register a global converter to have nicer native F# types JSON conversion
    JsonConvert.DefaultSettings <- fun () -> defaultJsonSerializerSettings

    let configPath = args.[0]
    let rawConfig = File.ReadAllText(configPath)
    let config = JsonConvert.DeserializeObject<Config>(rawConfig)

    let preloadedLayout =
        match config.preloadedLayout with
        | Some path -> path |> File.ReadAllText |> JsonConvert.DeserializeObject<SourceLayout>
        | None -> emptySourceLayout
    let migration =
        match config.migration with
        | Some path -> path |> File.ReadAllText
        | None -> ""
    let preloadedSettings =
        { layout = preloadedLayout
          migration = migration
        }

    let cacheStore = ContextCacheStore(config.connectionString, preloadedSettings)

    let protectedApi (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let userClaim = ctx.User.FindFirst "name"
        let userName = userClaim.Value
        let specifiedLang =
            ctx.Request.GetTypedHeaders().AcceptLanguage
            |> Seq.sortByDescending (fun lang -> if lang.Quality.HasValue then lang.Quality.Value else 1.0)
            |> Seq.first
        let lang =
            match specifiedLang with
            | Some l -> l.Value.Value
            | None -> "en-US"
        use rctx = new RequestContext(cacheStore, userName, lang)
        choose
            [ 
            ] next ctx

    let webApp =
        choose
            [ viewsApi cacheStore
              permissionsApi cacheStore
              entitiesApi cacheStore
              (setStatusCode 404 >=> text "Not Found")
            ]

    let errorHandler (ex : Exception) (logger : ILogger) =
        logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> setStatusCode 500 >=> text ex.Message

    let authenticationOptions (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme

    let jwtBearerOptions (cfg : JwtBearerOptions) =
        cfg.Authority <- config.authAuthority
        cfg.MetadataAddress <- config.authMetadata
        // We use internal network so it's okay
        cfg.RequireHttpsMetadata <- false
        cfg.TokenValidationParameters <- TokenValidationParameters (
            ValidateAudience = false
        )

    let configureCors (cfg : CorsPolicyBuilder) =
        ignore <| cfg.WithOrigins("*").AllowAnyHeader().AllowAnyMethod()

    let configureApp (app : IApplicationBuilder) =
        app
            .UseAuthentication()
            .UseHttpMethodOverride()
            .UseCors(configureCors)
            .UseGiraffeErrorHandler(errorHandler)
            .UseGiraffe(webApp)

    let configureServices (services : IServiceCollection) =
        ignore <| services.AddGiraffe()
        ignore <|
            services
                .AddAuthentication(authenticationOptions)
                .AddJwtBearer(Action<JwtBearerOptions> jwtBearerOptions)
        ignore <| services.AddCors()          
        ignore <| services.AddSingleton<IJsonSerializer>(NewtonsoftJsonSerializer defaultJsonSerializerSettings)

    let configureLogging (builder : ILoggingBuilder) =
        ignore <| builder.AddConsole()

    IdentityModelEventSource.ShowPII <- true    

    WebHostBuilder()
        .UseKestrel()
        .Configure(Action<IApplicationBuilder> configureApp)
        .ConfigureServices(configureServices)
        .ConfigureLogging(configureLogging)
        .UseUrls(config.url)
        .Build()
        .Run()

    0
