module FunWithFlags.FunDB.HTTP.RateLimit

open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http
open AspNetCoreRateLimit
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunUtils

type RateLimit =
    { Period : int // sec
      Limit : int
    }

type RateLimits =
    { Limits : RateLimit seq
      ClientId : string
    }

let private counterKeyBuilder = ClientCounterKeyBuilder (ClientRateLimitOptions ())

type private ContextClientResolveContributor () =
    interface IClientResolveContributor with
        member this.ResolveClientAsync httpContext =
            let rateLimits = httpContext.Features.Get<RateLimits>()
            if isRefNull rateLimits then
                Task.result null
            else
                Task.result rateLimits.ClientId
let private rateIncrementer = Func<float>(fun _ -> 1.0)

type private ContextRateLimitConfiguration () =
    let contributor = ContextClientResolveContributor () :> IClientResolveContributor
    let clientResolvers = List(seq { contributor }) : IList<IClientResolveContributor>
    let ipResolvers = List() : IList<IIpResolveContributor>

    interface IRateLimitConfiguration with
        member this.ClientResolvers = clientResolvers
        member this.IpResolvers = ipResolvers
        member this.EndpointCounterKeyBuilder = counterKeyBuilder
        member this.RateIncrementer = rateIncrementer
        member this.RegisterResolvers () = ()

let private convertRateLimit (rate : RateLimit) : RateLimitRule =
    RateLimitRule(
        Endpoint = "*",
        Limit = rate.Limit,
        Period = string rate.Period,
        PeriodTimespan = TimeSpan.FromSeconds(rate.Period)
    )

type ContextRateLimitProcessor (
            options : IOptions<ClientRateLimitOptions>,
            strategy : IProcessingStrategy,
            contextAccessor : IHttpContextAccessor
        ) =
    interface IRateLimitProcessor with
        member this.GetMatchingRulesAsync (identity, cancellationToken) =
            let rateLimits = contextAccessor.HttpContext.Features.Get<RateLimits>()
            if isRefNull rateLimits then
                Task.result Seq.empty
            else
                rateLimits.Limits |> Seq.map convertRateLimit |> Task.result

        member this.GetRateLimitHeaders (counter, rule, cancellationToken) =
            let (remaining, reset) =
                if counter.HasValue then
                    let remaining = rule.Limit - counter.Value.Count
                    let reset = counter.Value.Timestamp + rule.PeriodTimespan.Value
                    (remaining, reset)
                else
                    let reset = DateTime.Now + rule.PeriodTimespan.Value
                    (rule.Limit, reset)

            RateLimitHeaders(
                Limit = string rule.Limit,
                Remaining = string remaining,
                Reset = DateTimeOffset(reset).ToUnixTimeSeconds().ToString()
            )

        member this.ProcessRequestAsync (requestIdentity, rule, cancellationToken) =
            strategy.ProcessRequestAsync (requestIdentity, rule, counterKeyBuilder, options.Value, cancellationToken)

        member this.IsWhitelisted requestIdentity = false

// We create the middleware anew each time, because we have no way to use dynamic `RequestDelegate`s otherwise.
type private ContextRateLimitMiddleware (next : RequestDelegate, quotaExceeded : string -> HttpHandler, options : RateLimitOptions, processor : ContextRateLimitProcessor, config : IRateLimitConfiguration, logger : ILogger) =
    inherit RateLimitMiddleware<ContextRateLimitProcessor>(next, options, processor, config)

    override this.LogBlockedRequest (httpContext, identity, counter, rule) =
        logger.LogInformation("Request for {client_id} has been blocked, quota {limit}/{period} exceeded by {excess}", identity.ClientId, rule.Limit, rule.Period, counter.Count - rule.Limit)

    override this.ReturnQuotaExceededResponse (httpContext, rule, retryAfter) =
        let msg = sprintf "Maximum %i per %s second(s)" (int rule.Limit) rule.Period
        quotaExceeded msg earlyReturn httpContext

type RateLimiter (
            logger : ILogger<RateLimiter>,
            processor : ContextRateLimitProcessor,
            options : IOptions<RateLimitOptions>,
            config : IRateLimitConfiguration
        ) =
    member this.CheckRateLimit (quotaExceeded : string -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let mutable result = None
            let delegateNext (ctx : HttpContext) =
                unitTask {
                    let! ret = next ctx
                    result <- ret
                }
            let middleware = ContextRateLimitMiddleware (delegateNext, quotaExceeded, options.Value, processor, config, logger)
            do! middleware.Invoke ctx
            return result
        }

let addRateLimiter (services : IServiceCollection) =
    ignore <| services
        .AddSingleton<ContextRateLimitProcessor>()
        .AddSingleton<IRateLimitConfiguration, ContextRateLimitConfiguration>()
        .AddSingleton<RateLimiter, RateLimiter>()
        .AddHttpContextAccessor()
