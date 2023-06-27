module FunWithFlags.FunDB.HTTP.RateLimit

// All this is hacky as hell, because AspNetCoreRateLimit is too rigid for us and we try to use it anyway.

open System
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Microsoft.AspNetCore.Http
open AspNetCoreRateLimit
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunUtils

type RateLimit =
    { Period : int // sec
      Limit : int
    }

let private counterKeyBuilder = ClientCounterKeyBuilder (ClientRateLimitOptions ())

type private ConstClientResolveContributor (clientId : string) =
    interface IClientResolveContributor with
        member this.ResolveClientAsync httpContext = Task.result clientId

let private convertRateLimit (rate : RateLimit) : RateLimitRule =
    RateLimitRule(
        Endpoint = "*",
        Limit = rate.Limit,
        Period = string rate.Period,
        PeriodTimespan = TimeSpan.FromSeconds(rate.Period)
    )

type private StaticRateLimitProcessor (limits : RateLimit seq, strategy : IProcessingStrategy, options : RateLimitOptions) =
    interface IRateLimitProcessor with
        member this.GetMatchingRulesAsync (identity, cancellationToken) =
            limits |> Seq.map convertRateLimit |> Task.result

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
            strategy.ProcessRequestAsync (requestIdentity, rule, counterKeyBuilder, options, cancellationToken)

        member this.IsWhitelisted requestIdentity = false

let private emptyIpResolvers = List() : IList<IIpResolveContributor>
let private rateIncrementer = Func<float>(fun _ -> 1.0)

type private StaticRateLimitConfiguration (clientId : string) =
    let single = ConstClientResolveContributor clientId :> IClientResolveContributor
    let contribs = Seq.singleton single
    let clientResolvers =
        // Don't ask
        { new IList<IClientResolveContributor> with
            member this.Item
                with get key = raise <| NotSupportedException ()
                and set key value = raise <| NotSupportedException ()
            member this.IndexOf item = raise <| NotSupportedException ()
            member this.Insert (index, item) = raise <| NotSupportedException ()
            member this.RemoveAt idex = raise <| NotSupportedException ()
            member this.Count = 1
            member this.IsReadOnly = true
            member this.Add item = raise <| NotSupportedException ()
            member this.Clear () = raise <| NotSupportedException ()
            member this.Contains item = raise <| NotSupportedException ()
            member this.CopyTo (array, arrayIndex) = raise <| NotSupportedException ()
            member this.Remove item = raise <| NotSupportedException ()

          interface IEnumerable<IClientResolveContributor> with
            member this.GetEnumerator () = contribs.GetEnumerator ()

          interface Collections.IEnumerable with
            member this.GetEnumerator () = contribs.GetEnumerator () :> Collections.IEnumerator
        }

    interface IRateLimitConfiguration with
        member this.ClientResolvers = clientResolvers
        member this.IpResolvers = emptyIpResolvers
        member this.EndpointCounterKeyBuilder = counterKeyBuilder
        member this.RateIncrementer = rateIncrementer
        member this.RegisterResolvers () = ()

type private StaticRateLimitMiddleware (next : RequestDelegate, quotaExceeded : string -> HttpHandler, giraffeNext : HttpFunc, options : RateLimitOptions, processor : StaticRateLimitProcessor, config : IRateLimitConfiguration, logger : ILogger) =
    inherit RateLimitMiddleware<StaticRateLimitProcessor>(next, options, processor, config)

    override this.LogBlockedRequest (httpContext, identity, counter, rule) =
        logger.LogInformation("Request for {client_id} has been blocked, quota {limit}/{period} exceeded by {excess}", identity.ClientId, rule.Limit, rule.Period, counter.Count - rule.Limit)

    override this.ReturnQuotaExceededResponse (httpContext, rule, retryAfter) =
        let msg = sprintf "Maximum %i per %s second(s)" (int rule.Limit) rule.Period
        quotaExceeded msg giraffeNext httpContext

// Hacky! We need to merge ASP.NET-style middleware and Giraffe handlers here.
let inline private wrapMiddleware (invokeMiddleware : RequestDelegate -> HttpContext -> Task) (guarded : HttpHandler) (next : HttpFunc) (ctx : HttpContext) =
    task {
        let mutable result = None : HttpContext option option
        let myNext ctx =
            unitTask {
                let! ret = guarded next ctx
                result <- Some ret
            }
        do! invokeMiddleware myNext ctx
        match result with
        | None -> return! next ctx
        | Some ret -> return ret
    }

let checkRateLimit (limited : HttpHandler) (quotaExceeded : string -> HttpHandler) (clientId : string) (limits : RateLimit seq) (next : HttpFunc) (ctx : HttpContext) =
    if Seq.isEmpty limits then
        limited next ctx
    else
        task {
            let options = ctx.GetService<IOptions<ClientRateLimitOptions>>()
            let strategy = ctx.GetService<IProcessingStrategy>()
            let config = StaticRateLimitConfiguration(clientId)
            let processor = StaticRateLimitProcessor(limits, strategy, options.Value)
            let logger = ctx.GetLogger<StaticRateLimitMiddleware>()
            let invoke delNext ctx =
                let middleware = StaticRateLimitMiddleware (delNext, quotaExceeded, next, options.Value, processor, config, logger)
                middleware.Invoke ctx
            return! wrapMiddleware invoke limited next ctx
        }