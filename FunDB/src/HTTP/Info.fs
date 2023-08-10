module FunWithFlags.FunDB.HTTP.Info

open System
open System.Runtime.Serialization
open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open System.Data
open Npgsql
open Giraffe
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.ContextCache
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.HTTP.LongRunning

// TODO: make it private after we fix JSON serialization for the private F# types.
[<SerializeAsObject("error")>]
type MiscErrorInfo =
    | [<CaseKey("jobNotFound")>] MEJobNotFound
    with
        member this.LogMessage =
            match this with
            | MEJobNotFound -> "Job not found"

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | MEJobNotFound -> 404

        static member private LookupKey = prepareLookupCaseKey<MiscErrorInfo>
        member this.Error =
            MiscErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = false

        interface IErrorDetails with
            member this.LogMessage = this.LogMessage
            member this.Message = this.Message
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

// TODO: make it private after we fix JSON serialization for the private F# types.
type IsInitializedHTTPResponse =
    { IsInitialized : bool
    }

let infoApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let pingJob = Task.result (jobJson Map.empty)
    let ping = Map.empty |> json |> Successful.ok

    let isInitialized (inst : InstanceContext) (next : HttpFunc) (ctx : HttpContext) =
        let logFactory = ctx.GetService<ILoggerFactory>()
        let connectionString = instanceConnectionString inst.Instance inst.Source.SetExtraConnectionOptions
        openAndCheckTransaction logFactory connectionString IsolationLevel.ReadCommitted ctx.RequestAborted <| fun trans ->
            task {
                let! isInitialized = instanceIsInitialized trans
                let ret = { IsInitialized = isInitialized }
                return! Successful.ok (json ret) next ctx
            }

    let clearInstancesCache (info : UserTokenInfo) (next : HttpFunc) (ctx : HttpContext) =
        if not info.IsGlobalAdmin then
            requestError (RIAccessDenied "Access denied") next ctx
        else
            let instancesCache = ctx.GetService<InstancesCacheStore>()
            instancesCache.Clear ()
            NpgsqlConnection.ClearAllPools ()
            Successful.ok (json Map.empty) next ctx

    let checkIntegrity (api : IFunDBAPI) =
        task {
            match api.Request.User.Effective.Type with
            | RTRoot ->
                do! api.Request.Context.CheckIntegrity ()
                return! commitAndOk api
            | RTRole _ -> return jobError (RIAccessDenied "Access denied")
        }

    let doGetJobResult (id : JobId) (info : UserTokenInfo) =
        let handler remoteSettings next (ctx : HttpContext) =
            task {
                let remoteRef =
                    { Settings = remoteSettings
                      Id = id
                    } : RemoteJobRef
                match! LongRunningJob<HttpJobResponseHeader>.WaitRemote remoteRef utils.HybridLocalTimeout ctx.RequestAborted with
                | RJReply (result, resultData) ->
                    ctx.Response.StatusCode <- result.ResponseCode
                    for KeyValue(headerName,  headerValue) in result.Headers do
                        ctx.Response.Headers.[headerName] <- headerValue
                    ctx.Response.Headers.ContentLength <- Nullable(int64 resultData.Length)
                    do! ctx.Response.Body.WriteAsync(resultData, ctx.RequestAborted)
                    return Some ctx
                | RJPending ->
                    return! requestError (RINotFinished id) next ctx
                | RJNotFound ->
                    return! requestError MEJobNotFound next ctx
                | RJCanceled ->
                    return! requestError RICanceled next ctx
                | RJInternalError ->
                    return! requestError RIInternal next ctx
            }
        utils.WithRemoteJob handler

    let getJobResult (id : JobId) =
        utils.WithUser (doGetJobResult id)

    [ GET [ route "/check_access" <| utils.WithHiddenContext (fun _ -> pingJob)
            route "/is_initialized" <| utils.WithInstanceContext isInitialized
            route "/ping" ping
            routef "/jobs/%s/result" <| getJobResult
          ]
      POST [ route "/check_integrity" <| utils.WithHiddenContext checkIntegrity
             route "/clear_instances_cache" <| utils.WithUser clearInstancesCache
           ]
    ]
