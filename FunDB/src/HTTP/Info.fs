module FunWithFlags.FunDB.HTTP.Info

open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open System.Data
open Npgsql
open Giraffe
open Giraffe.EndpointRouting

open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.ContextCache
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.HTTP.Utils

type IsInitializedResponse =
    { IsInitialized : bool
    }

let infoApi : Endpoint list =
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
            requestError RIAccessDenied next ctx
        else
            let instancesCache = ctx.GetService<InstancesCacheStore>()
            instancesCache.Clear ()
            NpgsqlConnection.ClearAllPools ()
            Successful.ok (json Map.empty) next ctx

    let checkIntegrity (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) =
        task {
            match api.Request.User.Effective.Type with
            | RTRoot ->
                do! api.Request.Context.CheckIntegrity ()
                return! commitAndOk api next ctx
            | RTRole _ -> return! requestError RIAccessDenied next ctx
        }

    [ GET [ route "/check_access" <| withContextHidden (fun _ -> ping)
            route "/is_initialized" <| lookupInstance isInitialized
            route "/ping" ping
          ]
      POST [ route "/check_integrity" <| withContextHidden checkIntegrity
             route "/clear_instances_cache" <| resolveUser clearInstancesCache
           ]
    ]
