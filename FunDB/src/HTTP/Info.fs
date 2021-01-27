module FunWithFlags.FunDB.HTTP.Info

open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open System.Data
open Giraffe
open Npgsql

open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.API.InstancesCache
open FunWithFlags.FunDB.API.ContextCache
open FunWithFlags.FunDB.HTTP.Utils

type IsInitializedResponse =
    { IsInitialized : bool
    }

let infoApi : HttpHandler =
    let ping = Map.empty |> json |> Successful.ok

    let isInitialized inst (next : HttpFunc) (ctx : HttpContext) =
        task {
            let logFactory = ctx.GetService<ILoggerFactory>()
            let connectionString = instanceConnectionString inst.Instance
            use conn = new DatabaseConnection(logFactory, connectionString)
            use trans = new DatabaseTransaction(conn, IsolationLevel.ReadCommitted)
            let! isInitialized = instanceIsInitialized trans
            let ret = { IsInitialized = isInitialized }
            return! Successful.ok (json ret) next ctx
        }

    let clearInstancesCache info (next : HttpFunc) (ctx : HttpContext) =
        if not info.IsRoot then
            requestError REAccessDenied next ctx
        else
            let instancesCache = ctx.GetService<InstancesCacheStore>()
            instancesCache.Clear ()
            NpgsqlConnection.ClearAllPools ()
            Successful.ok (json Map.empty) next ctx

    choose
        [ route "/check_access" >=> GET >=> withContext (fun _ -> ping)
          route "/is_initialized" >=> GET >=> lookupInstance isInitialized
          route "/clear_instances_cache" >=> POST >=> resolveUser clearInstancesCache
          route "/ping" >=> GET >=> ping
        ]
