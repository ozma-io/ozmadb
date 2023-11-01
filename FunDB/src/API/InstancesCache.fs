module FunWithFlags.FunDB.API.InstancesCache

open System
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open FluidCaching

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.EventLogger
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.ContextCache

type InstancesCacheParams =
    { LoggerFactory : ILoggerFactory
      Preload : Preload
      AllowAutoMark : bool
      EventLogger : EventLogger
    }

type InstancesCacheStore (cacheParams : InstancesCacheParams) =
    let hashedPreload = HashedPreload cacheParams.Preload

    // FIXME: random values
    let instancesMemCache = FluidCache<ContextCacheStore>(16, TimeSpan.FromSeconds(60.0), TimeSpan.FromSeconds(60.0 * 60.0), (fun () -> DateTime.Now))
    let instancesMemIndex = instancesMemCache.AddIndex("ByConnectionString", fun entry -> entry.ConnectionString)

    let createInstance (connectionString : string) =
        let cacheParams =
            { ConnectionString = connectionString
              AllowAutoMark = cacheParams.AllowAutoMark
              LoggerFactory = cacheParams.LoggerFactory
              Preload = hashedPreload
              EventLogger = cacheParams.EventLogger
            }
        Task.FromResult <| ContextCacheStore cacheParams

    let instanceCreator = ItemCreator createInstance

    member this.GetContextCache (connectionString : string) = instancesMemIndex.GetItem (connectionString, instanceCreator)
    member this.Clear () = instancesMemCache.Clear ()