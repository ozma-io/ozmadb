module FunWithFlags.FunDB.API.InstancesCache

open System
open System.Threading.Tasks
open System.Collections.Concurrent
open Microsoft.Extensions.Logging
open FluidCaching

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.EventLogger
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.API.ContextCache

type InstancesCacheStore (loggerFactory : ILoggerFactory, preload : Preload, eventLogger : EventLogger) =
    let logger = loggerFactory.CreateLogger<InstancesCacheStore>()

    // FIXME: random values
    let instancesMemCache = FluidCache<ContextCacheStore>(8, TimeSpan.FromSeconds(60.0), TimeSpan.FromSeconds(3600.0), (fun () -> DateTime.Now))
    let instancesMemIndex = instancesMemCache.AddIndex("ByConnectionString", fun entry -> entry.ConnectionString)

    let createInstance (connectionString : string) =
        Task.FromResult <| ContextCacheStore (loggerFactory, preload, connectionString, eventLogger)

    let instanceCreator = ItemCreator createInstance

    member this.GetContextCache (connectionString : string) = instancesMemIndex.GetItem (connectionString, instanceCreator)
    member this.Clear () = instancesMemCache.Clear ()