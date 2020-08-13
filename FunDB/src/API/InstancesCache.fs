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
    let touchedInstances = ConcurrentDictionary<string, bool>()

    let createInstance (connectionString : string) =
        if touchedInstances.ContainsKey(connectionString) then
            logger.LogInformation("Initializing warm instance state")
            Task.FromResult <| ContextCacheStore (loggerFactory, preload, connectionString, eventLogger, true)
        else
            logger.LogInformation("Initializing cold instance state")
            let newStore = ContextCacheStore (loggerFactory, preload, connectionString, eventLogger, false)
            ignore <| touchedInstances.TryAdd(connectionString, true)
            Task.FromResult newStore

    let instanceCreator = ItemCreator createInstance

    member this.GetContextCache (connectionString : string) = instancesMemIndex.GetItem (connectionString, instanceCreator)