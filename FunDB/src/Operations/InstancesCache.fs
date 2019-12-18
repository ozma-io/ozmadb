module FunWithFlags.FunDB.Operations.InstancesCache

open System
open System.Threading.Tasks
open System.Collections.Concurrent
open Microsoft.Extensions.Logging
open FluidCaching
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Operations.ContextCache
open FunWithFlags.FunDB.Operations.EventLogger
open FunWithFlags.FunDB.Operations.Preload

[<AllowNullLiteral>]
type private InMemoryInstance (connectionString : string, constructorTask : Task<ContextCacheStore>) =
    member this.ConnectionString = connectionString
    member this.ConstructorTask = constructorTask

and InstancesCacheStore (loggerFactory : ILoggerFactory, preload : Preload, eventLogger : EventLogger) =
    let logger = loggerFactory.CreateLogger<InstancesCacheStore>()

    // FIXME: random values
    let instancesMemCache = FluidCache<InMemoryInstance>(8, TimeSpan.FromSeconds(60.0), TimeSpan.FromSeconds(3600.0), (fun () -> DateTime.Now))
    let instancesMemIndex = instancesMemCache.AddIndex("byConnectionString", fun entry -> entry.ConnectionString)
    let instancesMemLock = Object()
    let touchedInstances = ConcurrentDictionary<string, bool>()

    let loadInstance (connectionString : string) =
        if touchedInstances.ContainsKey(connectionString) then
            logger.LogInformation("Initializing warm instance state")
            ContextCacheStore (loggerFactory, preload, connectionString, eventLogger, true)
        else
            logger.LogInformation("Initializing cold instance state")
            let newStore = ContextCacheStore (loggerFactory, preload, connectionString, eventLogger, false)
            ignore <| touchedInstances.TryAdd(connectionString, true)
            newStore

    let createInstance (connectionString : string) =
        let constructorTask = Task.Run (fun () -> loadInstance connectionString)
        Task.FromResult <| InMemoryInstance (connectionString, constructorTask)

    let instanceCreator = ItemCreator createInstance

    member this.GetContextCache (connectionString : string) = task {
        match! instancesMemIndex.GetItem (connectionString) with
        | null ->
            let constr = lock instancesMemLock <| fun () ->
                Task.awaitSync <| instancesMemIndex.GetItem (connectionString, instanceCreator)
            return! constr.ConstructorTask
        | ret -> return! ret.ConstructorTask
    }