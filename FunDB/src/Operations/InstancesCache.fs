module FunWithFlags.FunDB.Operations.InstancesCache

open System
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open FluidCaching
open LiteDB
open System.IO
open Newtonsoft.Json
open Newtonsoft.Json.Bson
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Operations.ContextCache
open FunWithFlags.FunDB.Operations.EventLogger
open FunWithFlags.FunDB.Operations.Preload

[<AllowNullLiteral>]
type CachedInstance () =
    member val Id : string = null with get, set
    member val State : byte[] = null with get, set

[<AllowNullLiteral>]
type private InMemoryInstance (connectionString : string, constructorTask : Task<ContextCacheStore>) =
    member this.ConnectionString = connectionString
    member this.ConstructorTask = constructorTask

and InstancesCacheStore (loggerFactory : ILoggerFactory, preload : Preload, eventLogger : EventLogger) = 
    let logger = loggerFactory.CreateLogger<InstancesCacheStore>()

    let instancesDiskPath = Path.GetTempFileName()
    let instancesDiskCache = new LiteDatabase(sprintf "Filename=%s;Journal=false;Mode=Exclusive" instancesDiskPath)
    let instancesDiskCollection = instancesDiskCache.GetCollection<CachedInstance>()

    let serializer =
        let jsonOptions = makeDefaultJsonSerializerSettings ()
        jsonOptions.TypeNameHandling <- TypeNameHandling.Auto
        jsonOptions.Converters.Add(MapKeyValueConverter ())
        JsonSerializer.Create(jsonOptions)

    let updateCachedInstance (instance : InMemoryInstance) = task {
        try
            let! context = instance.ConstructorTask
            logger.LogInformation("Dumping unused instance to on-disk cache")
            use ms = new MemoryStream()
            do
                use writer = new BsonDataWriter(ms)
                serializer.Serialize(writer, context.CachedState)
            (*do
                let s = JsonConvert.SerializeObject(context.CachedState)
                printfn "%s" s*)
            let cached =
                CachedInstance (
                    Id = context.ConnectionString,
                    State = ms.ToArray()
                )
            ignore <| instancesDiskCollection.Upsert(cached)
            eprintfn "Pushed new state for connection string: %s" cached.Id
        with
        | ex ->
            logger.LogError(ex, "Failed to serialize unused instance")
    }

    // FIXME: random values
    // We store unfinished tasks in the cache instead of ContextCacheStore-s themselves.
    // This is so that we can enforce one ContextCacheStore created per connection string, even if several requests for it are made simultaneously.
    let instancesDisposer = ItemDisposer (fun ctx -> updateCachedInstance ctx :> Task)
    let instancesMemCache = FluidCache<InMemoryInstance>(8, TimeSpan.FromSeconds(60.0), TimeSpan.FromSeconds(3600.0), (fun () -> DateTime.Now), null, instancesDisposer)
    let instancesMemIndex = instancesMemCache.AddIndex("byConnectionString", fun entry -> entry.ConnectionString)
    let instancesMemLock = Object()

    let tryInstanceFromDisk (connectionString : string) =
        eprintfn "Searching for connection string %s" connectionString
        for cached in instancesDiskCollection.FindAll() do
            eprintfn "Cached state for connection string: %s" cached.Id
        match instancesDiskCollection.FindById(BsonValue connectionString) with
        | null ->
            logger.LogInformation("Initializing new instance state")
            ContextCacheStore (loggerFactory, preload, connectionString, eventLogger)
        | cached ->
            logger.LogInformation("Restoring instance from on-disk cache")
            use ms = new MemoryStream(cached.State)
            use reader = new BsonDataReader(ms)
            let state = serializer.Deserialize<CachedState>(reader)
            ContextCacheStore (loggerFactory, preload, connectionString, eventLogger, state)

    let createInstance (connectionString : string) =
        let constructorTask = Task.Run (fun () -> tryInstanceFromDisk connectionString)
        Task.FromResult <| InMemoryInstance (connectionString, constructorTask)

    let instanceCreator = ItemCreator createInstance

    interface IDisposable with
        member this.Dispose () =
            instancesDiskCache.Dispose()
            File.Delete(instancesDiskPath)

    member this.GetContextCache (connectionString : string) = task {
        let testFun () =
            Threading.Thread.Sleep (TimeSpan.FromSeconds 8.0)
            instancesMemCache.Clear()
        ignore <| Task.Run testFun
        match! instancesMemIndex.GetItem (connectionString) with
        | null ->
            let constr = lock instancesMemLock <| fun () ->
                let task = instancesMemIndex.GetItem (connectionString, instanceCreator)
                if not task.IsCompleted then
                    task.RunSynchronously()
                task.Result
            return! constr.ConstructorTask
        | ret -> return! ret.ConstructorTask
    }