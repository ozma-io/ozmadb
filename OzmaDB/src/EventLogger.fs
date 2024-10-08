module OzmaDB.EventLogger

open System
open System.Data
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

open OzmaDB.OzmaUtils
open OzmaDBSchema.System
open OzmaDB.Connection

type EventLogger(loggerFactory: ILoggerFactory) =
    inherit BackgroundService()

    let chan = Channel.CreateUnbounded<string * EventEntry>()
    let logger = loggerFactory.CreateLogger<EventLogger>()

    override this.ExecuteAsync(cancellationToken: CancellationToken) : Task =
        task {
            while not cancellationToken.IsCancellationRequested do
                match! chan.Reader.WaitToReadAsync cancellationToken with
                | false -> ()
                | true ->
                    let databaseConnections = Dictionary<string, DatabaseTransaction>()

                    use _ =
                        Task.toDisposable
                        <| fun () ->
                            task {
                                for KeyValue(connectionString, transaction) in databaseConnections do
                                    do! (transaction :> IAsyncDisposable).DisposeAsync()
                                    do! (transaction.Connection :> IAsyncDisposable).DisposeAsync()
                            }

                    do!
                        Task.loop ()
                        <| fun () ->
                            task {
                                match chan.Reader.TryRead() with
                                | (false, _) -> return Task.StopLoop()
                                | (true, (connectionString, entry)) ->
                                    try
                                        match databaseConnections.TryGetValue(connectionString) with
                                        | (false, _) ->
                                            let! transaction =
                                                openAndCheckTransaction
                                                    loggerFactory
                                                    connectionString
                                                    IsolationLevel.ReadCommitted
                                                    cancellationToken
                                                <| fun transaction ->
                                                    task {
                                                        let! _ = transaction.System.Events.AddAsync(entry)
                                                        // Check that connection works the first time.
                                                        let! _ = transaction.System.SaveChangesAsync(cancellationToken)

                                                        return transaction
                                                    }

                                            databaseConnections.Add(connectionString, transaction)
                                        | (true, transaction) ->
                                            let! _ = transaction.System.Events.AddAsync(entry)
                                            ()
                                    with ex ->
                                        logger.LogError(ex, "Exception while logging event")

                                    return Task.NextLoop()
                            }

                    let mutable totalChanged = 0

                    for KeyValue(connectionString, transaction) in databaseConnections do
                        try
                            let! changed = transaction.Commit(cancellationToken)
                            totalChanged <- totalChanged + changed
                        with ex ->
                            logger.LogError(ex, "Exception while commiting logged event")

                    if totalChanged > 0 then
                        logger.LogInformation("Logged {count} events into databases", totalChanged)
        }
        :> Task

    member this.WriteEvent(connectionString: string, entry: EventEntry) =
        if not <| chan.Writer.TryWrite((connectionString, entry)) then
            failwith "Failed to write event"
