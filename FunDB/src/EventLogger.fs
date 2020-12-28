module FunWithFlags.FunDB.EventLogger

open System
open System.Data
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Connection

type EventLogger (loggerFactory : ILoggerFactory) =
    inherit BackgroundService ()

    let chan = Channel.CreateUnbounded<string * EventEntry>()
    let logger = loggerFactory.CreateLogger<EventLogger>()

    override this.ExecuteAsync (cancellationToken : CancellationToken) : Task =
        task {
            while not cancellationToken.IsCancellationRequested do
                match! chan.Reader.WaitToReadAsync cancellationToken with
                | false -> ()
                | true ->
                    let databaseConnections = Dictionary()
                    try
                        let rec addOne () =
                            task {
                                match chan.Reader.TryRead() with
                                | (false, _) -> ()
                                | (true, (connectionString, entry)) ->
                                    try
                                        match databaseConnections.TryGetValue(connectionString) with
                                        | (false, _) ->
                                            let conn = new DatabaseConnection(loggerFactory, connectionString)
                                            let transaction = new DatabaseTransaction(conn, IsolationLevel.ReadCommitted)
                                            let! transaction = checkTransaction transaction <| fun transaction ->
                                                task {
                                                    let! _ = transaction.System.Events.AddAsync(entry)
                                                    return transaction
                                                }
                                            databaseConnections.Add(connectionString, transaction)
                                        | (true, transaction) ->
                                            let! _ = transaction.System.Events.AddAsync(entry)
                                            return ()
                                    with
                                    | ex ->
                                        logger.LogError(ex, "Exception while logging event")
                                    do! addOne ()
                            }
                        do! addOne ()
                        let mutable totalChanged = 0
                        for KeyValue(connectionString, transaction) in databaseConnections do
                            try
                                let! changed = transaction.Commit (cancellationToken)
                                totalChanged <- totalChanged + changed
                            with
                            | ex ->
                                logger.LogError(ex, "Exception while commiting logged event")
                        logger.LogInformation("Logged {0} events into databases", totalChanged)
                    finally
                        for KeyValue(connectionString, transaction) in databaseConnections do
                            (transaction :> IDisposable).Dispose ()
                            (transaction.Connection :> IDisposable).Dispose ()
        } :> Task

    member this.WriteEvent (connectionString : string, entry : EventEntry) =
        if not <| chan.Writer.TryWrite ((connectionString, entry)) then
            failwith "Failed to write event"