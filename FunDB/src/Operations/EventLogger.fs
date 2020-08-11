module FunWithFlags.FunDB.Operations.EventLogger

open System
open System.Data
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Connection

type EventLogger (loggerFactory : ILoggerFactory) =
    inherit BackgroundService ()

    let chan = Channel.CreateUnbounded<string * EventEntry>()
    let logger = loggerFactory.CreateLogger<EventLogger>()

    override this.ExecuteAsync (stoppingToken : CancellationToken) : Task =
        task {
            while not stoppingToken.IsCancellationRequested do
                match! chan.Reader.WaitToReadAsync stoppingToken with
                | false -> ()
                | true ->
                    let databaseConnections = Dictionary()
                    try
                        let rec addOne () =
                            match chan.Reader.TryRead() with
                            | (false, _) -> ()
                            | (true, (connectionString, entry)) ->
                                try
                                    let transaction =
                                        match databaseConnections.TryGetValue(connectionString) with
                                        | (false, _) ->
                                            let conn = new DatabaseConnection(loggerFactory, connectionString)
                                            let transaction = new DatabaseTransaction(conn, IsolationLevel.ReadCommitted)
                                            databaseConnections.Add(connectionString, transaction)
                                            transaction
                                        | (true, transaction) -> transaction
                                    ignore <| transaction.System.Events.Add(entry)
                                with
                                | ex ->
                                    logger.LogError(ex, "Exception while logging event")
                                addOne ()
                        addOne ()
                        let mutable totalChanged = 0
                        for KeyValue(connectionString, transaction) in databaseConnections do
                            try
                                let! changed = transaction.System.SaveChangesAsync stoppingToken
                                do! transaction.Commit ()
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

    member this.WriteEvent (connectionString : string, entry : EventEntry, cancellationToken : CancellationToken) =
        chan.Writer.WriteAsync ((connectionString, entry), cancellationToken)