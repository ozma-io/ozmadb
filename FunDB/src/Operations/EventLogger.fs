module FunWithFlags.FunDB.Operations.EventLogger

open System.Data
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDBSchema.Schema
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Connection

type EventLogger (loggerFactory : ILoggerFactory, connectionString : string) =
    inherit BackgroundService ()

    let chan = Channel.CreateUnbounded<EventEntry>()
    let logger = loggerFactory.CreateLogger<EventLogger>()

    override this.ExecuteAsync (stoppingToken : CancellationToken) : Task =
        task {
            while not stoppingToken.IsCancellationRequested do
                match! chan.Reader.WaitToReadAsync stoppingToken with
                | false -> ()
                | true ->
                    try
                        use conn = new DatabaseConnection(loggerFactory, connectionString)
                        use transaction = new DatabaseTransaction(conn, IsolationLevel.ReadCommitted)
                        let rec addOne () =
                            match chan.Reader.TryRead() with
                            | (false, _) -> ()
                            | (true, entry) ->
                                ignore <| transaction.System.Events.Add(entry)
                                addOne ()
                        addOne ()
                        let! changed = transaction.System.SaveChangesAsync stoppingToken
                        do! transaction.Commit ()
                        logger.LogInformation("Logged {0} events into database", changed)
                        ()
                    with
                    | ex ->
                        logger.LogError(ex, "Exception while logging events")
        } :> Task

    member this.WriteEvent (entry : EventEntry) =
        chan.Writer.WriteAsync entry