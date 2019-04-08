module FunWithFlags.FunDB.Operations.Connection

open System
open System.Data
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Npgsql

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL.Query

type DatabaseConnection (loggerFactory : ILoggerFactory, connectionString : string) =
    let logger = loggerFactory.CreateLogger<DatabaseConnection>()
    let connection = new NpgsqlConnection(connectionString)
    do
        connection.Open()
    let query = QueryConnection(loggerFactory, connection)
    let system =
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseLoggerFactory(loggerFactory)
                .UseNpgsql(connection)
        new SystemContext(systemOptions.Options)
    // FIXME: Maybe introduce more granular locking?
    let transaction = connection.BeginTransaction(IsolationLevel.Serializable)
    do
        ignore <| system.Database.UseTransaction(transaction)
        system.ChangeTracker.QueryTrackingBehavior <- QueryTrackingBehavior.NoTracking

    interface IDisposable with
        member this.Dispose () =
            transaction.Dispose()
            system.Dispose()
            connection.Dispose()

    member this.Commit () =
        logger.LogInformation("Commiting changes")
        transaction.CommitAsync()

    member this.Query = query
    member this.System = system
    member this.Connection = connection
    member this.Transaction = transaction
