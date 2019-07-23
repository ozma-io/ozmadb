module FunWithFlags.FunDB.Connection

open System
open System.Data
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Npgsql
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL.Query

type DatabaseConnection (loggerFactory : ILoggerFactory, connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)
    do
        connection.Open()
    let query = QueryConnection(loggerFactory, connection)

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()

    member this.Query = query
    member this.Connection = connection
    member this.LoggerFactory = loggerFactory

type DatabaseTransaction (conn : DatabaseConnection) =
    // FIXME: Maybe introduce more granular locking?
    let transaction = conn.Connection.BeginTransaction(IsolationLevel.Serializable)

    let system =
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseLoggerFactory(conn.LoggerFactory)
                .UseNpgsql(conn.Connection)
        new SystemContext(systemOptions.Options)
    do
        system.ChangeTracker.QueryTrackingBehavior <- QueryTrackingBehavior.NoTracking
        ignore <| system.Database.UseTransaction(transaction)

    interface IDisposable with
        member this.Dispose () = this.Rollback ()

    member this.Rollback () =
        transaction.Dispose()
        system.Dispose()

    member this.Commit () = task {
        let! _ = system.SaveChangesAsync ()
        do! transaction.CommitAsync ()
        this.Rollback()
    }

    member this.System = system
    member this.Transaction = transaction
    member this.Connection = conn
