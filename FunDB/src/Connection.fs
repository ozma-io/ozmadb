module FunWithFlags.FunDB.Connection

open System
open System.Data
open System.Threading
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Npgsql
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.SQL.Query

type DatabaseConnection (loggerFactory : ILoggerFactory, connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)
    do
        connection.Open()
    let query = QueryConnection(loggerFactory, connection)

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose ()

    interface IAsyncDisposable with
        member this.DisposeAsync () =
            connection.DisposeAsync ()

    member this.Query = query
    member this.Connection = connection
    member this.LoggerFactory = loggerFactory

type DatabaseTransaction (conn : DatabaseConnection, isolationLevel : IsolationLevel) =
    let transaction = conn.Connection.BeginTransaction(isolationLevel)

    let system =
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseLoggerFactory(conn.LoggerFactory)
                .UseNpgsql(conn.Connection)
        new SystemContext(systemOptions.Options)
    do
        system.ChangeTracker.QueryTrackingBehavior <- QueryTrackingBehavior.NoTracking
        ignore <| system.Database.UseTransaction(transaction)

    new (conn : DatabaseConnection) =
        // FIXME: Maybe introduce more granular locking?
        new DatabaseTransaction(conn, IsolationLevel.Serializable)

    member this.Rollback () =
        unitVtask {
            do! transaction.DisposeAsync ()
            do! system.DisposeAsync ()
        }

    member this.Commit (cancellationToken : CancellationToken) =
        task {
            let! changed = system.SaveChangesAsync (cancellationToken)
            do! transaction.CommitAsync (cancellationToken)
            do! this.Rollback ()
            return changed
        }

    interface IDisposable with
        member this.Dispose () =
            transaction.Dispose ()
            system.Dispose ()

    interface IAsyncDisposable with
        member this.DisposeAsync () = this.Rollback ()

    member this.System = system
    member this.Transaction = transaction
    member this.Connection = conn

// Run the first request to the database for a given transaction, expecting it to fail because of stale connections.
// If failed, we retry.
let rec checkTransaction (transaction : DatabaseTransaction) (check : DatabaseTransaction -> Task<'a>) : Task<'a> =
    task {
        try
            return! check transaction
        with
        // SQL states:
        // 57P01: terminating connection due to administrator command
        | :? PostgresException as ex when ex.SqlState = "57P01" ->
            do! (transaction :> IAsyncDisposable).DisposeAsync ()
            do! (transaction.Connection :> IAsyncDisposable).DisposeAsync ()
            let connection = new DatabaseConnection (transaction.Connection.LoggerFactory, transaction.Connection.Connection.ConnectionString)
            let transaction = new DatabaseTransaction (connection, transaction.Transaction.IsolationLevel)
            return! checkTransaction transaction check
    }