module FunWithFlags.FunDB.Connection

open System
open System.Data
open System.Threading
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Npgsql
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type DeferredConstraintsException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        DeferredConstraintsException (message, innerException, isUserException innerException)

    new (message : string) = DeferredConstraintsException (message, null, true)

type DatabaseConnection (loggerFactory : ILoggerFactory, connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)
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

    member this.OpenAsync (cancellationToken : CancellationToken) =
        connection.OpenAsync cancellationToken

    member this.CloseAsync () =
        connection.CloseAsync ()

let inline private tryEFUpdateQuery (f : unit -> Task<'a>) : Task<'a> =
    task {
        try
            return! f ()
        with
        | :? DbUpdateException as err ->
            match err.InnerException with
            // 40001: could not serialize access due to concurrent update
            | :? PostgresException as perr when perr.SqlState = "40001" ->
                return raisefWithInner ConcurrentUpdateException err "Concurrent update detected"
            | _ ->
                return reraise' err
    }

let serializedSaveChangesAsync (db : DbContext) (cancellationToken : CancellationToken) =
    tryEFUpdateQuery <| fun () ->
        task {
            let! changed = db.SaveChangesAsync cancellationToken
            return changed > 0
        }

type DatabaseTransaction (conn : DatabaseConnection, isolationLevel : IsolationLevel) =
    let transaction = conn.Connection.BeginTransaction(isolationLevel)
    let logger = conn.LoggerFactory.CreateLogger<DatabaseTransaction>()
    let mutable lastNameId = 0

    let system =
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseLoggerFactory(conn.LoggerFactory)
                .UseNpgsql(conn.Connection)
#if DEBUG
        ignore <| systemOptions.EnableSensitiveDataLogging()
#endif
        new SystemContext(systemOptions.Options)
    do
        system.ChangeTracker.QueryTrackingBehavior <- QueryTrackingBehavior.NoTracking
        ignore <| system.Database.UseTransaction(transaction)

    let mutable constraintsDeferred = false

    let setConstraintsImmediate (cancellationToken : CancellationToken) =
        unitTask {
            try
                let! _ = conn.Query.ExecuteNonQuery "SET CONSTRAINTS ALL IMMEDIATE" Map.empty cancellationToken
                constraintsDeferred <- false
            with
            | :? QueryException as ex -> raisefUserWithInner DeferredConstraintsException ex ""
        }

    new (conn : DatabaseConnection) =
        // FIXME: Maybe introduce more granular locking?
        new DatabaseTransaction(conn, IsolationLevel.Serializable)

    member this.Rollback () =
        unitVtask {
            do! transaction.DisposeAsync ()
            do! system.DisposeAsync ()
        }

    // Consumers should use this method instead of `System.SaveChangesAsync` to properly handle serialization errors.
    member this.SystemSaveChangesAsync (cancellationToken : CancellationToken) : Task<bool> =
        serializedSaveChangesAsync system cancellationToken

    member this.Commit (cancellationToken : CancellationToken) : Task<int> =
        tryEFUpdateQuery <| fun () -> task {
            let! changed = system.SaveChangesAsync (cancellationToken)
            do! transaction.CommitAsync (cancellationToken)
            do! this.Rollback ()
            return changed
        }

    member this.DeferConstraints (cancellationToken : CancellationToken) (f : unit -> Task<'a>) : Task<'a> =
        task {
            let prevState = constraintsDeferred
            if not prevState then
                let! _ = conn.Query.ExecuteNonQuery "SET CONSTRAINTS ALL DEFERRED" Map.empty cancellationToken
                constraintsDeferred <- true
            let! ret =
                task {
                    try
                        return! f ()
                    with
                    | e when not prevState ->
                        try
                            do! setConstraintsImmediate cancellationToken
                        with
                        | ce -> logger.LogError(ce, "Error when setting constraints immediate during handling of another exception")
                        return reraise' e
                }
            if not prevState then
                do! setConstraintsImmediate cancellationToken
            return ret
        }

    member this.NewAnonymousName (prefix : string) =
        let id = lastNameId
        lastNameId <- lastNameId + 1
        sprintf "%s_%i" prefix id

    member this.NewAnonymousSQLName (prefix : string) =
        SQL.SQLName (this.NewAnonymousName prefix)

    member this.NewAnonymousFunQLName (prefix : string) =
        FunQLName (this.NewAnonymousName prefix)

    interface IDisposable with
        member this.Dispose () =
            transaction.Dispose ()
            system.Dispose ()

    interface IAsyncDisposable with
        member this.DisposeAsync () = this.Rollback ()

    member this.System = system
    member this.Transaction = transaction
    member this.Connection = conn
    member this.ConstraintsDeferred = constraintsDeferred

// Create a connection and run the first request to the database for a given transaction, expecting it to maybe fail because of stale connections.
// If failed, we retry.
// Transfers ownership of `DatabaseTransaction` instance to the given `check` function.
let openAndCheckTransaction (loggerFactory : ILoggerFactory) (connectionString : string) (isolationLevel : IsolationLevel) (cancellationToken : CancellationToken) (check : DatabaseTransaction -> Task<'a>) : Task<'a> =
    let logger = loggerFactory.CreateLogger("openAndCheckTransaction")
    let rec tryOne () =
        task {
            let connection = new DatabaseConnection(loggerFactory, connectionString)
            do! connection.OpenAsync cancellationToken
            let transaction = new DatabaseTransaction (connection, isolationLevel)
            try
                return! check transaction
            with
            // SQL states:
            // 57P01: terminating connection due to administrator command
            | :? PostgresException as ex when ex.SqlState = "57P01" ->
                do! (transaction :> IAsyncDisposable).DisposeAsync ()
                do! (connection :> IAsyncDisposable).DisposeAsync ()
                logger.LogWarning("Stale connection detected, trying to reconnect")
                return! tryOne ()
            | e ->
                do! (transaction :> IAsyncDisposable).DisposeAsync ()
                do! (connection :> IAsyncDisposable).DisposeAsync ()
                return reraise' e
        }
    tryOne ()
