module OzmaDB.Connection

open System
open System.Data
open System.Threading
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Microsoft.EntityFrameworkCore.Diagnostics
open Npgsql
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils
open OzmaDBSchema.System
open OzmaDB.OzmaQL.AST
open OzmaDB.Exception
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST

type DeferredConstraintsException(message: string, innerException: QueryExecutionException, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: QueryExecutionException) =
        DeferredConstraintsException(message, innerException, isUserException innerException)

    member this.QueryException = innerException

type DatabaseConnection(loggerFactory: ILoggerFactory, connectionString: string) =
    let connection = new NpgsqlConnection(connectionString)
    let query = QueryConnection(loggerFactory, connection)

    interface IDisposable with
        member this.Dispose() = connection.Dispose()

    interface IAsyncDisposable with
        member this.DisposeAsync() = connection.DisposeAsync()

    member this.Query = query
    member this.Connection = connection
    member this.LoggerFactory = loggerFactory

    member this.OpenAsync(cancellationToken: CancellationToken) = connection.OpenAsync cancellationToken

    member this.CloseAsync() = connection.CloseAsync()

let inline private tryEFUpdateQuery (f: unit -> Task<'a>) : Task<'a> =
    task {
        try
            return! f ()
        with :? DbUpdateException as err ->
            match err.InnerException with
            // 40001: could not serialize access due to concurrent update
            | :? PostgresException as perr when perr.SqlState = "40001" ->
                return raisefWithInner ConcurrentUpdateException err "Concurrent update detected"
            | _ -> return reraise' err
    }

let serializedSaveChangesAsync (db: DbContext) (cancellationToken: CancellationToken) =
    tryEFUpdateQuery
    <| fun () ->
        task {
            let! changed = db.SaveChangesAsync cancellationToken
            return changed > 0
        }

let setupDbContextLogging (loggerFactory: ILoggerFactory) (builder: DbContextOptionsBuilder<'a>) =
    let configureWarnings (warnings: WarningsConfigurationBuilder) =
        ignore
        <| warnings.Log(
            [| struct (RelationalEventId.CommandExecuting, LogLevel.Debug)
               struct (RelationalEventId.CommandExecuted, LogLevel.Debug)
               struct (CoreEventId.ContextInitialized, LogLevel.Debug) |]
        )

    ignore
    <| builder.UseLoggerFactory(loggerFactory).ConfigureWarnings(configureWarnings)

type DatabaseTransaction private (conn: DatabaseConnection, transaction: NpgsqlTransaction) =
    let logger = conn.LoggerFactory.CreateLogger<DatabaseTransaction>()
    let mutable lastNameId = 0

    let system =
        let builder = DbContextOptionsBuilder<SystemContext>()
        setupDbContextLogging conn.LoggerFactory builder

        ignore
        <| builder.UseNpgsql(conn.Connection, (fun opts -> ignore <| opts.UseNodaTime()))
#if DEBUG
        ignore <| builder.EnableSensitiveDataLogging()
#endif
        new SystemContext(builder.Options)

    do
        system.ChangeTracker.QueryTrackingBehavior <- QueryTrackingBehavior.NoTracking
        ignore <| system.Database.UseTransaction(transaction)

    let mutable constraintsDeferred = false

    let setConstraintsImmediate (cancellationToken: CancellationToken) =
        unitTask {
            try
                let! _ = conn.Query.ExecuteNonQuery "SET CONSTRAINTS ALL IMMEDIATE" Map.empty cancellationToken
                constraintsDeferred <- false
            with :? QueryExecutionException as e ->
                raise <| DeferredConstraintsException("", e, true)
        }

    static member Begin(conn: DatabaseConnection) =
        DatabaseTransaction.Begin(conn, IsolationLevel.Serializable)

    static member Begin(conn: DatabaseConnection, isolationLevel: IsolationLevel) =
        task {
            let! transaction = conn.Connection.BeginTransactionAsync(isolationLevel)
            return new DatabaseTransaction(conn, transaction)
        }

    member this.Rollback() =
        unitVtask {
            do! system.DisposeAsync()
            do! transaction.DisposeAsync()
        }

    // Consumers should use this method instead of `System.SaveChangesAsync` to properly handle serialization errors.
    member this.SystemSaveChangesAsync(cancellationToken: CancellationToken) : Task<bool> =
        serializedSaveChangesAsync system cancellationToken

    member this.Commit(cancellationToken: CancellationToken) : Task<int> =
        tryEFUpdateQuery
        <| fun () ->
            task {
                let! changed = system.SaveChangesAsync(cancellationToken)
                do! transaction.CommitAsync(cancellationToken)
                do! this.Rollback()
                return changed
            }

    member this.DeferConstraints (cancellationToken: CancellationToken) (f: unit -> Task<'a>) : Task<'a> =
        task {
            let prevState = constraintsDeferred

            if not prevState then
                let! _ = conn.Query.ExecuteNonQuery "SET CONSTRAINTS ALL DEFERRED" Map.empty cancellationToken
                constraintsDeferred <- true

            let! ret =
                task {
                    try
                        return! f ()
                    with e when not prevState ->
                        try
                            do! setConstraintsImmediate cancellationToken
                        with ce ->
                            logger.LogError(
                                ce,
                                "Error when setting constraints immediate during handling of another exception"
                            )

                        return reraise' e
                }

            if not prevState then
                do! setConstraintsImmediate cancellationToken

            return ret
        }

    member this.NewAnonymousName(prefix: string) =
        let id = lastNameId
        lastNameId <- lastNameId + 1
        sprintf "__%s_%i" prefix id

    member this.NewAnonymousSQLName(prefix: string) =
        SQL.SQLName(this.NewAnonymousName prefix)

    member this.NewAnonymousOzmaQLName(prefix: string) =
        OzmaQLName(this.NewAnonymousName prefix)

    interface IDisposable with
        member this.Dispose() =
            system.Dispose()
            transaction.Dispose()

    interface IAsyncDisposable with
        member this.DisposeAsync() = this.Rollback()

    member this.System = system
    member this.Transaction = transaction
    member this.Connection = conn
    member this.ConstraintsDeferred = constraintsDeferred

// Create a connection and run the first request to the database for a given transaction, expecting it to maybe fail because of stale connections.
// If failed, we retry.
// Transfers ownership of `DatabaseTransaction` instance to the given `check` function.
let openAndCheckTransaction
    (loggerFactory: ILoggerFactory)
    (connectionString: string)
    (isolationLevel: IsolationLevel)
    (cancellationToken: CancellationToken)
    (check: DatabaseTransaction -> Task<'a>)
    : Task<'a> =
    let logger = loggerFactory.CreateLogger("openAndCheckTransaction")

    let rec tryOne () =
        task {
            let connection = new DatabaseConnection(loggerFactory, connectionString)
            do! connection.OpenAsync cancellationToken
            let! transaction = DatabaseTransaction.Begin(connection, isolationLevel)

            try
                return! check transaction
            with
            // SQL states:
            // 57P01: terminating connection due to administrator command
            | :? PostgresException as ex when ex.SqlState = "57P01" ->
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                do! (connection :> IAsyncDisposable).DisposeAsync()
                logger.LogWarning("Stale connection detected, trying to reconnect")
                return! tryOne ()
            | e ->
                do! (transaction :> IAsyncDisposable).DisposeAsync()
                do! (connection :> IAsyncDisposable).DisposeAsync()
                return reraise' e
        }

    tryOne ()
