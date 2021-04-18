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
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL.Query

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

    new (conn : DatabaseConnection) =
        // FIXME: Maybe introduce more granular locking?
        new DatabaseTransaction(conn, IsolationLevel.Serializable)

    member this.Rollback () =
        unitVtask {
            do! transaction.DisposeAsync ()
            do! system.DisposeAsync ()
        }

    // Consumers should use this method instead of `System.SaveChangesAsync` to properly handle serialization errors.
    member this.SystemSaveChangesAsync (cancellationToken : CancellationToken) =
        serializedSaveChangesAsync system cancellationToken

    member this.Commit (cancellationToken : CancellationToken) =
        tryEFUpdateQuery <| fun () -> task {
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

type SystemUpdaterException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = SystemUpdaterException (message, null)

type SystemUpdater(db : SystemContext) =
    let mutable deletedObjects = []

    member this.DeleteObject row =
        deletedObjects <- (row :> obj) :: deletedObjects

    member this.DeleteOldObjects () =
        for row in deletedObjects do
            let entry = db.Entry(row)
            // This is to prevent double deletes; first cascaded and second explicit.
            if entry.State <> EntityState.Detached then
                ignore <| db.Remove(row)

    member this.UpdateDifference (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
        updateDifference db updateFunc createFunc (fun _ -> this.DeleteObject) newObjects existingObjects

    member this.UpdateRelatedDifference (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
        let createFunc name = raisef SystemUpdaterException "Object %O doesn't exist" name
        let deleteFunc name obj = raisef SystemUpdaterException "Refusing to delete object %O" name
        updateDifference db updateFunc createFunc deleteFunc newObjects existingObjects

// We first load existing rows and update/create new ones, then delay the second stage when we remove old rows.
// Otherwise an issue with removing a row without removing related rows may happen:
// 1. A different `update` function deletes a referenced row;
// 2. Now this `update` function will miss all rows which reference deleted row because EF Core uses `INNER JOIN`, and the deleted row, well, doesn't exist.
// For example: an entity is removed and we don't remove all default attributes with `field_entity` referencing this one.
let genericSystemUpdate<'updater when 'updater :> SystemUpdater> (db : SystemContext) (cancellationToken : CancellationToken) (getUpdater : unit -> Task<'updater>) : Task<unit -> Task<bool>> =
    task {
        let! _ = serializedSaveChangesAsync db cancellationToken
        let! updater = getUpdater ()
        let! changed1 = serializedSaveChangesAsync db cancellationToken
        return fun () ->
            task {
                updater.DeleteOldObjects ()
                let! changed2 = serializedSaveChangesAsync db cancellationToken
                return changed1 || changed2
            }
    }
