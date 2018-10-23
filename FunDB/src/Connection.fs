module FunWithFlags.FunDB.Connection

open System
open System.Data
open Microsoft.EntityFrameworkCore
open Npgsql

open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL.Query

type DatabaseConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)
    do
        connection.Open()
    let query = new QueryConnection(connection)
    let system =
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseNpgsql(connection)
        new SystemContext(systemOptions.Options)
    // Introduce better locking
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
        transaction.Commit()
    
    member this.Query = query
    member this.System = system
    member this.Connection = connection
    member this.Transaction = transaction
