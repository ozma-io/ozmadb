namespace FunWithFlags.FunDB.Context

open System
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.View

type Context (db : DatabaseContext) =
    let dbQuery = new QueryConnection(db.Database.GetDbConnection().ConnectionString)
    let qualifier = new Qualifier(db)
    let resolver = new ViewResolver(dbQuery, db, qualifier)

    member this.Dispose () =
        (dbQuery :> IDisposable).Dispose ()
        db.Dispose ()

    interface IDisposable with
        member this.Dispose () = this.Dispose ()

    new (connectionString : string, loggerFactory : ILoggerFactory) =
        let dbOptions = (new DbContextOptionsBuilder<DatabaseContext>())
                         .UseNpgsql(connectionString)
                         .UseLoggerFactory(loggerFactory)
        let db = new DatabaseContext(dbOptions.Options)
        new Context(db)

    member this.Database = db
    member this.Resolver = resolver
    member this.Qualifier = qualifier
