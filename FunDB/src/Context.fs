module FunWithFlags.FunDB.Context

open System

open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Schema

type ContextCache (connectionString : string) =
    member this.ConnectionString = connectionString

type RequestContext (cache : ContextCache, userName : UserName) =
    let conn = new DatabaseConnection(cache.ConnectionString)
    let layout =
        let layoutSource = buildSchemaLayout conn.System
        resolveLayout layoutSource
    let allowedDatabase = buildAllowedDatabase layout

    member this.Connection = conn
    member this.Layout = layout
    member this.AllowedDatabase = allowedDatabase
    member this.UserName = userName

    interface IDisposable with
        member this.Dispose () =
            (conn :> IDisposable).Dispose()