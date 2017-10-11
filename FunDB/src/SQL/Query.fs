module internal FunWithFlags.FunDB.SQL.Query

open System
open Npgsql

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Render

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    let executeNonQuery queryStr =
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            command.ExecuteNonQuery()
        finally
            connection.Close()

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()

    // Make it return Values instead of strings.
    member this.Query (expr : SelectExpr) : string array array =
        let queryStr = renderSelect expr
        printf "Select query: %s" queryStr
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            use reader = command.ExecuteReader()
            seq { while reader.Read() do
                      yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i].ToString ()) |> Seq.toArray
                } |> Seq.toArray
        finally
            connection.Close()

    member this.Insert (expr : InsertExpr) =
        let queryStr = renderInsert expr
        printf "Insert query: %s" queryStr
        executeNonQuery queryStr

    member this.Update (expr : UpdateExpr) =
        let queryStr = renderUpdate expr
        printf "Update query: %s" queryStr
        executeNonQuery queryStr

    member this.Delete(expr: DeleteExpr) =
        let queryStr = renderDelete expr
        printf "Delete query: %s" queryStr
        executeNonQuery queryStr
