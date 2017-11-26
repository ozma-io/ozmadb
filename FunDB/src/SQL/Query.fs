module internal FunWithFlags.FunDB.SQL.Query

open System
open Npgsql

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Render

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    let convertValue valType value =
        if value = null
        then VNull
        else
            match valType with
                | Some(_) ->
                    printfn "Value type: %s" (value.GetType.ToString ())
                    VString(value.ToString ())
                | None -> VString(value.ToString ())

    let executeNonQuery queryStr =
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            ignore <| command.ExecuteNonQuery()
        finally
            connection.Close()

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()

    // Make it return Values instead of strings.
    member this.Query (expr : SelectExpr) : ((ValueType option) array) * (QualifiedValue array array) =
        let queryStr = renderSelect expr
        printfn "Select query: %s" queryStr
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            use reader = command.ExecuteReader()
            let types = seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.GetDataTypeName(i) |> parseCoerceValueType) |> Seq.toArray
            let values =
                seq { while reader.Read() do
                      yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i] |> convertValue types.[i]) |> Seq.toArray
                } |> Seq.toArray
            (types, values)
        finally
            connection.Close()

    member this.Insert (expr : InsertExpr) =
        let queryStr = renderInsert expr
        printfn "Insert query: %s" queryStr
        executeNonQuery queryStr

    member this.Update (expr : UpdateExpr) =
        let queryStr = renderUpdate expr
        printfn "Update query: %s" queryStr
        executeNonQuery queryStr

    member this.Delete (expr : DeleteExpr) =
        let queryStr = renderDelete expr
        printfn "Delete query: %s" queryStr
        executeNonQuery queryStr

    member this.ApplyOperation (op : SchemaOperation) =
        let queryStr = renderSchemaOperation op
        printfn "Schema operation query: %s" queryStr
        executeNonQuery queryStr
