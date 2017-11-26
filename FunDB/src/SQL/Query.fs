module internal FunWithFlags.FunDB.SQL.Query

open System
open Npgsql

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Render

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    let convertValue valType (rawValue : obj) =
        match (valType, rawValue) with
            | (_, null) -> VNull

            | (VTInt, (:? byte as value)) -> VInt(int value)
            | (VTInt, (:? sbyte as value)) -> VInt(int value)
            | (VTInt, (:? int16 as value)) -> VInt(int value)
            | (VTInt, (:? uint16 as value)) -> VInt(int value)
            | (VTInt, (:? int32 as value)) -> VInt(int value)
            | (VTInt, (:? uint32 as value)) -> VInt(int value)
            // XXX: possible casting problem
            | (VTInt, (:? int64 as value)) -> VInt(int value)
            | (VTInt, (:? uint64 as value)) -> VInt(int value)

            | (VTFloat, (:? single as value)) -> VFloat(double value)
            | (VTFloat, (:? double as value)) -> VFloat(double value)

            | (VTString, (:? string as value)) -> VString(value)
            | (VTBool, (:? bool as value)) -> VBool(value)
            | (VTDateTime, (:? DateTime as value)) -> VDateTime(value)
            | (VTDate, (:? DateTime as value)) -> VDateTime(value)
            | (typ, value) -> failwith <| sprintf "Unknown raw SQL value type: %s, value type: %O" (value.GetType ()).FullName typ

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
    member this.Query (expr : SelectExpr) : (ValueType array) * (QualifiedValue array array) =
        let queryStr = renderSelect expr
        eprintfn "Select query: %s" queryStr
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            use reader = command.ExecuteReader()
            let types = seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.GetDataTypeName(i) |> parseCoerceValueType |> Option.get) |> Seq.toArray
            let values =
                seq { while reader.Read() do
                      yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i] |> convertValue types.[i]) |> Seq.toArray
                } |> Seq.toArray
            (types, values)
        finally
            connection.Close()

    member this.Insert (expr : InsertExpr) =
        let queryStr = renderInsert expr
        eprintfn "Insert query: %s" queryStr
        executeNonQuery queryStr

    member this.Update (expr : UpdateExpr) =
        let queryStr = renderUpdate expr
        eprintfn "Update query: %s" queryStr
        executeNonQuery queryStr

    member this.Delete (expr : DeleteExpr) =
        let queryStr = renderDelete expr
        eprintfn "Delete query: %s" queryStr
        executeNonQuery queryStr

    member this.ApplyOperation (op : SchemaOperation) =
        let queryStr = renderSchemaOperation op
        eprintfn "Schema operation query: %s" queryStr
        executeNonQuery queryStr
