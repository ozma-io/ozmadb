module internal FunWithFlags.FunDB.SQL.Query

open System
open Npgsql

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Render

type EvaluateResult =
    { name: string;
      valueType: ValueType;
      value: QualifiedValue;
    }

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    let convertValue valType (rawValue : obj) =
        match (valType, rawValue) with
            | (_, (:? DBNull as value)) -> VNull

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
            | (VTDate, (:? DateTime as value)) -> VDate(value)
            | (typ, value) -> failwith <| sprintf "Unknown raw SQL value type: %s, value type: %O" (value.GetType ()).FullName typ

    let executeNonQuery queryStr =
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            ignore <| command.ExecuteNonQuery()
        finally
            connection.Close()

    let executeQuery queryStr : ((string * ValueType) array) * (QualifiedValue array array) =
        use command = new NpgsqlCommand(queryStr, connection)
        connection.Open()
        try
            use reader = command.ExecuteReader()
            let getColumn i =
                let name = reader.GetName(i)
                let typ = reader.GetDataTypeName(i) |> parseCoerceValueType |> Option.get
                (name, typ)
            let columns = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getColumn |> Seq.toArray
            let getRow i =
                let (_, typ) = columns.[i]
                reader.[i] |> convertValue typ
            let values =
                seq { while reader.Read() do
                          yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
                } |> Seq.toArray
            (columns, values)
        finally
            connection.Close()

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()

    // TODO: Make a cursored version
    member this.Query (expr : SelectExpr) =
        let queryStr = renderSelect expr
        eprintfn "Select query: %s" queryStr
        executeQuery queryStr

    member this.Evaluate (expr : EvaluateExpr) =
        let queryStr = renderEvaluate expr
        eprintfn "Evaluate query: %s" queryStr
        let (columns, results) = executeQuery queryStr
        if Array.length results <> 1 then
            failwith "Evaluate query is not expected to have other than one result row"
        Seq.map2 (fun (name, typ) value -> { name = name; valueType = typ; value = value; }) columns results.[0] |> Array.ofSeq

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
