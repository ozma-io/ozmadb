module FunWithFlags.FunDB.SQL.Query

open System
open Npgsql
open Npgsql.NpgsqlTypes

open FunWithFlags.FunDB.SQL.AST

exception QueryError of info : string with
    override this.Message = this.info

type ExprParameters = Map<int, ValueType * Value>

type QueryResult =
    { columns : (SQLName * ValueType) array
      values : Value array array
    }

type QueryConnection (connectionString : string) =
    let connection = NpgsqlConnection connectionString

    let convertValue valType (rawValue : obj) =
        match (valType, rawValue) with
            | (_, (:? DBNull as value)) -> VNull

            | (VTInt, (:? byte as value)) -> VInt (int value)
            | (VTInt, (:? sbyte as value)) -> VInt (int value)
            | (VTInt, (:? int16 as value)) -> VInt (int value)
            | (VTInt, (:? uint16 as value)) -> VInt (int value)
            | (VTInt, (:? int32 as value)) -> VInt (int value)
            | (VTInt, (:? uint32 as value)) -> VInt (int value)
            // XXX: possible casting problem
            | (VTInt, (:? int64 as value)) -> VInt (int value)
            | (VTInt, (:? uint64 as value)) -> VInt (int value)
            | (VTString, (:? string as value)) -> VString (value)
            | (VTBool, (:? bool as value)) -> VBool (value)
            | (VTDateTime, (:? DateTime as value)) -> VDateTime (value)
            | (VTDate, (:? DateTime as value)) -> VDate (value)
            | (typ, value) -> raise <| QueryError <| sprintf "Cannot convert raw SQL value: result type %s, value type %s" (typ.ToSQLName()) (value.GetType().FullName)

    let npgsqlSimpleType : SimpleType -> NpgsqlDbType = function
        | STInt -> NpgsqlDbType.Integer
        | STString -> NpgsqlDbType.Text
        | STBool -> NpgsqlDbType.Boolean
        | STDateTime -> NpgsqlDbType.Timestamp
        | STDate -> NpgsqlDbType.Date

    let npgsqlType : ValueType -> NpgsqlDbType = function
        | VTScalar s -> npsqlSimpleType s
        | VTArray s -> npgsqlSimpleType s ||| NpgsqlDbType.Array

    let rec npgsqlArrayValue (vals : ArrayValue<_> array) : object =
        let convertValue : ArrayValue<_> -> object = function
            | AVValue v -> v
            | AVArray vals -> npgsqlArrayValue vals
            | AVNull -> DBNull.Value
        Array.map convertValue vals

    let npgsqlValue : Value -> object = function
        | VInt i -> i
        | VString s -> s
        | VBool b -> b
        | VDateTime dt -> dt
        | VDate dt -> dt
        | VIntArray vals -> npgsqlArrayValue vals
        | VStringArray vals -> npgsqlArrayValue vals
        | VBoolArray vals -> npgsqlArrayValue vals
        | VDateTimeArray vals -> npgsqlArrayValue vals
        | VDateArray vals -> npgsqlArrayValue vals
        | VNull -> DBNull.Value

    let withCommand (queryStr : string) (params : ExprParameters) (runFunc : NpgsqlCommand -> 'r) =
        use command = NpgsqlCommand (queryStr, connection)
        for (name, (valueType, value)) in params do
            command.Parameters.AddWithValue(name.ToString(), npgsqlType valueType, npgsqlValue value)
        command.Prepare()
        connection.Open()
        try
            runFunc command
        finally
            connection.Close()

    member this.ExecuteNonQuery (queryStr : string) (params : ExprParameters) =
        withCommand queryStr params <| fun command ->
            ignore <| command.ExecuteNonQuery()

    member this.ExecuteQuery (queryStr : string) (params : ExprParameters) : QueryResult =
        withCommand queryStr params <| fun command ->
            use reader = command.ExecuteReader()
            let getColumn i =
                let name = reader.GetName(i)
                let typ =
                    match reader.GetDataTypeName(i) |> findSimpleType with
                        | Some t -> t
                        | None -> raise <| QueryError <| sprintf "Unknown result type: %s" (reader.GetDataTypeName(i))
                (SQLName name, typ)
            let columns = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getColumn |> Seq.toArray
            let getRow i =
                let (_, typ) = columns.[i]
                reader.[i] |> convertValue typ
            let values =
                seq {
                    while reader.Read() do
                        yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
                } |> Seq.toArray
            { columns = columns; values = values }

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()
