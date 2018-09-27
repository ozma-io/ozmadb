module FunWithFlags.FunDB.SQL.Query

open System
open Npgsql
open NpgsqlTypes

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Lexer
open FunWithFlags.FunDB.SQL.Parser

exception QueryError of info : string with
    override this.Message = this.info

type ExprParameters = Map<int, SimpleValueType * Value>

type QueryResult =
    { columns : (SQLName * SimpleValueType) array
      rows : Value array array
    }

type QueryConnection (connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)

    let parseType typeStr =
        match parse tokenizeSQL valueType typeStr with
            | Ok valType ->
                let coerceType typeName =
                    match findSimpleType typeName with
                        | None -> raise (QueryError <| sprintf "Unknown database type: %O" typeName)
                        | Some t -> t
                mapValueType coerceType valType
            | Error msg -> raise (QueryError <| sprintf "Cannot parse database type: %s" msg)

    let convertInt : obj -> int option = function
        | :? byte as value -> Some <| int value
        | :? sbyte as value -> Some <| int value
        | :? int16 as value -> Some <| int value
        | :? uint16 as value -> Some <| int value
        | :? int32 as value -> Some <| int value
        // XXX: possible casting problem
        | :? uint32 as value -> Some <| int value
        | :? int64 as value -> Some <| int value
        | :? uint64 as value -> Some <| int value
        | value -> None

    let convertValue valType (rawValue : obj) =
        match (valType, rawValue) with
            | (_, (:? DBNull as value)) -> VNull
            | (VTScalar STInt, value) ->
                match convertInt value with
                    | Some i -> VInt i
                    | None -> raise (QueryError <| sprintf "Unknown integer type: %s" (value.GetType().FullName))
            | (VTScalar STString, (:? string as value)) -> VString (value)
            | (VTScalar STBool, (:? bool as value)) -> VBool (value)
            | (VTScalar STDateTime, (:? DateTime as value)) -> VDateTime (value)
            | (VTScalar STDate, (:? DateTime as value)) -> VDate (value)
            | (VTArray scalarType, (:? (obj array) as rootVals)) ->
                let rec convertArray (convFunc : obj -> 'a option) (vals : obj array) : ValueArray<'a> =
                    let convertOne : obj -> ArrayValue<'a> = function
                        | :? DBNull as value -> AVNull
                        | :? (obj array) as subVals -> AVArray (convertArray convFunc subVals)
                        | value ->
                            match convFunc value with
                                | Some v -> AVValue v
                                | None -> raise (QueryError <| sprintf "Cannot convert array value: %O" value)
                    Array.map convertOne vals

                match scalarType with
                    | STInt -> VIntArray (convertArray convertInt rootVals)
                    | STString -> VStringArray (convertArray tryCast<string> rootVals)
                    | STBool -> VBoolArray (convertArray tryCast<bool> rootVals)
                    | STDateTime -> VDateTimeArray (convertArray tryCast<DateTime> rootVals)
                    | STDate -> VDateArray (convertArray tryCast<DateTime> rootVals)
                    | STRegclass -> raise (QueryError <| sprintf "Regclass arrays are not supported: %O" rootVals)
            | (typ, value) -> raise (QueryError <| sprintf "Cannot convert raw SQL value: result type %s, value type %s" (typ.ToSQLString()) (value.GetType().FullName))

    let npgsqlSimpleType : SimpleType -> NpgsqlDbType = function
        | STInt -> NpgsqlDbType.Integer
        | STString -> NpgsqlDbType.Text
        | STBool -> NpgsqlDbType.Boolean
        | STDateTime -> NpgsqlDbType.Timestamp
        | STDate -> NpgsqlDbType.Date
        | STRegclass -> raise <| QueryError "Regclass type is not supported"

    let npgsqlType : SimpleValueType -> NpgsqlDbType = function
        | VTScalar s -> npgsqlSimpleType s
        | VTArray s -> npgsqlSimpleType s ||| NpgsqlDbType.Array

    let rec npgsqlArrayValue (vals : ArrayValue<'a> array) : obj =
        let convertValue : ArrayValue<'a> -> obj = function
            | AVValue v -> upcast v
            | AVArray vals -> npgsqlArrayValue vals
            | AVNull -> upcast DBNull.Value
        upcast (Array.map convertValue vals)

    let npgsqlValue : Value -> obj = function
        | VInt i -> upcast i
        | VString s -> upcast s
        | VRegclass name -> raise (QueryError <| sprintf "Regclass arguments are not supported: %O" name)
        | VBool b -> upcast b
        | VDateTime dt -> upcast dt
        | VDate dt -> upcast dt
        | VIntArray vals -> npgsqlArrayValue vals
        | VStringArray vals -> npgsqlArrayValue vals
        | VBoolArray vals -> npgsqlArrayValue vals
        | VDateTimeArray vals -> npgsqlArrayValue vals
        | VDateArray vals -> npgsqlArrayValue vals
        | VRegclassArray vals -> raise (QueryError <| sprintf "Regclass arguments are not supported: %O" vals)
        | VNull -> upcast DBNull.Value

    let withCommand (queryStr : string) (pars : ExprParameters) (runFunc : NpgsqlCommand -> 'r) =
        use command = new NpgsqlCommand(queryStr, connection)
        for KeyValue (name, (valueType, value)) in pars do
            ignore <| command.Parameters.AddWithValue(name.ToString(), npgsqlType valueType, npgsqlValue value)
        command.Prepare()
        connection.Open()
        try
            runFunc command
        finally
            connection.Close()

    member this.ExecuteNonQuery (queryStr : string) (pars : ExprParameters) =
        withCommand queryStr pars <| fun command ->
            ignore <| command.ExecuteNonQuery()

    member this.ExecuteQuery (queryStr : string) (pars : ExprParameters) : QueryResult =
        withCommand queryStr pars <| fun command ->
            use reader = command.ExecuteReader()
            let getColumn i =
                let name = reader.GetName(i)
                let typ = parseType (reader.GetDataTypeName(i))
                (SQLName name, typ)
            let columns = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getColumn |> Seq.toArray
            let getRow i =
                let (_, typ) = columns.[i]
                reader.[i] |> convertValue typ
            let rows =
                seq {
                    while reader.Read() do
                        yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
                } |> Seq.toArray
            { columns = columns; rows = rows }

    interface IDisposable with
        member this.Dispose () =
            connection.Dispose()
