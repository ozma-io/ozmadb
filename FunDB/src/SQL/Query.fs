module FunWithFlags.FunDB.SQL.Query

open System
open System.Linq
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Npgsql
open NpgsqlTypes
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Lex
open FunWithFlags.FunDB.SQL.Parse

type QueryException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = QueryException (message, null)

type ExprParameters = Map<int, SimpleValueType * Value>

[<NoEquality; NoComparison>]
type QueryResult =
    { columns : (SQLName * SimpleValueType)[]
      rows : seq<Value[]>
    }

let private parseType typeStr =
    match parse tokenizeSQL valueType typeStr with
    | Ok valType ->
        let coerceType typeName =
            match findSimpleType typeName with
            | None -> raisef QueryException "Unknown database type: %O" typeName
            | Some t -> t
        mapValueType coerceType valType
    | Error msg -> raisef QueryException "Cannot parse database type: %s" msg

let private convertInt : obj -> int option = function
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

let private convertDate (rawDt : obj) =
    match rawDt with
    | :? DateTime as dt -> Some (DateTimeOffset dt)
    | _ -> None

let private convertValue valType (rawValue : obj) =
    match (valType, rawValue) with
    | (_, (:? DBNull as value)) -> VNull
    | (VTScalar STInt, value) ->
        match convertInt value with
        | Some i -> VInt i
        | None -> raisef QueryException "Unknown integer type: %s" (value.GetType().FullName)
    | (VTScalar STDecimal, (:? decimal as value)) -> VDecimal value
    | (VTScalar STString, (:? string as value)) -> VString value
    | (VTScalar STBool, (:? bool as value)) -> VBool value
    | (VTScalar STDateTime, (:? DateTimeOffset as value)) -> VDateTime value
    | (VTScalar STDateTime, (:? DateTime as value)) -> VDateTime (DateTimeOffset (value, TimeSpan.Zero))
    | (VTScalar STDate, (:? DateTime as value)) -> VDate (DateTimeOffset (value, TimeSpan.Zero))
    | (VTScalar STJson, (:? string as value)) ->
        match tryJson value with
        | Some j -> VJson j
        | None -> raisef QueryException "Invalid JSON value: %s" value
    | (VTArray scalarType, (:? Array as rootVals)) ->
        let rec convertArray (convFunc : obj -> 'a option) (vals : Array) : ValueArray<'a> =
            let convertOne : obj -> ArrayValue<'a> = function
                | :? DBNull -> AVNull
                | :? Array as subVals -> AVArray (convertArray convFunc subVals)
                | value ->
                    match convFunc value with
                    | Some v -> AVValue v
                    | None -> raisef QueryException "Cannot convert array value: %O" value
            Seq.map convertOne (vals.Cast<obj>()) |> Array.ofSeq

        match scalarType with
        | STInt -> VIntArray (convertArray convertInt rootVals)
        | STDecimal -> VDecimalArray (convertArray tryCast<decimal> rootVals)
        | STString -> VStringArray (convertArray tryCast<string> rootVals)
        | STBool -> VBoolArray (convertArray tryCast<bool> rootVals)
        | STDateTime -> VDateTimeArray (convertArray tryCast<DateTimeOffset> rootVals)
        | STDate -> VDateArray (convertArray convertDate rootVals)
        | STRegclass -> raisef QueryException "Regclass arrays are not supported: %O" rootVals
        | STJson -> VJsonArray (convertArray (tryCast<string> >> Option.bind tryJson) rootVals)
    | (typ, value) -> raisef QueryException "Cannot convert raw SQL value: result type %s, value type %s" (typ.ToSQLString()) (value.GetType().FullName)

let private npgsqlSimpleType : SimpleType -> NpgsqlDbType = function
    | STInt -> NpgsqlDbType.Integer
    | STDecimal -> NpgsqlDbType.Numeric
    | STString -> NpgsqlDbType.Text
    | STBool -> NpgsqlDbType.Boolean
    | STDateTime -> NpgsqlDbType.Timestamp
    | STDate -> NpgsqlDbType.Date
    | STRegclass -> raisef QueryException "Regclass type is not supported"
    | STJson -> NpgsqlDbType.Jsonb

let private npgsqlType : SimpleValueType -> NpgsqlDbType = function
    | VTScalar s -> npgsqlSimpleType s
    | VTArray s -> npgsqlSimpleType s ||| NpgsqlDbType.Array

let rec private npgsqlArrayValue (vals : ArrayValue<'a> array) : obj =
    let convertOne : ArrayValue<'a> -> obj = function
        | AVValue v -> upcast v
        | AVArray vals -> npgsqlArrayValue vals
        | AVNull -> upcast DBNull.Value
    upcast (Array.map convertOne vals)

let private npgsqlValue : Value -> obj = function
    | VInt i -> upcast i
    | VDecimal d -> upcast d
    | VString s -> upcast s
    | VRegclass name -> raisef QueryException "Regclass arguments are not supported: %O" name
    | VBool b -> upcast b
    | VDateTime dt -> upcast dt.UtcDateTime
    | VDate dt -> upcast dt.UtcDateTime
    | VJson j -> upcast j
    | VIntArray vals -> npgsqlArrayValue vals
    | VDecimalArray vals -> npgsqlArrayValue vals
    | VStringArray vals -> npgsqlArrayValue vals
    | VBoolArray vals -> npgsqlArrayValue vals
    | VDateTimeArray vals -> npgsqlArrayValue vals
    | VDateArray vals -> npgsqlArrayValue vals
    | VRegclassArray vals -> raisef QueryException "Regclass arguments are not supported: %O" vals
    | VJsonArray vals -> npgsqlArrayValue vals
    | VNull -> upcast DBNull.Value

type QueryConnection (loggerFactory : ILoggerFactory, connection : NpgsqlConnection) =
    let logger = loggerFactory.CreateLogger<QueryConnection> ()

    let withCommand (queryStr : string) (pars : ExprParameters) (runFunc : NpgsqlCommand -> Task<'a>) : Task<'a> =
        task {
            use command = new NpgsqlCommand(queryStr, connection)
            for KeyValue (name, (valueType, value)) in pars do
                ignore <| command.Parameters.AddWithValue(name.ToString(), npgsqlType valueType, npgsqlValue value)
            logger.LogInformation("Executing query with args {args}: {query}", pars, queryStr)
            try
                try
                    do! command.PrepareAsync()
                with :? PostgresException as ex ->
                    logger.LogError(ex, "Failed to prepare {query}", queryStr)
                    reraise' ex
                return! runFunc command
            with
            | :? PostgresException as ex ->
                return raisefWithInner QueryException ex "Error while executing"
        }

    member this.Connection = connection

    member this.ExecuteNonQuery (queryStr : string) (pars : ExprParameters) : Task<int> =
        withCommand queryStr pars <| fun command -> command.ExecuteNonQueryAsync()

    member this.ExecuteValueQuery (queryStr : string) (pars : ExprParameters) : Task<Value> =
        withCommand queryStr pars <| fun command -> task {
            use! reader = command.ExecuteReaderAsync()
            if reader.FieldCount <> 1 then
                raisef QueryException "Not one column"
            let typ = parseType (reader.GetDataTypeName(0))
            let! hasRow0 = reader.ReadAsync()
            if not hasRow0 then
                raisef QueryException "No first row"
            let result = reader.[0] |> convertValue typ
            let! hasRow1 = reader.ReadAsync()
            if hasRow1 then
                raisef QueryException "Has a second row"
            return result
        }

    member this.ExecuteQuery (queryStr : string) (pars : ExprParameters) (queryFunc : QueryResult -> Task<'a>) : Task<'a> =
        withCommand queryStr pars <| fun command -> task {
            use! reader = command.ExecuteReaderAsync()
            let getColumn i =
                let name = reader.GetName(i)
                let typ = parseType (reader.GetDataTypeName(i))
                (SQLName name, typ)
            let columns = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getColumn |> Seq.toArray
            let getRow i =
                let (_, typ) = columns.[i]
                reader.[i] |> convertValue typ

            let rows = List()
            // ??? F# ???
            let mutable hasRow = false
            let! hasRow0 = reader.ReadAsync()
            hasRow <- hasRow0
            while hasRow do
                let row = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
                rows.Add(row)
                let! hasRow1 = reader.ReadAsync()
                hasRow <- hasRow1

            return! queryFunc { columns = columns; rows = rows }
        }