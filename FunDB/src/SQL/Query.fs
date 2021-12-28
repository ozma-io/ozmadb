module FunWithFlags.FunDB.SQL.Query

open System
open System.Linq
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Npgsql
open NpgsqlTypes
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Misc
open FunWithFlags.FunDB.SQL.Lex
open FunWithFlags.FunDB.SQL.Parse

type QueryException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = QueryException (message, null)

type ConcurrentUpdateException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ConcurrentUpdateException (message, null)

type ExprParameters = Map<int, Value>

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
    | :? sbyte as value -> Some <| int value
    | :? int16 as value -> Some <| int value
    | :? int32 as value -> Some value
    | value -> None

let private convertDecimal : obj -> decimal option = function
    | :? decimal as value -> Some value
    | :? float32 as value -> Some <| decimal value
    | :? double as value -> Some <| decimal value
    | value -> None

let private convertValue valType (rawValue : obj) =
    match (valType, rawValue) with
    | (_, (:? DBNull as value)) -> VNull
    | (VTScalar STInt, value) ->
        match convertInt value with
        | Some i -> VInt i
        | None -> raisef QueryException "Unknown integer type: %s" (value.GetType().FullName)
    | (VTScalar STBigInt, (:? int64 as value)) -> VBigInt value
    | (VTScalar STDecimal, value) ->
        match convertDecimal value with
        | Some i -> VDecimal i
        | None -> raisef QueryException "Unknown decimal type: %s" (value.GetType().FullName)
    | (VTScalar STString, (:? string as value)) -> VString value
    | (VTScalar STBool, (:? bool as value)) -> VBool value
    | (VTScalar STDateTime, (:? NpgsqlDateTime as value)) -> VDateTime value
    | (VTScalar STDate, (:? NpgsqlDate as value)) -> VDate value
    | (VTScalar STInterval, (:? NpgsqlTimeSpan as value)) -> VInterval value
    | (VTScalar STJson, (:? string as value)) ->
        match tryJson value with
        | Some j -> VJson j
        | None -> raisef QueryException "Invalid JSON value: %s" value
    | (VTScalar STUuid, (:? Guid as value)) -> VUuid value
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
        | STBigInt -> VBigIntArray (convertArray tryCast<int64> rootVals)
        | STDecimal -> VDecimalArray (convertArray convertDecimal rootVals)
        | STString -> VStringArray (convertArray tryCast<string> rootVals)
        | STBool -> VBoolArray (convertArray tryCast<bool> rootVals)
        | STDateTime -> VDateTimeArray (convertArray tryCast<NpgsqlDateTime> rootVals)
        | STDate -> VDateArray (convertArray tryCast<NpgsqlDate> rootVals)
        | STInterval -> VIntervalArray (convertArray tryCast<NpgsqlTimeSpan> rootVals)
        | STRegclass -> raisef QueryException "Regclass arrays are not supported: %O" rootVals
        | STJson -> VJsonArray (convertArray (tryCast<string> >> Option.bind tryJson) rootVals)
        | STUuid -> VUuidArray (convertArray tryCast<Guid> rootVals)
    | (typ, value) -> raisef QueryException "Cannot convert raw SQL value: result type %s, value type %s" (typ.ToSQLString()) (value.GetType().FullName)

let rec private npgsqlArrayValue (vals : ArrayValue<'a> array) : obj =
    let convertOne : ArrayValue<'a> -> obj = function
        | AVValue v -> upcast v
        | AVArray vals -> npgsqlArrayValue vals
        | AVNull -> upcast DBNull.Value
    upcast (Array.map convertOne vals)

let private npgsqlArray (typ : NpgsqlDbType) (vals : ArrayValue<'a> array) : NpgsqlDbType option * obj =
    (Some (typ ||| NpgsqlDbType.Array), npgsqlArrayValue vals)

let private npgsqlValue : Value -> NpgsqlDbType option * obj = function
    | VInt i -> (Some NpgsqlDbType.Integer, upcast i)
    | VBigInt i -> (Some NpgsqlDbType.Bigint, upcast i)
    | VDecimal d -> (Some NpgsqlDbType.Numeric, upcast d)
    | VString s -> (Some NpgsqlDbType.Text, upcast s)
    | VRegclass name -> raisef QueryException "Regclass arguments are not supported: %O" name
    | VBool b -> (Some NpgsqlDbType.Boolean, upcast b)
    | VDateTime dt -> (Some NpgsqlDbType.Timestamp, upcast dt)
    | VDate dt -> (Some NpgsqlDbType.Date, upcast dt)
    | VInterval int -> (Some NpgsqlDbType.Interval, upcast int)
    | VJson j -> (Some NpgsqlDbType.Jsonb, upcast j)
    | VUuid u -> (Some NpgsqlDbType.Uuid, upcast u)
    | VIntArray vals -> npgsqlArray NpgsqlDbType.Integer vals
    | VBigIntArray vals -> npgsqlArray NpgsqlDbType.Bigint vals
    | VDecimalArray vals -> npgsqlArray NpgsqlDbType.Numeric vals
    | VStringArray vals -> npgsqlArray NpgsqlDbType.Text vals
    | VBoolArray vals -> npgsqlArray NpgsqlDbType.Boolean vals
    | VDateTimeArray vals -> npgsqlArray NpgsqlDbType.Timestamp vals
    | VDateArray vals -> npgsqlArray NpgsqlDbType.Date vals
    | VIntervalArray vals -> npgsqlArray NpgsqlDbType.Interval vals
    | VRegclassArray vals -> raisef QueryException "Regclass arguments are not supported: %O" vals
    | VJsonArray vals -> npgsqlArray NpgsqlDbType.Jsonb vals
    | VUuidArray vals -> npgsqlArray NpgsqlDbType.Uuid vals
    | VNull -> (None, upcast DBNull.Value)

type QueryConnection (loggerFactory : ILoggerFactory, connection : NpgsqlConnection) =
    let logger = loggerFactory.CreateLogger<QueryConnection> ()

    let withCommand (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) (runFunc : NpgsqlCommand -> Task<'a>) : Task<'a> =
        task {
            use command = new NpgsqlCommand(queryStr, connection)
            for KeyValue (name, value) in pars do
                match npgsqlValue value with
                | (None, obj) ->
                    ignore <| command.Parameters.AddWithValue(name.ToString(), obj)
                | (Some typ, obj) ->
                    ignore <| command.Parameters.AddWithValue(name.ToString(), typ, obj)
            logger.LogInformation("Executing query with args {args}: {query}", pars, queryStr)
            try
                return! runFunc command
            with
            // 40001: could not serialize access due to concurrent update
            | :? PostgresException as e when e.SqlState = "40001" ->
                return raisefWithInner ConcurrentUpdateException e "Concurrent update detected"
            | :? PostgresException as e ->
                return raisefWithInner QueryException e "Error while executing"
        }

    member this.Connection = connection

    member this.ExecuteNonQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<int> =
        withCommand queryStr pars cancellationToken <| fun command -> command.ExecuteNonQueryAsync(cancellationToken)

    member this.ExecuteValueQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<SQLName * SimpleValueType * Value> =
        task {
            match! this.ExecuteRowValuesQuery queryStr pars cancellationToken with
            | [|ret|] -> return ret
            | _ -> return raisef QueryException "Not a single column"
        }

    member this.ExecuteRowValuesQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<(SQLName * SimpleValueType * Value)[]> =
        withCommand queryStr pars cancellationToken <| fun command -> task {
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            let! hasRow0 = reader.ReadAsync(cancellationToken)
            if not hasRow0 then
                raisef QueryException "No first row"
            let getRow i =
                let name = reader.GetName(i)
                let typ = parseType (reader.GetDataTypeName(i))
                let value = reader.GetProviderSpecificValue(i) |> convertValue typ
                (SQLName name, typ, value)
            let result = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
            let! hasRow1 = reader.ReadAsync(cancellationToken)
            if hasRow1 then
                let secondResult = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toList
                raisef QueryException "Has a second row: %O" secondResult
            return result
        }

    member this.ExecuteColumnValuesQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) (processFunc : SQLName -> SimpleValueType -> IAsyncEnumerable<Value> -> Task<'a>) : Task<'a> =
        let processFunc' cols (rows : IAsyncEnumerable<Value[]>) =
            match cols with
                | [|(name, typ)|] -> processFunc name typ (rows.Select(fun row -> row.[0]))
                | _ -> raisef QueryException "Not a single column"
        this.ExecuteQuery queryStr pars cancellationToken processFunc'

    member this.ExecuteQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) (processFunc : (SQLName * SimpleValueType)[] -> IAsyncEnumerable<Value[]> -> Task<'a>) : Task<'a> =
        withCommand queryStr pars cancellationToken <| fun command -> task {
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            let getColumn i =
                let name = reader.GetName(i)
                let typ = parseType (reader.GetDataTypeName(i))
                (SQLName name, typ)
            let columns = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getColumn |> Seq.toArray

            let getRow i =
                let (_, typ) = columns.[i]
                reader.GetProviderSpecificValue(i) |> convertValue typ

            let mutable currentValue = None
            let mutable tookOnce = false
            let enumerable =
                { new IAsyncEnumerable<Value[]> with
                      member this.GetAsyncEnumerator (cancellationToken : CancellationToken) =
                          if tookOnce then
                              failwith "You can only enumerate row enumerable once"
                          else
                              tookOnce <- true
                              { new IAsyncEnumerator<Value[]> with
                                  member this.Current = Option.get currentValue
                                  member this.DisposeAsync () = reader.DisposeAsync ()
                                  member this.MoveNextAsync () =
                                      vtask {
                                          match! reader.ReadAsync(cancellationToken) with
                                          | false -> return false
                                          | true ->
                                              let row = seq { 0 .. reader.FieldCount - 1 } |> Seq.map getRow |> Seq.toArray
                                              currentValue <- Some row
                                              return true
                                      }
                              }
                }

            let! ret = processFunc columns enumerable
            return ret
        }

type ExplainOptions =
    { Analyze : bool option
      Costs : bool option
      Verbose : bool option
    }

let defaultExplainOptions =
    { Analyze = None
      Costs = None
      Verbose = None
    }

let runExplainQuery<'a when 'a :> ISQLString> (connection : QueryConnection) (query : 'a) (parameters : ExprParameters) (explainOpts : ExplainOptions) (cancellationToken : CancellationToken) : Task<JToken> =
    task {
        let explainQuery =
            { Statement = query
              Analyze = explainOpts.Analyze
              Costs = explainOpts.Costs
              Verbose = explainOpts.Verbose
              Format = Some EFJson
            } : ExplainExpr<'a>
        let queryStr = explainQuery.ToSQLString()
        match! connection.ExecuteValueQuery queryStr parameters cancellationToken with
        | (_, _, VJson j) -> return j
        | ret -> return failwithf "Unexpected EXPLAIN return value: %O" ret
    }