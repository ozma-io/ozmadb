module OzmaDB.SQL.Query

open System
open System.Linq
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Microsoft.Extensions.Logging
open NodaTime
open Npgsql
open NpgsqlTypes
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils.Serialization.Json
open OzmaDB.OzmaUtils
open OzmaDB.Parsing
open OzmaDB.SQL.Utils
open OzmaDB.SQL.AST
open OzmaDB.SQL.Misc
open OzmaDB.SQL.Lex
open OzmaDB.SQL.Parse

type QueryException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = QueryException (message, null)

type QueryExecutionException (message : string, innerException : Exception) =
    inherit QueryException(message, innerException)

    new (message : string) = QueryExecutionException (message, null)

type ConcurrentUpdateException (message : string, innerException : Exception) =
    inherit QueryException(message, innerException)

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
    | :? float32 as value ->
        try
            Some <| decimal value
        with
        | :? OverflowException -> None
    | :? double as value ->
        try
            Some <| decimal value
        with
        | :? OverflowException -> None
    | value -> None

let private convertValueOrThrow (valType : SimpleValueType) (rawValue : obj) =
    match (valType, rawValue) with
    | (_, (:? DBNull as value)) -> VNull
    | (_, (:? Value as value)) -> value
    | (VTScalar STInt, value) ->
        match convertInt value with
        | Some i -> VInt i
        | None -> raisef QueryException "Failed to convert integer value"
    | (VTScalar STBigInt, (:? int64 as value)) -> VBigInt value
    | (VTScalar STDecimal, value) ->
        match convertDecimal value with
        | Some i -> VDecimal i
        | None -> raisef QueryException "Failed to convert decimal value"
    | (VTScalar STString, (:? string as value)) -> VString value
    | (VTScalar STBool, (:? bool as value)) -> VBool value
    | (VTScalar STDateTime, (:? Instant as value)) -> VDateTime value
    | (VTScalar STLocalDateTime, (:? LocalDateTime as value)) -> VLocalDateTime value
    | (VTScalar STDate, (:? LocalDate as value)) -> VDate value
    | (VTScalar STInterval, (:? Period as value)) -> VInterval value
    | (VTScalar STJson, (:? string as value)) ->
        match tryJson value with
        | Some j -> VJson (ComparableJToken j)
        | None -> raisef QueryException "Invalid JSON value"
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
        | STDateTime -> VDateTimeArray (convertArray tryCast<Instant> rootVals)
        | STLocalDateTime -> VLocalDateTimeArray (convertArray tryCast<LocalDateTime> rootVals)
        | STDate -> VDateArray (convertArray tryCast<LocalDate> rootVals)
        | STInterval -> VIntervalArray (convertArray tryCast<Period> rootVals)
        | STRegclass -> raisef QueryException "Regclass arrays are not supported"
        | STJson -> VJsonArray (convertArray (tryCast<string> >> Option.bind tryJson >> Option.map ComparableJToken) rootVals)
        | STUuid -> VUuidArray (convertArray tryCast<Guid> rootVals)
    | (typ, value) -> raisef QueryException "Unknown value format"

let private convertValue (valType : SimpleValueType) (rawValue : obj) =
    try
        convertValueOrThrow valType rawValue
    with
    | e -> raisefWithInner QueryException e "Value type %O, raw value type %s, raw value %O" valType (rawValue.GetType().FullName) rawValue

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
    | VDateTime dt -> (Some NpgsqlDbType.TimestampTz, upcast dt)
    | VLocalDateTime dt -> (Some NpgsqlDbType.Timestamp, upcast dt)
    | VDate dt -> (Some NpgsqlDbType.Date, upcast dt)
    | VInterval int -> (Some NpgsqlDbType.Interval, upcast int)
    | VJson j -> (Some NpgsqlDbType.Jsonb, upcast j)
    | VUuid u -> (Some NpgsqlDbType.Uuid, upcast u)
    | VIntArray vals -> npgsqlArray NpgsqlDbType.Integer vals
    | VBigIntArray vals -> npgsqlArray NpgsqlDbType.Bigint vals
    | VDecimalArray vals -> npgsqlArray NpgsqlDbType.Numeric vals
    | VStringArray vals -> npgsqlArray NpgsqlDbType.Text vals
    | VBoolArray vals -> npgsqlArray NpgsqlDbType.Boolean vals
    | VDateTimeArray vals -> npgsqlArray NpgsqlDbType.TimestampTz vals
    | VLocalDateTimeArray vals -> npgsqlArray NpgsqlDbType.Timestamp vals
    | VDateArray vals -> npgsqlArray NpgsqlDbType.Date vals
    | VIntervalArray vals -> npgsqlArray NpgsqlDbType.Interval vals
    | VRegclassArray vals -> raisef QueryException "Regclass arguments are not supported: %O" vals
    | VJsonArray vals -> npgsqlArray NpgsqlDbType.Jsonb vals
    | VUuidArray vals -> npgsqlArray NpgsqlDbType.Uuid vals
    | VNull -> (None, upcast DBNull.Value)
    | VInvalid -> raisef QueryException "Invalid values cannot be serialized"

type QueryConnection (loggerFactory : ILoggerFactory, connection : NpgsqlConnection) =
    let logger = loggerFactory.CreateLogger<QueryConnection>()

    let withCommand (queryStr : string) (pars : ExprParameters) (runFunc : NpgsqlCommand -> Task<'a>) : Task<'a> =
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
                logger.LogInformation(
                    "SQL error, state: {state}, position: {position}, where: {where}, schema name: {schema}, table name: {table}, column name: {column}, data type name: {type}, constraint name: {constraint}, detail: {detail}, hint: {hint}",
                    e.SqlState,
                    e.Position,
                    e.Where,
                    e.SchemaName,
                    e.TableName,
                    e.ColumnName,
                    e.DataTypeName,
                    e.ConstraintName,
                    e.Detail,
                    e.Hint
                )
                return raisefWithInner QueryExecutionException e "Error while executing"
        }

    member this.Connection = connection

    member this.ExecuteNonQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<int> =
        withCommand queryStr pars <| fun command -> command.ExecuteNonQueryAsync(cancellationToken)

    member this.ExecuteValueQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<(SQLName * SimpleValueType * Value) option> =
        task {
            match! this.ExecuteRowValuesQuery queryStr pars cancellationToken with
            | None -> return None
            | Some [|ret|] -> return Some ret
            | _ -> return raisef QueryExecutionException "Not a single column"
        }

    member this.ExecuteRowValuesQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) : Task<(SQLName * SimpleValueType * Value)[] option> =
        withCommand queryStr pars <| fun command -> task {
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            let! hasRow0 = reader.ReadAsync(cancellationToken)
            if not hasRow0 then
                return None
            else
                let getValue i =
                    try
                        let name = SQLName <| reader.GetName(i)
                        let typ = parseType (reader.GetDataTypeName(i))
                        let rawValue = reader.GetProviderSpecificValue(i)
                        let value = convertValue typ rawValue
                        (name, typ, value)
                    with
                    | e -> raisefWithInner QueryException e "While reading column %i (\"%s\")" i  (reader.GetName(i))

                let mutable rowI = 0
                let getRow () =
                    try
                        let res = Array.init reader.FieldCount getValue
                        rowI <- rowI + 1
                        res
                    with
                    | e -> raisefWithInner QueryException e "While reading row %i" rowI

                let result = getRow ()
                let! hasRow1 = reader.ReadAsync(cancellationToken)
                if hasRow1 then
                    let secondResult = getRow ()
                    raisef QueryExecutionException "Has a second row: %O" secondResult
                return Some result
        }

    member this.ExecuteColumnValuesQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) (processFunc : SQLName -> SimpleValueType -> IAsyncEnumerable<Value> -> Task<'a>) : Task<'a> =
        let processFunc' cols (rows : IAsyncEnumerable<Value[]>) =
            match cols with
                | [|(name, typ)|] -> processFunc name typ (rows.Select(fun row -> row.[0]))
                | _ -> raisef QueryExecutionException "Not a single column"
        this.ExecuteQuery queryStr pars cancellationToken processFunc'

    member this.ExecuteQuery (queryStr : string) (pars : ExprParameters) (cancellationToken : CancellationToken) (processFunc : (SQLName * SimpleValueType)[] -> IAsyncEnumerable<Value[]> -> Task<'a>) : Task<'a> =
        withCommand queryStr pars <| fun command -> task {
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            let getColumn i =
                let name = reader.GetName(i)
                let typ = parseType (reader.GetDataTypeName(i))
                (SQLName name, typ)
            let columns = Array.init reader.FieldCount getColumn

            let getValue i =
                let (name, typ) = columns.[i]
                try
                    let rawValue = 
                        try
                            reader.GetProviderSpecificValue(i)
                        with
                        | :? OverflowException -> VInvalid
                        | :? InvalidCastException -> VInvalid
                    convertValue typ rawValue
                with
                | e ->
                    raisefWithInner QueryException e "While reading column %i (\"%s\")" i  (reader.GetName(i))

            let mutable rowI = 0
            let getRow () =
                try
                    let res = Array.init reader.FieldCount getValue
                    rowI <- rowI + 1
                    res
                with
                | e -> raisefWithInner QueryException e "While reading row %i" rowI

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
                                              let row = getRow ()
                                              currentValue <- Some row
                                              return true
                                      }
                              }
                }

            let! ret = processFunc columns enumerable
            return ret
        }

type ExplainOptions =
    { [<DataMember(EmitDefaultValue = false)>]
      Analyze : bool option
      [<DataMember(EmitDefaultValue = false)>]
      Costs : bool option
      [<DataMember(EmitDefaultValue = false)>]
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
        | Some (_, _, VJson j) -> return j.Json
        | ret -> return failwithf "Unexpected EXPLAIN return value: %O" ret
    }
