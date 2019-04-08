module FunWithFlags.FunDB.Operations.Entity

open System.Threading.Tasks

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type EntityExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityExecutionException (message, null)

type EntityId = int
type EntityArguments = Map<FieldName, FieldValue>

let private runQuery (connection : QueryConnection) (placeholders : ExprParameters) (query : ISQLString) : Task<unit> =
    try
        connection.ExecuteNonQuery (query.ToSQLString()) placeholders
    with
        | :? QueryException as ex -> raisefWithInner EntityExecutionException ex.InnerException "%s" ex.Message

let insertEntity (connection : QueryConnection) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : EntityArguments) : Task<unit> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind fieldName args with
        | None when Option.isSome field.defaultValue -> None
        | None when field.isNullable -> None
        | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
        | Some arg -> Some (compileName fieldName, (field.valueType, compileFieldValueSingle arg))
    let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
    let columns = Seq.append (Seq.singleton sqlFunId) (parameters |> Seq.map fst) |> Array.ofSeq
    let placeholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i, valueWithType)) |> Map.ofSeq
    let values = parameters |> Seq.mapi (fun i (name, arg) -> SQL.IVValue <| SQL.VEPlaceholder i)
    let valuesWithId = Seq.append (Seq.singleton SQL.IVDefault) values |> Array.ofSeq
    let query =
        { name = compileResolvedEntityRef entityRef
          columns = columns
          values = SQL.IValues [| valuesWithId |]
        } : SQL.InsertExpr
    runQuery connection placeholders query

let updateEntity (connection : QueryConnection) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : EntityId) (args : EntityArguments) : Task<unit> =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind fieldName args with
        | None -> None
        | Some arg -> Some (compileName fieldName, (field.valueType, compileFieldValueSingle arg))
    let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
    // We reserve placeholder 0 for id
    let columns = parameters |> Seq.mapi (fun i (name, arg) -> (name, SQL.VEPlaceholder (i + 1))) |> Map.ofSeq
    let columnPlaceholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i + 1, valueWithType)) |> Map.ofSeq
    let where = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)
    let placeholders = Map.add 0 (SQL.VTScalar SQL.STInt, SQL.VInt id) columnPlaceholders
    let query =
        { name = compileResolvedEntityRef entityRef
          columns = columns
          where = Some where
        } : SQL.UpdateExpr
    runQuery connection placeholders query

let deleteEntity (connection : QueryConnection) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : EntityId) : Task<unit> =
    let where = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)
    let placeholders = Map.singleton 0 (SQL.VTScalar SQL.STInt, SQL.VInt id)
    let query =
        { name = compileResolvedEntityRef entityRef
          where = Some where
        } : SQL.DeleteExpr
    runQuery connection placeholders query
