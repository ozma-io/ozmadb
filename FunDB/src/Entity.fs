module FunWithFlags.FunDB.Entity

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

exception EntityExecutionError of info : string with
    override this.Message = this.info

type EntityId = int
type EntityArguments = Map<FieldName, FieldValue>

let insertEntity (connection : QueryConnection) (entityRef : EntityRef) (entity : ResolvedEntity) (args : EntityArguments) : unit =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind fieldName args with
            | None when Option.isSome field.defaultValue -> None
            | None -> raise (EntityExecutionError <| sprintf "Required field not provided: %O" fieldName)
            | Some arg -> Some (compileName fieldName, (field.valueType, compileArgument arg))
    let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
    let columns = parameters |> Seq.map fst |> Array.ofSeq
    let placeholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i, valueWithType)) |> Map.ofSeq
    let values = parameters |> Seq.mapi (fun i (name, arg) -> SQL.VEPlaceholder i) |> Array.ofSeq
    let query =
        { name = compileEntityRef entityRef
          columns = columns
          values = [| values |]
        } : SQL.InsertExpr
    connection.ExecuteNonQuery (query.ToSQLString()) placeholders

let updateEntity (connection : QueryConnection) (entityRef : EntityRef) (entity : ResolvedEntity) (id : EntityId) (args : EntityArguments) : unit =
    let getValue (fieldName : FieldName, field : ResolvedColumnField) =
        match Map.tryFind fieldName args with
            | None -> None
            | Some arg -> Some (compileName fieldName, (field.valueType, compileArgument arg))
    let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
    // We reserve placeholder 0 for id
    let columns = parameters |> Seq.mapi (fun i (name, arg) -> (name, SQL.VEPlaceholder (i + 1))) |> Map.ofSeq
    let columnPlaceholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i + 1, valueWithType)) |> Map.ofSeq
    let where = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)
    let placeholders = Map.add 0 (SQL.VTScalar SQL.STInt, SQL.VInt id) columnPlaceholders
    let query =
        { name = compileEntityRef entityRef
          columns = columns
          where = Some where
        } : SQL.UpdateExpr
    connection.ExecuteNonQuery (query.ToSQLString()) placeholders

let deleteEntity (connection : QueryConnection) (entityRef : EntityRef) (entity : ResolvedEntity) (id : EntityId) : unit =
    let where = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)
    let placeholders = Map.singleton 0 (SQL.VTScalar SQL.STInt, SQL.VInt id)
    let query =
        { name = compileEntityRef entityRef
          where = Some where
        } : SQL.DeleteExpr
    connection.ExecuteNonQuery (query.ToSQLString()) placeholders