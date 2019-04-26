module FunWithFlags.FunDB.Operations.Entity

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Flatten
open FunWithFlags.FunDB.Permissions.Entity
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type EntityExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityExecutionException (message, null)

type EntityDeniedException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityDeniedException (message, null)

type EntityId = int

type EntityArguments = Map<FieldName, FieldValue>

let private runQuery (connection : QueryConnection) (placeholders : ExprParameters) (query : ISQLString) : Task<int> =
    try
        connection.ExecuteNonQuery (query.ToSQLString()) placeholders
    with
        | :? QueryException as ex -> raisefWithInner EntityExecutionException ex.InnerException "%s" ex.Message

let insertEntity (connection : QueryConnection) (role : FlatRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (args : EntityArguments) : Task<unit> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName args with
            | None when Option.isSome field.defaultValue -> None
            | None when field.isNullable -> None
            | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
            | Some arg -> Some (compileName fieldName, (field.valueType, compileFieldValueSingle arg))
        
        let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        // Id is needed so that we always have at least one value inserted.
        let columns = Seq.append (Seq.singleton sqlFunId) (parameters |> Seq.map fst) |> Array.ofSeq
        let values = parameters |> Seq.mapi (fun i (name, arg) -> SQL.IVValue <| SQL.VEPlaceholder i)
        let valuesWithId = Seq.append (Seq.singleton SQL.IVDefault) values |> Array.ofSeq

        let placeholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i, valueWithType)) |> Map.ofSeq
        let query =
            { name = compileResolvedEntityRef entityRef
              columns = columns
              values = SQL.IValues [| valuesWithId |]
            } : SQL.InsertExpr
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleInsert role entityRef entity query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message
                
        let! affected = runQuery connection placeholders restricted
        return ()
    }

let updateEntity (connection : QueryConnection) (role : FlatRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : EntityId) (args : EntityArguments) : Task<unit> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName args with
            | None -> None
            | Some arg -> Some (compileName fieldName, (field.valueType, compileFieldValueSingle arg))
        
        let parameters = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        // We reserve placeholder 0 for id
        let columns = parameters |> Seq.mapi (fun i (name, arg) -> (name, SQL.VEPlaceholder (i + 1))) |> Map.ofSeq
        let columnPlaceholders = parameters |> Seq.mapi (fun i (name, valueWithType) -> (i + 1, valueWithType)) |> Map.ofSeq

        let tableRef = compileResolvedEntityRef entityRef
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)

        let placeholders = Map.add 0 (SQL.VTScalar SQL.STInt, SQL.VInt id) columnPlaceholders
        let query =
            { name = tableRef
              columns = columns
              where = Some whereId
            } : SQL.UpdateExpr
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleUpdate role entityRef entity query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message
                
        let! affected = runQuery connection placeholders restricted
        if affected = 0 then
            raisef EntityDeniedException "Access denied for update"
    }

let deleteEntity (connection : QueryConnection) (role : FlatRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (id : EntityId) : Task<unit> =
    task {
        let tableRef = compileResolvedEntityRef entityRef        
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder 0)

        let placeholders = Map.singleton 0 (SQL.VTScalar SQL.STInt, SQL.VInt id)
        let query =
            { name = tableRef
              where = Some whereId
            } : SQL.DeleteExpr
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleDelete role entityRef entity query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message        
        
        let! affected = runQuery connection placeholders restricted
        if affected = 0 then
            raisef EntityDeniedException "Access denied to delete"    
    }