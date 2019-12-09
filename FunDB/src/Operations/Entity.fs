﻿module FunWithFlags.FunDB.Operations.Entity

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Entity
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

type EntityExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityExecutionException (message, null)

type EntityNotFoundException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityNotFoundException (message, null)

type EntityDeniedException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityDeniedException (message, null)

type EntityId = int

type EntityArguments = Map<FieldName, FieldValue>

let private runQuery (runFunc : string -> ExprParameters -> Task<'a>) (globalArgs : EntityArguments) (query : Query<'q>) (placeholders : EntityArguments) : Task<'a> =
    task {
        try
            // FIXME: Slow
            let args = Map.union (Map.mapKeys PGlobal globalArgs) (Map.mapKeys PLocal placeholders)
            return! runFunc (query.expression.ToSQLString()) (prepareArguments query.arguments args)
        with
            | :? QueryException as ex ->
                return raisefWithInner EntityExecutionException ex.InnerException "%s" ex.Message
    }

let private runNonQuery (connection : QueryConnection) = runQuery connection.ExecuteNonQuery

let private runIntQuery (connection : QueryConnection) globalArgs query placeholders =
    task {
        match! runQuery connection.ExecuteValueQuery globalArgs query placeholders with
        | SQL.VInt i -> return i
        | _ -> return raisef EntityExecutionException "Non-integer result"
    }

let private clearFieldType : ResolvedFieldType -> ArgumentFieldType = function
    | FTReference (r, _) -> FTReference (r, None)
    | FTEnum vals -> FTEnum vals
    | FTType t -> FTType t

let getEntityInfo (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SerializedEntity =
    if entity.hidden then
        raisef EntityExecutionException "Entity %O is hidden" entityRef

    match role with
    | None -> serializeEntity entity
    | Some role ->
        try
            applyRoleInfo layout role entityRef
        with
        | :? PermissionsEntityException as e ->
            raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

let countAndThrow (connection : QueryConnection) (tableRef : SQL.TableRef) (whereExpr : SQL.ValueExpr) =
    task {
        let testExpr =
            SQL.SSelect
                { columns = [| SQL.SCExpr (None, SQL.VEAggFunc (SQL.SQLName "count", SQL.AEStar)) |]
                  from = Some <| SQL.FTable (null, None, tableRef)
                  where = Some whereExpr
                  groupBy = [||]
                  orderLimit = SQL.emptyOrderLimitClause
                  extra = null
                }
        let testQuery =
            { expression = testExpr
              arguments = emptyArguments
            }
        let! count = runIntQuery connection Map.empty testQuery Map.empty
        if count > 0 then
            raisef EntityDeniedException "Access denied"
        else
            raisef EntityNotFoundException "Entry not found"
    }

type private ValueColumn =
    { placeholder : Placeholder
      argument : Argument<ResolvedEntityRef, FunQLVoid>
      column : SQL.ColumnName
      extra : obj
    }

let private fieldIsOptional (field : ResolvedColumnField) = Option.isSome field.defaultValue || field.isNullable

let insertEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (rawArgs : EntityArguments) : Task<int> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            let isOptional = fieldIsOptional field
            match Map.tryFind fieldName rawArgs with
            | None when isOptional -> None
            | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
            | Some arg ->
                Some
                    { placeholder = PLocal fieldName
                      argument = { argType = clearFieldType field.fieldType; optional = isOptional }
                      column = field.columnName
                      extra = ({ name = fieldName } : RestrictedColumnInfo)
                    }

        let entity = layout.FindEntity entityRef |> Option.get

        if entity.isAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef
        if entity.hidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef
        let (subEntityValue, rawArgs) =
            if hasSubType entity then
                let value =
                    { placeholder = PLocal funSubEntity
                      argument = { argType = FTType (FETScalar SFTString); optional = false }
                      column = sqlFunSubEntity
                      extra = null
                    }
                let newArgs = Map.add funSubEntity (FString entity.typeName) rawArgs
                (Seq.singleton value, newArgs)
            else
                (Seq.empty, rawArgs)

        let argumentTypes = Seq.append subEntityValue (entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue) |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun value -> (value.placeholder, value.argument)) |> Map.ofSeq |> compileArguments
        // Id is needed so that we always have at least one column inserted.
        let insertColumns = argumentTypes |> Seq.map (fun value -> (value.extra, value.column))
        let columns = Seq.append (Seq.singleton (null, sqlFunId)) insertColumns |> Array.ofSeq
        let values = argumentTypes |> Seq.map (fun value -> arguments.types.[value.placeholder].placeholderId |> SQL.VEPlaceholder |> SQL.IVValue)
        let valuesWithSys = Seq.append (Seq.singleton SQL.IVDefault) values |> Array.ofSeq

        let expr =
            { name = compileResolvedEntityRef entity.root
              columns = columns
              values = SQL.IValues [| valuesWithSys |]
              returning = [| SQL.SCExpr (None, SQL.VEColumn { table = None; name = sqlFunId }) |]
              extra = ({ ref = entityRef } : RestrictedTableInfo)
            } : SQL.InsertExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let query =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleInsert layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        return! runIntQuery connection globalArgs query rawArgs
    }

let private funIdArg = { argType = FTType (FETScalar SFTInt); optional = false }

let updateEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (rawArgs : EntityArguments) : Task<unit> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None -> None
            | Some arg when field.isImmutable -> raisef EntityDeniedException "Field %O is immutable" { entity = entityRef; name = fieldName }
            | Some arg ->
                Some
                    { placeholder = PLocal fieldName
                      argument = { argType = clearFieldType field.fieldType; optional = fieldIsOptional field }
                      column = field.columnName
                      extra = ({ name = fieldName } : RestrictedColumnInfo)
                    }

        let entity = layout.FindEntity entityRef |> Option.get
        if entity.hidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef

        let argumentTypes = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun value -> (value.placeholder, value.argument)) |> Map.ofSeq |> compileArguments
        let columns = argumentTypes |> Seq.map (fun value -> (value.column, (value.extra, SQL.VEPlaceholder arguments.types.[value.placeholder].placeholderId))) |> Map.ofSeq

        let (idPlaceholder, arguments) = addArgument (PLocal funId) funIdArg arguments

        let tableRef = compileResolvedEntityRef entity.root
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder idPlaceholder)
        let whereExpr =
            match entity.inheritance with
            | Some inheritance -> SQL.VEAnd (inheritance.checkExpr, whereId)
            | None -> whereId

        let expr =
            { name = tableRef
              columns = columns
              where = Some whereExpr
              extra = ({ ref = entityRef } : RestrictedTableInfo)
            } : SQL.UpdateExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleUpdate layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        let! affected = runNonQuery connection globalArgs restricted (Map.add funId (FInt id) rawArgs)
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr
    }

let deleteEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) : Task<unit> =
    task {
        let entity = layout.FindEntity entityRef |> Option.get

        if entity.hidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef
        if entity.isAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef

        let (idPlaceholder, arguments) = addArgument (PLocal funId) funIdArg emptyArguments
        let whereExpr = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder idPlaceholder)
        let whereExpr =
            if hasSubType entity then
                let subEntityCheck = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunSubEntity }, SQL.VEValue (SQL.VString entity.typeName))
                SQL.VEAnd (subEntityCheck, whereExpr)
            else
                whereExpr
        let tableRef = compileResolvedEntityRef entity.root

        let expr =
            { name = tableRef
              where = Some whereExpr
              extra = ({ ref = entityRef } : RestrictedTableInfo)
            } : SQL.DeleteExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleDelete layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        let! affected = runNonQuery connection globalArgs restricted (Map.singleton funId (FInt id))
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr
    }