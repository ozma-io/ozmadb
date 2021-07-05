module FunWithFlags.FunDB.Operations.Entity

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
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

let private subEntityColumn = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

let private runQuery (runFunc : string -> ExprParameters -> CancellationToken -> Task<'a>) (globalArgs : LocalArgumentsMap) (query : Query<'q>) (placeholders : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task<'a> =
    task {
        try
            let args = Map.union (Map.mapKeys PGlobal globalArgs) (Map.mapKeys PLocal placeholders)
            return! runFunc (query.Expression.ToSQLString()) (prepareArguments query.Arguments args) cancellationToken
        with
            | :? QueryException as ex ->
                return raisefWithInner EntityExecutionException ex ""
    }

let private runNonQuery (connection : QueryConnection) = runQuery connection.ExecuteNonQuery

let private runIntQuery (connection : QueryConnection) globalArgs query placeholders cancellationToken =
    task {
        match! runQuery connection.ExecuteValueQuery globalArgs query placeholders cancellationToken with
        | SQL.VInt i -> return i
        // FIXME FIXME: should use int64 everywhere instead!
        | SQL.VBigInt i -> return (int i)
        | ret -> return failwithf "Non-integer result: %O" ret
    }

let getEntityInfo (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SerializedEntity =
    match role with
    | None -> serializeEntity layout entity
    | Some role ->
        try
            serializeEntityRestricted layout role entityRef
        with
        | :? PermissionsEntityException as e ->
            raisefWithInner EntityDeniedException e ""

let private countAndThrow (connection : QueryConnection) (tableRef : SQL.TableRef) (whereExpr : SQL.ValueExpr) (cancellationToken : CancellationToken) =
    unitTask {
        let testExpr =
            SQL.SSelect
                { Columns = [| SQL.SCExpr (None, SQL.VEAggFunc (SQL.SQLName "count", SQL.AEStar)) |]
                  From = Some <| SQL.FTable (null, None, tableRef)
                  Where = Some whereExpr
                  GroupBy = [||]
                  OrderLimit = SQL.emptyOrderLimitClause
                  Extra = null
                }
        let testQuery =
            { Expression = testExpr
              Arguments = emptyArguments
            }
        let! count = runIntQuery connection Map.empty testQuery Map.empty cancellationToken
        if count > 0 then
            raisef EntityDeniedException "Access denied"
        else
            raisef EntityNotFoundException "Entry not found"
    }

type private ValueColumn =
    { Placeholder : Placeholder
      Argument : ResolvedArgument
      Column : SQL.ColumnName
      Extra : obj
    }

let private fieldIsOptional (field : ResolvedColumnField) = Option.isSome field.DefaultValue || field.IsNullable

let insertEntity (connection : QueryConnection) (globalArgs : LocalArgumentsMap) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (rawArgs : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task<int> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            let isOptional = fieldIsOptional field
            match Map.tryFind fieldName rawArgs with
            | None when isOptional -> None
            | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
            | Some arg ->
                Some
                    { Placeholder = PLocal fieldName
                      Argument = { ArgType = field.FieldType; Optional = isOptional }
                      Column = field.ColumnName
                      Extra = ({ Name = fieldName } : RestrictedColumnInfo)
                    }

        let entity = layout.FindEntity entityRef |> Option.get

        if entity.IsAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef
        let (subEntityValue, rawArgs) =
            if hasSubType entity then
                let value =
                    { Placeholder = PLocal funSubEntity
                      Argument = { ArgType = FTType (FETScalar SFTString); Optional = false }
                      Column = sqlFunSubEntity
                      Extra = null
                    }
                let newArgs = Map.add funSubEntity (FString entity.TypeName) rawArgs
                (Seq.singleton value, newArgs)
            else
                (Seq.empty, rawArgs)

        let argumentTypes = Seq.append subEntityValue (entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe getValue) |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun value -> (value.Placeholder, value.Argument)) |> Map.ofSeq |> compileArguments
        // Id is needed so that we always have at least one column inserted.
        let insertColumns = argumentTypes |> Seq.map (fun value -> (value.Extra, value.Column))
        let columns = Seq.append (Seq.singleton (null, sqlFunId)) insertColumns |> Array.ofSeq
        let values = argumentTypes |> Seq.map (fun value -> arguments.Types.[value.Placeholder].PlaceholderId |> SQL.VEPlaceholder |> SQL.IVValue)
        let valuesWithSys = Seq.append (Seq.singleton SQL.IVDefault) values |> Array.ofSeq

        let expr =
            { Name = compileResolvedEntityRef entity.Root
              Columns = columns
              Values = SQL.IValues [| valuesWithSys |]
              Returning = [| SQL.SCExpr (None, SQL.VEColumn { Table = None; Name = sqlFunId }) |]
              Extra = ({ Ref = entityRef } : RestrictedTableInfo)
            } : SQL.InsertExpr
        let query =
            { Expression = expr
              Arguments = arguments
            }
        let query =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleInsert layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e ""

        return! runIntQuery connection globalArgs query rawArgs cancellationToken
    }

let private funIdArg = { ArgType = FTType (FETScalar SFTInt); Optional = false }

let updateEntity (connection : QueryConnection) (globalArgs : LocalArgumentsMap) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (rawArgs : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None -> None
            | Some arg when field.IsImmutable -> raisef EntityDeniedException "Field %O is immutable" { Entity = entityRef; Name = fieldName }
            | Some arg ->
                Some
                    { Placeholder = PLocal fieldName
                      Argument = { ArgType = field.FieldType; Optional = fieldIsOptional field }
                      Column = field.ColumnName
                      Extra = ({ Name = fieldName } : RestrictedColumnInfo)
                    }

        let entity = layout.FindEntity entityRef |> Option.get
        if entity.IsHidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef
        let argumentTypes = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun value -> (value.Placeholder, value.Argument)) |> Map.ofSeq |> compileArguments
        let columns = argumentTypes |> Seq.map (fun value -> (value.Column, (value.Extra, SQL.VEPlaceholder arguments.Types.[value.Placeholder].PlaceholderId))) |> Map.ofSeq

        let (idArg, arguments) = addArgument (PLocal funId) funIdArg arguments

        let tableRef = compileResolvedEntityRef entity.Root
        let whereId = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
        let whereExpr =
            if Option.isSome entity.Parent then
                let checkExpr = makeCheckExpr subEntityColumn layout entityRef
                SQL.VEAnd (checkExpr, whereId)
            else
                whereId

        let expr =
            { Name = tableRef
              Columns = columns
              From = None
              Where = Some whereExpr
              Extra = ({ Ref = entityRef } : RestrictedTableInfo)
            } : SQL.UpdateExpr
        let query =
            { Expression = expr
              Arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleUpdate layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e ""

        let! affected = runNonQuery connection globalArgs restricted (Map.add funId (FInt id) rawArgs) cancellationToken
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr cancellationToken
    }

let deleteEntity (connection : QueryConnection) (globalArgs : LocalArgumentsMap) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let entity = layout.FindEntity entityRef |> Option.get

        if entity.IsHidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef
        if entity.IsAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef

        let (idArg, arguments) = addArgument (PLocal funId) funIdArg emptyArguments
        let whereExpr = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
        let whereExpr =
            if hasSubType entity then
                let subEntityCheck = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunSubEntity }, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))
                SQL.VEAnd (subEntityCheck, whereExpr)
            else
                whereExpr
        let tableRef = compileResolvedEntityRef entity.Root

        let expr =
            { Name = tableRef
              Where = Some whereExpr
              Extra = ({ Ref = entityRef } : RestrictedTableInfo)
            } : SQL.DeleteExpr
        let query =
            { Expression = expr
              Arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleDelete layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e ""

        let! affected = runNonQuery connection globalArgs restricted (Map.singleton funId (FInt id)) cancellationToken
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr cancellationToken
    }