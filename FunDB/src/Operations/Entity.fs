module FunWithFlags.FunDB.Operations.Entity

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
module SQL = FunWithFlags.FunDB.SQL.DDL

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
            let args = Map.unionUnique (Map.mapKeys PGlobal globalArgs) (Map.mapKeys PLocal placeholders)
            return! runFunc (query.expression.ToSQLString()) (prepareArguments query.arguments args)
        with
            | :? QueryException as ex ->
                return raisefWithInner EntityExecutionException ex.InnerException "%s" ex.Message
    }

let private runNonQuery (connection : QueryConnection) = runQuery connection.ExecuteNonQuery

let private runIntQuery (connection : QueryConnection) = runQuery connection.ExecuteIntQuery

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

let insertEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (rawArgs : EntityArguments) : Task<int> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None when Option.isSome field.defaultValue -> None
            | None when field.isNullable -> None
            | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
            | Some arg -> Some (fieldName, field.columnName, { argType = clearFieldType field.fieldType; optional = field.isNullable })

        let entity = layout.FindEntity entityRef |> Option.get

        if entity.isAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef
        if entity.hidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef
        let (subEntityColumn, subEntityArg, newRawArgs) =
            if hasSubType entity then
                let col = Seq.singleton (null, sqlFunSubEntity)
                let arg = Seq.singleton (PLocal funSubEntity, { argType = FTType (FETScalar SFTString); optional = false })
                let newArgs = Map.add funSubEntity (FString entity.typeName) rawArgs
                (col, arg, newArgs)
            else
                (Seq.empty, Seq.empty, rawArgs)

        // FIXME: Lots of shuffling types around; make arguments API better?
        let argumentTypes = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun (name, colName, arg) -> (PLocal name, arg)) |> Seq.append subEntityArg |> Map.ofSeq |> compileArguments
        // Id is needed so that we always have at least one value inserted.
        let insertColumns = argumentTypes |> Seq.map (fun (name, colName, arg) -> (({ name = name } : RestrictedColumnInfo) :> obj, colName))
        let columns = Seq.concat [Seq.singleton (null, sqlFunId); subEntityColumn; insertColumns] |> Array.ofSeq
        let values = arguments.types |> Map.toSeq |> Seq.map (fun (name, arg) -> SQL.IVValue <| SQL.VEPlaceholder arg.placeholderId)
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
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleInsert layout role query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        return! runIntQuery connection globalArgs restricted newRawArgs
    }

let private funIdArg = { argType = FTType (FETScalar SFTInt); optional = false }

let updateEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (rawArgs : EntityArguments) : Task<unit> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None -> None
            | Some arg when field.isImmutable -> raisef EntityDeniedException "Field %O is immutable" { entity = entityRef; name = fieldName }
            | Some arg -> Some (fieldName, field.columnName, { argType = clearFieldType field.fieldType; optional = field.isNullable })

        let entity = layout.FindEntity entityRef |> Option.get
        if entity.hidden then
            raisef EntityExecutionException "Entity %O is hidden" entityRef

        let argumentTypes = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments' = argumentTypes |> Seq.map (fun (name, colName, arg) -> (PLocal name, arg)) |> Map.ofSeq |> compileArguments
        let arguments = addArgument (PLocal funId) funIdArg arguments'
        let columns = argumentTypes |> Seq.map (fun (name, colName, arg) -> (colName, (({ name = name } : RestrictedColumnInfo) :> obj, SQL.VEPlaceholder arguments.types.[PLocal name].placeholderId))) |> Map.ofSeq

        let tableRef = compileResolvedEntityRef entity.root
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder arguments.types.[PLocal funId].placeholderId)
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

        let arguments = addArgument (PLocal funId) funIdArg emptyArguments
        let whereExpr = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder arguments.types.[PLocal funId].placeholderId)
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