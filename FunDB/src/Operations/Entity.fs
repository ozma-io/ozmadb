﻿module FunWithFlags.FunDB.Operations.Entity

open FSharpPlus
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.UsedReferences
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.Permissions.Entity
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type EntityArgumentsException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        EntityArgumentsException (message, innerException, isUserException innerException)

    new (message : string) = EntityArgumentsException (message, null, true)

type EntityExecutionException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        EntityExecutionException (message, innerException, isUserException innerException)

    new (message : string) = EntityExecutionException (message, null, true)

type EntityNotFoundException(message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        EntityNotFoundException (message, innerException, isUserException innerException)

    new (message : string) = EntityNotFoundException (message, null, true)

type EntityDeniedException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        EntityDeniedException (message, innerException, isUserException innerException)

    new (message : string) = EntityDeniedException (message, null, true)

type EntityId = int

let private subEntityColumn = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

let private runQuery (runFunc : string -> ExprParameters -> CancellationToken -> Task<'a>) (query : Query<'q>) (comments : string option) (placeholders : ArgumentValuesMap) (cancellationToken : CancellationToken) : Task<'a> =
    task {
        try
            let prefix = SQL.convertComments comments
            return! runFunc (prefix + query.Expression.ToSQLString()) (prepareArguments query.Arguments placeholders) cancellationToken
        with
            | :? QueryException as ex ->
                return raisefUserWithInner EntityExecutionException ex ""
    }

let private runNonQuery (connection : QueryConnection) = runQuery connection.ExecuteNonQuery

let private parseIntValue = function
    | SQL.VInt i -> i
    // FIXME FIXME: should use int64 everywhere instead!
    | SQL.VBigInt i -> int i
    | ret -> failwithf "Non-integer result: %O" ret

let private runIntQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    task {
        let! ret = runQuery connection.ExecuteValueQuery query comments placeholders cancellationToken
        return parseIntValue ret
    }

let private runIntsQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    let execute query args cancellationToken =
        connection.ExecuteColumnValuesQuery query args cancellationToken (Seq.map parseIntValue >> Seq.toArray >> Task.result)
    runQuery execute query comments placeholders cancellationToken

let getEntityInfo (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SerializedEntity =
    match role with
    | None -> serializeEntity layout entity
    | Some role ->
        try
            serializeEntityRestricted layout role entityRef
        with
        | :? PermissionsEntityException as e ->
            raisefWithInner EntityDeniedException e ""

let private countAndThrow (connection : QueryConnection) (tableRef : SQL.TableRef) (whereExpr : SQL.ValueExpr) (arguments : QueryArguments) (argumentValues : ArgumentValuesMap) (cancellationToken : CancellationToken) =
    unitTask {
        let testExpr =
            { SQL.emptySingleSelectExpr with
                  Columns = [| SQL.SCExpr (None, SQL.VEAggFunc (SQL.SQLName "count", SQL.AEStar)) |]
                  From = Some <| SQL.FTable (SQL.fromTable tableRef)
                  Where = Some whereExpr
            }
        let testQuery =
            { Expression = SQL.SSelect testExpr
              Arguments = arguments
            }
        let! count = runIntQuery connection testQuery None argumentValues cancellationToken
        if count > 0 then
            raisef EntityDeniedException "Access denied"
        else
            raisef EntityNotFoundException "Entry not found"
    }

let private realEntityAnnotation (entityRef : ResolvedEntityRef) : RealEntityAnnotation =
    { RealEntity = entityRef
      FromPath = false
      IsInner = true
      AsRoot = false
    }

let insertEntities
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (role : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (comments : string option)
        (entitiesArgs : LocalArgumentsMap seq)
        (cancellationToken : CancellationToken) : Task<int[]> =
    task {
        let entity = layout.FindEntity entityRef |> Option.get

        // We only check database consistency here; user restriction flags like IsHidden and IsFrozen are checked at API level.
        if entity.IsAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef

        let mutable arguments = emptyArguments
        let mutable argumentValues = Map.mapKeys PGlobal globalArgs

        let (subEntityColumn, subEntityValue) =
            if hasSubType entity then
                let (argInfo, argName, newArguments) = addAnonymousArgument (requiredArgument <| FTScalar SFTString) arguments
                arguments <- newArguments
                argumentValues <- Map.add (PLocal argName) (FString entity.TypeName) argumentValues
                (Seq.singleton (null, sqlFunSubEntity), Seq.singleton (argInfo.PlaceholderId |> SQL.VEPlaceholder |> SQL.IVValue))
            else
                (Seq.empty, Seq.empty)

        let getColumn (fieldName : FieldName, field : ResolvedColumnField) =
            let extra = { Name = fieldName } : RealFieldAnnotation
            (extra :> obj, field.ColumnName)

        let getField (fieldName : FieldName) =
            let field = Map.find fieldName entity.ColumnFields
            (fieldName, field)

        let dataFields = entitiesArgs |> Seq.map Map.keysSet |> Seq.fold Set.union Set.empty |> Set.toSeq |> Seq.map getField |> Seq.toArray
        let dataColumns = dataFields |> Seq.map getColumn
        let idColumn = Seq.singleton (null, sqlFunId)
        let insertColumns = Seq.append idColumn (Seq.append subEntityColumn dataColumns) |> Seq.toArray
        let idValue = Seq.singleton SQL.IVDefault
        let prefixValues = Seq.append idValue subEntityValue |> Seq.toArray

        let getRow (rowArgs : LocalArgumentsMap) =
            let getValue (fieldName : FieldName, field : ResolvedColumnField) =
                match Map.tryFind fieldName rowArgs with
                | None -> SQL.IVDefault
                | Some arg ->
                    let (argInfo, argName, newArguments) = addAnonymousArgument (requiredArgument field.FieldType) arguments
                    arguments <- newArguments
                    argumentValues <- Map.add (PLocal argName) arg argumentValues
                    SQL.IVValue (SQL.VEPlaceholder argInfo.PlaceholderId)
            
            for requiredName in entity.RequiredFields do
                if not <| Map.containsKey requiredName rowArgs then
                    raisef EntityArgumentsException "Required field not provided: %O" requiredName
            
            let rowValues = dataFields |> Seq.map getValue
            Seq.append prefixValues rowValues |> Seq.toArray

        let insertValues = entitiesArgs |> Seq.map getRow |> Seq.toArray

        let opTable = SQL.operationTable <| compileResolvedEntityRef entity.Root
        let expr =
            { SQL.insertExpr opTable insertColumns (SQL.ISValues insertValues) with
                  Returning = [| SQL.SCExpr (None, SQL.VEColumn { Table = None; Name = sqlFunId }) |]
                  Extra = realEntityAnnotation entityRef
            }
        match role with
        | None -> ()
        | Some role ->
            // We circumvent full `flattenUsedDatabase` call for performance.
            let getUsedField (fieldName, field : ResolvedColumnField) =
                let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
                let fieldRef = { Entity = fieldEntity; Name = fieldName }
                (fieldRef, usedFieldInsert)

            let usedFields = dataFields |> Seq.map getUsedField
            let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntityInsert usedFields
            try
                checkPermissions layout role usedDatabase
            with
            | :? PermissionsApplyException as e ->
                raisefWithInner EntityDeniedException e ""
    
        let query =
            { Expression = expr
              Arguments = arguments
            }

        return! runIntsQuery connection query comments argumentValues cancellationToken
    }

let private funIdArg = requiredArgument <| FTScalar SFTInt

let updateEntity (connection : QueryConnection) (globalArgs : LocalArgumentsMap) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (comments : string option) (updateArgs : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let entity = layout.FindEntity entityRef |> Option.get

        let (idArg, initialArguments) = addArgument (PLocal funId) funIdArg emptyArguments
        let mutable arguments = initialArguments
        let initialArgumentValues = Map.singleton (PLocal funId) (FInt id)
        let mutable argumentValues = Map.union (Map.mapKeys PGlobal globalArgs) initialArgumentValues

        let getValue (fieldName : FieldName) (fieldValue : FieldValue) =
            let field = Map.find fieldName entity.ColumnFields
            if field.IsImmutable then
                raisef EntityDeniedException "Field %O is immutable" fieldName
            let argument = { requiredArgument field.FieldType with Optional = fieldIsOptional field }
            let (argInfo, newArguments) = addArgument (PLocal fieldName) argument arguments
            arguments <- newArguments
            argumentValues <- Map.add (PLocal fieldName) fieldValue argumentValues
            let extra = { Name = fieldName } : RealFieldAnnotation
            (field.ColumnName, (extra :> obj, SQL.VEPlaceholder argInfo.PlaceholderId))

        let columns = Map.mapWithKeys getValue updateArgs

        let tableRef = compileResolvedEntityRef entity.Root
        let whereId = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
        let whereExpr =
            if Option.isSome entity.Parent then
                let checkExpr = makeCheckExpr subEntityColumn layout entityRef
                SQL.VEAnd (checkExpr, whereId)
            else
                whereId

        let opTable = SQL.operationTable tableRef
        let expr =
            { SQL.updateExpr opTable with
                  Columns = columns
                  Where = Some whereExpr
                  Extra = realEntityAnnotation entityRef
            }
        let query =
            { Expression = expr
              Arguments = arguments
            }
        let query =
            match role with
            | None -> query
            | Some role ->
                let getUsedField fieldName =
                    let field = Map.find fieldName entity.ColumnFields
                    let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
                    let fieldRef = { Entity = fieldEntity; Name = fieldName }
                    (fieldRef, usedFieldInsert)
            
                let usedFields = updateArgs |> Map.keys |> Seq.map getUsedField
                let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntityUpdate usedFields
                let appliedDb =
                    try
                        applyPermissions layout role usedDatabase
                    with
                    | :? PermissionsApplyException as e ->
                        raisefWithInner EntityDeniedException e ""
                applyRoleUpdateExpr layout appliedDb query

        let! affected = runNonQuery connection query comments argumentValues cancellationToken
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr initialArguments initialArgumentValues cancellationToken
    }

let deleteEntity (connection : QueryConnection) (globalArgs : LocalArgumentsMap) (layout : Layout) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (comments : string option) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let entity = layout.FindEntity entityRef |> Option.get

        if entity.IsAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef

        let (idArg, initialArguments) = addArgument (PLocal funId) funIdArg emptyArguments
        let initialArgumentValues = Map.singleton (PLocal funId) (FInt id)
        let argumentValues = Map.union (Map.mapKeys PGlobal globalArgs) initialArgumentValues

        let whereExpr = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
        let whereExpr =
            if hasSubType entity then
                let subEntityCheck = SQL.VEBinaryOp (SQL.VEColumn { Table = None; Name = sqlFunSubEntity }, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))
                SQL.VEAnd (subEntityCheck, whereExpr)
            else
                whereExpr
        let tableRef = compileResolvedEntityRef entity.Root

        let opTable = SQL.operationTable tableRef
        let expr =
            { SQL.deleteExpr opTable with
                  Where = Some whereExpr
                  Extra = realEntityAnnotation entityRef
            }
        let query =
            { Expression = expr
              Arguments = initialArguments
            }
        let query =
            match role with
            | None -> query
            | Some role ->
                let getUsedField (fieldName, field : ResolvedColumnField) =
                    let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
                    let fieldRef = { Entity = fieldEntity; Name = fieldName }
                    (fieldRef, usedFieldSelect)

                let usedFields =
                    entity.ColumnFields
                    |> Map.toSeq
                    |> Seq.map getUsedField
                let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntityDelete usedFields
                let appliedDb =
                    try
                        applyPermissions layout role usedDatabase
                    with
                    | :? PermissionsApplyException as e ->
                        raisefWithInner EntityDeniedException e ""
                applyRoleDeleteExpr layout appliedDb query

        let! affected = runNonQuery connection query comments argumentValues cancellationToken
        if affected = 0 then
            do! countAndThrow connection tableRef whereExpr initialArguments initialArgumentValues cancellationToken
    }
