module FunWithFlags.FunDB.Operations.Entity

open FSharpPlus
open System.Linq
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
open FunWithFlags.FunDB.Triggers.Merge
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

type RowId = int

type RowKey =
    | RKId of RowId
    | RKAlt of Name : ConstraintName * Keys : LocalArgumentsMap

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

let private runIntQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    task {
        match! runQuery connection.ExecuteValueQuery query comments placeholders cancellationToken with
        | None -> return failwith "Unexpected empty result"
        | Some (name, typ, ret) -> return SQL.parseIntValue ret
    }

let private runOptionalIntQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    task {
        match! runQuery connection.ExecuteValueQuery query comments placeholders cancellationToken with
        | None -> return None
        | Some (name, typ, ret) -> return Some (SQL.parseIntValue ret)
    }

let private runIntsQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    let execute query args cancellationToken =
        connection.ExecuteColumnValuesQuery query args cancellationToken <| fun name typ rows -> task { return! rows.Select(SQL.parseIntValue).ToArrayAsync(cancellationToken) }
    runQuery execute query comments placeholders cancellationToken

let private runOptionalStringQuery (connection : QueryConnection) query comments placeholders cancellationToken =
    task {
        match! runQuery connection.ExecuteValueQuery query comments placeholders cancellationToken with
        | None -> return None
        | Some (name, typ, ret) -> return Some (SQL.parseStringValue ret)
    }

let getEntityInfo (layout : Layout) (triggers : MergedTriggers) (role : ResolvedRole option) (entityRef : ResolvedEntityRef) : SerializedEntity =
    match role with
    | None ->
        let entity = layout.FindEntity entityRef |> Option.get
        let entityTriggers = Option.defaultValue emptyMergedTriggersEntity (triggers.FindEntity entityRef)
        serializeEntity layout entityTriggers entity
    | Some role ->
        try
            serializeEntityRestricted layout triggers role entityRef
        with
        | :? PermissionsEntityException as e ->
            raisefWithInner EntityDeniedException e ""

let private countAndThrow
        (connection : QueryConnection)
        (applyRole : ResolvedRole option)
        (tableRef : SQL.TableRef)
        (whereExpr : SQL.ValueExpr)
        (arguments : QueryArguments)
        (argumentValues : ArgumentValuesMap)
        (cancellationToken : CancellationToken) =
    unitTask {
        if Option.isNone applyRole then
            raisef EntityNotFoundException "Entry not found"
        else
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

let private runQueryAndGetId
        (connection : QueryConnection)
        (applyRole : ResolvedRole option)
        (tableRef : SQL.TableRef)
        (query : Query<'a>)
        (comments : string option)
        (argumentValues : ArgumentValuesMap)
        (whereExprWithoutRole : SQL.ValueExpr)
        (argumentsWithoutRole : QueryArguments)
        (key : RowKey)
        (cancellationToken : CancellationToken) : Task<RowId> =
    task {
        let! rowId =
            task {
                match key with
                | RKId id ->
                    match! runNonQuery connection query comments argumentValues cancellationToken with
                    | 0 -> return None
                    | 1 -> return Some id
                    | _ -> return failwith "Impossible"
                | RKAlt (name, keys) -> return! runOptionalIntQuery connection query comments argumentValues cancellationToken
            }
        match rowId with
        | None ->
            // Notice we don't pass `Where` with applied role -- our aim here is to check if the row exists at all.
            do! countAndThrow connection applyRole tableRef whereExprWithoutRole argumentsWithoutRole argumentValues cancellationToken
            return failwith "Impossible"
        | Some id -> return id
    }

let private realEntityAnnotation (entityRef : ResolvedEntityRef) : RealEntityAnnotation =
    { RealEntity = entityRef
      FromPath = false
      IsInner = true
      AsRoot = false
    }

type private AltKeyResult =
    { Arguments : QueryArguments
      ArgumentValues : ArgumentValuesMap
      Where : SQL.ValueExpr
      UsedFields : (ResolvedFieldRef * UsedField) seq
    }

let private altKeyCheck
        (layout : Layout)
        (entityRef : ResolvedEntityRef)
        (keyName : ConstraintName)
        (keyArgs : LocalArgumentsMap)
        (aliasRef : SQL.TableRef) : AltKeyResult =
    let entity = layout.FindEntity entityRef |> Option.get
    let constr =
        match Map.tryFind keyName entity.UniqueConstraints with
        | None -> raisef EntityArgumentsException "Unique constraint %O does not exist" keyName
        | Some constr when not constr.IsAlternateKey -> raisef EntityArgumentsException "Unique constraint %O is not an alternate key" keyName
        | Some constr -> constr

    let mutable arguments = emptyArguments
    let mutable argumentValues = Map.empty
    let mutable usedFields = []

    let getCheck (fieldName : FieldName) =
        match Map.tryFind fieldName keyArgs with
        | None -> raisef EntityArgumentsException "Key column %O is not specified" fieldName
        | Some arg ->
            let field = Map.find fieldName entity.ColumnFields
            let argName = PLocal fieldName
            let (argInfo, newArguments) = addArgument argName (requiredArgument field.FieldType) arguments
            arguments <- newArguments
            argumentValues <- Map.add argName arg argumentValues
            let usedField = { Entity = Option.defaultValue entityRef field.InheritedFrom; Name = fieldName }
            usedFields <- (usedField, usedFieldSelect) :: usedFields
            SQL.VEBinaryOp (SQL.VEColumn { Table = Some aliasRef; Name = field.ColumnName }, SQL.BOEq, SQL.VEPlaceholder argInfo.PlaceholderId)

    let check = constr.Columns |> Seq.map getCheck |> Seq.fold1 (curry SQL.VEAnd)
    { Arguments = arguments
      ArgumentValues = argumentValues
      Where = check
      UsedFields = usedFields
    }

let private funIdArg = requiredArgument <| FTScalar SFTInt

let private rowKeyCheck
        (layout : Layout)
        (entityRef : ResolvedEntityRef)
        (key : RowKey)
        (aliasRef : SQL.TableRef) : AltKeyResult =
    match key with
    | RKId id ->
        let entity = layout.FindEntity entityRef |> Option.get
        let (idArg, arguments) = addArgument (PLocal funId) funIdArg emptyArguments
        let argumentValues = Map.singleton (PLocal funId) (FInt id)

        let tableRef = compileResolvedEntityRef entity.Root
        let aliasRef = { Schema = None; Name = tableRef.Name } : SQL.TableRef
        let whereId = SQL.VEBinaryOp (SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
        { Arguments = arguments
          ArgumentValues = argumentValues
          Where = whereId
          UsedFields = Seq.empty
        }
    | RKAlt (name, keys) -> altKeyCheck layout entityRef name keys aliasRef

type private SelectOperationBits =
    { Arguments : QueryArguments
      Where : SQL.ValueExpr
      Columns : SQL.ValueExpr seq
      UsedFields : (ResolvedFieldRef * UsedField) seq
    }

type private SelectOperationQuery =
    { Query : Query<SQL.SelectExpr>
      ArgumentsWithoutRole : QueryArguments
      Table : SQL.TableRef
      WhereWithoutRole : SQL.ValueExpr
    }

let private buildSelectOperation
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (getBits : SQL.TableRef -> SelectOperationBits) =
    let entity = layout.FindEntity entityRef |> Option.get

    let tableRef = compileResolvedEntityRef entity.Root
    let aliasRef = { Schema = None; Name = tableRef.Name } : SQL.TableRef
    let bits = getBits aliasRef

    let checkExpr = makeCheckExpr subEntityColumn layout entityRef
    let whereExpr = Option.addWith (curry SQL.VEAnd) bits.Where checkExpr
    let fromExpr = SQL.FTable { Table = compileResolvedEntityRef entity.Root; Alias = None; Extra = ObjectMap.empty }
    let entityInfo =
        { Ref = entityRef
          IsInner = true
          AsRoot = false
          Check = checkExpr
        }
    let selectInfo =
        { Entities = Map.singleton aliasRef.Name entityInfo
          Joins = emptyJoinPaths
          WhereWithoutSubentities = Some bits.Where
        } : SelectFromInfo
    let results = bits.Columns |> Seq.map (fun expr -> SQL.SCExpr (None, expr)) |> Seq.toArray
    let single =
        { SQL.emptySingleSelectExpr with
            Columns = results
            From = Some fromExpr
            Where = Some whereExpr
            Extra = ObjectMap.singleton selectInfo
        }
    let compiled = SQL.selectExpr (SQL.SSelect single)

    let query =
        { Expression = compiled
          Arguments = bits.Arguments
        }

    let query =
        match applyRole with
        | None -> query
        | Some role ->
            let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntitySelect bits.UsedFields
            let appliedDb =
                try
                    applyPermissions layout role usedDatabase
                with
                | :? PermissionsApplyException as e ->
                    raisefWithInner EntityDeniedException e ""
            applyRoleSelectExpr layout appliedDb query

    { Query = query
      Table = tableRef
      WhereWithoutRole = whereExpr
      ArgumentsWithoutRole = bits.Arguments
    }

let resolveAltKey
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (comments : string option)
        (keyName : ConstraintName)
        (keyArgs : LocalArgumentsMap)
        (cancellationToken : CancellationToken) : Task<RowId> =
    task {
        let mutable argumentValues = Map.mapKeys PGlobal globalArgs

        let entity = layout.FindEntity entityRef |> Option.get

        let opQuery = buildSelectOperation layout applyRole entityRef <| fun aliasRef ->
            let alt = altKeyCheck layout entityRef keyName keyArgs aliasRef
            argumentValues <- Map.unionUnique argumentValues alt.ArgumentValues
            let entityId = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }
            { Where = alt.Where
              Columns = seq { entityId }
              UsedFields = Seq.append alt.UsedFields (Seq.singleton ({ Entity = entityRef; Name = funId }, usedFieldSelect))
              Arguments = alt.Arguments
            }

        match! runOptionalIntQuery connection opQuery.Query comments argumentValues cancellationToken with
        | None ->
            do! countAndThrow connection applyRole opQuery.Table opQuery.WhereWithoutRole opQuery.ArgumentsWithoutRole argumentValues cancellationToken
            return failwith "Impossible"
        | Some rawId -> return rawId
    }

let resolveKey
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (comments : string option)
        (key : RowKey)
        (cancellationToken : CancellationToken) : ValueTask<RowId> =
    vtask {
        match key with
        | RKId id -> return id
        | RKAlt (name, args) -> return! resolveAltKey connection globalArgs layout applyRole entityRef comments name args cancellationToken
    }

let insertEntities
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
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
                (Seq.singleton (null, sqlFunSubEntity), Seq.singleton (argInfo.PlaceholderId |> SQL.VEPlaceholder |> SQL.IVExpr))
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
                    SQL.IVExpr (SQL.VEPlaceholder argInfo.PlaceholderId)

            for requiredName in entity.RequiredFields do
                if not <| Map.containsKey requiredName rowArgs then
                    raisef EntityArgumentsException "Required field not provided: %O" requiredName

            let rowValues = dataFields |> Seq.map getValue
            Seq.append prefixValues rowValues |> Seq.toArray

        let insertValues = entitiesArgs |> Seq.map getRow |> Seq.toArray

        let opTable = SQL.operationTable <| compileResolvedEntityRef entity.Root
        let expr =
            { SQL.insertExpr opTable (SQL.ISValues insertValues) with
                Columns = insertColumns
                Returning = [| SQL.SCExpr (None, SQL.VEColumn { Table = None; Name = sqlFunId }) |]
                Extra = realEntityAnnotation entityRef
            }
        match applyRole with
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

let updateEntity
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (key : RowKey)
        (comments : string option)
        (updateArgs : LocalArgumentsMap)
        (cancellationToken : CancellationToken) : Task<RowId> =
    task {
        let entity = layout.FindEntity entityRef |> Option.get

        let tableRef = compileResolvedEntityRef entity.Root
        let aliasRef = { Schema = None; Name = tableRef.Name } : SQL.TableRef
        let keyCheck = rowKeyCheck layout entityRef key aliasRef

        let mutable arguments = keyCheck.Arguments
        let mutable argumentValues = Map.unionUnique (Map.mapKeys PGlobal globalArgs) keyCheck.ArgumentValues

        let getValue (fieldName : FieldName, fieldValue : FieldValue) =
            let field = Map.find fieldName entity.ColumnFields
            if field.IsImmutable then
                raisef EntityDeniedException "Field %O is immutable" fieldName
            let argument = { requiredArgument field.FieldType with Optional = fieldIsOptional field }
            let (argInfo, newArguments) = addArgument (PLocal fieldName) argument arguments
            arguments <- newArguments
            argumentValues <- Map.add (PLocal fieldName) fieldValue argumentValues
            let extra = { Name = fieldName } : RealFieldAnnotation
            let colName =
                { Name = field.ColumnName
                  Extra = extra :> obj
                } : SQL.UpdateColumnName
            SQL.UAESet (colName, SQL.IVExpr <| SQL.VEPlaceholder argInfo.PlaceholderId)

        let assigns = updateArgs |> Map.toSeq |> Seq.map getValue |> Seq.toArray

        let checkExpr = makeCheckExpr subEntityColumn layout entityRef
        let whereExpr = Option.addWith (curry SQL.VEAnd) keyCheck.Where checkExpr

        let opTable = SQL.operationTable tableRef
        let returning =
            match key with
            | RKId id -> [||]
            | RKAlt (name, keys) ->
                let entityId = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }
                [| SQL.SCExpr (None, entityId) |]

        let expr =
            { SQL.updateExpr opTable with
                  Assignments = assigns
                  Where = Some whereExpr
                  Returning = returning
                  Extra = realEntityAnnotation entityRef
            }
        let query =
            { Expression = expr
              Arguments = arguments
            }
        let query =
            match applyRole with
            | None -> query
            | Some role ->
                let getUsedField fieldName =
                    let field = Map.find fieldName entity.ColumnFields
                    let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
                    let fieldRef = { Entity = fieldEntity; Name = fieldName }
                    (fieldRef, usedFieldUpdate)

                let usedFields =
                    updateArgs
                    |> Map.keys
                    |> Seq.map getUsedField
                    |> Seq.append keyCheck.UsedFields
                let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntityUpdate usedFields
                let appliedDb =
                    try
                        applyPermissions layout role usedDatabase
                    with
                    | :? PermissionsApplyException as e ->
                        raisefWithInner EntityDeniedException e ""
                applyRoleUpdateExpr layout appliedDb query

        return! runQueryAndGetId connection applyRole tableRef query comments argumentValues whereExpr arguments key cancellationToken
    }

let deleteEntity
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (key : RowKey)
        (comments : string option)
        (cancellationToken : CancellationToken) : Task<RowId> =
    task {
        let entity = layout.FindEntity entityRef |> Option.get

        let tableRef = compileResolvedEntityRef entity.Root
        let aliasRef = { Schema = None; Name = tableRef.Name } : SQL.TableRef
        let keyCheck = rowKeyCheck layout entityRef key aliasRef
        let argumentValues = Map.unionUnique (Map.mapKeys PGlobal globalArgs) keyCheck.ArgumentValues

        if entity.IsAbstract then
            raisef EntityExecutionException "Entity %O is abstract" entityRef

        let whereExpr =
            if hasSubType entity then
                let subEntityCheck =
                    SQL.VEBinaryOp (SQL.VEColumn { Table = Some aliasRef; Name = sqlFunSubEntity }, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))
                SQL.VEAnd (subEntityCheck, keyCheck.Where)
            else
                keyCheck.Where

        let opTable = SQL.operationTable tableRef
        let returning =
            match key with
            | RKId id -> [||]
            | RKAlt (name, keys) ->
                let entityId = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }
                [| SQL.SCExpr (None, entityId) |]

        let expr =
            { SQL.deleteExpr opTable with
                  Where = Some whereExpr
                  Returning = returning
                  Extra = realEntityAnnotation entityRef
            }
        let query =
            { Expression = expr
              Arguments = keyCheck.Arguments
            }
        let query =
            match applyRole with
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
                    |> Seq.append keyCheck.UsedFields
                let usedDatabase = singleKnownFlatEntity entity.Root entityRef usedEntityDelete usedFields
                let appliedDb =
                    try
                        applyPermissions layout role usedDatabase
                    with
                    | :? PermissionsApplyException as e ->
                        raisefWithInner EntityDeniedException e ""
                applyRoleDeleteExpr layout appliedDb query

        return! runQueryAndGetId connection applyRole tableRef query comments argumentValues whereExpr keyCheck.Arguments key cancellationToken
    }

let private getSubEntity
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (entityRef : ResolvedEntityRef)
        (key : RowKey)
        (comments : string option)
        (cancellationToken : CancellationToken) : Task<RowId * ResolvedEntityRef> =
    task {
        let possibleTypes = allPossibleEntitiesList layout entityRef |> Seq.toArray
        match possibleTypes with
        | [||] -> return raisef EntityNotFoundException "Entry not found"
        | [|(realRef, bits)|] ->
            let! id = resolveKey connection globalArgs layout applyRole entityRef comments key cancellationToken
            return (id, realRef)
        | _ ->
            let entity = layout.FindEntity entityRef |> Option.get

            let mutable argumentValues = Map.mapKeys PGlobal globalArgs

            let opQuery = buildSelectOperation layout applyRole entityRef <| fun aliasRef ->
                let keyCheck = rowKeyCheck layout entityRef key aliasRef
                argumentValues <- Map.unionUnique argumentValues keyCheck.ArgumentValues
                let entitySubEntity = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunSubEntity }

                let results =
                    match key with
                    | RKId id -> seq { entitySubEntity }
                    | RKAlt (name, args) ->
                        let entityId = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }
                        seq { entityId; entitySubEntity }

                { Where = keyCheck.Where
                  Columns = results
                  UsedFields = keyCheck.UsedFields
                  Arguments = keyCheck.Arguments
                }

            let execute query args cancellationToken = connection.ExecuteRowValuesQuery query args cancellationToken
            match! runQuery execute opQuery.Query comments argumentValues cancellationToken with
            | None ->
                do! countAndThrow connection applyRole opQuery.Table opQuery.WhereWithoutRole opQuery.ArgumentsWithoutRole argumentValues cancellationToken
                return failwith "Impossible"
            | Some row ->
                match key with
                | RKId id ->
                    match row with
                    | [|(_, _, subEntityValue)|] ->
                        let rawSubEntity = SQL.parseStringValue subEntityValue
                        let subEntity = parseTypeName entity.Root rawSubEntity
                        return (id, subEntity)
                    | _ -> return failwith "Impossible"
                | RKAlt (name, args) ->
                    match row with
                    | [|(_, _, idValue); (_, _, subEntityValue)|] ->
                        let rawSubEntity = SQL.parseStringValue subEntityValue
                        let subEntity = parseTypeName entity.Root rawSubEntity
                        return (SQL.parseIntValue idValue, subEntity)
                    | _ -> return failwith "Impossible"
    }

let private funRelatedId = FunQLName "related_id"

let private getRelatedRowIds
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (fieldRef : ResolvedFieldRef)
        (relatedId : RowId)
        (comments : string option)
        (cancellationToken : CancellationToken) : Task<(RowId * ResolvedEntityRef)[]> =
    task {
        let entity = layout.FindEntity fieldRef.Entity |> Option.get
        let field = Map.find fieldRef.Name entity.ColumnFields
        let possibleTypes = allPossibleEntitiesList layout fieldRef.Entity |> Seq.toArray
        let singleSubEntity =
            match possibleTypes with
            | [||] -> raisef EntityNotFoundException "Entry not found"
            | [|(realRef, bits)|] -> Some realRef
            | _ -> None

        let initialArgumentValues = Map.singleton (PLocal funRelatedId) (FInt relatedId)
        let argumentValues = Map.unionUnique (Map.mapKeys PGlobal globalArgs) initialArgumentValues

        let opQuery = buildSelectOperation layout applyRole fieldRef.Entity <| fun aliasRef ->
            let (idArg, arguments) = addArgument (PLocal funRelatedId) funIdArg emptyArguments
            let whereRelated = SQL.VEBinaryOp (SQL.VEColumn { Table = Some aliasRef; Name = field.ColumnName }, SQL.BOEq, SQL.VEPlaceholder idArg.PlaceholderId)
            let entityId = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunId }
            let usedFields = Seq.singleton ({ Entity = Option.defaultValue fieldRef.Entity field.InheritedFrom; Name = fieldRef.Name }, usedFieldSelect)

            let results =
                if Option.isSome singleSubEntity then
                    seq { entityId }
                else
                    let entitySubEntity = SQL.VEColumn { Table = Some aliasRef; Name = sqlFunSubEntity }
                    seq { entityId; entitySubEntity }

            { Where = whereRelated
              Columns = results
              UsedFields = usedFields
              Arguments = arguments
            }

        let processOne =
            match singleSubEntity with
            | None -> function
                | [|idValue; subEntityValue|] ->
                    let rawSubEntity = SQL.parseStringValue subEntityValue
                    let subEntity = parseTypeName entity.Root rawSubEntity
                    (SQL.parseIntValue idValue, subEntity)
                | _ -> failwith "Impossible"
            | Some subEntity -> function
                | [|value|] -> (SQL.parseIntValue value, subEntity)
                | _ -> failwith "Impossible"

        let execute query args cancellationToken = connection.ExecuteQuery query args cancellationToken <| fun columns rows -> task { return! rows.Select(processOne).ToArrayAsync(cancellationToken) }
        return! runQuery execute opQuery.Query comments argumentValues cancellationToken
    }

type private ProcessedMap = Map<ResolvedEntityRef * RowId, int>

type ReferencesRowIndex = int

type ReferencesChild =
    { Field : FieldName
      Row : ReferencesRowIndex
      DeleteAction : ReferenceDeleteAction
    }

and ReferencesNode =
    { Entity : ResolvedEntityRef
      Id : RowId
      References : ReferencesChild[]
    }

type ReferencesTree =
    { Nodes : ReferencesNode[]
      Root : ReferencesRowIndex
    }

let iterDeleteReferences (f : ResolvedEntityRef -> RowId -> Task) (tree : ReferencesTree) : Task<Set<ReferencesRowIndex>> =
    task {
        let mutable visited = Set.empty
        let rec go deleteAction i =
            unitTask {
                visited <- Set.add i visited
                let node = tree.Nodes.[i]
                for child in node.References do
                    match child.DeleteAction with
                    // Walk into CASCADE and NO ACTION nodes, as they are deleted explicitly (in NO ACTION case)
                    // or their children need to be inspected (in CASCADE case).
                    | RDACascade
                    | RDANoAction when not (Set.contains child.Row visited) ->
                        do! go (Some child.DeleteAction) child.Row
                    | _ -> ()
                // Don't explicitly delete CASCADE nodes.
                match deleteAction with
                | Some RDACascade -> ()
                | _ -> do! f node.Entity node.Id
            }
        do! go None tree.Root
        return visited
    }

let getRelatedEntities
        (connection : QueryConnection)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (filterFields : ResolvedEntityRef -> RowId -> ResolvedFieldRef -> bool)
        (initialEntityRef : ResolvedEntityRef)
        (key : RowKey)
        (comments : string option)
        (cancellationToken : CancellationToken) : Task<ReferencesTree> =
    task {
        let mutable processed = Map.empty
        let mutable nodes = Map.empty
        let mutable lastId = 0

        let rec findOne (id : RowId) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : Task<int> =
            task {
                let idx = lastId
                processed <- Map.add (entity.Root, id) idx processed
                lastId <- lastId + 1
                let! referenceLists = entity.ReferencingFields |> Map.toSeq |> Seq.mapTask (findRelated entityRef id)
                let node =
                    { Entity = entityRef
                      Id = id
                      References =  referenceLists |> Seq.concat |> Seq.toArray
                    }
                nodes <- Map.add idx node nodes
                return idx
            }

        and findRelated (entityRef : ResolvedEntityRef) (relatedId : RowId) (refFieldRef : ResolvedFieldRef, deleteAction : ReferenceDeleteAction) : Task<ReferencesChild seq> =
            task {
                if not <| filterFields entityRef relatedId refFieldRef then
                    return Seq.empty
                else
                    let! related = getRelatedRowIds connection globalArgs layout applyRole refFieldRef relatedId comments cancellationToken
                    let getOne (id, subEntityRef) =
                        task {
                            let entity = layout.FindEntity subEntityRef |> Option.get
                            let! idx =
                                task {
                                    match Map.tryFind (entity.Root, id) processed with
                                    | Some idx -> return idx
                                    | None -> return! findOne id subEntityRef entity
                                }
                            return
                                { Field = refFieldRef.Name
                                  Row = idx
                                  DeleteAction = deleteAction
                                }
                        }
                    return! Seq.mapTask getOne related
            }

        let! (id, subEntityRef) = getSubEntity connection globalArgs layout applyRole initialEntityRef key comments cancellationToken
        let subEntity = layout.FindEntity subEntityRef |> Option.get
        let! root = findOne id subEntityRef subEntity
        let nodes = seq {0..lastId - 1} |> Seq.map (fun idx -> nodes.[idx]) |> Seq.toArray
        return
            { Nodes = nodes
              Root = root
            }
    }
