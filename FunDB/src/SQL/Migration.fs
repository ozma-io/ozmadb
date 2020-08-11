module FunWithFlags.FunDB.SQL.Migration

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.SQL.Query

type MigrationPlan = SchemaOperation[]

// Order for table operations so that dependencies are not violated.
let private tableOperationOrder = function
    | TODeleteColumn _ -> 1
    | TOCreateColumn _ -> 2
    | TOAlterColumnType _ -> 3
    | TOAlterColumnNull _ -> 4
    | TOAlterColumnDefault _ -> 5

// Order for database operations so that dependencies are not violated.
let private schemaOperationOrder = function
    | SODeleteConstraint _ -> 1
    | SODeleteIndex _ -> 2
    | SODropTrigger _ -> 3
    | SODropFunction _ -> 4
    | SODeleteTable _ -> 5
    | SODeleteSequence _ -> 6
    | SODeleteSchema _ -> 7
    | SOCreateSchema _ -> 8
    | SOCreateTable _ -> 9
    | SOCreateSequence _ -> 10
    | SOAlterTable _ -> 11
    | SOCreateOrReplaceFunction _ -> 12
    | SOCreateTrigger _ -> 13
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 14
    | SOCreateConstraint (_, _, CMUnique _) -> 15
    | SOCreateConstraint (_, _, CMForeignKey _) -> 16
    | SOCreateConstraint (_, _, CMCheck _) -> 17
    | SOCreateIndex _ -> 18

let private deleteBuildTable (table : TableRef) (tableMeta : TableMeta) : SchemaOperation seq =
    seq {
        yield SODeleteTable table
    }

let private deleteBuildSchema (schemaName : SchemaName) (schemaMeta : SchemaMeta) : SchemaOperation seq =
    seq {
        for KeyValue (objectName, obj) in schemaMeta.objects do
            let objRef = { schema = Some schemaName; name = objectName }
            match obj with
            | OMTable tableMeta ->
                yield! deleteBuildTable objRef tableMeta
            | OMSequence ->
                yield SODeleteSequence objRef
            | OMConstraint (tableName, constraintMeta) ->
                yield SODeleteConstraint (objRef, tableName)
            | OMIndex (tableName, indexMeta) ->
                yield SODeleteIndex objRef
            | OMFunction overloads ->
                for KeyValue (signature, def) in overloads do
                    yield SODropFunction (objRef, signature)
            | OMTrigger (table, def) ->
                yield SODropTrigger (objRef, table)
        yield SODeleteSchema schemaName
    }

[<NoEquality; NoComparison>]
type private AColumnMeta =
    { ColumnType : DBValueType
      IsNullable : bool
      DefaultExpr : ValueExpr option
    }

// Convert tree the way PostgreSQL converts it internally, but drop type casts.
let private normalizeLocalExpr : ValueExpr -> ValueExpr =
    let rec traverse = function
        | VECast (v, typ) -> traverse v
        | VEValue v as node ->
            match v with
            | VStringArray arr -> normalizeArray VString arr
            | VIntArray arr -> normalizeArray VInt arr
            | VDecimalArray arr -> normalizeArray VDecimal arr
            | VBoolArray arr -> normalizeArray VBool arr
            | VDateArray arr -> normalizeArray VDate arr
            | VDateTimeArray arr -> normalizeArray VDateTime arr
            | VRegclassArray arr -> normalizeArray VRegclass arr
            | VJsonArray arr -> normalizeArray VJson arr
            | _ -> node
        | VEColumn c -> VEColumn c
        | VEPlaceholder i -> VEPlaceholder i
        | VENot e -> VENot (traverse e)
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEDistinct (a, b) -> VEDistinct (traverse a, traverse b)
        | VENotDistinct (a, b) -> VENotDistinct (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VEEqAny (e, arr) -> VEEqAny (traverse e, traverse arr)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
        | VENotEqAll (e, arr) -> VENotEqAll (traverse e, traverse arr)
        | VELike (e, pat) -> VELike (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEEqAny (traverse e, VEArray (Array.map traverse vals))
        | VENotIn (e, vals) -> VENotEqAll (traverse e, VEArray (Array.map traverse vals))
        | VEInQuery (e, query) -> failwithf "Invalid subquery in local expression: %O" query
        | VENotInQuery (e, query) -> failwithf "Invalid subquery in local expression: %O" query
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> failwithf "Invalid aggregate function in local expression: %O" name
        | VECase (es, els) -> VECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | VECoalesce vals -> VECoalesce (Array.map traverse vals)
        | VEJsonArrow (a, b) -> VEJsonArrow (traverse a, traverse b)
        | VEJsonTextArrow (a, b) -> VEJsonTextArrow (traverse a, traverse b)
        | VEPlus (a, b) -> VEPlus (traverse a, traverse b)
        | VEMinus (a, b) -> VEMinus (traverse a, traverse b)
        | VEMultiply (a, b) -> VEMultiply (traverse a, traverse b)
        | VEDivide (a, b) -> VEDivide (traverse a, traverse b)
        | VEArray vals -> VEArray (Array.map traverse vals)
        | VESubquery query -> failwithf "Invalid subquery in local expression: %O" query
    traverse

let private migrateBuildTable (fromMeta : TableMeta) (toMeta : TableMeta) : TableOperation seq =
    seq {
        for KeyValue (columnName, columnMeta) in toMeta.columns do
            match Map.tryFind columnName fromMeta.columns with
            | None -> yield TOCreateColumn (columnName, columnMeta)
            | Some oldColumnMeta ->
                if oldColumnMeta.columnType <> columnMeta.columnType then
                    yield TOAlterColumnType (columnName, columnMeta.columnType)
                if oldColumnMeta.isNullable <> columnMeta.isNullable then
                    yield TOAlterColumnNull (columnName, columnMeta.isNullable)
                if Option.map normalizeLocalExpr oldColumnMeta.defaultExpr <> Option.map normalizeLocalExpr columnMeta.defaultExpr then
                    yield TOAlterColumnDefault (columnName, columnMeta.defaultExpr)

        for KeyValue (columnName, columnMeta) in fromMeta.columns do
            if not (Map.containsKey columnName toMeta.columns) then
                yield TODeleteColumn columnName
    }

let private migrateAlterTable (objRef : SchemaObject) (fromMeta : TableMeta) (toMeta : TableMeta) : SchemaOperation seq =
    seq {
        let changes = migrateBuildTable fromMeta toMeta |> Array.ofSeq
        if not <| Array.isEmpty changes then
            yield SOAlterTable (objRef, changes |> Array.sortBy tableOperationOrder)
    }

let private normalizeConstraint : ConstraintMeta -> ConstraintMeta = function
    | CMCheck expr -> CMCheck (normalizeLocalExpr expr)
    | CMForeignKey (ref, f) -> CMForeignKey (ref, f)
    | CMPrimaryKey p -> CMPrimaryKey p
    | CMUnique u -> CMUnique u

let private migrateOverloads (objRef : SchemaObject) (oldOverloads : Map<FunctionSignature, FunctionDefinition>) (newOverloads : Map<FunctionSignature, FunctionDefinition>) : SchemaOperation seq =
    seq {
        for KeyValue (signature, newDefinition) in newOverloads do
            match Map.tryFind signature oldOverloads with
            | Some oldDefinition when oldDefinition = newDefinition -> ()
            | _ -> yield SOCreateOrReplaceFunction (objRef, signature, newDefinition)
        for KeyValue (signature, definition) in oldOverloads do
            match Map.tryFind signature newOverloads with
            | Some _ -> ()
            | None -> yield SODropFunction (objRef, signature)
    }

let private migrateBuildSchema (schema : SchemaName) (fromMeta : SchemaMeta) (toMeta : SchemaMeta) : SchemaOperation seq =
    seq {
        for KeyValue (objectName, obj) in toMeta.objects do
            let objRef = { schema = Some schema; name = objectName }
            match obj with
            | OMTable tableMeta ->
                match Map.tryFind objectName fromMeta.objects with
                | Some (OMTable oldTableMeta) ->
                    yield! migrateAlterTable objRef oldTableMeta tableMeta
                | _ ->
                    yield SOCreateTable objRef
                    yield! migrateAlterTable objRef emptyTableMeta tableMeta
            | OMSequence ->
                match Map.tryFind objectName fromMeta.objects with
                | Some OMSequence -> ()
                | _ -> yield SOCreateSequence objRef
            | OMConstraint (tableName, constraintType) ->
                match Map.tryFind objectName fromMeta.objects with
                | Some (OMConstraint (oldTableName, oldConstraintType)) ->
                    if tableName <> oldTableName || normalizeConstraint constraintType <> normalizeConstraint oldConstraintType then
                        yield SODeleteConstraint (objRef, oldTableName)
                        yield SOCreateConstraint (objRef, tableName, constraintType)
                | _ -> yield SOCreateConstraint (objRef, tableName, constraintType)
            | OMIndex (tableName, index) ->
                match Map.tryFind objectName fromMeta.objects with
                | Some (OMIndex (oldTableName, oldIndex)) ->
                    if tableName <> oldTableName || index <> oldIndex then
                        yield SODeleteIndex objRef
                        yield SOCreateIndex (objRef, tableName, index)
                | _ -> yield SOCreateIndex (objRef, tableName, index)
            | OMFunction newOverloads ->
                match Map.tryFind objectName fromMeta.objects with
                | Some (OMFunction oldOverloads) -> yield! migrateOverloads objRef oldOverloads newOverloads
                | _ -> yield! migrateOverloads objRef Map.empty newOverloads
            | OMTrigger (tableName, trigger) ->
                match Map.tryFind objectName fromMeta.objects with
                | Some (OMTrigger (oldTableName, oldTrigger)) ->
                    if tableName <> oldTableName || trigger <> oldTrigger then
                        yield SODropTrigger (objRef, oldTableName)
                        yield SOCreateTrigger (objRef, tableName, trigger)
                | _ -> yield SOCreateTrigger (objRef, tableName, trigger)

        for KeyValue (objectName, obj) in fromMeta.objects do
            let objRef = { schema = Some schema; name = objectName }
            match obj with
            | OMTable tableMeta ->
                match Map.tryFind objectName toMeta.objects with
                | Some (OMTable _) -> ()
                | _ -> yield! deleteBuildTable objRef tableMeta
            | OMSequence ->
                match Map.tryFind objectName toMeta.objects with
                | Some OMSequence -> ()
                | _ -> yield SODeleteSequence objRef
            | OMConstraint (tableName, _) ->
                match Map.tryFind objectName toMeta.objects with
                | Some (OMConstraint _) -> ()
                | _ -> yield SODeleteConstraint (objRef, tableName)
            | OMIndex (tableName, _) ->
                match Map.tryFind objectName toMeta.objects with
                | Some (OMIndex _) -> ()
                | _ -> yield SODeleteIndex objRef
            | OMFunction overloads ->
                match Map.tryFind objectName toMeta.objects with
                | Some (OMFunction _) -> ()
                | _ ->
                    for KeyValue (signature, def) in overloads do
                        yield SODropFunction (objRef, signature)
            | OMTrigger (tableName, _) ->
                match Map.tryFind objectName toMeta.objects with
                | Some (OMTrigger _) -> ()
                | _ -> yield SODropTrigger (objRef, tableName)
        }

let private migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : SchemaOperation seq =
    seq {
        for KeyValue (schemaName, schemaMeta) in toMeta.schemas do
            match Map.tryFind schemaName fromMeta.schemas with
            | None ->
                yield SOCreateSchema schemaName
                yield! migrateBuildSchema schemaName emptySchemaMeta schemaMeta
            | Some oldSchemaMeta -> yield! migrateBuildSchema schemaName oldSchemaMeta schemaMeta

        for KeyValue (schemaName, schemaMeta) in fromMeta.schemas do
            if not (Map.containsKey schemaName toMeta.schemas) then
                yield! deleteBuildSchema schemaName schemaMeta
    }

let planDatabaseMigration (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy schemaOperationOrder |> Seq.toArray

let migrateDatabase (query : QueryConnection) (plan : MigrationPlan) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let mutable touched = false
        for action in plan do
            let! _ = query.ExecuteNonQuery (action.ToSQLString()) Map.empty cancellationToken
            touched <- true
            ()
        if touched then
            // Clear prepared statements so that things don't break if e.g. database types have changed.
            query.Connection.UnprepareAll ()
        return touched
    }