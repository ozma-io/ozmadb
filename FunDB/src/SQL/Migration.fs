module FunWithFlags.FunDB.SQL.Migration

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
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
    | SODeleteTable _ -> 3
    | SODeleteSequence _ -> 4
    | SODeleteSchema _ -> 5
    | SOCreateSchema _ -> 6
    | SOCreateTable _ -> 7
    | SOCreateSequence _ -> 8
    | SOAlterTable _ -> 9
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 10
    | SOCreateConstraint (_, _, CMUnique _) -> 11
    | SOCreateConstraint (_, _, CMForeignKey _) -> 12
    | SOCreateConstraint (_, _, CMCheck _) -> 13
    | SOCreateIndex _ -> 14

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
        yield SODeleteSchema schemaName
    }

[<NoComparison>]
type AColumnMeta =
    { columnType : DBValueType
      isNullable : bool
      defaultExpr : ValueExpr option
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

let migrateDatabase (query : QueryConnection) (plan : MigrationPlan) : Task<bool> =
    task {
        for action in plan do
            eprintfn "Migration step: %O" action
        let mutable touched = false
        for action in plan do
            let! _ = query.ExecuteNonQuery (action.ToSQLString()) Map.empty
            touched <- true
            ()
        if touched then
            // Clear prepared statements so that things don't break if e.g. database types have changed.
            query.Connection.UnprepareAll ()
        return touched
    }