module FunWithFlags.FunDB.SQL.Migration

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.SQL.Query

type MigrationPlan = SchemaOperation[]

// Order for table operations so that dependencies are not violated.
let private tableOperationOrder = function
    | TODeleteColumn _ -> 1
    | TOCreateColumn _ -> 1
    | TOAlterColumnType _ -> 2
    | TOAlterColumnNull _ -> 2
    | TOAlterColumnDefault _ -> 2

// Order for database operations so that dependencies are not violated.
let private schemaOperationOrder = function
    // Tricky cases:
    // *. DEFAULT values using sequences and/or functions.

    // First, delete indexes and triggers. Also delete all constraints except primary keys first (we separate them by secondary number).
    | SODropConstraint _ -> 0
    | SODropIndex _ -> 0
    | SODropTrigger _ -> 0
    // Cool, now create new schemas, sequences and functions.
    | SOCreateSchema _ -> 1
    | SORenameSchema _ -> 1
    | SOCreateSequence _ -> 2
    | SORenameSequence _ -> 2
    | SORenameFunction _ -> 2
    | SOCreateOrReplaceFunction _ -> 3
    // Next, create tables and alter columns.
    | SOCreateTable _ -> 4
    | SORenameTable _ -> 4
    | SORenameTableColumn _ -> 5
    | SOAlterTable _ -> 6
    // Remove remaining stuff.
    | SODropTable _ -> 7
    | SODropFunction _ -> 8
    | SODropSequence _ -> 8
    | SODropSchema _ -> 9
    // Create triggers, indexes and primary key constraints.
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 10
    | SORenameConstraint _ -> 10
    | SOCreateIndex _ -> 10
    | SORenameIndex _ -> 10
    | SOCreateTrigger _ -> 10
    | SORenameTrigger _ -> 10
    // Finally, create other constraints.
    | SOCreateConstraint _ -> 11

type private OrderedSchemaOperation = SchemaOperation * int

let private deleteBuildTable (table : TableRef) (tableMeta : TableMeta) : OrderedSchemaOperation seq =
    seq {
        yield (SODropTable table, 0)
    }

let private deleteBuildSchema (schemaName : SchemaName) (schemaMeta : SchemaMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (_, (objectName, obj)) in schemaMeta.Objects do
            let objRef = { schema = Some schemaName; name = objectName }
            match obj with
            | OMTable tableMeta ->
                yield! deleteBuildTable objRef tableMeta
            | OMSequence ->
                yield (SODropSequence objRef, 0)
            | OMConstraint (tableName, constraintType) ->
                let isPrimaryKey =
                    match constraintType with
                    | CMPrimaryKey _ -> 1
                    | _ -> 0
                yield (SODropConstraint (objRef, tableName), isPrimaryKey)
            | OMIndex (tableName, indexMeta) ->
                yield (SODropIndex objRef, 0)
            | OMFunction overloads ->
                for KeyValue (signature, def) in overloads do
                    yield (SODropFunction (objRef, signature), 0)
            | OMTrigger (table, def) ->
                yield (SODropTrigger (objRef, table), 0)
        yield (SODropSchema schemaName, 0)
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
        for KeyValue (columnKey, columnMeta) in toMeta.Columns do
            match Map.tryFind columnKey fromMeta.Columns with
            | None -> yield TOCreateColumn columnMeta
            | Some oldColumnMeta ->
                if oldColumnMeta.ColumnType <> columnMeta.ColumnType then
                    yield TOAlterColumnType (columnMeta.Name, columnMeta.ColumnType)
                if oldColumnMeta.IsNullable <> columnMeta.IsNullable then
                    yield TOAlterColumnNull (columnMeta.Name, columnMeta.IsNullable)
                if Option.map normalizeLocalExpr oldColumnMeta.DefaultExpr <> Option.map normalizeLocalExpr columnMeta.DefaultExpr then
                    yield TOAlterColumnDefault (columnMeta.Name, columnMeta.DefaultExpr)

        for KeyValue (columnKey, columnMeta) in fromMeta.Columns do
            if not (Map.containsKey columnKey toMeta.Columns) then
                yield TODeleteColumn columnMeta.Name
    }

let private migrateAlterTable (objRef : SchemaObject) (fromMeta : TableMeta) (toMeta : TableMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (columnKey, columnMeta) in toMeta.Columns do
            match Map.tryFind columnKey fromMeta.Columns with
            | Some oldColumnMeta when oldColumnMeta.Name <> columnMeta.Name ->
                yield (SORenameTableColumn (objRef, oldColumnMeta.Name, columnMeta.Name), 0)
            | _ -> ()
        let changes = migrateBuildTable fromMeta toMeta |> Array.ofSeq
        if not <| Array.isEmpty changes then
            yield (SOAlterTable (objRef, changes |> Array.sortBy tableOperationOrder), 0)
    }

let private normalizeConstraint : ConstraintMeta -> ConstraintMeta = function
    | CMCheck expr -> CMCheck (normalizeLocalExpr expr)
    | CMForeignKey (ref, f) -> CMForeignKey (ref, f)
    | CMPrimaryKey p -> CMPrimaryKey p
    | CMUnique u -> CMUnique u

let private migrateOverloads (objRef : SchemaObject) (oldOverloads : Map<FunctionSignature, FunctionDefinition>) (newOverloads : Map<FunctionSignature, FunctionDefinition>) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (signature, newDefinition) in newOverloads do
            match Map.tryFind signature oldOverloads with
            | Some oldDefinition when oldDefinition = newDefinition -> ()
            | _ -> yield (SOCreateOrReplaceFunction (objRef, signature, newDefinition), 0)
        for KeyValue (signature, definition) in oldOverloads do
            match Map.tryFind signature newOverloads with
            | Some _ -> ()
            | None -> yield (SODropFunction (objRef, signature), 0)
    }

let private migrateBuildSchema (fromObjects : SchemaObjects) (toMeta : SchemaMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (objectKey, (objectName, obj)) in toMeta.Objects do
            let objRef = { schema = Some toMeta.Name; name = objectName }
            match obj with
            | OMTable tableMeta ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMTable oldTableMeta) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        yield (SORenameTable (oldObjRef, objectName), 0)
                    yield! migrateAlterTable objRef oldTableMeta tableMeta
                | _ ->
                    yield (SOCreateTable objRef, 0)
                    yield! migrateAlterTable objRef emptyTableMeta tableMeta
            | OMSequence ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMSequence) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        yield (SORenameSequence (oldObjRef, objectName), 0)
                | _ -> yield (SOCreateSequence objRef, 0)
            | OMConstraint (tableName, constraintType) ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMConstraint (oldTableName, oldConstraintType)) ->
                    if tableName <> oldTableName || normalizeConstraint constraintType <> normalizeConstraint oldConstraintType then
                        let isPrimaryKey =
                            match oldConstraintType with
                            | CMPrimaryKey _ -> 1
                            | _ -> 0
                        yield (SODropConstraint (objRef, oldTableName), isPrimaryKey)
                        yield (SOCreateConstraint (objRef, tableName, constraintType), 0)
                    else if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        yield (SORenameConstraint (oldObjRef, tableName, objectName), 0)
                | _ -> yield (SOCreateConstraint (objRef, tableName, constraintType), 0)
            | OMIndex (tableName, index) ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMIndex (oldTableName, oldIndex)) ->
                    if tableName <> oldTableName || index <> oldIndex then
                        yield (SODropIndex objRef, 0)
                        yield (SOCreateIndex (objRef, tableName, index), 0)
                    else if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        yield (SORenameIndex (oldObjRef, objectName), 0)
                | _ -> yield (SOCreateIndex (objRef, tableName, index), 0)
            | OMFunction newOverloads ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMFunction oldOverloads) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        for KeyValue (signature, _) in oldOverloads do
                            yield (SORenameFunction (oldObjRef, signature, objectName), 0)
                    yield! migrateOverloads objRef oldOverloads newOverloads
                | _ -> yield! migrateOverloads objRef Map.empty newOverloads
            | OMTrigger (tableName, trigger) ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMTrigger (oldTableName, oldTrigger)) ->
                    if tableName <> oldTableName || trigger <> oldTrigger then
                        yield (SODropTrigger (objRef, oldTableName), 0)
                        yield (SOCreateTrigger (objRef, tableName, trigger), 0)
                    else if oldObjectName <> objectName then
                        let oldObjRef = { schema = Some toMeta.Name; name = oldObjectName }
                        yield (SORenameTrigger (oldObjRef, tableName, objectName), 0)
                | _ -> yield (SOCreateTrigger (objRef, tableName, trigger), 0)

        for KeyValue (objectKey, (objectName, obj)) in fromObjects do
            let objRef = { schema = Some toMeta.Name; name = objectName }
            match obj with
            | OMTable tableMeta ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMTable _) -> ()
                | _ -> yield! deleteBuildTable objRef tableMeta
            | OMSequence ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMSequence) -> ()
                | _ -> yield (SODropSequence objRef, 0)
            | OMConstraint (tableName, oldConstraintType) ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMConstraint _) -> ()
                | _ ->
                    let isPrimaryKey =
                        match oldConstraintType with
                        | CMPrimaryKey _ -> 1
                        | _ -> 0
                    yield (SODropConstraint (objRef, tableName), isPrimaryKey)
            | OMIndex (tableName, _) ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMIndex _) -> ()
                | _ -> yield (SODropIndex objRef, 0)
            | OMFunction overloads ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMFunction _) -> ()
                | _ ->
                    for KeyValue (signature, def) in overloads do
                        yield (SODropFunction (objRef, signature), 0)
            | OMTrigger (tableName, _) ->
                match Map.tryFind objectKey toMeta.Objects with
                | Some (newObjectName, OMTrigger _) -> ()
                | _ -> yield (SODropTrigger (objRef, tableName), 0)
        }

let private migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (schemaKey, schemaMeta) in toMeta.Schemas do
            match Map.tryFind schemaKey fromMeta.Schemas with
            | None ->
                yield (SOCreateSchema schemaMeta.Name, 0)
                yield! migrateBuildSchema Map.empty schemaMeta
            | Some oldSchemaMeta ->
                if oldSchemaMeta.Name <> schemaMeta.Name then
                    yield (SORenameSchema (oldSchemaMeta.Name, schemaMeta.Name), 0)
                yield! migrateBuildSchema oldSchemaMeta.Objects schemaMeta

        for KeyValue (schemaKey, schemaMeta) in fromMeta.Schemas do
            if not (Map.containsKey schemaKey toMeta.Schemas) then
                yield! deleteBuildSchema schemaMeta.Name schemaMeta
    }

let planDatabaseMigration (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy (fun (op, order) -> (schemaOperationOrder op, order)) |> Seq.map fst |> Seq.toArray

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
