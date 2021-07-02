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
    | TODropColumn _ -> 0
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
    // Cool, now create new schemas.
    | SOCreateSchema _ -> 1
    | SORenameSchema _ -> 1
    // Create new extensions.
    | SOCreateExtension _ -> 2
    // Create new sequences and functions.
    | SOCreateSequence _ -> 3
    | SORenameSequence _ -> 3
    | SORenameFunction _ -> 3
    | SOCreateOrReplaceFunction _ -> 4
    // Next, create tables and alter columns.
    | SOCreateTable _ -> 5
    | SORenameTable _ -> 5
    | SORenameTableColumn _ -> 6
    | SOAlterTable _ -> 7
    // Remove remaining stuff.
    | SODropTable _ -> 8
    | SODropSequence _ -> 9
    | SODropFunction _ -> 10
    | SODropSchema _ -> 11
    | SODropExtension _ -> 12
    // Create triggers, indexes and primary key constraints.
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 13
    | SORenameConstraint _ -> 13
    | SOCreateIndex _ -> 13
    | SORenameIndex _ -> 13
    | SOCreateTrigger _ -> 13
    | SORenameTrigger _ -> 13
    // Finally, create and alter other constraints.
    | SOAlterConstraint _ -> 14
    | SOCreateConstraint _ -> 14

type private OrderedSchemaOperation = SchemaOperation * int

let private deleteBuildTable (table : TableRef) (tableMeta : TableMeta) : OrderedSchemaOperation seq =
    seq {
        yield (SODropTable table, 0)
    }

let private deleteBuildSchema (schemaName : SchemaName) (schemaMeta : SchemaMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (_, (objectName, obj)) in schemaMeta.Objects do
            let objRef = { Schema = Some schemaName; Name = objectName }
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
        | VEDistinct (a, b) -> VEDistinct (traverse a, traverse b)
        | VENotDistinct (a, b) -> VENotDistinct (traverse a, traverse b)
        | VEBinaryOp (a, op, b) -> VEBinaryOp (traverse a, op, traverse b)
        | VESimilarTo (e, pat) -> VESimilarTo (traverse e, traverse pat)
        | VENotSimilarTo (e, pat) -> VENotSimilarTo (traverse e, traverse pat)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VEIn (e, vals) -> VEAny (traverse e, BOEq, VEArray (Array.map traverse vals))
        | VENotIn (e, vals) -> VEAll (traverse e, BONotEq, VEArray (Array.map traverse vals))
        | VEAny (e, op, arr) -> VEAny (traverse e, op, traverse arr)
        | VEAll (e, op, arr) -> VEAny (traverse e, op, traverse arr)
        | VEInQuery (e, query) -> failwithf "Invalid subquery in local expression: %O" query
        | VENotInQuery (e, query) -> failwithf "Invalid subquery in local expression: %O" query
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VESpecialFunc (name, args) -> VESpecialFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> failwithf "Invalid aggregate function in local expression: %O" name
        | VECase (es, els) -> VECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | VEArray vals -> VEArray (Array.map traverse vals)
        | VESubquery query -> failwithf "Invalid subquery in local expression: %O" query
    traverse

let private normalizeIndexKey = function
    | IKColumn name -> IKColumn name
    | IKExpression expr -> expr.Value |> normalizeLocalExpr |> String.comparable |> IKExpression

let private normalizeIndexColumn (col : IndexColumn) : IndexColumn =
    { col with Key = normalizeIndexKey col.Key }

let private normalizeIndex (index : IndexMeta) : IndexMeta =
    { index with
          Columns = Array.map normalizeIndexColumn index.Columns
          Predicate = index.Predicate |> Option.map (fun x -> x.Value |> normalizeLocalExpr |> String.comparable)
    }

let private migrateColumnAttrs (fromMeta : ColumnMeta) (toMeta : ColumnMeta) : TableOperation seq =
    seq {
        if fromMeta.DataType <> toMeta.DataType then
            yield TOAlterColumnType (toMeta.Name, toMeta.DataType)
        if fromMeta.IsNullable <> toMeta.IsNullable then
            yield TOAlterColumnNull (toMeta.Name, toMeta.IsNullable)
    }

let private migrateBuildTable (fromMeta : TableMeta) (toMeta : TableMeta) : TableOperation seq =
    seq {
        for KeyValue (columnKey, columnMeta) in toMeta.Columns do
            match Map.tryFind columnKey fromMeta.Columns with
            | None -> yield TOCreateColumn columnMeta
            | Some oldColumnMeta ->
                match (oldColumnMeta.ColumnType, columnMeta.ColumnType) with
                | (CTPlain oldPlain, CTPlain plain) ->
                    if Option.map (normalizeLocalExpr >> string) oldPlain.DefaultExpr <> Option.map (normalizeLocalExpr >> string) plain.DefaultExpr then
                        yield TOAlterColumnDefault (columnMeta.Name, plain.DefaultExpr)
                    yield! migrateColumnAttrs oldColumnMeta columnMeta
                | (CTGeneratedStored oldGenExpr, CTGeneratedStored genExpr) ->
                    if string (normalizeLocalExpr oldGenExpr) <> string (normalizeLocalExpr genExpr) then
                        yield TODropColumn columnMeta.Name
                        yield TOCreateColumn columnMeta
                    else
                        yield! migrateColumnAttrs oldColumnMeta columnMeta
                | (_, _) ->
                    yield TODropColumn columnMeta.Name
                    yield TOCreateColumn columnMeta

        for KeyValue (columnKey, columnMeta) in fromMeta.Columns do
            if not (Map.containsKey columnKey toMeta.Columns) then
                yield TODropColumn columnMeta.Name
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

let private migrateOverloads (objRef : SchemaObject) (oldOverloads : Map<FunctionSignature, FunctionDefinition>) (newOverloads : Map<FunctionSignature, FunctionDefinition>) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (signature, newDefinition) in newOverloads do
            match Map.tryFind signature oldOverloads with
            | Some oldDefinition when string oldDefinition = string newDefinition -> ()
            | _ -> yield (SOCreateOrReplaceFunction (objRef, signature, newDefinition), 0)
        for KeyValue (signature, definition) in oldOverloads do
            match Map.tryFind signature newOverloads with
            | Some _ -> ()
            | None -> yield (SODropFunction (objRef, signature), 0)
    }

let private migrateDeferrableConstraint (objRef : SchemaObject) (tableName : TableName) (oldDefer : DeferrableConstraint) (newDefer : DeferrableConstraint) : OrderedSchemaOperation seq =
    seq {
        if oldDefer <> newDefer then
            yield (SOAlterConstraint (objRef, tableName, newDefer), 0)
    }

let private migrateBuildSchema (fromObjects : SchemaObjects) (toMeta : SchemaMeta) : OrderedSchemaOperation seq =
    seq {
        for KeyValue (objectKey, (objectName, obj)) in toMeta.Objects do
            let objRef = { Schema = Some toMeta.Name; Name = objectName }
            match obj with
            | OMTable tableMeta ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMTable oldTableMeta) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
                        yield (SORenameTable (oldObjRef, objectName), 0)
                    yield! migrateAlterTable objRef oldTableMeta tableMeta
                | _ ->
                    yield (SOCreateTable objRef, 0)
                    yield! migrateAlterTable objRef emptyTableMeta tableMeta
            | OMSequence ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMSequence) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
                        yield (SORenameSequence (oldObjRef, objectName), 0)
                | _ -> yield (SOCreateSequence objRef, 0)
            | OMConstraint (tableName, constraintType) ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMConstraint (oldTableName, oldConstraintType)) ->
                    let mutable oldIsGood = false
                    match (oldConstraintType, constraintType) with
                    | (CMCheck expr1, CMCheck expr2) ->
                        let expr1 = normalizeLocalExpr expr1.Value
                        let expr2 = normalizeLocalExpr expr2.Value
                        oldIsGood <- string expr1 = string expr2
                    | (CMForeignKey (ref1, f1, oldDefer), CMForeignKey (ref2, f2, newDefer)) ->
                        if ref1 = ref2 && f1 = f2 then
                            yield! migrateDeferrableConstraint objRef tableName oldDefer newDefer
                            oldIsGood <- true
                    | (CMPrimaryKey (p1, oldDefer), CMPrimaryKey (p2, newDefer)) ->
                        if p1 = p2 then
                            yield! migrateDeferrableConstraint objRef tableName oldDefer newDefer
                            oldIsGood <- true
                    | (CMUnique (u1, oldDefer), CMUnique (u2, newDefer)) ->
                        if u1 = u2 then
                            yield! migrateDeferrableConstraint objRef tableName oldDefer newDefer
                            oldIsGood <- true
                    | _ -> ()

                    if oldIsGood then
                        if oldObjectName <> objectName then
                            let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
                            yield (SORenameConstraint (oldObjRef, tableName, objectName), 0)
                    else
                        let isPrimaryKey =
                            match oldConstraintType with
                            | CMPrimaryKey _ -> 1
                            | _ -> 0
                        yield (SODropConstraint (objRef, oldTableName), isPrimaryKey)
                        yield (SOCreateConstraint (objRef, tableName, constraintType), 0)
                | _ -> yield (SOCreateConstraint (objRef, tableName, constraintType), 0)
            | OMIndex (tableName, index) ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMIndex (oldTableName, oldIndex)) ->
                    if tableName <> oldTableName || normalizeIndex oldIndex <> normalizeIndex index then
                        yield (SODropIndex objRef, 0)
                        yield (SOCreateIndex (objRef, tableName, index), 0)
                    else if oldObjectName <> objectName then
                        let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
                        yield (SORenameIndex (oldObjRef, objectName), 0)
                | _ -> yield (SOCreateIndex (objRef, tableName, index), 0)
            | OMFunction newOverloads ->
                match Map.tryFind objectKey fromObjects with
                | Some (oldObjectName, OMFunction oldOverloads) ->
                    if oldObjectName <> objectName then
                        let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
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
                        let oldObjRef = { Schema = Some toMeta.Name; Name = oldObjectName }
                        yield (SORenameTrigger (oldObjRef, tableName, objectName), 0)
                | _ -> yield (SOCreateTrigger (objRef, tableName, trigger), 0)

        for KeyValue (objectKey, (objectName, obj)) in fromObjects do
            let objRef = { Schema = Some toMeta.Name; Name = objectName }
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
        for extName in toMeta.Extensions do
            if not <| Set.contains extName fromMeta.Extensions then
                yield (SOCreateExtension extName, 0)
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
        for extName in fromMeta.Extensions do
            if not <| Set.contains extName toMeta.Extensions then
                yield (SODropExtension extName, 0)
    }

let planDatabaseMigration (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy (fun (op, order) -> (schemaOperationOrder op, order)) |> Seq.map fst |> Seq.toArray

let migrateDatabase (query : QueryConnection) (plan : MigrationPlan) (cancellationToken : CancellationToken) : Task=
    unitTask {
        if not <| Array.isEmpty plan then
            for action in plan do
                match action with
                | SOAlterTable (ref, ops) ->
                    if not (ref.Schema = None || ref.Schema = Some (SQLName "public")) then
                        for op in ops do
                            match op with
                            | TODropColumn name -> failwithf "Refusing to drop column %O.%O" ref name
                            | _ -> ()
                | SODropSequence ref ->
                    if not (ref.Schema = None || ref.Schema = Some (SQLName "public")) then
                        failwithf "Refusing to drop sequence %O" ref.Name
                | _ -> ()
                let! _ = query.ExecuteNonQuery (action.ToSQLString()) Map.empty cancellationToken
                ()
            // Clear prepared statements so that things don't break if e.g. database types have changed.
            query.Connection.UnprepareAll ()
    }
