module FunWithFlags.FunDB.SQL.Migration

open System.Threading
open System.Threading.Tasks
open FSharpPlus
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
        for KeyValue (objectName, (keys, obj)) in schemaMeta.Objects do
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
// I couldn't find any spec on how exactly does PostgreSQL converts values, so this all is based on experiments.
let private normalizeIn (constr : ValueExpr * BinaryOperator * ValueExpr -> ValueExpr) (e : ValueExpr) (op : BinaryOperator) (vals: ValueExpr[]) =
    if vals.Length = 1 then
        VEBinaryOp (e, op, vals.[0])
    else
        constr (e, op, VEArray vals)

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
        // PostgreSQL converts IN and NOT IN, but not ANY and ALL.
        | VEIn (e, vals) -> normalizeIn VEAny (traverse e) BOEq (Array.map traverse vals)
        | VENotIn (e, vals) -> normalizeIn VEAll (traverse e) BONotEq (Array.map traverse vals)
        | VEAny (e, op, arr) -> VEAny (traverse e, op, traverse arr)
        | VEAll (e, op, arr) -> VEAll (traverse e, op, traverse arr)
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

let private migrateColumnAttrs (name : ColumnName) (fromMeta : ColumnMeta) (toMeta : ColumnMeta) : TableOperation seq =
    seq {
        if fromMeta.DataType <> toMeta.DataType then
            yield TOAlterColumnType (name, toMeta.DataType)
        if fromMeta.IsNullable <> toMeta.IsNullable then
            yield TOAlterColumnNull (name, toMeta.IsNullable)
    }

let private buildKeyedObjects (objects : MigrationObjectsMap<'a>) : Map<string, Set<SQLName>> =
    let keyObjs = objects |> Map.keys |> Seq.map (fun name -> (string name, Set.singleton name)) |> Map.ofSeq
    let addKey keyObjs (name, (keys, o)) =
        let addMyKey keyObjs key = Map.addWith Set.union key (Set.singleton name) keyObjs
        keys |> Seq.fold addMyKey keyObjs
    objects |> Map.toSeq |> Seq.fold addKey keyObjs

let private matchMigrationObjects (isSameObject : 'a -> 'a -> bool) (createNew : SQLName -> 'a -> 'b seq) (modifyOld : SQLName -> 'a -> SQLName -> 'a -> 'b seq) (dropOld : SQLName -> 'a -> 'b seq) (fromObjects : MigrationObjectsMap<'a>) (toObjects : MigrationObjectsMap<'a>) : 'b seq =
    seq {
        let keyedFromObjects = buildKeyedObjects fromObjects
        let mutable currFromObjects = fromObjects
        for KeyValue (newObjectName, (newObjectKeys, newObject)) in toObjects do
            let tryPossibleObject oldObjectName =
                match Map.tryFind oldObjectName currFromObjects with
                | Some (oldObjectKeys, oldObject) when isSameObject oldObject newObject ->
                    currFromObjects <- Map.remove oldObjectName currFromObjects
                    Some <| modifyOld oldObjectName oldObject newObjectName newObject
                | _ -> None
            let tryPossibleKey oldKey =
                match Map.tryFind oldKey keyedFromObjects with
                | Some objectNames -> objectNames |> Seq.mapMaybe tryPossibleObject |> Seq.first
                | None -> None

            let possibleKeys = Seq.append (Seq.singleton <| string newObjectName) newObjectKeys
            match possibleKeys |> Seq.mapMaybe tryPossibleKey |> Seq.first with
            | None -> yield! createNew newObjectName newObject
            | Some existing -> yield! existing

        for KeyValue (oldObjectName, (oldObjectKeys, oldObject)) in currFromObjects do
            yield! dropOld oldObjectName oldObject
    }

let private migrateBuildTable (fromMeta : TableMeta) (toMeta : TableMeta) : (ColumnName * ColumnName)[] * TableOperation[] =
    let mutable renames = []

    let createNew columnName columnMeta = Seq.singleton <| TOCreateColumn (columnName, columnMeta)
    let modifyOld oldColumnName (oldColumnMeta : ColumnMeta) newColumnName (newColumnMeta : ColumnMeta) =
        seq {
            match (oldColumnMeta.ColumnType, newColumnMeta.ColumnType) with
            | (CTPlain oldPlain, CTPlain plain) ->
                if oldColumnName <> newColumnName then
                    renames <- (oldColumnName, newColumnName) :: renames
                if Option.map (normalizeLocalExpr >> string) oldPlain.DefaultExpr <> Option.map (normalizeLocalExpr >> string) plain.DefaultExpr then
                    yield TOAlterColumnDefault (newColumnName, plain.DefaultExpr)
                yield! migrateColumnAttrs newColumnName oldColumnMeta newColumnMeta
            | (CTGeneratedStored oldGenExpr, CTGeneratedStored genExpr) ->
                if oldColumnName <> newColumnName then
                    renames <- (oldColumnName, newColumnName) :: renames
                if string (normalizeLocalExpr oldGenExpr) <> string (normalizeLocalExpr genExpr) then
                    yield TODropColumn oldColumnName
                    yield TOCreateColumn (newColumnName, newColumnMeta)
                else
                    yield! migrateColumnAttrs newColumnName oldColumnMeta newColumnMeta
            | (_, _) ->
                yield TODropColumn oldColumnName
                yield TOCreateColumn (newColumnName, newColumnMeta)
        }
    let dropOld columnName columnMeta = Seq.singleton <| TODropColumn columnName

    let tableOps = matchMigrationObjects (fun _ _ -> true) createNew modifyOld dropOld fromMeta.Columns toMeta.Columns |> Seq.toArray
    (Array.ofList renames, tableOps)

let private migrateAlterTable (objRef : SchemaObject) (fromMeta : TableMeta) (toMeta : TableMeta) : OrderedSchemaOperation seq =
    seq {
        let (renames, tableOps) = migrateBuildTable fromMeta toMeta
        for (oldName, newName) in renames do
            yield (SORenameTableColumn (objRef, oldName, newName), 0)
        if not <| Array.isEmpty tableOps then
            yield (SOAlterTable (objRef, tableOps |> Array.sortBy tableOperationOrder), 0)
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

let private migrateBuildSchema (schemaName : SchemaName) (fromObjects : SchemaObjects) (toObjects : SchemaObjects) : OrderedSchemaOperation seq =
    let isSameObject oldObject newObject =
        match (oldObject, newObject) with
        | (OMTable _, OMTable _) -> true
        | (OMSequence, OMSequence) -> true
        | (OMConstraint (oldTableName, oldConstraintType), OMConstraint (newTableName, newConstraintType)) when oldTableName = newTableName ->
            // Compare everything except deferrable setting.
            match (oldConstraintType, newConstraintType) with
            | (CMCheck expr1, CMCheck expr2) ->
                let expr1 = normalizeLocalExpr expr1.Value
                let expr2 = normalizeLocalExpr expr2.Value
                string expr1 = string expr2
            | (CMForeignKey opts1, CMForeignKey opts2) ->
                opts1.Columns = opts2.Columns && opts1.OnDelete = opts2.OnDelete && opts1.OnUpdate = opts2.OnUpdate && opts1.ToTable = opts2.ToTable
            | (CMPrimaryKey (p1, oldDefer), CMPrimaryKey (p2, newDefer)) ->
                p1 = p2
            | (CMUnique (u1, oldDefer), CMUnique (u2, newDefer)) ->
                u1 = u2
            | _ -> false
        | (OMIndex (oldTableName, oldIndex), OMIndex (newTableName, newIndex)) ->
            oldTableName = newTableName && normalizeIndex oldIndex = normalizeIndex newIndex
        | (OMFunction _, OMFunction _) -> true
        | (OMTrigger (oldTableName, oldTrigger), OMTrigger (newTableName, newTrigger)) ->
            oldTableName = newTableName  && oldTrigger = newTrigger
        | _ -> false

    let createNew objectName objectMeta =
        let objRef = { Schema = Some schemaName; Name = objectName }
        match objectMeta with
        | OMTable tableMeta ->
            seq {
                let opts =
                    { Table = objRef
                      Temporary = None
                    }
                yield (SOCreateTable opts, 0)
                yield! migrateAlterTable objRef emptyTableMeta tableMeta
            }
        | OMSequence -> Seq.singleton <| (SOCreateSequence objRef, 0)
        | OMConstraint (tableName, constraintType) -> Seq.singleton (SOCreateConstraint (objRef, tableName, constraintType), 0)
        | OMIndex (tableName, index) -> Seq.singleton (SOCreateIndex (objRef, tableName, index), 0)
        | OMFunction newOverloads -> migrateOverloads objRef Map.empty newOverloads
        | OMTrigger (tableName, trigger) -> Seq.singleton (SOCreateTrigger (objRef, tableName, trigger), 0)

    let modifyOld oldObjectName (oldObjectMeta : ObjectMeta) newObjectName (newObjectMeta : ObjectMeta) =
        seq {
            let objRef = { Schema = Some schemaName; Name = newObjectName }
            match newObjectMeta with
            | OMTable newTableMeta ->
                match oldObjectMeta with
                | OMTable oldTableMeta ->
                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        yield (SORenameTable (oldObjRef, newObjectName), 0)
                    yield! migrateAlterTable objRef oldTableMeta newTableMeta
                | _ -> failwith "Impossible"
            | OMSequence ->
                match oldObjectMeta with
                | OMSequence ->
                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        yield (SORenameSequence (oldObjRef, newObjectName), 0)
                | _ -> failwith "Impossible"
            | OMConstraint (newTableName, newConstraintType) ->
                match oldObjectMeta with
                | OMConstraint (oldTableName, oldConstraintType) ->
                    match (oldConstraintType, newConstraintType) with
                    | (CMCheck expr1, CMCheck expr2) -> ()
                    | (CMForeignKey opts1, CMForeignKey opts2) ->
                        yield! migrateDeferrableConstraint objRef newTableName opts1.Defer opts2.Defer
                    | (CMPrimaryKey (p1, oldDefer), CMPrimaryKey (p2, newDefer)) ->
                        yield! migrateDeferrableConstraint objRef newTableName oldDefer newDefer
                    | (CMUnique (u1, oldDefer), CMUnique (u2, newDefer)) ->
                        yield! migrateDeferrableConstraint objRef newTableName oldDefer newDefer
                    | _ -> ()

                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        yield (SORenameConstraint (oldObjRef, newTableName, newObjectName), 0)
                | _ -> failwith "Impossible"
            | OMIndex (newTableName, newIndex) ->
                match oldObjectMeta with
                | OMIndex (oldTableName, oldIndex) ->
                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        yield (SORenameIndex (oldObjRef, newObjectName), 0)
                | _ -> failwith "Impossible"
            | OMFunction newOverloads ->
                match oldObjectMeta with
                | OMFunction oldOverloads ->
                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        for KeyValue (signature, _) in oldOverloads do
                            yield (SORenameFunction (oldObjRef, signature, newObjectName), 0)
                    yield! migrateOverloads objRef oldOverloads newOverloads
                | _ -> failwith "Impossible"
            | OMTrigger (newTableName, newTrigger) ->
                match oldObjectMeta with
                | OMTrigger (oldTableName, oldTrigger) ->
                    if oldObjectName <> newObjectName then
                        let oldObjRef = { Schema = Some schemaName; Name = oldObjectName }
                        yield (SORenameTrigger (oldObjRef, newTableName, newObjectName), 0)
                | _ -> failwith "Impossible"
        }

    let dropOld objectName objectMeta =
            let objRef = { Schema = Some schemaName; Name = objectName }
            match objectMeta with
            | OMTable tableMeta -> deleteBuildTable objRef tableMeta
            | OMSequence -> Seq.singleton (SODropSequence objRef, 0)
            | OMConstraint (tableName, constraintType) ->
                let isPrimaryKey =
                    match constraintType with
                    | CMPrimaryKey _ -> 1
                    | _ -> 0
                Seq.singleton (SODropConstraint (objRef, tableName), isPrimaryKey)
            | OMIndex _ -> Seq.singleton (SODropIndex objRef, 0)
            | OMFunction overloads -> overloads |> Map.keys |> Seq.map (fun signature -> (SODropFunction (objRef, signature), 0))
            | OMTrigger (tableName, _) -> Seq.singleton (SODropTrigger (objRef, tableName), 0)

    matchMigrationObjects isSameObject createNew modifyOld dropOld fromObjects toObjects

let private migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : OrderedSchemaOperation seq =
    seq {
        for extName in toMeta.Extensions do
            if not <| Set.contains extName fromMeta.Extensions then
                yield (SOCreateExtension extName, 0)

        for extName in fromMeta.Extensions do
            if not <| Set.contains extName toMeta.Extensions then
                yield (SODropExtension extName, 0)

        let createNew schemaName schemaMeta =
            seq {
                yield (SOCreateSchema schemaName, 0)
                yield! migrateBuildSchema schemaName Map.empty schemaMeta.Objects
            }
        let modifyOld oldSchemaName oldSchemaMeta newSchemaName newSchemaMeta =
            seq {
                if oldSchemaName <> newSchemaName then
                    yield (SORenameSchema (oldSchemaName, newSchemaName), 0)
                yield! migrateBuildSchema newSchemaName oldSchemaMeta.Objects newSchemaMeta.Objects
            }
        let dropOld schemaName schemaMeta = deleteBuildSchema schemaName schemaMeta

        yield! matchMigrationObjects (fun _ _ -> true) createNew modifyOld dropOld fromMeta.Schemas toMeta.Schemas
    }

let planDatabaseMigration (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy (fun (op, order) -> (schemaOperationOrder op, order)) |> Seq.map fst |> Seq.toArray

let migrateDatabase (query : QueryConnection) (plan : MigrationPlan) (cancellationToken : CancellationToken) : Task=
    unitTask {
        if not <| Array.isEmpty plan then
            for action in plan do
                let! _ = query.ExecuteNonQuery (action.ToSQLString()) Map.empty cancellationToken
                ()
            // Clear prepared statements so that things don't break if e.g. database types have changed.
            query.Connection.UnprepareAll ()
    }
