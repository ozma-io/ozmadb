module FunWithFlags.FunDB.SQL.Meta

open Npgsql
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Parsing
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.SQL.Lex
open FunWithFlags.FunDB.SQL.Parse
open FunWithFlags.FunDB.SQL.Array.Lex
open FunWithFlags.FunDB.SQL.Array.Parse
open FunWithFlags.FunDBSchema.PgCatalog

type ColumnNum = int16

type SQLMetaException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = SQLMetaException (message, null)

let private publicSchema = SQLName "public"

let private tryRegclass (str : string) : SchemaObject option =
    match parse tokenizeSQL schemaObject str with
    | Ok obj ->
        // "public" schema gets special handling because its mentions are omitted in PostgreSQL.
        let normalizedObj =
            if Option.isNone obj.schema then
                { schema = Some publicSchema; name = obj.name }
            else
                obj
        Some normalizedObj
    | Error msg -> None

let private makeTableFromName (schema : string) (table : string) : TableRef =
    { schema = Some (SQLName schema)
      name = SQLName table
    }

let private makeColumnFromName (schema : string) (table : string) (column : string) : ResolvedColumnRef =
    { table = makeTableFromName schema table
      name = SQLName column
    }

let private runCast castFunc str =
    match castFunc str with
    | Some v -> v
    | None -> raisef SQLMetaException "Cannot cast scalar value: %s" str

let private parseToSimpleType : SimpleType -> (string -> Value) = function
    | STString -> VString
    | STInt -> runCast tryIntInvariant >> VInt
    | STBigInt -> runCast tryInt64Invariant >> VBigInt
    | STDecimal -> runCast tryDecimalInvariant >> VDecimal
    | STBool -> runCast tryBool >> VBool
    | STDateTime -> runCast trySqlDateTime >> VDateTime
    | STDate -> runCast trySqlDate >> VDate
    | STInterval -> runCast trySqlInterval >> VInterval
    | STRegclass -> runCast tryRegclass >> VRegclass
    | STJson -> runCast tryJson >> VJson

// Convert string-cast patterns into actual values so that we reverse lost type information.
// Don't reduce the expressions beyond that!
let private castLocalExpr : ValueExpr -> ValueExpr =
    let omitArrayCast (valType : SimpleType) (value : ValueExpr) (typ : DBValueType) =
        match typ with
        | VTArray scalarType ->
            match findSimpleType scalarType with
            | Some dbType when valType = dbType -> value
            | _ -> VECast (value, typ)
        | _ -> VECast (value, typ)
    let rec traverse = function
        | VECast (v, typ) ->
            match traverse v with
            | VEValue (VString str) as value ->
                match typ with
                | VTArray scalarType ->
                    match parse tokenizeArray stringArray str with
                    | Error msg -> raisef SQLMetaException "Cannot parse array: %s" msg
                    | Ok array ->
                        match findSimpleType scalarType with
                        | None -> VECast (normalizeArray VString array, typ)
                        | Some typ -> normalizeArray (parseToSimpleType typ) array
                | VTScalar scalarType ->
                    let runCast castFunc =
                        match castFunc str with
                        | Some v -> v
                        | None -> raisef SQLMetaException "Cannot cast scalar value to type %O: %s" scalarType str

                    match findSimpleType scalarType with
                    | None -> VECast (value, typ)
                    | Some typ -> VEValue (parseToSimpleType typ str)
            | value -> VECast (value, typ)
        | VEValue _ as node -> node
        | VEColumn _ as node -> node
        | VEPlaceholder i -> raisef SQLMetaException "Invalid placeholder in local expression: %i" i
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
        | VESimilarTo (e, pat) -> VESimilarTo (traverse e, traverse pat)
        | VENotSimilarTo (e, pat) -> VENotSimilarTo (traverse e, traverse pat)
        | VEMatchRegex (e, pat) -> VEMatchRegex (traverse e, traverse pat)
        | VEMatchRegexCI (e, pat) -> VEMatchRegexCI (traverse e, traverse pat)
        | VENotMatchRegex (e, pat) -> VENotMatchRegex (traverse e, traverse pat)
        | VENotMatchRegexCI (e, pat) -> VENotMatchRegexCI (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEInQuery (e, query) -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
        | VENotInQuery (e, query) -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> raisef SQLMetaException "Invalid aggregate function in local expression: %O" name
        | VECase (es, els) -> VECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
        | VECoalesce vals -> VECoalesce (Array.map traverse vals)
        | VEJsonArrow (a, b) -> VEJsonArrow (traverse a, traverse b)
        | VEJsonTextArrow (a, b) -> VEJsonTextArrow (traverse a, traverse b)
        | VEPlus (a, b) -> VEPlus (traverse a, traverse b)
        | VEMinus (a, b) -> VEMinus (traverse a, traverse b)
        | VEMultiply (a, b) -> VEMultiply (traverse a, traverse b)
        | VEDivide (a, b) -> VEDivide (traverse a, traverse b)
        | VEArray vals -> VEArray (Array.map traverse vals)
        | VESubquery query -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
    traverse

let parseLocalExpr (raw : string) : ValueExpr =
    match parse tokenizeSQL valueExpr raw with
    | Ok expr -> castLocalExpr expr
    | Error msg -> raisef SQLMetaException "Cannot parse local expression %s: %s" raw msg

let parseUdtName (str : string) : DBValueType =
    if str.StartsWith("_") then
        VTArray <| SQLRawString (str.Substring(1))
    else
        VTScalar <| SQLRawString str

let private makeColumnMeta (attr : Attribute) : ColumnMeta =
    try
        let columnType =
            if attr.Type.TypType <> 'b' then
                raisef SQLMetaException "Unsupported non-base type: %s" attr.Type.TypName
            parseUdtName attr.Type.TypName
        let defaultExpr = attr.AttrDef |> Option.ofNull |> Option.map (fun def -> parseLocalExpr def.Source)
        { Name = SQLName attr.AttName
          ColumnType = columnType
          IsNullable = not attr.AttNotNull
          DefaultExpr = defaultExpr
        }
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in column %s: %s" attr.AttName e.Message

type private TableColumnIds = Map<ColumnNum, ColumnName>

[<NoEquality; NoComparison>]
type private PgTableMeta =
    { Columns : TableColumnIds
      Constraints : Constraint seq
      Indexes : Index seq
    }

[<NoEquality; NoComparison>]
type private PgSchemaMeta =
    { Objects : Map<MigrationKey, SQLName * ObjectMeta>
      Tables : Map<TableName, PgTableMeta>
    }
type private PgSchemas = Map<SchemaName, PgSchemaMeta>

// https://stackoverflow.com/questions/23634550/meanings-of-bits-in-trigger-type-field-tgtype-of-postgres-pg-trigger
let private triggerTypeRow = 1s <<< 0
let private triggerTypeBefore = 1s <<< 1
let private triggerTypeInsert = 1s <<< 2
let private triggerTypeDelete = 1s <<< 3
let private triggerTypeUpdate = 1s <<< 4
let private triggerTypeTruncate = 1s <<< 5
let private triggerTypeInstead = 1s <<< 6

let private makeTriggerMeta (columnIds : TableColumnIds) (trigger : Trigger) : TriggerName * TriggerDefinition =
    try
        if not (Array.isEmpty trigger.TgArgs) then
            raisef SQLMetaException "Arguments for trigger functions are not supported"
        let order =
            if trigger.TgType &&& triggerTypeBefore <> 0s then
                TOBefore
            else if trigger.TgType &&& triggerTypeInstead <> 0s then
                TOInsteadOf
            else
                TOAfter
        let mode =
            if trigger.TgType &&& triggerTypeRow <> 0s then
                TMEachRow
            else
                TMEachStatement
        let events =
            seq {
                if trigger.TgType &&& triggerTypeInsert <> 0s then
                    yield TEInsert
                if trigger.TgType &&& triggerTypeDelete <> 0s then
                    yield TEDelete
                if trigger.TgType &&& triggerTypeUpdate <> 0s then
                    let columns = trigger.TgAttr |> Array.map (fun num -> Map.find num columnIds)
                    if Array.isEmpty columns then
                        yield TEUpdate None
                    else
                        yield TEUpdate (Some columns)
                if trigger.TgType &&& triggerTypeTruncate <> 0s then
                    yield TETruncate
            }
        let condition =
            // FIXME: Any better way???
            // See https://postgrespro.com/list/thread-id/1558141
            match trigger.Source with
            | Regex @"WHEN \((.*)\) EXECUTE (?:PROCEDURE|FUNCTION)" [cond] -> Some <| parseLocalExpr cond
            | _ -> None
        let functionName =
            { schema = Some <| SQLName trigger.Function.Namespace.NspName
              name = SQLName trigger.Function.ProName
            }
        let def =
            { IsConstraint = trigger.TgConstraint.HasValue
              Order = order
              Events = events |> Array.ofSeq
              Mode = mode
              Condition = condition
              FunctionName = functionName
              FunctionArgs = [||]
            }
        (SQLName trigger.TgName, def)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in trigger %s: %s" trigger.TgName e.Message

let private makeUnconstrainedTableMeta (cl : Class) : TableName * (Map<SQLName, ObjectMeta> * TableMeta * PgTableMeta) =
    try
        let columnIds = cl.Attributes |> Seq.map (fun attr -> (attr.AttNum, SQLName attr.AttName)) |> Map.ofSeqUnique
        let columns = cl.Attributes |> Seq.map (makeColumnMeta >> (fun col -> (col.Name.ToString(), col))) |> Map.ofSeqUnique
        let tableName = SQLName cl.RelName
        let makeTrigger trig =
            let (name, def) =  makeTriggerMeta columnIds trig
            (name, OMTrigger (tableName, def))
        let triggers = cl.Triggers |> Seq.map makeTrigger |> Map.ofSeqUnique
        let res = { Columns = columns }
        let meta =
            { Columns = columnIds
              Constraints = cl.Constraints
              Indexes = cl.Indexes
            }
        (tableName, (triggers, res, meta))
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in table %s: %s" cl.RelName e.Message

let private makeSequenceMeta (cl : Class) : SequenceName * ObjectMeta =
    (SQLName cl.RelName, OMSequence)

let private makeFunctionMeta (proc : Proc) : FunctionName * Map<FunctionSignature, FunctionDefinition> =
    try
        if proc.ProNArgs <> 0s then
            raisef SQLMetaException "Function with arguments is not supported"
        if proc.ProRetSet then
            raisef SQLMetaException "Function which return tables are not supported"
        let behaviour =
            match proc.ProVolatile with
            | 'i' -> FBImmutable
            | 's' -> FBStable
            | 'v' -> FBVolatile
            | _ -> raisef SQLMetaException "Unknown volatile specifier %c" proc.ProVolatile

        let signature = [||]
        let def =
            { Arguments = [||]
              ReturnValue = FRValue <| SQLRawString proc.RetType.TypName
              Behaviour = behaviour
              Language = SQLName proc.Language.LanName
              Definition = proc.ProSrc
            }
        (SQLName proc.ProName, Map.singleton signature def)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in function %s: %s" proc.ProName e.Message

let private tagName (name : SQLName) a = (name.ToString(), (name, a))

let private makeUnconstrainedSchemaMeta (ns : Namespace) : SchemaName * PgSchemaMeta =
    try
        let tableObjects = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'r') |> Seq.map makeUnconstrainedTableMeta |> Map
        let tablesMeta = tableObjects |> Map.map (fun name (triggers, table, meta) -> meta)
        let tables = tableObjects |> Map.mapWithKeys (fun name (triggers, table, meta) -> (name.ToString(), (name, OMTable table)))
        let triggers = tableObjects |> Map.toSeq |> Seq.map (fun (name, (triggers, table, meta)) -> triggers) |> Seq.fold Map.unionUnique Map.empty |> Map.mapWithKeys tagName
        let sequences = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'S') |> Seq.map (makeSequenceMeta >> uncurry tagName) |> Map.ofSeqUnique
        let functions = ns.Procs |> Seq.map makeFunctionMeta |> Map.ofSeqWith (fun name -> Map.unionUnique) |> Map.mapWithKeys (fun name overloads -> (name.ToString(), (name, OMFunction overloads)))
        let res =
            { Objects = List.fold Map.unionUnique Map.empty [tables; sequences; triggers; functions]
              Tables = tablesMeta
            }
        (SQLName ns.NspName, res)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in schema %s: %s" ns.NspName e.Message

let private makeDeferrableConstraint (constr : Constraint) : DeferrableConstraint =
    { Deferrable = constr.ConDeferrable
      InitiallyDeferred = constr.ConDeferred
    }

// Two phases of resolution to resolve constraints which address columns ty their numbers.
type private Phase2Resolver (schemaIds : PgSchemas) =
    let makeConstraintMeta (tableName : TableName) (columnIds : TableColumnIds) (constr : Constraint) : (ConstraintName * ConstraintMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        let ret =
            match constr.ConType with
            | 'c' ->
                Some <| CMCheck (parseLocalExpr constr.Source)
            | 'f' ->
                let refSchema = SQLName constr.FRelClass.Namespace.NspName
                let refName = SQLName constr.FRelClass.RelName
                let refTable = Map.find refName (Map.find refSchema schemaIds).Tables
                let makeRefColumn (fromNum : ColumnNum) (toNum : ColumnNum) =
                    let fromName = makeLocalColumn fromNum
                    let toName = Map.find toNum refTable.Columns
                    (fromName, toName)

                let tableRef = { schema = Some refSchema; name = refName }
                let cols = Seq.map2 makeRefColumn constr.ConKey constr.ConFKey |> Seq.toArray
                Some <| CMForeignKey (tableRef, cols, makeDeferrableConstraint constr)
            | 'p' ->
                Some <| CMPrimaryKey (Array.map makeLocalColumn constr.ConKey, makeDeferrableConstraint constr)
            | 'u' ->
                Some <| CMUnique (Array.map makeLocalColumn constr.ConKey, makeDeferrableConstraint constr)
            | _ -> None

        Option.map (fun r -> (SQLName constr.ConName, r)) ret

    let makeIndexMeta (tableName : TableName) (columnIds : TableColumnIds) (index : Index) : (IndexName * IndexMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        if index.IndIsUnique || index.IndIsPrimary then
            None
        else
            let cols = Array.map makeLocalColumn index.IndKey
            let ret =
                { Columns = cols
                } : IndexMeta
            Some (SQLName index.Class.RelName, ret)

    let finishSchemaMeta (schemaName : SchemaName) (schema : PgSchemaMeta) : SchemaMeta =
        let makeConstraints (tableName : TableName, table : PgTableMeta) =
            let constraints =
                table.Constraints
                |> Seq.mapMaybe (makeConstraintMeta tableName table.Columns)
                |> Seq.map ((fun (constrName, constr) -> (constrName, OMConstraint (tableName, constr))) >> uncurry tagName)
            let indexes =
                table.Indexes
                |> Seq.mapMaybe (makeIndexMeta tableName table.Columns)
                |> Seq.map ((fun (indexName, index) -> (indexName, OMIndex (tableName, index))) >> uncurry tagName)
            Seq.append constraints indexes
        let newObjects = schema.Tables |> Map.toSeq |> Seq.collect makeConstraints |> Map.ofSeqUnique
        { Name = schemaName
          Objects = Map.unionUnique schema.Objects newObjects
        }

    member this.FinishSchemaMeta = finishSchemaMeta

let createPgCatalogContext (transaction : NpgsqlTransaction) =
        let dbOptions =
            (DbContextOptionsBuilder<PgCatalogContext> ())
                .UseNpgsql(transaction.Connection)
#if DEBUG
        use loggerFactory = LoggerFactory.Create(fun builder -> ignore <| builder.AddConsole())
        ignore <| dbOptions.UseLoggerFactory(loggerFactory)
#endif
        let db = new PgCatalogContext(dbOptions.Options)
        try
            ignore <| db.Database.UseTransaction(transaction)
        with
        | _ ->
            db.Dispose ()
            reraise ()
        db

let buildDatabaseMeta (transaction : NpgsqlTransaction) (cancellationToken : CancellationToken) : Task<DatabaseMeta> =
    task {
        use db = createPgCatalogContext transaction
        let! namespaces = db.GetObjects(cancellationToken)

        let unconstrainedSchemas = namespaces |> Seq.map makeUnconstrainedSchemaMeta |> Map.ofSeqUnique
        let phase2 = Phase2Resolver(unconstrainedSchemas)
        let schemas = unconstrainedSchemas |> Map.mapWithKeys (fun name meta -> (name.ToString(), phase2.FinishSchemaMeta name meta))
        return { Schemas = schemas }
    }
