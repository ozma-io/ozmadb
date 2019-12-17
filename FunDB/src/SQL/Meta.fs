module FunWithFlags.FunDB.SQL.Meta

open Npgsql
open System.Threading.Tasks
open System.Text.RegularExpressions
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Parsing
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
    | STDecimal -> runCast tryDecimalInvariant >> VDecimal
    | STBool -> runCast tryBool >> VBool
    | STDateTime -> runCast tryDateTimeInvariant >> VDateTime
    | STDate -> runCast tryDateInvariant >> VDate
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

let private makeColumnMeta (attr : Attribute) : ColumnName * ColumnMeta =
    try
        let columnType =
            if attr.Type.TypType <> 'b' then
                raisef SQLMetaException "Unsupported non-base type: %s" attr.Type.TypName
            parseUdtName attr.Type.TypName
        let defaultExpr = attr.AttrDefs |> Seq.first |> Option.map (fun def -> parseLocalExpr def.Source)
        let res =
            { columnType = columnType
              isNullable = not attr.AttNotNull
              defaultExpr = defaultExpr
            }
        (SQLName attr.AttName, res)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in column %s: %s" attr.AttName e.Message

type private TableColumnIds = Map<ColumnNum, ColumnName>

[<NoEquality; NoComparison>]
type private PgTableMeta =
    { columns : TableColumnIds
      constraints : Constraint seq
      indexes : Index seq
    }

[<NoEquality; NoComparison>]
type private PgSchemaMeta =
    { objects : Map<SQLName, ObjectMeta>
      tables : Map<TableName, PgTableMeta>
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
let private triggerWhenRegex = Regex "WHEN \\((.*)\\) EXECUTE PROCEDURE"

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
            // Any better way???
            // See https://postgrespro.com/list/thread-id/1558141
            let res = triggerWhenRegex.Match(trigger.Source)
            if res.Success then
                Some <| parseLocalExpr res.Groups.[1].Value
            else
                None
        let functionName =
            { schema = Some <| SQLName trigger.Function.Namespace.NspName
              name = SQLName trigger.Function.ProName
            }
        let def =
            { isConstraint = trigger.TgConstraint.HasValue
              order = order
              events = events |> Array.ofSeq
              mode = mode
              condition = condition
              functionName = functionName
              functionArgs = [||]
            }
        (SQLName trigger.TgName, def)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in trigger %s: %s" trigger.TgName e.Message

let private makeUnconstrainedTableMeta (cl : Class) : TableName * (Map<SQLName, ObjectMeta> * TableMeta * PgTableMeta) =
    try
        let columnIds = cl.Attributes |> Seq.map (fun attr -> (attr.AttNum, SQLName attr.AttName)) |> Map.ofSeqUnique
        let columns = cl.Attributes |> Seq.map makeColumnMeta |> Map.ofSeqUnique
        let tableName = SQLName cl.RelName
        let makeTrigger trig =
            let (name, def) =  makeTriggerMeta columnIds trig
            (name, OMTrigger (tableName, def))
        let triggers = cl.Triggers |> Seq.map makeTrigger |> Map.ofSeqUnique
        let res = { columns = columns }
        let meta =
            { columns = columnIds
              constraints = cl.Constraints
              indexes = cl.Indexes
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

        let signature = { items = [||] }
        let def =
            { arguments = [||]
              returnValue = FRValue <| SQLRawString proc.RetType.TypName
              behaviour = behaviour
              language = SQLName proc.Language.LanName
              definition = proc.ProSrc
            }
        (SQLName proc.ProName, Map.singleton signature def)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in function %s: %s" proc.ProName e.Message

let private makeUnconstrainedSchemaMeta (ns : Namespace) : SchemaName * PgSchemaMeta =
    try
        let tableObjects = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'r') |> Seq.map makeUnconstrainedTableMeta |> Map.ofSeqUnique
        let tablesMeta = tableObjects |> Map.map (fun name (triggers, table, meta) -> meta)
        let tables = tableObjects |> Map.map (fun name (triggers, table, meta) -> OMTable table)
        let triggers = tableObjects |> Map.toSeq |> Seq.map (fun (name, (triggers, table, meta)) -> triggers) |> Seq.fold Map.unionUnique Map.empty
        let sequences = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'S') |> Seq.map makeSequenceMeta |> Map.ofSeqUnique
        let functions = ns.Procs |> Seq.map makeFunctionMeta |> Map.ofSeqWith (fun name -> Map.unionUnique) |> Map.map (fun name overloads -> OMFunction overloads)
        let res =
            { objects = List.fold Map.unionUnique Map.empty [tables; sequences; triggers; functions]
              tables = tablesMeta
            }
        (SQLName ns.NspName, res)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in schema %s: %s" ns.NspName e.Message

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
                let refTable = Map.find refName (Map.find refSchema schemaIds).tables
                let makeRefColumn (fromNum : ColumnNum) (toNum : ColumnNum) =
                    let fromName = makeLocalColumn fromNum
                    let toName = Map.find toNum refTable.columns
                    (fromName, toName)

                let tableRef = { schema = Some refSchema; name = refName }
                let cols = Seq.map2 makeRefColumn constr.ConKey constr.ConFKey |> Seq.toArray
                Some <| CMForeignKey (tableRef, cols)
            | 'p' ->
                Some <| CMPrimaryKey (Array.map makeLocalColumn constr.ConKey)
            | 'u' ->
                Some <| CMUnique (Array.map makeLocalColumn constr.ConKey)
            | _ -> None

        Option.map (fun r -> (SQLName constr.ConName, r)) ret

    let makeIndexMeta (tableName : TableName) (columnIds : TableColumnIds) (index : Index) : (IndexName * IndexMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        if index.IndIsUnique || index.IndIsPrimary then
            None
        else
            let cols = Array.map makeLocalColumn index.IndKey
            let ret =
                { columns = cols
                } : IndexMeta
            Some (SQLName index.Class.RelName, ret)

    let finishSchemaMeta (schemaName : SchemaName) (schema : PgSchemaMeta) : SchemaMeta =
        let makeConstraints (tableName : TableName, table : PgTableMeta) =
            let constraints =
                table.constraints
                |> Seq.mapMaybe (makeConstraintMeta tableName table.columns)
                |> Seq.map (fun (constrRame, constr) -> (constrRame, OMConstraint (tableName, constr)))
            let indexes =
                table.indexes
                |> Seq.mapMaybe (makeIndexMeta tableName table.columns)
                |> Seq.map (fun (indexName, index) -> (indexName, OMIndex (tableName, index)))
            Seq.append constraints indexes
        let newObjects = schema.tables |> Map.toSeq |> Seq.collect makeConstraints |> Map.ofSeqUnique
        { objects = Map.unionUnique schema.objects newObjects
        }

    member this.FinishSchemaMeta = finishSchemaMeta

let buildDatabaseMeta (transaction : NpgsqlTransaction) : Task<DatabaseMeta> =
    task {
        let dbOptions =
            (DbContextOptionsBuilder<PgCatalogContext> ())
                .UseNpgsql(transaction.Connection)
#if DEBUG
        use loggerFactory = LoggerFactory.Create(fun builder -> ignore <| builder.AddConsole())
        ignore <| dbOptions.UseLoggerFactory(loggerFactory)
#endif
        use db = new PgCatalogContext(dbOptions.Options)
        ignore <| db.Database.UseTransaction(transaction)

        let! namespaces = db.GetObjects()

        let unconstrainedSchemas = namespaces |> Seq.map makeUnconstrainedSchemaMeta |> Map.ofSeqUnique
        let phase2 = Phase2Resolver(unconstrainedSchemas)
        let schemas = unconstrainedSchemas |> Map.map phase2.FinishSchemaMeta
        return { schemas = schemas }
    }