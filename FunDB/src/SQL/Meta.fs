module FunWithFlags.FunDB.SQL.Meta

open System
open System.Linq
open Npgsql
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
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

// Convert string-cast and array-cast patterns into actual types so that we reverse lost type information.
// Don't reduce the expressions beyond that!
let normalizeLocalExpr : ValueExpr -> ValueExpr =
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
            | VEValue (VStringArray arr) as value -> omitArrayCast STString value typ
            | VEValue (VIntArray arr) as value -> omitArrayCast STInt value typ
            | VEValue (VDecimalArray arr) as value -> omitArrayCast STDecimal value typ
            | VEValue (VBoolArray arr) as value -> omitArrayCast STBool value typ
            | VEValue (VDateTimeArray arr) as value -> omitArrayCast STDateTime value typ
            | VEValue (VDateArray arr) as value -> omitArrayCast STDate value typ
            | VEValue (VRegclassArray arr) as value -> omitArrayCast STRegclass value typ
            | VEValue (VJsonArray arr) as value -> omitArrayCast STJson value typ

            | VEValue (VString str) as value ->
                match typ with
                | VTArray scalarType ->
                    match parse tokenizeArray stringArray str with
                    | Error msg -> raisef SQLMetaException "Cannot parse array: %s" msg
                    | Ok array ->
                        let runArrayCast (castFunc : string -> 'a option) : ValueArray<'a> =
                            let runCast (str : string) =
                                match castFunc str with
                                | Some v -> v
                                | None -> raisef SQLMetaException "Cannot cast array value to type %O: %s" scalarType str
                            mapValueArray runCast array

                        match findSimpleType scalarType with
                        | None -> VECast (value, typ)
                        | Some STString -> VEValue <| VStringArray array
                        | Some STInt -> VEValue (VIntArray <| runArrayCast tryIntInvariant)
                        | Some STDecimal -> VEValue (VDecimalArray <| runArrayCast tryDecimalInvariant)
                        | Some STBool -> VEValue (VBoolArray <| runArrayCast tryBool)
                        | Some STDateTime -> VEValue (VDateTimeArray <| runArrayCast tryDateTimeOffsetInvariant)
                        | Some STDate -> VEValue (VDateArray <| runArrayCast tryDateInvariant)
                        | Some STRegclass -> VEValue (VRegclassArray <| runArrayCast tryRegclass)
                        | Some STJson -> VEValue (VJsonArray <| runArrayCast tryJson)
                | VTScalar scalarType ->
                    let runCast castFunc =
                        match castFunc str with
                        | Some v -> v
                        | None -> raisef SQLMetaException "Cannot cast scalar value to type %O: %s" scalarType str

                    match findSimpleType scalarType with
                    | None -> VECast (value, typ)
                    | Some STString -> VEValue (VString str)
                    | Some STInt -> VEValue (VInt <| runCast tryIntInvariant)
                    | Some STDecimal -> VEValue (VDecimal <| runCast tryDecimalInvariant)
                    | Some STBool -> VEValue (VBool <| runCast tryBool)
                    | Some STDateTime -> VEValue (VDateTime <| runCast tryDateTimeOffsetInvariant)
                    | Some STDate -> VEValue (VDate <| runCast tryDateInvariant)
                    | Some STRegclass -> VEValue (VRegclass <| runCast tryRegclass)
                    | Some STJson -> VEValue (VJson <| runCast tryJson)
            | VEValue v as value ->
                match findSimpleValueType typ with
                | Some styp when valueSimpleType v = Some styp -> VEValue v
                | _ -> VECast (value, typ)
            | value -> VECast (value, typ)
        | VEValue _ as node -> node
        | VEColumn ({ table = None } as c) -> VEColumn c
        | VEColumn c -> raisef SQLMetaException "Invalid non-local reference in local expression: %O" c
        | VEPlaceholder i -> raisef SQLMetaException "Invalid placeholder in local expression: %i" i
        | VENot e -> VENot (traverse e)
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VEEqAny (e, arr) ->
            let newE = traverse e
            let rec expandArrayValue (expandOne : 'a -> Value) = function
                | AVValue v -> VEValue (expandOne v)
                | AVArray vals -> VEArray (Array.map (expandArrayValue expandOne) vals)
                | AVNull -> VEValue VNull
            let buildIn (expandOne : 'a -> Value) (arr : ArrayValue<'a>[]) =
                VEIn (e, Array.map (expandArrayValue expandOne) arr)
            match traverse arr with
            | VEValue (VStringArray arr) -> buildIn VString arr
            | VEValue (VIntArray arr) -> buildIn VInt arr
            | VEValue (VDecimalArray arr) -> buildIn VDecimal arr
            | VEValue (VBoolArray arr) -> buildIn VBool arr
            | VEValue (VDateTimeArray arr) -> buildIn VDateTime arr
            | VEValue (VDateArray arr) -> buildIn VDate arr
            | VEValue (VRegclassArray arr) -> buildIn VRegclass arr
            | VEValue (VJsonArray arr) -> buildIn VJson arr
            | newArr -> VEEqAny (newE, newArr)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
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
        | VEArray vals ->
            let newVals = Array.map traverse vals
            let tryValue = function
                | VEValue (VString v) -> Some (v :> obj, STString)
                | VEValue (VInt v) -> Some (v :> obj, STInt)
                | VEValue (VDecimal v) -> Some (v :> obj, STDecimal)
                | VEValue (VBool v) -> Some (v :> obj, STBool)
                | VEValue (VDateTime v) -> Some (v :> obj, STDateTime)
                | VEValue (VDate v) -> Some (v :> obj, STDate)
                | VEValue (VRegclass v) -> Some (v :> obj, STRegclass)
                | VEValue (VJson v) -> Some (v :> obj, STJson)
                | _ -> None
            match Seq.traverseOption tryValue newVals |> Option.map Seq.toArray with
            | None -> VEArray newVals
            | Some conVals when Array.isEmpty conVals -> VEValue (VStringArray [||])
            | Some conVals ->
                let (_, firstType) = conVals.[0]
                if Array.forall (fun (_, typ) -> typ = firstType) conVals then
                    let makeArray constr castFunc = VEValue <| constr (Array.map (fun (v, typ) -> AVValue (castFunc v)) conVals)
                    match firstType with
                    | STString -> makeArray VStringArray (fun x -> x :?> string)
                    | STInt -> makeArray VIntArray (fun x -> x :?> int)
                    | STDecimal -> makeArray VDecimalArray (fun x -> x :?> decimal)
                    | STBool -> makeArray VBoolArray (fun x -> x :?> bool)
                    | STDateTime -> makeArray VDateTimeArray (fun x -> x :?> DateTimeOffset)
                    | STDate -> makeArray VDateArray (fun x -> x :?> DateTimeOffset)
                    | STRegclass -> makeArray VRegclassArray (fun x -> x :?> SchemaObject)
                    | STJson -> makeArray VJsonArray (fun x -> x :?> JToken)
                else
                    VEArray newVals
        | VESubquery query -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
    traverse

let parseLocalExpr (raw : string) : ValueExpr =
    match parse tokenizeSQL valueExpr raw with
    | Ok expr -> normalizeLocalExpr expr
    | Error msg -> raisef SQLMetaException "Cannot parse local expression %s: %s" raw msg

let parseUdtName (str : string) =
    if str.StartsWith("_") then
        VTArray <| SQLRawString (str.Substring(1))
    else
        VTScalar <| SQLRawString str

let private makeColumnMeta (attr : Attribute) : ColumnName * ColumnMeta =
    try
        let columnType =
            if attr.pgType.typtype <> 'b' then
                raisef SQLMetaException "Unsupported non-base type: %s" attr.pgType.typname
            parseUdtName attr.pgType.typname
        let defaultExpr = attr.attrDefs |> Seq.first |> Option.map (fun def -> parseLocalExpr def.adsrc)
        let res =
            { columnType = columnType
              isNullable = not attr.attnotnull
              defaultExpr = defaultExpr
            }
        (SQLName attr.attname, res)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in column %s: %s" attr.attname e.Message

type private TableColumnIds = Map<ColumnNum, ColumnName>

[<NoComparison>]
type private PgTableMeta =
    { columns : TableColumnIds
      constraints : Constraint seq
      indexes : Index seq
    }

[<NoComparison>]
type private PgSchemaMeta =
    { objects : Map<SQLName, ObjectMeta>
      tables : Map<TableName, PgTableMeta>
    }
type private PgSchemas = Map<SchemaName, PgSchemaMeta>

let private makeUnconstrainedTableMeta (cl : Class) : TableName * (TableMeta * PgTableMeta) =
    try
        let filteredAttrs = cl.attributes |> Seq.filter (fun attr -> attr.attnum > 0s && not attr.attisdropped)
        let columnIds = filteredAttrs |> Seq.map (fun attr -> (attr.attnum, SQLName attr.attname)) |> Map.ofSeqUnique
        let columns = filteredAttrs |> Seq.map makeColumnMeta |> Map.ofSeqUnique
        let res = { columns = columns }
        let meta =
            { columns = columnIds
              constraints = cl.constraints
              indexes = cl.indexes
            }
        (SQLName cl.relname, (res, meta))
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in table %s: %s" cl.relname e.Message

let private makeSequenceMeta (cl : Class) : TableName =
    SQLName cl.relname

let private makeUnconstrainedSchemaMeta (ns : Namespace) : SchemaName * PgSchemaMeta =
    try
        let tableObjects = ns.classes |> Seq.filter (fun cl -> cl.relkind = 'r') |> Seq.map makeUnconstrainedTableMeta |> Map.ofSeqUnique
        let tablesMeta = tableObjects |> Map.map (fun name (table, meta) -> meta)
        let tables = tableObjects |> Map.map (fun name (table, meta) -> OMTable table)
        let sequences = ns.classes |> Seq.filter (fun cl -> cl.relkind = 'S') |> Seq.map (fun cl -> (makeSequenceMeta cl, OMSequence)) |> Map.ofSeqUnique
        let res =
            { objects = Map.unionUnique tables sequences
              tables = tablesMeta
            }
        (SQLName ns.nspname, res)
    with
    | :? SQLMetaException as e -> raisefWithInner SQLMetaException e.InnerException "Error in schema %s: %s" ns.nspname e.Message

// Two phases of resolution to resolve constraints which address columns ty their numbers.
type private Phase2Resolver (schemaIds : PgSchemas) =
    let makeConstraintMeta (tableName : TableName) (columnIds : TableColumnIds) (constr : Constraint) : (ConstraintName * ConstraintMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        let ret =
            match constr.contype with
            | 'c' ->
                Some <| CMCheck (parseLocalExpr constr.consrc)
            | 'f' ->
                let refSchema = SQLName constr.pgRelClass.pgNamespace.nspname
                let refName = SQLName constr.pgRelClass.relname
                let refTable = Map.find refName (Map.find refSchema schemaIds).tables
                let makeRefColumn (fromNum : ColumnNum) (toNum : ColumnNum) =
                    let fromName = makeLocalColumn fromNum
                    let toName = Map.find toNum refTable.columns
                    (fromName, toName)

                let tableRef = { schema = Some refSchema; name = refName }
                let cols = Seq.map2 makeRefColumn constr.conkey constr.confkey |> Seq.toArray
                Some <| CMForeignKey (tableRef, cols)
            | 'p' ->
                Some <| CMPrimaryKey (Array.map makeLocalColumn constr.conkey)
            | 'u' ->
                Some <| CMUnique (Array.map makeLocalColumn constr.conkey)
            | _ -> None

        Option.map (fun r -> (SQLName constr.conname, r)) ret

    let makeIndexMeta (tableName : TableName) (columnIds : TableColumnIds) (index : Index) : (IndexName * IndexMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        if index.indisunique || index.indisprimary then
            None
        else
            let cols = Array.map makeLocalColumn index.indkey
            let ret =
                { columns = cols
                } : IndexMeta
            Some (SQLName index.pgClass.relname, ret)

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

        let! namespaces =
            db.Namespaces.AsNoTracking().Where(fun ns -> not (ns.nspname.StartsWith("pg_")) && ns.nspname <> "information_schema")
                .Include("classes")
                .Include("classes.attributes")
                .Include("classes.attributes.attrDefs")
                .Include("classes.attributes.pgType")
                .Include("classes.constraints")
                .Include("classes.constraints.pgRelClass")
                .Include("classes.constraints.pgRelClass.pgNamespace")
                .Include("classes.indexes")
                .Include("classes.indexes.pgClass")
                .ToListAsync()

        let unconstrainedSchemas = namespaces |> Seq.map makeUnconstrainedSchemaMeta |> Map.ofSeqUnique
        let phase2 = Phase2Resolver(unconstrainedSchemas)
        let schemas = unconstrainedSchemas |> Map.map phase2.FinishSchemaMeta
        return { schemas = schemas }
    }