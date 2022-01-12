module FunWithFlags.FunDB.SQL.Meta

open System
open Npgsql
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Parsing
open FunWithFlags.FunDB.Exception
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

type SQLMetaException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        SQLMetaException (message, innerException, isUserException innerException)

    new (message : string) = SQLMetaException (message, null, true)

let private publicSchema = SQLName "public"

let private tryRegclass (str : string) : SchemaObject option =
    match parse tokenizeSQL schemaObject str with
    | Ok obj ->
        // "public" schema gets special handling because its mentions are omitted in PostgreSQL.
        let normalizedObj =
            if Option.isNone obj.Schema then
                { Schema = Some publicSchema; Name = obj.Name }
            else
                obj
        Some normalizedObj
    | Error msg -> None

let private makeTableFromName (schema : string) (table : string) : TableRef =
    { Schema = Some (SQLName schema)
      Name = SQLName table
    }

let private makeColumnFromName (schema : string) (table : string) (column : string) : ResolvedColumnRef =
    { Table = makeTableFromName schema table
      Name = SQLName column
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
    | STUuid -> runCast tryUuid >> VUuid

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
        | VEDistinct (a, b) -> VEDistinct (traverse a, traverse b)
        | VENotDistinct (a, b) -> VENotDistinct (traverse a, traverse b)
        | VEBinaryOp (a, op, b) -> VEBinaryOp (traverse a, op, traverse b)
        | VESimilarTo (e, pat) -> VESimilarTo (traverse e, traverse pat)
        | VENotSimilarTo (e, pat) -> VENotSimilarTo (traverse e, traverse pat)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEAny (e, op, arr) -> VEAny (traverse e, op, traverse arr)
        | VEAll (e, op, arr) -> VEAll (traverse e, op, traverse arr)
        | VEInQuery (e, query) -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
        | VENotInQuery (e, query) -> raisef SQLMetaException "Invalid subquery in local expression: %O" query
        | VEFunc (name, args) -> VEFunc (name, Array.map traverse args)
        | VESpecialFunc (name, args) -> VESpecialFunc (name, Array.map traverse args)
        | VEAggFunc (name, args) -> raisef SQLMetaException "Invalid aggregate function in local expression: %O" name
        | VECase (es, els) -> VECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
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
        let dataType =
            match attr.Type.TypType with
            | 'b' -> parseUdtName attr.Type.TypName
            | typ -> raisef SQLMetaException "Unsupported column data type type %c" typ
        let genExpr = attr.AttrDef |> Option.ofObj |> Option.map (fun def -> parseLocalExpr def.Source)
        let columnType =
            match attr.AttGenerated with
            | '\000' -> CTPlain { DefaultExpr = genExpr }
            | 's' ->
                match genExpr with
                | None -> raisef SQLMetaException "Stored generated column must have default expression"
                | Some e -> CTGeneratedStored e
            | typ -> raisef SQLMetaException "Unsupported GENERATED type %c" typ

        { DataType = dataType
          IsNullable = not attr.AttNotNull
          ColumnType = columnType
        }
    with
    | e -> raisefWithInner SQLMetaException e "In column %s" attr.AttName

type private TableColumnIds = Map<ColumnNum, ColumnName>

[<NoEquality; NoComparison>]
type private PgTableMeta =
    { Columns : TableColumnIds
      Constraints : Constraint seq
      Indexes : Index seq
    }

[<NoEquality; NoComparison>]
type private PgSchemaMeta =
    { Objects : Map<SQLName, MigrationKeysSet * ObjectMeta>
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

let private makeDeferrableConstraint (constr : Constraint) : DeferrableConstraint =
    if constr.ConDeferrable then
        DCDeferrable constr.ConDeferred
    else
        DCNotDeferrable

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
            | Regex @"WHEN \((.*)\) EXECUTE (?:PROCEDURE|FUNCTION)" [cond] -> parseLocalExpr cond |> String.comparable |> Some
            | _ -> None
        let functionName =
            { Schema = Some <| SQLName trigger.Function.Namespace.NspName
              Name = SQLName trigger.Function.ProName
            }
        let isConstraint =
            if isNull trigger.Constraint then
                None
            else
                Some <| makeDeferrableConstraint trigger.Constraint
        let def =
            { IsConstraint = isConstraint
              Order = order
              Events = events |> Array.ofSeq
              Mode = mode
              Condition = condition
              FunctionName = functionName
              FunctionArgs = [||]
            }
        (SQLName trigger.TgName, def)
    with
    | e -> raisefWithInner SQLMetaException e "In trigger %s" trigger.TgName

let private makeUnconstrainedTableMeta (cl : Class) : TableName * (Map<SQLName, ObjectMeta> * TableMeta * PgTableMeta) =
    try
        let columnIds = cl.Attributes |> Seq.ofObj |> Seq.map (fun attr -> (attr.AttNum, SQLName attr.AttName)) |> Map.ofSeqUnique
        let columns = cl.Attributes |> Seq.ofObj |> Seq.map (fun col -> (SQLName col.AttName, (Set.empty, makeColumnMeta col))) |> Map.ofSeqUnique
        let tableName = SQLName cl.RelName
        let makeTrigger trig =
            let (name, def) =  makeTriggerMeta columnIds trig
            (name, OMTrigger (tableName, def))
        let triggers = cl.Triggers |> Seq.ofObj |> Seq.map makeTrigger |> Map.ofSeqUnique
        let res = { Columns = columns }
        let meta =
            { Columns = columnIds
              Constraints = cl.Constraints |> Seq.ofObj
              Indexes = cl.Indexes |> Seq.ofObj
            }
        (tableName, (triggers, res, meta))
    with
    | e -> raisefWithInner SQLMetaException e "In table %s" cl.RelName

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
    | e -> raisefWithInner SQLMetaException e "In function %s" proc.ProName

let private tagName (name : SQLName) a = (name, (Set.empty, a))

let private makeUnconstrainedSchemaMeta (ns : Namespace) : SchemaName * PgSchemaMeta =
    try
        let tableObjects = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'r') |> Seq.map makeUnconstrainedTableMeta |> Map
        let tablesMeta = tableObjects |> Map.map (fun name (triggers, table, meta) -> meta)
        let tables = tableObjects |> Map.mapWithKeys (fun name (triggers, table, meta) -> (name, (Set.empty, OMTable table)))
        let triggers = tableObjects |> Map.toSeq |> Seq.map (fun (name, (triggers, table, meta)) -> triggers) |> Seq.fold Map.unionUnique Map.empty |> Map.mapWithKeys tagName
        let sequences = ns.Classes |> Seq.filter (fun cl -> cl.RelKind = 'S') |> Seq.map (makeSequenceMeta >> uncurry tagName) |> Map.ofSeqUnique
        let functions = ns.Procs |> Seq.ofObj |> Seq.map makeFunctionMeta |> Map.ofSeqWith (fun name -> Map.unionUnique) |> Map.mapWithKeys (fun name overloads -> (name, (Set.empty, OMFunction overloads)))
        let res =
            { Objects = List.fold Map.unionUnique Map.empty [tables; sequences; triggers; functions]
              Tables = tablesMeta
            }
        (SQLName ns.NspName, res)
    with
    | e -> raisefWithInner SQLMetaException e "In schema %s" ns.NspName

// Two phases of resolution to resolve constraints which address columns ty their numbers.
type private Phase2Resolver (schemaIds : PgSchemas) =
    let makeForeignKeyMeta (columnIds : TableColumnIds) (constr : Constraint) : ForeignKeyMeta =
        let refSchema = SQLName constr.FRelClass.Namespace.NspName
        let refName = SQLName constr.FRelClass.RelName
        let refTable = Map.find refName (Map.find refSchema schemaIds).Tables
        let makeRefColumn (fromNum : ColumnNum) (toNum : ColumnNum) =
            let fromName = Map.find fromNum columnIds
            let toName = Map.find toNum refTable.Columns
            (fromName, toName)

        let tableRef = { Schema = Some refSchema; Name = refName }
        let cols = Seq.map2 makeRefColumn constr.ConKey constr.ConFKey |> Seq.toArray

        let parseAction (c : Nullable<char>) =
            match c.GetValueOrDefault() with
            | '\x00'
            | 'a' -> DANoAction
            | 'r' -> DARestrict
            | 'c' -> DACascade
            | 'n' -> DASetNull
            | 'd' -> DASetDefault
            | c -> failwithf "Unknown foreign key action code: %c" c

        { ToTable = tableRef
          Columns = cols
          OnUpdate = parseAction constr.ConFUpdType
          OnDelete = parseAction constr.ConFDelType
          Defer = makeDeferrableConstraint constr
        }

    let makeConstraintMeta (tableName : TableName) (columnIds : TableColumnIds) (constr : Constraint) : (ConstraintName * ConstraintMeta) option =
        let makeLocalColumn (num : ColumnNum) = Map.find num columnIds
        let ret =
            match constr.ConType with
            | 'c' ->
                parseLocalExpr constr.Source |> String.comparable |> CMCheck |> Some
            | 'f' ->
                Some <| CMForeignKey (makeForeignKeyMeta columnIds constr)
            | 'p' ->
                Some <| CMPrimaryKey (Array.map makeLocalColumn constr.ConKey, makeDeferrableConstraint constr)
            | 'u' ->
                Some <| CMUnique (Array.map makeLocalColumn constr.ConKey, makeDeferrableConstraint constr)
            | _ -> None

        Option.map (fun r -> (SQLName constr.ConName, r)) ret

    let makeIndexMeta (columnIds : TableColumnIds) (index : Index) : (IndexName * IndexMeta) =
        let expressions =
            if isNull index.ExprsSource then
                [||]
            else
                match parse tokenizeSQL valueExprList index.ExprsSource with
                | Error msg -> raisef SQLMetaException "Cannot parse index expressions: %s" msg
                | Ok exprs -> Array.map castLocalExpr exprs

        let canOrder = index.Class.Am.CanOrder.Value

        let mutable exprI = 0

        let makeKey (i : int) =
            let num = index.IndKey.[i]
            if num <> 0s then
                IKColumn <| Map.find num columnIds
            else
                let ret = expressions.[exprI]
                exprI <- exprI + 1
                IKExpression <| String.comparable ret

        let makeColumn (i : int) =
            let key = makeKey i
            let opClass = index.Classes.[i] |> Option.ofObj |> Option.map SQLName
            let options = index.IndOption.[i]
            let order =
                if not canOrder then
                    None
                else
                    if options.HasFlag IndexOptions.Desc then
                        Some Desc
                    else
                        Some Asc
            let nullsOrder =
                if not canOrder then
                    None
                else
                    if options.HasFlag IndexOptions.NullsFirst then
                        Some NullsFirst
                    else
                        Some NullsLast
            { Key = key
              OpClass = opClass
              Order = order
              Nulls = nullsOrder
            }

        let pred =
            if isNull index.PredSource then
                None
            else
                parseLocalExpr index.PredSource |> String.comparable |> Some

        let columns = seq { for i in 0 .. int index.IndNKeyAtts - 1 -> makeColumn i } |> Seq.toArray
        let includedColumns = seq { for i in int index.IndNKeyAtts .. index.IndKey.Length - 1 -> makeKey i } |> Seq.toArray
        let ret =
            { Columns = columns
              IncludedColumns = includedColumns
              IsUnique = index.IndIsUnique
              Predicate = pred
              AccessMethod = SQLName index.Class.Am.AmName
            } : IndexMeta
        (SQLName index.Class.RelName, ret)

    let finishSchemaMeta (schemaName : SchemaName) (schema : PgSchemaMeta) : SchemaMeta =
        let makeConstraints (tableName : TableName, table : PgTableMeta) =
            let constraints =
                table.Constraints
                |> Seq.mapMaybe (makeConstraintMeta tableName table.Columns)
                |> Seq.map ((fun (constrName, constr) -> (constrName, OMConstraint (tableName, constr))) >> uncurry tagName)
            let indexes =
                table.Indexes
                |> Seq.map (makeIndexMeta table.Columns)
                |> Seq.map ((fun (indexName, index) -> (indexName, OMIndex (tableName, index))) >> uncurry tagName)
            Seq.append constraints indexes
        let newObjects = schema.Tables |> Map.toSeq |> Seq.collect makeConstraints |> Map.ofSeqUnique
        { Objects = Map.unionUnique schema.Objects newObjects
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

let private getExtensions (ns : Namespace) : ExtensionName seq =
    let getExt (ext : Extension) =
        if ns.NspName <> "public" then
            failwith "Extensions not in 'public' are not supported"
        SQLName ext.ExtName
    ns.Extensions |> Seq.map getExt

let buildDatabaseMeta (transaction : NpgsqlTransaction) (cancellationToken : CancellationToken) : Task<DatabaseMeta> =
    task {
        use db = createPgCatalogContext transaction
        let! namespaces = db.GetObjects(cancellationToken)

        let unconstrainedSchemas = namespaces |> Seq.map makeUnconstrainedSchemaMeta |> Map.ofSeqUnique
        let phase2 = Phase2Resolver(unconstrainedSchemas)
        let schemas = unconstrainedSchemas |> Map.mapWithKeys (fun name meta -> (name, (Set.empty, phase2.FinishSchemaMeta name meta)))
        let extensions = namespaces |> Seq.collect getExtensions |> Set.ofSeq
        return
            { Schemas = schemas
              Extensions = extensions
            }
    }
