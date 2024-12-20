module OzmaDB.OzmaQL.Compile

open System
open FSharpPlus
open NodaTime
open Newtonsoft.Json.Linq

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization.Json
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Typecheck
open OzmaDB.OzmaQL.UsedReferences
open OzmaDB.Layout.Types
open OzmaDB.Attributes.Merge

module SQL = OzmaDB.SQL.Utils
module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Rename

type DomainIdColumn = int

type QueryCompileException(message: string, innerException: Exception, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: Exception) =
        QueryCompileException(message, innerException, isUserException innerException)

    new(message: string) = QueryCompileException(message, null, true)

// These are not the same domains as in Layout!
//
// Domains are a way to distinguish rows after set operations so that row types and IDs can be traced back.
// Domain is a map of assigned references to fields to each column.
// Domain key is cross product of all local domain ids i.e. map from domain namespace ids to local domain ids.
// Local domain ids get assigned to all sets in a set operation.
// Each row has a different domain key assigned, so half of rows may come from entity A and half from entity B.
// This way one can make use if IDs assigned to cells to reference and update them.
type DomainField =
    { Ref: ResolvedFieldRef
      // Needed for fast parsing of subentity names.
      RootEntity: ResolvedEntityRef
      // A field with assigned `IdColumn` of 42 will use id column "__id__42" and sub-entity column "__sub_entity__42"
      IdColumn: DomainIdColumn
      // If this field is not filtered according to user role.
      AsRoot: bool }

let private idDefault: DomainIdColumn = 0

type GenericDomain<'e> when 'e: comparison = Map<'e, DomainField>
type GlobalDomainId = int
type DomainNamespaceId = int
type LocalDomainId = int

type DomainsTree<'a> =
    | DSingle of GlobalDomainId * 'a
    | DMulti of DomainNamespaceId * Map<LocalDomainId, DomainsTree<'a>>

type GenericDomains<'e> when 'e: comparison = DomainsTree<GenericDomain<'e>>
type Domain = GenericDomain<FieldName>
type Domains = GenericDomains<FieldName>

let rec private mapDomainsTree (f: 'a -> 'b) : DomainsTree<'a> -> DomainsTree<'b> =
    function
    | DSingle(id, dom) -> DSingle(id, f dom)
    | DMulti(ns, doms) -> DMulti(ns, Map.map (fun name -> mapDomainsTree f) doms)

let private mapDomainsFields (f: 'e1 -> 'e2) = mapDomainsTree (Map.mapKeys f)

let private renameDomainFields (names: Map<'e1, 'e2>) =
    mapDomainsFields (fun k -> Map.find k names)

let private tryRenameDomainFields (names: Map<'e, 'e>) =
    mapDomainsFields (fun k -> Option.defaultValue k (Map.tryFind k names))

let rec private appendDomain (dom: GenericDomain<'e>) (doms: GenericDomains<'e>) : GenericDomains<'e> =
    match doms with
    | DSingle(id, d) -> DSingle(id, Map.unionUnique dom d)
    | DMulti(ns, subdomains) -> DMulti(ns, Map.map (fun name doms -> appendDomain dom doms) subdomains)

let rec private mergeDomains (doms1: GenericDomains<'e>) (doms2: GenericDomains<'e>) : GenericDomains<'e> =
    match (doms1, doms2) with
    | (DSingle(oldId, dom), doms)
    | (doms, DSingle(oldId, dom)) -> appendDomain dom doms
    | (DMulti(ns1, subdoms1), (DMulti(ns2, subdoms2))) when ns1 = ns2 ->
        DMulti(ns1, Map.unionWith mergeDomains subdoms1 subdoms2)
    | (DMulti(ns1, subdoms1), (DMulti _ as doms2)) ->
        DMulti(ns1, Map.map (fun name doms1 -> mergeDomains doms1 doms2) subdoms1)

type MetaType =
    | CMRowAttribute of AttributeName
    | CMArgAttribute of ArgumentName * AttributeName
    | CMDomain of DomainNamespaceId
    | CMId of DomainIdColumn
    | CMSubEntity of DomainIdColumn
    | CMMainId
    | CMMainSubEntity

type ColumnMetaType =
    | CCCellAttribute of AttributeName
    | CCPun

type GenericColumnType<'e> =
    | CTMeta of MetaType
    | CTColumnMeta of 'e * ColumnMetaType
    | CTColumn of 'e

let sqlFunView = compileName funView

let sqlFunIdType = SQL.VTScalar SQL.STInt

// FIXME: drop when we implement typecheck.
let private metaSQLType: MetaType -> SQL.SimpleValueType option =
    function
    | CMRowAttribute _ -> None
    | CMArgAttribute _ -> None
    | CMDomain _ -> Some(SQL.VTScalar SQL.STInt)
    | CMId _ -> Some sqlFunIdType
    | CMSubEntity _ -> Some(SQL.VTScalar SQL.STString)
    | CMMainId -> Some(SQL.VTScalar SQL.STInt)
    | CMMainSubEntity -> Some(SQL.VTScalar SQL.STInt)

let private mapColumnTypeFields (f: 'a -> 'b) : GenericColumnType<'a> -> GenericColumnType<'b> =
    function
    | CTMeta meta -> CTMeta meta
    | CTColumnMeta(name, meta) -> CTColumnMeta(f name, meta)
    | CTColumn name -> CTColumn(f name)

let private columnSQLType: GenericColumnType<'a> -> SQL.SimpleValueType option =
    function
    | CTMeta meta -> metaSQLType meta
    | CTColumnMeta(name, meta) -> None
    | CTColumn name -> None

let private compileSQLColumnType (typ: SQL.SimpleValueType) =
    typ |> SQL.mapValueType (fun x -> x.ToSQLRawString())

type ColumnType = GenericColumnType<FieldName>

type private NameReplacer() =
    let mutable lastIds: Map<string, int> = Map.empty
    let mutable existing: Map<string, SQL.SQLName> = Map.empty

    member this.ConvertName(name: string) =
        if String.length name <= SQL.sqlIdentifierLength then
            SQL.SQLName name
        else
            match Map.tryFind name existing with
            | Some n -> n
            | None ->
                let trimmed = String.truncate (SQL.sqlIdentifierLength - 12) name
                let num = Map.findWithDefault trimmed 0 lastIds + 1
                let newName = SQL.SQLName <| sprintf "%s%i" trimmed num
                lastIds <- Map.add trimmed num lastIds
                existing <- Map.add name newName existing
                newName

// We use temporary names when going up the expression tree, renaming them on the go.
// At the top levels (for example, in sub-expressions) all names are expected to be assigned (non-temporary).
type private TempFieldName =
    | TName of FieldName
    | TTemp of int

let private unionTempName (a: TempFieldName) (b: TempFieldName) : TempFieldName =
    match (a, b) with
    | (TTemp _, TName name)
    | (TName name, TTemp _) -> TName name
    | (a, b) -> a

let private getFinalName: TempFieldName -> FieldName =
    function
    | TName name -> name
    | TTemp i -> failwith "Encountered a temporary name where not expected"

type private TempColumnType = GenericColumnType<TempFieldName>
type private TempDomain = GenericDomain<TempFieldName>
type private TempDomains = GenericDomains<TempFieldName>

// These fields don't make sense for most of column types.
// TODO: Possibly better is to have a separate `ExtraColumnType` and friends with needed info attached.
[<NoEquality; NoComparison>]
type ColumnMetaInfo =
    { Mapping: BoundMapping option
      Dependency: DependencyStatus
      Internal: bool
      SingleRow: SQL.ValueExpr option
      // This is not fully filled at the compilation stage for now, but we get it after the dry run.
      ValueType: SQL.SimpleValueType option }

let emptyColumnMetaInfo =
    { Mapping = None
      Dependency = DSPerRow
      Internal = false
      SingleRow = None
      ValueType = None }

type private ResultMetaColumn =
    { Expression: SQL.ValueExpr
      Info: ColumnMetaInfo }

let private resultMetaColumn (expr: SQL.ValueExpr) : ResultMetaColumn =
    { Expression = expr
      Info = emptyColumnMetaInfo }

// Expressions are not fully assembled right away. Instead we build SELECTs with no selected columns
// and this record in `extra`. During first pass through a union expression we collect signatures
// of all sub-SELECTs and build an ordered list of all columns, including meta-. Then we do a second
// pass, filling missing columns in individual sub-SELECTs with NULLs. This ensures the number and order
// of columns is consistent in all subexpressions, and allows user to omit attributes in some
// sub-expressions,
[<NoEquality; NoComparison>]
type private SelectColumn =
    { Name: TempFieldName
      Column: SQL.ValueExpr
      Meta: Map<ColumnMetaType, ResultMetaColumn> }

type FromEntityInfo =
    { Ref: ResolvedEntityRef
      IsInner: bool
      AsRoot: bool
      Check: SQL.ValueExpr option }

type FromEntitiesMap = Map<SQL.TableName, FromEntityInfo>

type JoinKey =
    { Table: SQL.TableName
      Column: SQL.ColumnName
      ToRootEntity: ResolvedEntityRef
      AsRoot: bool }

type JoinPath =
    { RealEntity: ResolvedEntityRef // Real entity, may differ from `ToRootEntity`.
      Name: SQL.TableName }

type JoinTree =
    { Path: JoinPath; Nested: JoinPathsMap }

and JoinPathsMap = Map<JoinKey, JoinTree>

type JoinPathsPair = (JoinKey * JoinPath)

type JoinNamespace = OzmaQLName

let rootJoinNamespace = OzmaQLName "": JoinNamespace

type JoinPaths =
    { NextJoinId: int
      Map: JoinPathsMap
      Namespace: JoinNamespace }

let emptyJoinPaths =
    { NextJoinId = 0
      Map = Map.empty
      Namespace = rootJoinNamespace }

[<NoEquality; NoComparison>]
type SelectFromInfo =
    { Entities: FromEntitiesMap
      Joins: JoinPaths
      WhereWithoutSubentities: SQL.ValueExpr option }

type NoSelectFromInfo = NoSelectFromInfo

[<NoEquality; NoComparison>]
type UpdateFromInfo =
    { WhereWithoutSubentities: SQL.ValueExpr option }

[<NoEquality; NoComparison>]
type private HalfCompiledSingleSelect =
    { Domains: TempDomains
      MetaColumns: Map<MetaType, ResultMetaColumn>
      Columns: SelectColumn[]
      FromInfo: SelectFromInfo option }

[<NoEquality; NoComparison>]
type private SelectColumnSignature =
    { Name: TempFieldName
      Meta: Map<ColumnMetaType, ColumnMetaInfo> }

[<NoEquality; NoComparison>]
type private SelectSignature =
    { MetaColumns: Map<MetaType, ColumnMetaInfo>
      Columns: SelectColumnSignature[] }

type private GenericColumnInfo<'e> when 'e: comparison =
    { Type: GenericColumnType<'e>
      Info: ColumnMetaInfo }

[<NoEquality; NoComparison>]
type private GenericSelectInfo<'e> when 'e: comparison =
    { Domains: GenericDomains<'e>
      // PostgreSQL column length is limited to 63 bytes, so we store column types separately.
      Columns: GenericColumnInfo<'e>[] }

let private emptySelectInfo: GenericSelectInfo<'e> =
    { Domains = DSingle(0, Map.empty)
      Columns = [||] }

let private mapColumnInfoFields (f: 'e1 -> 'e2) (column: GenericColumnInfo<'e1>) : GenericColumnInfo<'e2> =
    { Type = mapColumnTypeFields f column.Type
      Info = column.Info }

let private mapSelectInfoFields (f: 'e1 -> 'e2) (select: GenericSelectInfo<'e1>) : GenericSelectInfo<'e2> =
    { Domains = mapDomainsFields f select.Domains
      Columns = Array.map (mapColumnInfoFields f) select.Columns }

type private ColumnInfo = GenericColumnInfo<FieldName>
type private TempColumnInfo = GenericColumnInfo<TempFieldName>

type private SelectInfo = GenericSelectInfo<FieldName>
type private TempSelectInfo = GenericSelectInfo<TempFieldName>

let private finalSelectInfo = mapSelectInfoFields getFinalName

type FlattenedDomains = Map<GlobalDomainId, Domain>

type private EntityAttributes =
    { Entity: Set<AttributeName>
      Fields: Set<FieldName * AttributeName> }

type private EntityAttributesMap = Map<SQL.TableName, EntityAttributes>

let private emptyEntityAttributes =
    { Entity = Set.empty
      Fields = Set.empty }
    : EntityAttributes

let private unionEntityAttributes (a: EntityAttributes) (b: EntityAttributes) =
    { Entity = Set.union a.Entity b.Entity
      Fields = Set.union a.Fields b.Fields }

let private unionEntityAttributesMap: EntityAttributesMap -> EntityAttributesMap -> EntityAttributesMap =
    Map.unionWith unionEntityAttributes

[<NoEquality; NoComparison>]
type private FromType =
    | FTEntity of GlobalDomainId * Domain // Domain ID is used for merging.
    | FTSubquery of SelectInfo

[<NoEquality; NoComparison>]
type private FromInfo =
    { FromType: FromType
      Entity: FromEntityInfo option
      MainId: SQL.ColumnName option
      MainSubEntity: SQL.ColumnName option
      Attributes: EntityAttributes }

type private FromMap = Map<SQL.TableName, FromInfo>

type private FromResult = { Tables: FromMap; Joins: JoinPaths }

type private JoinId = int

let private epochDateTime = Instant.FromUnixTimeSeconds(0)
let private epochDate = epochDateTime.InUtc().LocalDateTime.Date

let private defaultCompiledArgumentValue: FieldType<_> -> FieldValue =
    function
    | FTArray SFTString -> FStringArray [||]
    | FTArray SFTInt -> FIntArray [||]
    | FTArray SFTDecimal -> FDecimalArray [||]
    | FTArray SFTBool -> FBoolArray [||]
    | FTArray SFTDateTime -> FDateTimeArray [||]
    | FTArray SFTDate -> FDateArray [||]
    | FTArray SFTInterval -> FIntervalArray [||]
    | FTArray SFTJson -> FJsonArray [||]
    | FTArray SFTUserViewRef -> FUserViewRefArray [||]
    | FTArray SFTUuid -> FUuidArray [||]
    | FTArray(SFTReference(entityRef, opts)) -> FIntArray [||]
    | FTArray(SFTEnum vals) -> FStringArray [||]
    | FTScalar SFTString -> FString ""
    | FTScalar SFTInt -> FInt 0
    | FTScalar SFTDecimal -> FDecimal 0m
    | FTScalar SFTBool -> FBool false
    | FTScalar SFTDateTime -> FDateTime epochDateTime
    | FTScalar SFTDate -> FDate epochDate
    | FTScalar SFTInterval -> FInterval Period.Zero
    | FTScalar SFTJson -> FJson(ComparableJToken(JObject()))
    | FTScalar SFTUserViewRef -> FUserViewRef { Schema = None; Name = OzmaQLName "" }
    | FTScalar SFTUuid -> FUuid Guid.Empty
    | FTScalar(SFTReference(entityRef, opts)) -> FInt 0
    | FTScalar(SFTEnum vals) -> vals |> Seq.first |> Option.get |> FString

let defaultCompiledArgument (arg: CompiledArgument) : FieldValue =
    match arg.DefaultValue with
    | Some v -> v
    | None ->
        if arg.Optional then
            FNull
        else
            defaultCompiledArgumentValue arg.FieldType

let private subselectAttributes (info: SelectInfo) : EntityAttributes =
    let getAttribute info =
        match info.Type with
        | CTMeta(CMRowAttribute name) ->
            Some
                { emptyEntityAttributes with
                    Entity = Set.singleton name }
        | CTColumnMeta(colName, CCCellAttribute name) ->
            Some
                { emptyEntityAttributes with
                    Fields = Set.singleton (colName, name) }
        | _ -> None

    info.Columns
    |> Seq.mapMaybe getAttribute
    |> Seq.fold unionEntityAttributes emptyEntityAttributes

type CompiledColumnInfo =
    { Type: ColumnType
      Name: SQL.ColumnName
      Info: ColumnMetaInfo }

// Evaluation of column-wise or global attributes
type CompiledSingleRowExpr =
    { ConstColumns: (CompiledColumnInfo * SQL.ValueExpr)[]
      SingleRowColumns: (CompiledColumnInfo * SQL.ValueExpr)[] }

type CompiledPragmasMap = Map<SQL.ParameterName, SQL.Value>

[<NoEquality; NoComparison>]
type CompiledViewExpr =
    { Pragmas: CompiledPragmasMap
      SingleRowQuery: CompiledSingleRowExpr
      Query: Query<SQL.SelectExpr>
      UsedDatabase: FlatUsedDatabase
      Columns: CompiledColumnInfo[]
      Domains: Domains
      MainRootEntity: ResolvedEntityRef option
      FlattenedDomains: FlattenedDomains }

[<NoEquality; NoComparison>]
type CompiledCommandExpr =
    { Pragmas: CompiledPragmasMap
      Command: Query<SQL.DataExpr>
      UsedDatabase: FlatUsedDatabase }

let compileOrder: SortOrder -> SQL.SortOrder =
    function
    | Asc -> SQL.Asc
    | Desc -> SQL.Desc

let compileNullsOrder: NullsOrder -> SQL.NullsOrder =
    function
    | NullsFirst -> SQL.NullsFirst
    | NullsLast -> SQL.NullsLast

let private compileJoin: JoinType -> SQL.JoinType =
    function
    | Left -> SQL.Left
    | Right -> SQL.Right
    | Inner -> SQL.Inner
    | Outer -> SQL.Full

let private compileSetOp: SetOperation -> SQL.SetOperation =
    function
    | Union -> SQL.Union
    | Except -> SQL.Except
    | Intersect -> SQL.Intersect

let compileEntityRef (entityRef: EntityRef) : SQL.TableRef =
    { Schema = Option.map compileName entityRef.Schema
      Name = compileName entityRef.Name }

// We rename entity references to no-schema prefixed names. This way we guarantee uniqueness between automatically generated sub-entity SELECTs and actual entities.
// For example, `schema2.foo` inherits `schema1.foo`, and they are both used in the query. Without renaming, we would have:
// FROM schema1.foo .... (SELECT * FROM schema2.foo WHERE ...) as foo.
// and a conflict would ensue.
let renameResolvedEntityRef (entityRef: ResolvedEntityRef) : SQL.SQLName =
    SQL.SQLName
    <| sprintf "%s__%s" (string entityRef.Schema) (string entityRef.Name)

let compileRenamedResolvedEntityRef (entityRef: ResolvedEntityRef) : SQL.TableRef =
    { Schema = None
      Name = renameResolvedEntityRef entityRef }

let compileRenamedEntityRef (entityRef: EntityRef) : SQL.TableRef =
    match entityRef.Schema with
    | Some schemaName ->
        compileRenamedResolvedEntityRef
            { Schema = schemaName
              Name = entityRef.Name }
    | None ->
        { Schema = None
          Name = compileName entityRef.Name }

let compileResolvedEntityRef (entityRef: ResolvedEntityRef) : SQL.TableRef =
    { Schema = Some(compileName entityRef.Schema)
      Name = compileName entityRef.Name }

let compileFieldRef (fieldRef: FieldRef) : SQL.ColumnRef =
    { Table = Option.map compileEntityRef fieldRef.Entity
      Name = compileName fieldRef.Name }

let compileResolvedFieldRef (fieldRef: ResolvedFieldRef) : SQL.ColumnRef =
    { Table = Some <| compileResolvedEntityRef fieldRef.Entity
      Name = compileName fieldRef.Name }

// Be careful -- one shouldn't use these functions when compiling real field names, only references to sub-entities!
// That's because real fields use `ColumnName`s.
let compileRenamedFieldRef (fieldRef: FieldRef) : SQL.ColumnRef =
    { Table = Option.map compileRenamedEntityRef fieldRef.Entity
      Name = compileName fieldRef.Name }

let decompileTableRef (tableRef: SQL.TableRef) : EntityRef =
    { Schema = Option.map decompileName tableRef.Schema
      Name = decompileName tableRef.Name }

let compileAliasFromName (name: EntityName) : SQL.TableAlias =
    { Name = compileName name
      Columns = None }

let compileNameFromEntity (entityRef: ResolvedEntityRef) (pun: EntityName option) : SQL.TableName =
    match pun with
    | Some punName -> compileName punName
    | None -> renameResolvedEntityRef entityRef

let compileAliasFromEntity (entityRef: ResolvedEntityRef) (pun: EntityName option) : SQL.TableAlias =
    { Name = compileNameFromEntity entityRef pun
      Columns = None }

let private composeExhaustingIf
    (compileTag: 'tag -> SQL.ValueExpr)
    (options: ('tag * SQL.ValueExpr) array)
    : SQL.ValueExpr =
    if Array.isEmpty options then
        SQL.nullExpr
    else if Array.length options = 1 then
        let (tag, expr) = options.[0]
        expr
    else
        let last = Array.length options - 1
        let makeCase (tag, expr) = (compileTag tag, expr)
        let cases = options |> Seq.take last |> Seq.map makeCase |> Array.ofSeq
        let (lastTag, lastExpr) = options.[last]
        SQL.VECase(cases, Some lastExpr)

// We forcibly convert subentity fields to JSON format when they are used in regular expression context.
// In type expressions, however, we use them raw.
type private ReferenceContext =
    | RCExpr
    | RCTypeExpr

let makeCheckExprFor (subEntityColumn: SQL.ValueExpr) (entities: IEntityBits seq) : SQL.ValueExpr =
    let options =
        entities
        |> Seq.map (fun x -> x.TypeName |> SQL.VString |> SQL.VEValue)
        |> Seq.toArray

    if Array.isEmpty options then
        SQL.VEValue(SQL.VBool false)
    else if Array.length options = 1 then
        SQL.VEBinaryOp(subEntityColumn, SQL.BOEq, options.[0])
    else
        SQL.VEIn(subEntityColumn, options)

let makeCheckExpr
    (subEntityColumn: SQL.ValueExpr)
    (layout: ILayoutBits)
    (entityRef: ResolvedEntityRef)
    : SQL.ValueExpr option =
    match allPossibleEntities layout entityRef with
    // FIXME: return `option` instead
    | PEAny -> None
    | PEList entities -> Some <| makeCheckExprFor subEntityColumn (Seq.map snd entities)

let private compileEntityTag (subEntityColumn: SQL.ValueExpr) (entity: IEntityBits) =
    SQL.VEBinaryOp(subEntityColumn, SQL.BOEq, SQL.VEValue(SQL.VString entity.TypeName))

let private makeEntityCheckExpr
    (subEntityColumn: SQL.ValueExpr)
    (entity: ResolvedFromEntity)
    (layout: ILayoutBits)
    (entityRef: ResolvedEntityRef)
    : SQL.ValueExpr option =
    if entity.Only then
        let entity = layout.FindEntity entityRef |> Option.get

        if hasSubType entity then
            Some <| compileEntityTag subEntityColumn entity
        else
            None
    else
        makeCheckExpr subEntityColumn layout entityRef

let private makeSubEntityParseExprFor
    (layout: ILayoutBits)
    (subEntityColumn: SQL.ValueExpr)
    (entities: ResolvedEntityRef seq)
    : SQL.ValueExpr =
    let getName (ref: ResolvedEntityRef) =
        let entity = layout.FindEntity ref |> Option.get

        if entity.IsAbstract then
            None
        else
            let json = JToken.FromObject ref |> ComparableJToken |> SQL.VJson |> SQL.VEValue
            Some(entity, json)

    let options = entities |> Seq.mapMaybe getName |> Seq.toArray

    composeExhaustingIf (compileEntityTag subEntityColumn) options

type private ColumnPair = ColumnType * SQL.SelectedColumn

// This type is used internally in getResultEntry.
[<NoEquality; NoComparison>]
type private ResultColumn =
    { Domains: TempDomains option
      MetaColumns: Map<MetaType, ResultMetaColumn>
      Column: SelectColumn }

type private SelectFlags =
    { MainEntity: ResolvedEntityRef option
      IsTopLevel: bool
      MetaColumns: bool }

let private subSelectFlags: SelectFlags =
    { MainEntity = None
      IsTopLevel = false
      MetaColumns = false }

type RealEntityAnnotation =
    { RealEntity: ResolvedEntityRef
      FromPath: bool
      // `IsInner` is `true` when we can filter rows in outer query, not in the subquery. For example,
      // > foo LEFT JOIN bar
      // If we need an additional constraint on `foo`, we can add it to outer WHERE clause.
      // We cannot do the same for `bar` however, because missing rows will be "visible" as NULLs due to LEFT JOIN.
      IsInner: bool
      AsRoot: bool }

type NoEntityAnnotation = NoEntityAnnotation

type RealFieldAnnotation = { Name: FieldName }

let private fromToEntitiesMap: FromMap -> FromEntitiesMap =
    Map.mapMaybe (fun name info -> info.Entity)

type private CTEBindings = Map<SQL.TableName, SelectInfo>

type private UpdateRecCTEBindings = SelectSignature -> TempDomains -> CTEBindings

type ExprCompilationFlags =
    { ForceNoTableRef: bool
      ForceNoMaterialized: bool }

let emptyExprCompilationFlags =
    { ForceNoTableRef = false
      ForceNoMaterialized = false }

type private ExprContext =
    { CTEs: CTEBindings
      EntityAttributes: EntityAttributesMap
      Flags: ExprCompilationFlags
      JoinNamespace: JoinNamespace
      RefContext: ReferenceContext }

let private rootExprContext =
    { CTEs = Map.empty
      EntityAttributes = Map.empty
      Flags = emptyExprCompilationFlags
      JoinNamespace = rootJoinNamespace
      RefContext = RCExpr }

let private selectSignature (half: HalfCompiledSingleSelect) : SelectSignature =
    { MetaColumns = half.MetaColumns |> Map.map (fun name col -> col.Info)
      Columns =
        half.Columns
        |> Array.map (fun col ->
            { Name = col.Name
              Meta = col.Meta |> Map.map (fun name col -> col.Info) }) }

let private mergeSelectSignature
    (a: SelectSignature)
    (b: SelectSignature)
    : Map<TempFieldName, TempFieldName> * SelectSignature =
    let mutable renames = Map.empty

    let mergeOne (a: SelectColumnSignature) (b: SelectColumnSignature) =
        let newName = unionTempName a.Name b.Name

        if newName <> a.Name then
            renames <- Map.add a.Name newName renames
        elif newName <> b.Name then
            renames <- Map.add b.Name newName renames

        { Name = newName
          Meta = Map.unionWith (fun _ _ -> emptyColumnMetaInfo) a.Meta b.Meta }

    let ret =
        { MetaColumns = Map.unionWith (fun _ _ -> emptyColumnMetaInfo) a.MetaColumns b.MetaColumns
          Columns = Array.map2 mergeOne a.Columns b.Columns }

    (renames, ret)

// Should be in sync with `signatureColumns`. They are not the same function because `signatureColumnTypes` requires names,
// but `signatureColumns` doesn't.
let private signatureColumnTypes (sign: SelectSignature) : TempColumnInfo seq =
    seq {
        for KeyValue(metaName, metaInfo) in sign.MetaColumns do
            yield
                { Type = CTMeta metaName
                  Info = metaInfo }

        for col in sign.Columns do
            for KeyValue(metaName, metaInfo) in col.Meta do
                yield
                    { Type = CTColumnMeta(col.Name, metaName)
                      Info = metaInfo }

            yield
                { Type = CTColumn col.Name
                  Info = emptyColumnMetaInfo }
    }

// Used for deduplicating id columns.
// We use `init [entityName, fieldName, ...path]` as key (as the last component doesn't matter).
let rec private idKey (entityName: EntityName) (fieldName: FieldName) (path: FieldName[]) : string =
    Seq.append (Seq.singleton entityName) (Seq.take path.Length (Seq.append (Seq.singleton fieldName) path))
    |> Seq.map string
    |> String.concat "__"

let private infoFromSignature (domains: TempDomains) (signature: SelectSignature) : TempSelectInfo =
    { Columns = Array.ofSeq (signatureColumnTypes signature)
      Domains = domains }

let private makeJoinNode (layout: Layout) (joinKey: JoinKey) (join: JoinPath) (from: SQL.FromExpr) : SQL.FromExpr =
    let tableRef = { Schema = None; Name = joinKey.Table }: SQL.TableRef
    let toTableRef = { Schema = None; Name = join.Name }: SQL.TableRef
    let entity = layout.FindEntity join.RealEntity |> Option.get

    let fromColumn =
        SQL.VEColumn
            { Table = Some tableRef
              Name = joinKey.Column }

    let toColumn =
        SQL.VEColumn
            { Table = Some toTableRef
              Name = sqlFunId }

    let joinExpr = SQL.VEBinaryOp(fromColumn, SQL.BOEq, toColumn)
    let alias = { Name = join.Name; Columns = None }: SQL.TableAlias

    let ann =
        { RealEntity = join.RealEntity
          FromPath = true
          IsInner = false
          AsRoot = joinKey.AsRoot }
        : RealEntityAnnotation

    let fTable =
        { Extra = ann
          Alias = Some alias
          Table = compileResolvedEntityRef entity.Root }
        : SQL.FromTable

    let subquery = SQL.FTable fTable

    SQL.FJoin
        { Type = SQL.Left
          A = from
          B = subquery
          Condition = joinExpr }

let fromTableName (table: SQL.FromTable) =
    table.Alias
    |> Option.map (fun a -> a.Name)
    |> Option.defaultValue table.Table.Name

// Join a table by key in existing FROM expression.
let joinPath (layout: Layout) (joinKey: JoinKey) (join: JoinPath) (topFrom: SQL.FromExpr) : SQL.FromExpr option =
    // TODO: this is implemented as such to insert JOINs at proper places considering LATERAL JOINs.
    // However, we don't support outer join paths inside sub-selects anyway, so JOINs are always appended
    // at the upper level.
    let rec findNode =
        function
        | SQL.FTable fTable as from ->
            let realName = fromTableName fTable

            if realName = joinKey.Table then
                (true, true, from)
            else
                (false, false, from)
        | SQL.FTableExpr subsel as from ->
            match subsel.Alias with
            | Some alias when alias.Name = joinKey.Table -> (true, true, from)
            | _ -> (false, false, from)
        | SQL.FJoin joinExpr as from ->
            let (foundA, insertA, fromA) = findNode joinExpr.A

            if foundA then
                if insertA then
                    (true, true, from)
                else
                    (true, false, SQL.FJoin { joinExpr with A = fromA })
            else
                let (foundB, insertB, fromB) = findNode joinExpr.B

                if foundB then
                    if insertB then
                        (true, true, from)
                    else
                        (true, false, SQL.FJoin { joinExpr with B = fromB })
                else
                    (false, false, from)

    let (found, insert, newFrom) = findNode topFrom

    if not found then
        None
    else
        let newFrom =
            if insert then
                makeJoinNode layout joinKey join newFrom
            else
                newFrom

        Some newFrom

let rec joinsToSeq (paths: JoinPathsMap) : JoinPathsPair seq =
    paths |> Map.toSeq |> Seq.collect joinToSeq

and private joinToSeq (joinKey: JoinKey, tree: JoinTree) : (JoinKey * JoinPath) seq =
    let me = Seq.singleton (joinKey, tree.Path)
    Seq.append me (joinsToSeq tree.Nested)

// Add multiple joins, populating FromEntitiesMap in process.
let buildJoins
    (layout: Layout)
    (initialEntitiesMap: FromEntitiesMap)
    (initialFrom: SQL.FromExpr)
    (paths: JoinPathsPair seq)
    : FromEntitiesMap * SQL.FromExpr =
    let foldOne (entitiesMap, from) (joinKey: JoinKey, join: JoinPath) =
        let from =
            Option.getOrFailWith (fun () -> "Failed to find the table for JOIN")
            <| joinPath layout joinKey join from

        let entity =
            { Ref = join.RealEntity
              IsInner = false
              AsRoot = joinKey.AsRoot
              Check = None }

        let entitiesMap = Map.add join.Name entity entitiesMap
        (entitiesMap, from)

    Seq.fold foldOne (initialEntitiesMap, initialFrom) paths

let private buildInternalJoins
    (layout: Layout)
    (initialFromMap: FromMap)
    (initialFrom: SQL.FromExpr)
    (paths: JoinPathsPair seq)
    : FromMap * SQL.FromExpr =
    let (entitiesMap, newFrom) = buildJoins layout Map.empty initialFrom paths

    let makeFromInfo name entityInfo =
        { FromType = FTSubquery emptySelectInfo
          Entity = Some entityInfo
          MainId = None
          MainSubEntity = None
          Attributes = emptyEntityAttributes }

    let addedFromMap = Map.map makeFromInfo entitiesMap
    let newFromMap = Map.unionUnique initialFromMap addedFromMap
    (newFromMap, newFrom)

let private compileJoinId (joinNs: JoinNamespace) (jid: JoinId) : SQL.TableName =
    let name =
        match joinNs with
        | OzmaQLName "" -> sprintf "__join__%i" jid
        | ns -> sprintf "__join__%O__%i" ns jid

    SQL.SQLName name

// Returned join paths sequence are only those paths that need to be added to an existing FROM expression
// with `oldPaths` added.
// Also returned are new `JoinPaths` with everything combined.
let augmentJoinPaths (oldPaths: JoinPaths) (newPaths: JoinPaths) : SQL.RenamesMap * JoinPathsPair seq * JoinPaths =
    if oldPaths.Namespace <> newPaths.Namespace then
        failwithf "Incompatible join namespaces: %O vs %O" oldPaths.Namespace newPaths.Namespace

    let mutable lastId = oldPaths.NextJoinId
    let mutable addedPaths = []
    let mutable renamesMap = Map.empty

    let rec renameNewPath
        (newKeyName: SQL.TableName option)
        (oldMap: JoinPathsMap)
        (joinKey: JoinKey)
        (tree: JoinTree)
        =
        // In nested join maps, join key table is always the same and is equal to table name of the previous level join.
        // We need to rename it to the new name.
        let joinKey =
            match newKeyName with
            | None -> joinKey
            | Some name -> { joinKey with Table = name }

        let (oldNested, newPath) =
            match Map.tryFind joinKey oldMap with
            | Some existing -> (existing.Nested, existing.Path)
            | None ->
                let newPath =
                    { Name = compileJoinId oldPaths.Namespace lastId
                      RealEntity = tree.Path.RealEntity }

                lastId <- lastId + 1
                addedPaths <- (joinKey, newPath) :: addedPaths
                (Map.empty, newPath)

        renamesMap <- Map.add tree.Path.Name newPath.Name renamesMap
        let nestedMap = renameNewPaths (Some newPath.Name) oldNested tree.Nested
        { Path = newPath; Nested = nestedMap }

    and renameNewPaths (newKeyName: SQL.TableName option) (oldMap: JoinPathsMap) (newMap: JoinPathsMap) =
        Map.map (renameNewPath newKeyName oldMap) newMap

    let renamedNewPaths = renameNewPaths None oldPaths.Map newPaths.Map

    let ret =
        { Map = Map.union renamedNewPaths oldPaths.Map
          NextJoinId = lastId
          Namespace = oldPaths.Namespace }

    (renamesMap, Seq.rev (List.toSeq addedPaths), ret)

let rec private prependColumnsToSelectTree (cols: SQL.SelectedColumn seq) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr =
    function
    | SQL.SSelect query ->
        SQL.SSelect
            { query with
                Columns = Seq.append cols query.Columns |> Seq.toArray }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp setOp ->
        SQL.SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = prependColumnsToSelect cols setOp.A
              B = prependColumnsToSelect cols setOp.B
              OrderLimit = setOp.OrderLimit }

and private prependColumnsToSelect (cols: SQL.SelectedColumn seq) (select: SQL.SelectExpr) : SQL.SelectExpr =
    let tree = prependColumnsToSelectTree cols select.Tree
    { select with Tree = tree }

let addEntityChecks (entitiesMap: FromEntitiesMap) (where: SQL.ValueExpr option) : SQL.ValueExpr option =
    let addWhere where check =
        match where with
        | None -> Some check
        | Some oldWhere -> Some(SQL.VEAnd(oldWhere, check))

    entitiesMap
    |> Map.values
    |> Seq.mapMaybe (fun info -> info.Check)
    |> Seq.fold addWhere where

let private getFieldName (fieldInfo: FieldRefMeta) (currName: FieldName) : SQL.ColumnName =
    match fieldInfo.Column with
    | Some { ForceSQLName = Some name } -> name
    | _ -> compileName currName

let private boundAttributeToMapping =
    function
    | BAMapping mapping -> Some mapping
    | BAArrayMapping mapping -> Some mapping
    | BAExpr(FEValue v) ->
        let mapping =
            { Entries = HashMap.empty
              Default = Some v }

        Some mapping
    | _ -> None

let private compileMappingValues (compiledBound: SQL.ValueExpr) (mapping: BoundMapping) =
    let compileVal v = SQL.VEValue(compileFieldValue v)

    let compileCheck (matchValue, ret) =
        (SQL.VEBinaryOp(compiledBound, SQL.BOEq, compileVal matchValue), compileVal ret)

    let convCases =
        mapping.Entries |> HashMap.toSeq |> Seq.map compileCheck |> Array.ofSeq

    let convElse = Option.map compileVal mapping.Default
    SQL.VECase(convCases, convElse)

let private replaceRefForcedSQLName
    (forcedName: SQL.ColumnName option)
    (ref: LinkedBoundFieldRef)
    : LinkedBoundFieldRef =
    let fieldInfo = ObjectMap.findType<FieldRefMeta> ref.Extra

    match fieldInfo.Column with
    | None -> ref
    | Some columnInfo ->
        let newColumnInfo =
            { columnInfo with
                ForceSQLName = forcedName }

        let newFieldInfo =
            { fieldInfo with
                Column = Some newColumnInfo }

        { ref with
            Extra = ObjectMap.add newFieldInfo ref.Extra }

let private resolvedFieldColumnName
    : GenericResolvedField<ResolvedColumnField, 'comp> -> SQL.ColumnName when 'comp :> IComputedFieldBits =
    function
    | RId -> sqlFunId
    | RSubEntity -> sqlFunSubEntity
    | RColumnField col -> col.ColumnName
    | RComputedField comp -> comp.ColumnName

type CompiledSingleFrom =
    { From: SQL.FromExpr
      Where: SQL.ValueExpr option
      WhereWithoutSubentities: SQL.ValueExpr option
      Entities: FromEntitiesMap
      Joins: JoinPaths }

let private selfTableName = SQL.SQLName "__this"
let private selfTableRef = { Schema = None; Name = selfTableName }: SQL.TableRef

type private CompileBoundRefArgs =
    { Context: ExprContext
      Extra: ObjectMap
      Paths: JoinPaths
      CheckNullExpr: SQL.ValueExpr option
      TableRef: SQL.TableRef option
      AsRoot: bool }

type private ExpandedFieldKey = DomainsTree<FromFieldKey>

type CompilationFlags = { SubExprJoinNamespace: JoinNamespace }

let defaultCompilationFlags = { SubExprJoinNamespace = rootJoinNamespace }

// Expects metadata:
// * `FieldMeta` for all immediate FERefs in result expressions when meta columns are required;
// * `FieldMeta` with `Bound` filled for all field references with paths;
// * `ReferenceArgumentMeta` for all placeholders with paths.
type private QueryCompiler
    (
        globalFlags: CompilationFlags,
        layout: Layout,
        defaultAttrs: MergedDefaultAttributes,
        initialArguments: QueryArguments
    ) =
    // Only compiler can robustly detect used schemas and arguments, accounting for meta columns.
    let mutable arguments = initialArguments
    let mutable usedDatabase = emptyUsedDatabase

    let replacer = NameReplacer()

    let getArgumentType =
        function
        | PGlobal globalName as arg ->
            let (argType, newArguments) =
                addArgument arg (Map.find globalName globalArgumentTypes) arguments

            arguments <- newArguments
            argType
        | PLocal _ as arg -> arguments.Types.[arg]

    let getJoinNs (flags: SelectFlags) =
        if flags.IsTopLevel then
            rootJoinNamespace
        else
            globalFlags.SubExprJoinNamespace

    let getEntityByRef (entityRef: ResolvedEntityRef) =
        match layout.FindEntity entityRef with
        | None -> raisef QueryCompileException "Failed to find entity %O" entityRef
        | Some e -> e

    let columnName: ColumnType -> SQL.SQLName =
        function
        | CTMeta(CMRowAttribute(OzmaQLName name)) -> replacer.ConvertName <| sprintf "__row_attr__%s" name
        | CTColumnMeta(OzmaQLName field, CCCellAttribute(OzmaQLName name)) ->
            replacer.ConvertName <| sprintf "__cell_attr__%s__%s" field name
        | CTColumnMeta(OzmaQLName field, CCPun) -> replacer.ConvertName <| sprintf "__pun__%s" field
        | CTMeta(CMDomain id) -> replacer.ConvertName <| sprintf "__domain__%i" id
        | CTMeta(CMId field) -> replacer.ConvertName <| sprintf "__id__%O" field
        | CTMeta(CMSubEntity field) -> replacer.ConvertName <| sprintf "__sub_entity__%O" field
        | CTMeta CMMainId -> SQL.SQLName "__main_id"
        | CTMeta CMMainSubEntity -> SQL.SQLName "__main_sub_entity"
        | CTMeta(CMArgAttribute(OzmaQLName arg, OzmaQLName name)) ->
            replacer.ConvertName <| sprintf "__arg_attr__%s__%s" arg name
        | CTColumn(OzmaQLName column) -> SQL.SQLName column

    let compileEntityAttribute
        (entityAttributes: EntityAttributesMap)
        (entityRef: EntityRef)
        (attrName: AttributeName)
        : SQL.ValueExpr =
        let attrs = Map.find (compileName entityRef.Name) entityAttributes
        let colName = columnName <| CTMeta(CMRowAttribute attrName)

        let colRef =
            { Table = Some(compileEntityRef entityRef)
              Name = colName }
            : SQL.ColumnRef

        SQL.VEColumn colRef

    let renameSelectInfo (columns: 'f2 seq) (info: GenericSelectInfo<'f1>) : GenericSelectInfo<'f2> =
        let newColumn = columns.GetEnumerator()
        let mutable namesMap = Map.empty

        for column in info.Columns do
            match column.Type with
            | CTColumn name ->
                ignore <| newColumn.MoveNext()
                let newName = newColumn.Current
                namesMap <- Map.add name newName namesMap
            | _ -> ()

        let renameColumns (colInfo: GenericColumnInfo<'f1>) =
            let newType =
                match colInfo.Type with
                | CTColumn name -> CTColumn namesMap.[name]
                | CTColumnMeta(name, meta) -> CTColumnMeta(namesMap.[name], meta)
                | CTMeta meta -> CTMeta meta

            { Type = newType; Info = colInfo.Info }

        let columns = Array.map renameColumns info.Columns
        let domains = renameDomainFields namesMap info.Domains

        { Columns = columns; Domains = domains }

    let signatureColumns
        (skipNames: bool)
        (sign: SelectSignature)
        (half: HalfCompiledSingleSelect)
        : SQL.SelectedColumn seq =
        seq {
            for KeyValue(metaCol, metaInfo) in sign.MetaColumns do
                let name =
                    if not skipNames then
                        CTMeta metaCol |> columnName |> Some
                    else
                        None

                let expr =
                    match Map.tryFind metaCol half.MetaColumns with
                    | Some e -> e.Expression
                    | None when skipNames -> SQL.nullExpr
                    | None ->
                        match metaSQLType metaCol with
                        | Some typ -> SQL.VECast(SQL.nullExpr, compileSQLColumnType typ)
                        // This will break when current query is a recursive one, because PostgreSQL can't derive
                        // type of column and assumes it as `text`.
                        | None -> SQL.nullExpr

                yield SQL.SCExpr(name, expr)

            for colSig, col in Seq.zip sign.Columns half.Columns do
                for KeyValue(metaCol, metaInfo) in colSig.Meta do
                    let name =
                        match col.Name with
                        | TName name when not skipNames -> CTColumnMeta(name, metaCol) |> columnName |> Some
                        | _ -> None

                    let expr =
                        match Map.tryFind metaCol col.Meta with
                        | Some e -> e.Expression
                        | None -> SQL.nullExpr

                    yield SQL.SCExpr(name, expr)

                let name =
                    match col.Name with
                    | TName name when not skipNames -> CTColumn name |> columnName |> Some
                    | _ -> None

                yield SQL.SCExpr(name, col.Column)
        }

    let signatureValueColumns
        (skipNames: bool)
        (sign: SelectSignature)
        (valsRow: SQL.ValueExpr array)
        : SQL.ValueExpr seq =
        seq {
            for KeyValue(metaCol, metaInfo) in sign.MetaColumns do
                if skipNames then
                    yield SQL.nullExpr
                else
                    match metaSQLType metaCol with
                    | Some typ -> yield SQL.VECast(SQL.nullExpr, compileSQLColumnType typ)
                    | None -> failwithf "Failed to add type to meta column %O" metaCol

            for colSig, col in Seq.zip sign.Columns valsRow do
                for metaCol in colSig.Meta do
                    if skipNames then
                        yield SQL.nullExpr
                    else
                        failwithf "Failed to add type to meta column %O for column %O" metaCol col

                yield col
        }

    let rec setSelectTreeExprColumns
        (sign: SelectSignature)
        (skipNames: bool)
        : SQL.SelectTreeExpr -> SQL.SelectTreeExpr =
        function
        | SQL.SSelect sel ->
            let half = sel.Extra :?> HalfCompiledSingleSelect
            let columns = signatureColumns skipNames sign half |> Array.ofSeq

            let extra =
                match half.FromInfo with
                | None -> NoSelectFromInfo :> obj
                | Some info -> info :> obj

            SQL.SSelect
                { sel with
                    Columns = columns
                    Extra = extra }
        | SQL.SSetOp setOp ->
            let a = setSelectExprColumns sign skipNames setOp.A
            let b = setSelectExprColumns sign true setOp.B
            SQL.SSetOp { setOp with A = a; B = b }
        | SQL.SValues vals ->
            let newVals = Array.map (signatureValueColumns skipNames sign >> Array.ofSeq) vals
            SQL.SValues newVals

    and setSelectExprColumns (sign: SelectSignature) (skipNames: bool) (select: SQL.SelectExpr) : SQL.SelectExpr =
        let tree = setSelectTreeExprColumns sign skipNames select.Tree
        { select with Tree = tree }

    let setSelectColumns (sign: SelectSignature) = setSelectExprColumns sign false

    let rec domainExpression (tableRef: SQL.TableRef) (f: Domain -> SQL.ValueExpr) =
        function
        | DSingle(id, dom) -> f dom
        | DMulti(ns, nested) ->
            let makeCase (localId, subcase) =
                match domainExpression tableRef f subcase with
                | SQL.VEValue SQL.VNull -> None
                | subexpr ->
                    let case =
                        SQL.VEBinaryOp(
                            SQL.VEColumn
                                { Table = Some tableRef
                                  Name = columnName (CTMeta(CMDomain ns)) },
                            SQL.BOEq,
                            SQL.VEValue(SQL.VInt localId)
                        )

                    Some(case, subexpr)

            let cases = nested |> Map.toSeq |> Seq.mapMaybe makeCase |> Seq.toArray

            if Array.isEmpty cases then
                SQL.nullExpr
            else
                SQL.VECase(cases, None)

    let fromInfoExpression (tableRef: SQL.TableRef) (f: Domain -> SQL.ValueExpr) =
        function
        | FTEntity(id, dom) -> f dom
        | FTSubquery info -> domainExpression tableRef f info.Domains

    let mutable lastDomainNamespaceId = 0

    let newDomainNamespaceId () =
        let id = lastDomainNamespaceId
        lastDomainNamespaceId <- lastDomainNamespaceId + 1
        id

    let mutable lastGlobalDomainId = 0

    let newGlobalDomainId () =
        let id = lastGlobalDomainId
        lastGlobalDomainId <- lastGlobalDomainId + 1
        id

    let mutable lastTempFieldId = 0

    let newTempFieldName () =
        let id = lastTempFieldId
        lastTempFieldId <- lastTempFieldId + 1
        TTemp id

    let subentityFromInfo (mainEntity: ResolvedEntityRef option) (selectSig: SelectInfo) : FromInfo =
        let (mainId, mainSubEntity) =
            match mainEntity with
            | None -> (None, None)
            | Some mainRef ->
                let mainId = Some(columnName <| CTMeta CMMainId)
                let mainEntityInfo = getEntityByRef mainRef

                let subEntity =
                    if Map.isEmpty mainEntityInfo.Children then
                        None
                    else
                        Some(columnName <| CTMeta CMMainSubEntity)

                (mainId, subEntity)

        { FromType = FTSubquery selectSig
          Entity = None
          MainId = mainId
          MainSubEntity = mainSubEntity
          Attributes = subselectAttributes selectSig }

    let compileOperationEntity (opEntity: ResolvedOperationEntity) : SQL.TableName * FromInfo * SQL.OperationTable =
        let entityRef = getResolvedEntityRef opEntity.Ref
        let entity = getEntityByRef entityRef

        let makeDomainEntry name field =
            { Ref = { Entity = entityRef; Name = name }
              RootEntity = entity.Root
              IdColumn = idDefault
              AsRoot = false }

        let domain = mapAllFields makeDomainEntry entity

        let mainEntry =
            { Ref =
                { Entity = entityRef
                  Name = entity.MainField }
              RootEntity = entity.Root
              IdColumn = idDefault
              AsRoot = false }

        let domain = Map.add funMain mainEntry domain
        let newAlias = compileNameFromEntity entityRef opEntity.Alias

        let ann =
            { RealEntity = entityRef
              FromPath = false
              IsInner = true
              AsRoot = false }
            : RealEntityAnnotation

        let tableRef = compileResolvedEntityRef entity.Root

        let fTable =
            { Extra = ann
              Alias = Some newAlias
              Table = tableRef }
            : SQL.OperationTable

        let subEntityCol =
            SQL.VEColumn
                { Table = Some { Schema = None; Name = newAlias }
                  Name = sqlFunSubEntity }

        let checkExpr = makeEntityCheckExpr subEntityCol opEntity layout entityRef

        let entityInfo =
            { Ref = entityRef
              IsInner = true
              AsRoot = opEntity.AsRoot
              Check = checkExpr }

        let fromInfo =
            { FromType = FTEntity(newGlobalDomainId (), domain)
              Entity = Some entityInfo
              MainId = None
              MainSubEntity = None
              Attributes = emptyEntityAttributes }

        usedDatabase <- addUsedEntityRef entityRef usedEntitySelect usedDatabase

        (newAlias, fromInfo, fTable)

    let rec compileBoundRef
        (args: CompileBoundRefArgs)
        (boundRef: ResolvedFieldRef)
        (columnName: SQL.ColumnName)
        : JoinPaths * SQL.ValueExpr =
        let realColumn () : SQL.ValueExpr =
            SQL.VEColumn(
                { Table = args.TableRef
                  Name = columnName }
                : SQL.ColumnRef
            )

        let entity = getEntityByRef boundRef.Entity

        let fieldInfo =
            match entity.FindField boundRef.Name with
            | None -> raisef QueryCompileException "Failed to find field: %O" boundRef
            | Some f -> f

        match fieldInfo.Field with
        | RId ->
            usedDatabase <- addUsedFieldRef boundRef usedFieldSelect usedDatabase
            (args.Paths, realColumn ())
        | RSubEntity ->
            usedDatabase <- addUsedFieldRef boundRef usedFieldSelect usedDatabase

            match args.Context.RefContext with
            | RCExpr ->
                let newColumn = realColumn ()

                let entities =
                    entity.Children |> Map.keys |> Seq.append (Seq.singleton boundRef.Entity)

                let entities =
                    match ObjectMap.tryFindType<PossibleSubtypesMeta> args.Extra with
                    | None -> entities
                    | Some meta -> entities |> Seq.filter (fun ref -> Seq.contains ref meta.PossibleSubtypes)

                let expr = makeSubEntityParseExprFor layout newColumn entities
                (args.Paths, expr)
            | RCTypeExpr -> (args.Paths, realColumn ())
        | RColumnField col ->
            usedDatabase <-
                addUsedField boundRef.Entity.Schema boundRef.Entity.Name fieldInfo.Name usedFieldSelect usedDatabase

            (args.Paths, realColumn ())
        | RComputedField comp when comp.IsMaterialized && not args.Context.Flags.ForceNoMaterialized ->
            let rootInfo =
                match comp.Virtual with
                | Some { InheritedFrom = Some rootRef } ->
                    let rootEntity = getEntityByRef rootRef
                    let rootField = Map.find boundRef.Name rootEntity.ComputedFields |> Result.get
                    Option.get rootField.Root
                | _ -> Option.get comp.Root

            usedDatabase <- unionUsedDatabases rootInfo.UsedDatabase usedDatabase
            (args.Paths, realColumn ())
        | RComputedField comp ->
            // Right now don't support renamed fields for computed fields.
            // It's okay because this is impossible: `FROM foo (a, b, c)`, and we don't propagate computed fields.
            // This will change when we support `SELECT *` or column aliases for entities, as one may do:
            // > SELECT computed FROM (SELECT * FROM foo) AS bar
            // We will need to revamp the resolver and compiler at this point to instead:
            // 1. Build set of used computed fields throughout an entity;
            // 2. Set it as an entity metadata;
            // 3. Add these fields in compiler and just use them.
            // We also always assume that all fields referenced in computed field are immediate. This is set during computed fields resolution.
            let localRef =
                args.TableRef
                |> Option.map (fun ref ->
                    { Schema = Option.map decompileName ref.Schema
                      Name = decompileName ref.Name }
                    : EntityRef)

            let mutable paths = args.Paths

            let possibleEntities =
                match ObjectMap.tryFindType<PossibleSubtypesMeta> args.Extra with
                | None -> None
                | Some meta -> Some meta.PossibleSubtypes

            let compileCase (case: VirtualFieldCase, caseComp: ResolvedComputedField) =
                let entityRefs =
                    match possibleEntities with
                    | None -> case.PossibleEntities
                    | Some possible -> Set.intersect possible case.PossibleEntities

                if Set.isEmpty entityRefs then
                    None
                else
                    let entities = entityRefs |> Seq.map (fun ref -> getEntityByRef ref :> IEntityBits)

                    let (newPaths, newExpr) =
                        compileFieldExpr args.Context paths
                        <| replaceEntityRefInExpr localRef caseComp.Expression

                    paths <- newPaths
                    Some(entities, newExpr)

            // It's safe to use even in case when a virtual field exists in an entity with no `sub_entity` column, because
            // in this case `composeExhaustingIf` will omit the check completely.
            let subEntityColumn =
                SQL.VEColumn
                    { Table = args.TableRef
                      Name = sqlFunSubEntity }

            let expr =
                computedFieldCases layout args.Extra { boundRef with Name = fieldInfo.Name } comp
                |> Seq.mapMaybe compileCase
                |> Seq.toArray
                |> composeExhaustingIf (makeCheckExprFor subEntityColumn)

            let expr =
                match args.CheckNullExpr with
                | None -> expr
                | Some nullExpr ->
                    // We need to check that the innermost reference is not NULL, otherwise computed column can "leak", returning non-NULL result for NULL reference.
                    SQL.VECase([| (SQL.VEIsNotNull nullExpr, expr) |], None)

            (paths, expr)

    and compileBoundPathFrom
        (args: CompileBoundRefArgs)
        (oldColumnName: SQL.ColumnName)
        (newEntityRef: ResolvedEntityRef)
        (arrow: PathArrow)
        (tail: (ResolvedEntityRef * PathArrow) list)
        : JoinPaths * SQL.ValueExpr =
        let newEntity = getEntityByRef newEntityRef
        let newFieldInfo = newEntity.FindField arrow.Name |> Option.get
        let newColumnName = resolvedFieldColumnName newFieldInfo.Field
        usedDatabase <- addUsedEntityRef newEntityRef usedEntitySelect usedDatabase

        let pathKey =
            { Table = (Option.get args.TableRef).Name
              Column = oldColumnName
              ToRootEntity = newEntity.Root
              AsRoot = args.AsRoot }

        let newFieldRef =
            { Entity = newEntityRef
              Name = arrow.Name }

        let newCheckNullExpr =
            Some
            <| SQL.VEColumn
                { Table = Some { Schema = None; Name = pathKey.Table }
                  Name = pathKey.Column }

        let (newPath, nextJoinId, res) =
            match Map.tryFind pathKey args.Paths.Map with
            | None ->
                let newRealName = compileJoinId args.Context.JoinNamespace args.Paths.NextJoinId
                let newTableRef = { Schema = None; Name = newRealName }: SQL.TableRef

                let bogusPaths =
                    { Map = Map.empty
                      NextJoinId = args.Paths.NextJoinId + 1
                      Namespace = args.Context.JoinNamespace }

                let newArgs =
                    { args with
                        // We allow both materialized fields and table refs for the referenced rows.
                        Context =
                            { args.Context with
                                Flags = emptyExprCompilationFlags }
                        Paths = bogusPaths
                        CheckNullExpr = newCheckNullExpr
                        TableRef = Some newTableRef
                        AsRoot = arrow.AsRoot }

                let (nestedPaths, res) = compileBoundPath newArgs newFieldRef newColumnName tail

                let path =
                    { Name = newRealName
                      RealEntity = newEntityRef }

                let tree =
                    { Path = path
                      Nested = nestedPaths.Map }

                (tree, nestedPaths.NextJoinId, res)
            | Some tree ->
                let newTableRef = { Schema = None; Name = tree.Path.Name }: SQL.TableRef

                let bogusPaths =
                    { Map = tree.Nested
                      NextJoinId = args.Paths.NextJoinId
                      Namespace = args.Context.JoinNamespace }

                let newArgs =
                    { args with
                        Context =
                            { args.Context with
                                Flags = emptyExprCompilationFlags }
                        Paths = bogusPaths
                        CheckNullExpr = newCheckNullExpr
                        TableRef = Some newTableRef
                        AsRoot = arrow.AsRoot }

                let (nestedPaths, res) = compileBoundPath newArgs newFieldRef newColumnName tail
                let newTree = { tree with Nested = nestedPaths.Map }
                (newTree, nestedPaths.NextJoinId, res)

        let newPaths =
            { Map = Map.add pathKey newPath args.Paths.Map
              NextJoinId = nextJoinId
              Namespace = args.Context.JoinNamespace }

        (newPaths, res)

    and compileBoundPath
        (args: CompileBoundRefArgs)
        (fieldRef: ResolvedFieldRef)
        (columnName: SQL.ColumnName)
        : (ResolvedEntityRef * PathArrow) list -> JoinPaths * SQL.ValueExpr =
        function
        | [] -> compileBoundRef args fieldRef columnName
        | (newEntityRef, arrow) :: refs ->
            usedDatabase <- addUsedFieldRef fieldRef usedFieldSelect usedDatabase
            compileBoundPathFrom args columnName newEntityRef arrow refs

    and compileReferenceArgument
        (extra: ObjectMap)
        (ctx: ExprContext)
        (arg: CompiledArgument)
        (asRoot: bool)
        (path: PathArrow seq)
        (boundPath: ResolvedEntityRef seq)
        : SQL.SelectExpr =
        let (referencedRef, remainingBoundPath) = Seq.snoc boundPath
        let (firstArrow, remainingPath) = Seq.snoc path

        let argTableRef = compileRenamedResolvedEntityRef referencedRef
        let pathWithEntities = Seq.zip remainingBoundPath remainingPath |> List.ofSeq
        let firstField = layout.FindField referencedRef firstArrow.Name |> Option.get

        let fromEntity =
            { Ref = relaxEntityRef referencedRef
              Only = false
              Alias = None
              AsRoot = asRoot
              Extra = ObjectMap.empty }

        let (fromRes, from) = compileFromExpr ctx 0 None true (FEntity fromEntity)
        assert (fromRes.Joins.NextJoinId = 0)

        let argIdRef =
            SQL.VEColumn
                { Table = Some argTableRef
                  Name = sqlFunId }

        let pathArgs =
            { Context = ctx
              Extra = extra
              Paths = emptyJoinPaths
              CheckNullExpr = Some argIdRef
              TableRef = Some argTableRef
              AsRoot = asRoot }
            : CompileBoundRefArgs

        let fieldRef =
            { Entity = referencedRef
              Name = firstArrow.Name }

        let columnName = resolvedFieldColumnName firstField.Field

        let (argPaths, expr) =
            compileBoundPath pathArgs fieldRef columnName pathWithEntities

        let (entitiesMap, from) =
            buildJoins layout (fromToEntitiesMap fromRes.Tables) from (joinsToSeq argPaths.Map)

        let whereWithoutSubentities =
            SQL.VEBinaryOp(argIdRef, SQL.BOEq, SQL.VEPlaceholder arg.PlaceholderId)

        let where = addEntityChecks entitiesMap (Some whereWithoutSubentities)

        let extra =
            { Entities = entitiesMap
              Joins = argPaths
              WhereWithoutSubentities = Some whereWithoutSubentities }
            : SelectFromInfo

        // TODO: This SELECT could be moved into a CTE to improve case with multiple usages of the same argument.
        let singleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = [| SQL.SCExpr(None, expr) |]
                From = Some from
                Where = where
                Extra = extra }

        { CTEs = None
          Tree = SQL.SSelect singleSelect
          Extra = null }

    and compileLinkedFieldRef
        (ctx: ExprContext)
        (paths0: JoinPaths)
        (linked: LinkedBoundFieldRef)
        : JoinPaths * SQL.ValueExpr =
        let refInfo =
            ObjectMap.tryFindType<FieldRefMeta> linked.Extra
            |> Option.defaultValue emptyFieldRefMeta

        assert (Array.length linked.Ref.Path = Array.length refInfo.Path)

        match linked.Ref.Ref with
        | VRColumn ref ->
            let pathWithEntities = Seq.zip refInfo.Path linked.Ref.Path |> List.ofSeq

            match refInfo.Bound with
            | Some(BMColumn boundInfo) ->
                let tableRef =
                    if ctx.Flags.ForceNoTableRef then
                        None
                    else
                        match refInfo.Column with
                        | Some { ForceSQLTable = Some table } -> Some table
                        | _ ->
                            match ref.Entity with
                            | Some renamedTable -> Some <| compileRenamedEntityRef renamedTable
                            | None -> Some <| compileRenamedResolvedEntityRef boundInfo.Ref.Entity

                let columnName = getFieldName refInfo ref.Name

                let checkNullRef =
                    if boundInfo.IsInner then
                        None
                    else
                        Some <| SQL.VEColumn { Table = tableRef; Name = sqlFunId }

                let pathArgs =
                    { Context = ctx
                      Extra = linked.Extra
                      Paths = paths0
                      CheckNullExpr = checkNullRef
                      TableRef = tableRef
                      AsRoot = linked.Ref.AsRoot }

                compileBoundPath pathArgs boundInfo.Ref columnName pathWithEntities
            | _ ->
                let tableRef =
                    if ctx.Flags.ForceNoTableRef then
                        None
                    else
                        match refInfo.Column with
                        | Some { ForceSQLTable = Some table } -> Some table
                        | _ -> Option.map compileRenamedEntityRef ref.Entity

                let columnName = getFieldName refInfo ref.Name

                match pathWithEntities with
                | [] ->
                    let columnRef = { Table = tableRef; Name = columnName }: SQL.ColumnRef
                    (paths0, SQL.VEColumn columnRef)
                | (newEntityRef, arrow) :: arrows ->
                    let pathArgs =
                        { Context = ctx
                          Extra = linked.Extra
                          Paths = paths0
                          CheckNullExpr = None
                          TableRef = tableRef
                          AsRoot = linked.Ref.AsRoot }

                    compileBoundPathFrom pathArgs columnName newEntityRef arrow arrows
        | VRArgument arg ->
            let argType = getArgumentType arg

            if Array.isEmpty linked.Ref.Path then
                // Explicitly set argument type to avoid ambiguity,
                (paths0, SQL.VECast(SQL.VEPlaceholder argType.PlaceholderId, argType.DbType))
            else
                let selectExpr =
                    compileReferenceArgument linked.Extra ctx argType linked.Ref.AsRoot linked.Ref.Path refInfo.Path

                (paths0, SQL.VESubquery selectExpr)

    and compileFieldAttribute
        (ctx: ExprContext)
        (paths: JoinPaths)
        (linked: LinkedBoundFieldRef)
        (attrName: AttributeName)
        : JoinPaths * SQL.ValueExpr =
        let getDefaultAttribute
            (updateEntityRef: LinkedBoundFieldRef -> LinkedBoundFieldRef)
            (entityRef: ResolvedEntityRef)
            (name: FieldName)
            (singleColumn: SQL.ColumnName option)
            =
            let attrs = defaultAttrs.FindField entityRef name |> Option.get
            let attr = Map.find attrName attrs
            // FIXME: support single per-row attributes.
            // FIXME: support queries in `replaceFieldRefInExpr`.
            let mapper =
                match singleColumn with
                | None -> updateEntityRef
                | Some _ as forcedName -> replaceRefForcedSQLName forcedName >> updateEntityRef

            let converted =
                mapBoundAttributeExpr (replaceFieldRefInExpr mapper) attr.Attribute.Value.Expression

            compileBoundAttributeExpr ctx paths (FERef linked) converted

        match linked.Ref.Ref with
        | VRArgument arg ->
            let argType = getArgumentType arg

            if Array.isEmpty linked.Ref.Path then
                match Map.tryFind attrName argType.Attributes with
                | None -> (paths, SQL.nullExpr)
                | Some attr -> compileBoundAttributeExpr ctx paths (FERef linked) attr.Expression
            else
                let refMeta = ObjectMap.findType<FieldRefMeta> linked.Extra
                let lastEntityRef = Array.last refMeta.Path
                let lastArrow = Array.last linked.Ref.Path
                getDefaultAttribute (replacePathInField layout linked true) lastEntityRef lastArrow.Name None

        | VRColumn ref ->
            if Array.isEmpty linked.Ref.Path then
                let entityName = (Option.get ref.Entity).Name

                match Map.tryFind (compileName entityName) ctx.EntityAttributes with
                | Some attrs when Set.contains (ref.Name, attrName) attrs.Fields ->
                    let colName = columnName <| CTColumnMeta(ref.Name, CCCellAttribute attrName)

                    let colRef =
                        { Table = Option.map compileEntityRef ref.Entity
                          Name = colName }
                        : SQL.ColumnRef

                    (paths, SQL.VEColumn colRef)
                | _ ->
                    match ObjectMap.tryFindType<FieldRefMeta> linked.Extra with
                    | Some({ Bound = Some(BMColumn boundInfo) } as fieldInfo) ->
                        let single =
                            if boundInfo.Single then
                                Some <| getFieldName fieldInfo ref.Name
                            else
                                None

                        getDefaultAttribute
                            (replaceEntityRefInField ref.Entity)
                            boundInfo.Ref.Entity
                            boundInfo.Ref.Name
                            single
                    | _ -> (paths, SQL.nullExpr)
            else
                let fieldMeta = ObjectMap.findType<FieldRefMeta> linked.Extra
                let lastEntityRef = Array.last fieldMeta.Path
                let lastArrow = Array.last linked.Ref.Path
                getDefaultAttribute (replacePathInField layout linked true) lastEntityRef lastArrow.Name None

    and compileFieldExpr (ctx: ExprContext) (paths0: JoinPaths) (expr: ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0

        let compileLinkedRef ctx linked =
            let (newPaths, ret) = compileLinkedFieldRef ctx paths linked
            paths <- newPaths
            ret

        let compileSubSelectExpr =
            let subCtx =
                { ctx with
                    JoinNamespace = globalFlags.SubExprJoinNamespace }

            snd << compileSelectExpr subSelectFlags subCtx None

        let compileFieldAttr fref attr =
            let (newPaths, ret) = compileFieldAttribute ctx paths fref attr
            paths <- newPaths
            ret

        let compileTypeCheck includeChildren c (subEntityRef: SubEntityRef) =
            let checkForTypes =
                match ObjectMap.tryFindType<SubEntityMeta> subEntityRef.Extra with
                | Some info -> info.PossibleEntities
                | None ->
                    // Check for everything.
                    let entityRef = getResolvedEntityRef subEntityRef.Ref

                    if includeChildren then
                        allPossibleEntities layout entityRef
                        |> mapPossibleEntities (Seq.map fst >> Set.ofSeq)
                    else
                        let entity = layout.FindEntity entityRef |> Option.get

                        if entity.IsAbstract then
                            PEList Set.empty
                        else
                            PEList <| Set.singleton entityRef

            match checkForTypes with
            | PEAny -> SQL.VEValue(SQL.VBool true)
            | PEList types ->
                let col = compileLinkedRef { ctx with RefContext = RCTypeExpr } c
                let entities = types |> Seq.map (fun typ -> getEntityByRef typ :> IEntityBits)
                makeCheckExprFor col entities

        let rec traverse =
            function
            | FEValue v -> SQL.VEValue <| compileFieldValue v
            | FERef c -> compileLinkedRef ctx c
            | FEEntityAttr(eref, attr) -> compileEntityAttribute ctx.EntityAttributes eref attr
            | FEFieldAttr(fref, attr) -> compileFieldAttr fref attr
            | FENot a -> SQL.VENot(traverse a)
            | FEAnd(a, b) -> SQL.VEAnd(traverse a, traverse b)
            | FEOr(a, b) -> SQL.VEOr(traverse a, traverse b)
            | FEDistinct(a, b) -> SQL.VEDistinct(traverse a, traverse b)
            | FENotDistinct(a, b) -> SQL.VENotDistinct(traverse a, traverse b)
            | FEBinaryOp(a, op, b) -> SQL.VEBinaryOp(traverse a, compileBinaryOp op, traverse b)
            | FESimilarTo(e, pat) -> SQL.VESimilarTo(traverse e, traverse pat)
            | FENotSimilarTo(e, pat) -> SQL.VENotSimilarTo(traverse e, traverse pat)
            | FEIn(a, arr) -> SQL.VEIn(traverse a, Array.map traverse arr)
            | FENotIn(a, arr) -> SQL.VENotIn(traverse a, Array.map traverse arr)
            | FEInQuery(a, query) -> SQL.VEInQuery(traverse a, compileSubSelectExpr query)
            | FENotInQuery(a, query) -> SQL.VENotInQuery(traverse a, compileSubSelectExpr query)
            | FEAny(e, op, arr) -> SQL.VEAny(traverse e, compileBinaryOp op, traverse arr)
            | FEAll(e, op, arr) -> SQL.VEAll(traverse e, compileBinaryOp op, traverse arr)
            | FECast(e, typ) ->
                SQL.VECast(
                    traverse e,
                    SQL.mapValueType (fun (x: SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType typ)
                )
            | FEIsNull a -> SQL.VEIsNull(traverse a)
            | FEIsNotNull a -> SQL.VEIsNotNull(traverse a)
            | FECase(es, els) ->
                SQL.VECase(Array.map (fun (cond, expr) -> (traverse cond, traverse expr)) es, Option.map traverse els)
            | FEMatch(expr, es, els) ->
                let compiledExpr = traverse expr

                SQL.VECase(
                    Array.map
                        (fun (cond, ret) -> (SQL.VEBinaryOp(compiledExpr, SQL.BOEq, traverse cond), traverse ret))
                        es,
                    Option.map traverse els
                )
            | FEJsonArray vals ->
                let compiled = Array.map traverse vals

                let tryExtract =
                    function
                    | SQL.VEValue v -> Some v
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract compiled

                match optimized with
                | Some optimizedVals ->
                    optimizedVals |> Seq.map JToken.FromObject |> jsonArray :> JToken
                    |> ComparableJToken
                    |> SQL.VJson
                    |> SQL.VEValue
                | None -> SQL.VEFunc(SQL.SQLName "jsonb_build_array", Array.map traverse vals)
            | FEJsonObject obj ->
                let compiled = Map.map (fun name -> traverse) obj

                let tryExtract =
                    function
                    | (OzmaQLName name, SQL.VEValue v) -> Some(name, v)
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract (Map.toSeq compiled)

                match optimized with
                | Some optimizedVals ->
                    optimizedVals
                    |> Seq.map (fun (name, v) -> (name, JToken.FromObject v))
                    |> jsonObject
                    :> JToken
                    |> ComparableJToken
                    |> SQL.VJson
                    |> SQL.VEValue
                | None ->
                    let args =
                        obj
                        |> Map.toSeq
                        |> Seq.collect (fun (OzmaQLName name, v) -> [ SQL.VEValue <| SQL.VString name; traverse v ])
                        |> Seq.toArray

                    SQL.VEFunc(SQL.SQLName "jsonb_build_object", args)
            | FEFunc(name, args) ->
                let compArgs = Array.map traverse args

                match Map.tryFind name allowedFunctions with
                | None -> raisef QueryCompileException "Unknown function: %O" name
                | Some(FRFunction name) -> SQL.VEFunc(name, compArgs)
                | Some(FRSpecial special) -> SQL.VESpecialFunc(special, compArgs)
            | FEAggFunc(name, args) -> SQL.VEAggFunc(Map.find name allowedAggregateFunctions, compileAggExpr args)
            | FESubquery query -> SQL.VESubquery(compileSubSelectExpr query)
            | FEInheritedFrom(c, subEntityRef) -> compileTypeCheck true c subEntityRef
            | FEOfType(c, subEntityRef) -> compileTypeCheck false c subEntityRef

        and compileAggExpr: ResolvedAggExpr -> SQL.AggExpr =
            function
            | AEAll exprs -> SQL.AEAll(Array.map traverse exprs)
            | AEDistinct expr -> SQL.AEDistinct(traverse expr)
            | AEStar -> SQL.AEStar

        let ret = traverse expr
        (paths, ret)

    and compileBoundAttributeExpr
        (ctx: ExprContext)
        (paths0: JoinPaths)
        (boundExpr: ResolvedFieldExpr)
        : ResolvedBoundAttributeExpr -> (JoinPaths * SQL.ValueExpr) =
        function
        | BAExpr e -> compileFieldExpr ctx paths0 e
        | BAMapping mapping ->
            let (paths, compiledBound) = compileFieldExpr ctx paths0 boundExpr
            let expr = compileMappingValues compiledBound mapping
            (paths, expr)
        | BAArrayMapping mapping ->
            let (paths, compiledBound) = compileFieldExpr ctx paths0 boundExpr
            let fromExprFunc = (SQL.TEFunc(SQL.SQLName "unnest", [| compiledBound |]))
            let valueName = SQL.SQLName "value"

            let fromAlias =
                { Name = SQL.SQLName "values"
                  Columns = Some [| valueName |] }
                : SQL.TableAlias

            let fromExpr =
                { SQL.fromTableExpr fromExprFunc with
                    Alias = Some fromAlias }

            let valueRef = SQL.VEColumn { Table = None; Name = valueName }
            let innerResExpr = compileMappingValues valueRef mapping
            let resExpr = SQL.VEAggFunc(SQL.SQLName "array_agg", SQL.AEAll [| innerResExpr |])

            let singleSelectExpr =
                { SQL.emptySingleSelectExpr with
                    Columns = [| SQL.SCExpr(None, resExpr) |]
                    From = Some(SQL.FTableExpr fromExpr)
                    Extra = NoSelectFromInfo }

            let expr = singleSelectExpr |> SQL.SSelect |> SQL.selectExpr |> SQL.VESubquery
            (paths, expr)

    and compileOrderColumn
        (ctx: ExprContext)
        (paths: JoinPaths)
        (ord: ResolvedOrderColumn)
        : JoinPaths * SQL.OrderColumn =
        let (paths, expr) = compileFieldExpr ctx paths ord.Expr

        let ret =
            { Expr = expr
              Order = Option.map compileOrder ord.Order
              Nulls = Option.map compileNullsOrder ord.Nulls }
            : SQL.OrderColumn

        (paths, ret)

    and compileOrderLimitClause
        (ctx: ExprContext)
        (paths0: JoinPaths)
        (clause: ResolvedOrderLimitClause)
        : JoinPaths * SQL.OrderLimitClause =
        let mutable paths = paths0

        let compileOrderColumn' ord =
            let (newPaths, ret) = compileOrderColumn ctx paths ord
            paths <- newPaths
            ret

        let compileFieldExpr' expr =
            let (newPaths, ret) = compileFieldExpr ctx paths expr
            paths <- newPaths
            ret

        let ret =
            { OrderBy = Array.map compileOrderColumn' clause.OrderBy
              Limit = Option.map compileFieldExpr' clause.Limit
              Offset = Option.map compileFieldExpr' clause.Offset }
            : SQL.OrderLimitClause

        (paths, ret)

    and compileInsideSelectExpr
        (flags: SelectFlags)
        (compileTree: ExprContext -> ResolvedSelectTreeExpr -> SelectSignature * 'a * SQL.SelectTreeExpr)
        (ctx: ExprContext)
        (select: ResolvedSelectExpr)
        : SelectSignature * 'a * SQL.SelectExpr =
        let (ctx, ctes) =
            match select.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (newCtx, newCtes) = compileCommonTableExprs flags ctx ctes
                (newCtx, Some newCtes)

        let (signature, domains, tree) = compileTree ctx select.Tree

        let ret =
            { CTEs = ctes
              Tree = tree
              Extra = null }
            : SQL.SelectExpr

        (signature, domains, ret)

    and compileExprValuesValue (ctx: ExprContext) : ResolvedValuesValue -> SQL.ValueExpr =
        function
        | VVDefault -> failwith "Impossible"
        | VVExpr expr ->
            let (newPaths, newExpr) = compileFieldExpr ctx emptyJoinPaths expr
            newExpr

    and compileValues
        (ctx: ExprContext)
        (values: ResolvedValuesValue[][])
        : SelectSignature * TempDomains * SQL.SelectTreeExpr =
        let compiledValues = values |> Array.map (Array.map (compileExprValuesValue ctx))

        let newColumn () =
            { Name = newTempFieldName ()
              Meta = Map.empty }

        let info =
            { MetaColumns = Map.empty
              Columns = Array.init (Array.length values.[0]) (fun _ -> newColumn ()) }

        let emptyDomains = DSingle(newGlobalDomainId (), Map.empty)
        (info, emptyDomains, SQL.SValues compiledValues)

    // `updateBindings` is used to update CTEBindings after non-recursive part of recursive CTE expression is compiled.
    and compileSelectExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (initialUpdateBindings: UpdateRecCTEBindings option)
        (expr: ResolvedSelectExpr)
        : TempSelectInfo * SQL.SelectExpr =
        let (signature, domains, ret) =
            match expr.Tree with
            // We assemble a new domain when SetOp is found, and we need a different tree traversal for that.
            | SSetOp _ when flags.MetaColumns ->
                let ns = newDomainNamespaceId ()
                let domainColumn = CMDomain ns
                let mutable lastId = 0

                let rec compileTreeExpr
                    (updateBindings: UpdateRecCTEBindings option)
                    (ctx: ExprContext)
                    : ResolvedSelectTreeExpr -> SelectSignature * Map<LocalDomainId, TempDomains> * SQL.SelectTreeExpr =
                    function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags ctx query
                        let id = lastId
                        lastId <- lastId + 1

                        let metaColumns =
                            Map.add domainColumn (resultMetaColumn (SQL.VEValue <| SQL.VInt id)) info.MetaColumns

                        let info = { info with MetaColumns = metaColumns }
                        let expr = { expr with Extra = info }
                        (selectSignature info, Map.singleton id info.Domains, SQL.SSelect expr)
                    | SValues values ->
                        let (info, domains, ret) = compileValues ctx values
                        let id = lastId
                        lastId <- lastId + 1
                        (info, Map.singleton id domains, ret)
                    | SSetOp setOp ->
                        let (sig1, domainsMap1, expr1) = compileExpr None ctx setOp.A

                        let ctx =
                            match updateBindings with
                            | None -> ctx
                            | Some update ->
                                { ctx with
                                    CTEs = update sig1 (DMulti(ns, domainsMap1)) }

                        let (sig2, domainsMap2, expr2) = compileExpr None ctx setOp.B

                        let (limitPaths, compiledLimits) =
                            compileOrderLimitClause ctx emptyJoinPaths setOp.OrderLimit

                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        let domainsMap1 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap1
                        let domainsMap2 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap2
                        assert Map.isEmpty limitPaths.Map

                        let ret =
                            { Operation = compileSetOp setOp.Operation
                              AllowDuplicates = setOp.AllowDuplicates
                              A = expr1
                              B = expr2
                              OrderLimit = compiledLimits }
                            : SQL.SetOperationExpr

                        (newSig, Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp ret)

                and compileExpr (updateBindings: UpdateRecCTEBindings option) =
                    compileInsideSelectExpr flags (compileTreeExpr updateBindings)

                let (signature, domainsMap, expr) = compileExpr initialUpdateBindings ctx expr
                (signature, DMulti(ns, domainsMap), expr)
            | _ ->
                let rec compileTreeExpr (ctx: ExprContext) =
                    function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags ctx query
                        (selectSignature info, info.Domains, SQL.SSelect expr)
                    | SValues values -> compileValues ctx values
                    | SSetOp setOp ->
                        let (sig1, doms1, expr1) = compileExpr ctx setOp.A
                        let (sig2, doms2, expr2) = compileExpr ctx setOp.B

                        let (limitPaths, compiledLimits) =
                            compileOrderLimitClause ctx emptyJoinPaths setOp.OrderLimit

                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        assert Map.isEmpty limitPaths.Map

                        let ret =
                            { Operation = compileSetOp setOp.Operation
                              AllowDuplicates = setOp.AllowDuplicates
                              A = expr1
                              B = expr2
                              OrderLimit = compiledLimits }
                            : SQL.SetOperationExpr
                        // We don't use domains when `MetaColumns = false`, so we return a random value (`doms1` in this case).
                        (newSig, doms1, SQL.SSetOp ret)

                and compileExpr = compileInsideSelectExpr flags compileTreeExpr

                compileExpr ctx expr

        let ret = setSelectColumns signature ret
        let info = infoFromSignature domains signature
        (info, ret)

    and compileCommonTableExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (name: SQL.TableName)
        (cte: ResolvedCommonTableExpr)
        : SelectInfo * SQL.CommonTableExpr =
        let extra = ObjectMap.findType<ResolvedCommonTableExprInfo> cte.Extra

        let flags =
            { flags with
                MainEntity = if extra.MainEntity then flags.MainEntity else None }

        let updateCteBindings (signature: SelectSignature) (domains: TempDomains) =
            let info = infoFromSignature domains signature

            let finalInfo =
                match cte.Fields with
                | None -> finalSelectInfo info
                | Some fields -> renameSelectInfo fields info

            Map.add name finalInfo ctx.CTEs

        let (info, expr) = compileSelectExpr flags ctx (Some updateCteBindings) cte.Expr

        let (info, fields) =
            match cte.Fields with
            | None -> (finalSelectInfo info, None)
            | Some fields ->
                let info = renameSelectInfo fields info
                let retFields = info.Columns |> Array.map (fun info -> columnName info.Type)
                (info, Some retFields)

        let ret =
            { Fields = fields
              Expr = SQL.DESelect expr
              Materialized = Some <| Option.defaultValue false cte.Materialized }
            : SQL.CommonTableExpr

        (info, ret)

    and compileCommonTableExprs
        (flags: SelectFlags)
        (ctx: ExprContext)
        (ctes: ResolvedCommonTableExprs)
        : ExprContext * SQL.CommonTableExprs =
        let mutable ctx = ctx

        let compileOne (name, cte) =
            let name' = compileName name
            let (info, expr) = compileCommonTableExpr flags ctx name' cte

            ctx <-
                { ctx with
                    CTEs = Map.add name' info ctx.CTEs }

            (name', expr)

        let exprs = Array.map compileOne ctes.Exprs

        let ret =
            { Recursive = ctes.Recursive
              Exprs = exprs }
            : SQL.CommonTableExprs

        (ctx, ret)

    and compileSingleSelectExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (select: ResolvedSingleSelectExpr)
        : HalfCompiledSingleSelect * SQL.SingleSelectExpr =
        let extra =
            match ObjectMap.tryFindType<ResolvedSingleSelectMeta> select.Extra with
            | None -> { HasAggregates = false }
            | Some extra -> extra

        let (fromMap, fromPaths, from) =
            match select.From with
            | Some from ->
                let (fromMap, newFrom) = compileFromExpr ctx 0 flags.MainEntity true from
                (fromMap.Tables, fromMap.Joins, Some newFrom)
            | None -> (Map.empty, emptyJoinPaths, None)

        let ctx =
            { ctx with
                EntityAttributes = fromMap |> Map.map (fun name fromInfo -> fromInfo.Attributes) }

        let mutable paths = fromPaths

        let whereWithoutSubentities =
            match select.Where with
            | None -> None
            | Some where ->
                let (newPaths, ret) = compileFieldExpr ctx paths where
                paths <- newPaths
                Some ret

        let entitiesMap = fromToEntitiesMap fromMap
        let where = addEntityChecks entitiesMap whereWithoutSubentities

        let compileGroupBy expr =
            let (newPaths, compiled) = compileFieldExpr ctx paths expr
            paths <- newPaths
            compiled

        let groupBy = Array.map compileGroupBy select.GroupBy

        let compileRowAttr (name, attr: ResolvedAttribute) =
            let (newPaths, colExpr) = compileFieldExpr ctx paths attr.Expression
            paths <- newPaths

            let info =
                { Mapping = None
                  Dependency = attr.Dependency
                  Internal = attr.Internal
                  SingleRow = if attr.Dependency = DSPerRow then None else Some colExpr
                  ValueType = None }

            let col = { Expression = colExpr; Info = info }
            (CMRowAttribute name, col)

        let attributeColumns = select.Attributes |> Map.toSeq |> Seq.map compileRowAttr

        let addMetaColumns = flags.MetaColumns && not extra.HasAggregates

        let mutable idColumns = Map.empty: Map<string, DomainIdColumn>
        let mutable lastIdColumn = 1

        let getIdColumn (key: string) : bool * DomainIdColumn =
            match Map.tryFind key idColumns with
            | Some maybeId -> (false, maybeId)
            | None ->
                let idCol = lastIdColumn
                lastIdColumn <- lastIdColumn + 1
                idColumns <- Map.add key idCol idColumns
                (true, idCol)

        let ownDomainId = newGlobalDomainId ()

        let getResultColumnEntry (i: int) (result: ResolvedQueryColumnResult) : ResultColumn =
            let (newPaths, resultColumn) = compileColumnResult ctx paths flags.IsTopLevel result
            paths <- newPaths

            let getDefaultAttributes
                (updateEntityRef: LinkedBoundFieldRef -> LinkedBoundFieldRef)
                (resultRef: LinkedBoundFieldRef)
                (fieldRef: ResolvedFieldRef)
                (singleColumn: SQL.ColumnName option)
                =
                match defaultAttrs.FindField fieldRef.Entity fieldRef.Name with
                | None -> Seq.empty
                | Some attrs ->
                    let makeDefaultAttr (name, attr: MergedAttribute) =
                        if
                            Map.containsKey name result.Attributes
                            || Option.isSome singleColumn && not attr.Attribute.Single
                        then
                            None
                        else
                            let mapper =
                                match singleColumn with
                                | None -> updateEntityRef
                                | Some _ as forcedName -> replaceRefForcedSQLName forcedName >> updateEntityRef
                            // FIXME: support queries in `replaceFieldRefInExpr`.
                            let converted =
                                mapBoundAttributeExpr (replaceFieldRefInExpr mapper) attr.Attribute.Value.Expression

                            let (newPaths, compiled) =
                                compileBoundAttributeExpr ctx paths (FERef resultRef) converted

                            let mapping = boundAttributeToMapping attr.Attribute.Value.Expression
                            let attrCol = CCCellAttribute name
                            paths <- newPaths

                            let info =
                                { Mapping = mapping
                                  Dependency = attr.Attribute.Value.Dependency
                                  Internal = attr.Attribute.Value.Internal
                                  SingleRow =
                                    if attr.Attribute.Value.Dependency = DSPerRow then
                                        None
                                    else
                                        Some compiled
                                  ValueType = None }

                            let ret = { Expression = compiled; Info = info }
                            Some(attrCol, ret)

                    attrs |> Map.toSeq |> Seq.mapMaybe makeDefaultAttr

            let getDefaultArgumentAttributes (argExpr: ResolvedFieldExpr) (argInfo: CompiledArgument) =
                let makeArgumentAttributeColumn (name: AttributeName, attr: ResolvedBoundAttribute) =
                    let mapping = boundAttributeToMapping attr.Expression
                    let attrCol = CCCellAttribute name

                    let (newPaths, compiled) =
                        compileBoundAttributeExpr ctx paths argExpr attr.Expression

                    paths <- newPaths

                    let info =
                        { Mapping = mapping
                          Dependency = attr.Dependency
                          Internal = attr.Internal
                          SingleRow = Some compiled
                          ValueType = None }

                    let ret = { Expression = compiled; Info = info }
                    (attrCol, ret)

                argInfo.Attributes |> Map.toSeq |> Seq.map makeArgumentAttributeColumn

            match result.Result with
            // References with paths.
            | FERef resultRef when not (Array.isEmpty resultRef.Ref.Path) && addMetaColumns ->
                let fieldInfo = ObjectMap.findType<FieldRefMeta> resultRef.Extra
                assert (Array.length resultRef.Ref.Path = Array.length fieldInfo.Path)

                let replacePath path boundPath =
                    assert (Array.length path = Array.length boundPath)
                    let newFieldInfo = { fieldInfo with Path = boundPath }

                    { Ref = { resultRef.Ref with Path = path }
                      Extra = ObjectMap.add newFieldInfo resultRef.Extra }

                let finalEntityRef = Array.last fieldInfo.Path
                let finalEntity = getEntityByRef finalEntityRef
                let finalArrow = Array.last resultRef.Ref.Path
                let finalField = finalEntity.FindField finalArrow.Name |> Option.get

                let finalRef =
                    { Entity = finalEntityRef
                      Name = finalArrow.Name }

                // Add system columns (id or sub_entity - this is a generic function).
                let makeSystemColumn (systemName: FieldName) =
                    let systemArrow = { finalArrow with Name = systemName }

                    let systemPath =
                        Seq.append (Seq.skipLast 1 resultRef.Ref.Path) (Seq.singleton systemArrow)
                        |> Array.ofSeq

                    let systemRef = replacePath systemPath fieldInfo.Path

                    let (newPaths, systemExpr) =
                        compileLinkedFieldRef { ctx with RefContext = RCTypeExpr } paths systemRef

                    paths <- newPaths
                    systemExpr

                let idExpr = makeSystemColumn funId

                let (createIdCol, idCol) = getIdColumn (string idExpr)

                let systemColumns =
                    if not createIdCol then
                        Seq.empty
                    else
                        let idColumn = (CMId idCol, resultMetaColumn idExpr)

                        let subEntityColumns =
                            // We don't need to select entity if there are no possible children.
                            if Seq.length (allPossibleEntitiesList layout finalEntityRef) <= 1 then
                                Seq.empty
                            else
                                Seq.singleton (CMSubEntity idCol, resultMetaColumn <| makeSystemColumn funSubEntity)

                        Seq.append (Seq.singleton idColumn) subEntityColumns

                let newDomains =
                    let newInfo =
                        { Ref = finalRef
                          RootEntity = finalEntity.Root
                          IdColumn = idCol
                          AsRoot = finalArrow.AsRoot }

                    DSingle(ownDomainId, Map.singleton resultColumn.Name newInfo)

                let punColumns =
                    if not flags.IsTopLevel then
                        Seq.empty
                    else
                        match finalField.Field with
                        | RColumnField { FieldType = FTScalar(SFTReference(newEntityRef, opts)) } ->
                            let mainArrow = { Name = funMain; AsRoot = false }

                            let punRef =
                                replacePath
                                    (Array.append resultRef.Ref.Path [| mainArrow |])
                                    (Array.append fieldInfo.Path [| newEntityRef |])

                            let (newPaths, punExpr) = compileLinkedFieldRef ctx paths punRef
                            paths <- newPaths
                            Seq.singleton (CCPun, resultMetaColumn punExpr)
                        | _ -> Seq.empty

                let replaceRef = replacePathInField layout resultRef true
                let attrColumns = getDefaultAttributes replaceRef resultRef finalRef None
                let myMeta = [ attrColumns; punColumns ] |> Seq.concat |> Map.ofSeq

                { Domains = Some newDomains
                  MetaColumns = Map.ofSeq systemColumns
                  Column =
                    { resultColumn with
                        Meta = Map.unionUnique resultColumn.Meta myMeta } }
            // Column references without paths.
            | FERef({ Ref = { Ref = VRColumn fieldRef } } as resultRef) as resultExpr when addMetaColumns ->
                // Entity ref always exists here after resolution step.
                let entityRef = Option.get fieldRef.Entity
                let tableRef = compileRenamedEntityRef entityRef
                let fromInfo = Map.find tableRef.Name fromMap
                let fieldInfo = ObjectMap.findType<FieldRefMeta> resultRef.Extra

                // Add system columns (id or sub_entity - this is a generic function).
                let makeMaybeSystemColumn
                    (needColumn: ResolvedFieldRef -> bool)
                    (columnConstr: int -> MetaType)
                    (systemName: FieldName)
                    =
                    let getSystemColumn (domain: Domain) =
                        match Map.tryFind fieldRef.Name domain with
                        | None -> SQL.nullExpr
                        | Some info ->
                            if needColumn info.Ref then
                                let colName =
                                    if info.IdColumn = idDefault then
                                        compileName systemName
                                    else
                                        columnName (CTMeta(columnConstr info.IdColumn))

                                SQL.VEColumn
                                    { Table = Some tableRef
                                      Name = colName }
                            else
                                SQL.nullExpr

                    match fromInfoExpression tableRef getSystemColumn fromInfo.FromType with
                    | SQL.VEValue SQL.VNull -> None
                    | systemExpr -> Some systemExpr

                let maybeIdExpr = makeMaybeSystemColumn (fun _ -> true) CMId funId

                let (maybeIdCol, systemColumns) =
                    match maybeIdExpr with
                    | None -> (None, Seq.empty)
                    | Some idExpr ->
                        let (createIdCol, idCol) = getIdColumn (string idExpr)

                        let systemColumns =
                            if not createIdCol then
                                Seq.empty
                            else
                                let needsSubEntity (ref: ResolvedFieldRef) =
                                    Seq.length (allPossibleEntitiesList layout ref.Entity) > 1

                                let maybeSubEntityExpr =
                                    Option.map (fun x -> (CMSubEntity idCol, resultMetaColumn x))
                                    <| makeMaybeSystemColumn needsSubEntity CMSubEntity funSubEntity

                                Seq.append
                                    (Seq.singleton (CMId idCol, resultMetaColumn idExpr))
                                    (Option.toSeq maybeSubEntityExpr)

                        (Some idCol, systemColumns)

                let getNewDomain (domain: Domain) =
                    match Map.tryFind fieldRef.Name domain with
                    | Some info ->
                        let newInfo =
                            { info with
                                IdColumn = Option.get maybeIdCol
                                AsRoot = info.AsRoot || resultRef.Ref.AsRoot }

                        Map.singleton resultColumn.Name newInfo
                    | None -> Map.empty

                let newDomains =
                    match fromInfo.FromType with
                    | FTEntity(domainId, domain) -> DSingle(domainId, getNewDomain domain)
                    | FTSubquery info -> mapDomainsTree getNewDomain info.Domains

                let rec getDomainColumns =
                    function
                    | DSingle(id, domain) -> Seq.empty
                    | DMulti(ns, nested) ->
                        let colName = CMDomain ns

                        let metaCol =
                            resultMetaColumn
                            <| SQL.VEColumn
                                { Table = Some tableRef
                                  Name = columnName (CTMeta colName) }

                        let col = (colName, metaCol)
                        Seq.append (Seq.singleton col) (nested |> Map.values |> Seq.collect getDomainColumns)

                // Propagate domain columns from subquery,
                let subqueryDomainColumns =
                    match fromInfo.FromType with
                    | FTEntity _ -> Seq.empty
                    | FTSubquery info -> getDomainColumns info.Domains

                let punColumns =
                    if not flags.IsTopLevel then
                        Seq.empty
                    else
                        let getPunColumn (domain: Domain) =
                            match Map.tryFind fieldRef.Name domain with
                            | None -> SQL.nullExpr
                            | Some info ->
                                match layout.FindField info.Ref.Entity info.Ref.Name |> Option.get with
                                | { Field = RColumnField { FieldType = FTScalar(SFTReference(newEntityRef, opts)) } } ->
                                    let mainArrow = { Name = funMain; AsRoot = false }
                                    let newName = getFieldName fieldInfo fieldRef.Name

                                    let pathArgs =
                                        { Context = ctx
                                          Extra = ObjectMap.empty
                                          Paths = paths
                                          // We filter out puns for NULL references later in query, no need to do it in the database.
                                          CheckNullExpr = None
                                          TableRef = Some tableRef
                                          AsRoot = info.AsRoot || resultRef.Ref.AsRoot }

                                    let (newPaths, expr) =
                                        compileBoundPath pathArgs info.Ref newName [ (newEntityRef, mainArrow) ]

                                    paths <- newPaths
                                    expr
                                | _ -> SQL.nullExpr

                        match fromInfoExpression tableRef getPunColumn fromInfo.FromType with
                        | SQL.VEValue SQL.VNull -> Seq.empty
                        | punExpr ->
                            let res =
                                { Expression = punExpr
                                  Info = emptyColumnMetaInfo }

                            let col = (CCPun, res)
                            Seq.singleton col

                // Inherited and default attributes.
                let inheritedAttrColumns =
                    match fromInfo.FromType with
                    | FTEntity(domainId, domain) -> Seq.empty
                    | FTSubquery queryInfo ->
                        // Inherit column and cell attributes from subquery.
                        let makeInheritedAttr (colInfo: ColumnInfo) =
                            match colInfo.Type with
                            | CTColumnMeta(colName, CCCellAttribute name) when
                                colName = fieldRef.Name && not (Map.containsKey name result.Attributes)
                                ->
                                let attrCol = CCCellAttribute name

                                let attrExpr =
                                    SQL.VEColumn
                                        { Table = Some tableRef
                                          Name = columnName (CTColumnMeta(fieldRef.Name, attrCol)) }

                                let res =
                                    { Expression = attrExpr
                                      Info = colInfo.Info }

                                Some(attrCol, res)
                            | _ -> None

                        queryInfo.Columns |> Seq.mapMaybe makeInheritedAttr

                let defaultAttrColumns =
                    match fieldInfo.Bound with
                    | Some(BMColumn boundCol) when boundCol.Immediate ->
                        // All initial fields for given entity are always in a domain.
                        let single =
                            if boundCol.Single then
                                Some <| getFieldName fieldInfo fieldRef.Name
                            else
                                None

                        getDefaultAttributes (replaceEntityRefInField (Some entityRef)) resultRef boundCol.Ref single
                    | Some(BMArgument boundArg) when boundArg.Immediate ->
                        let argInfo = Map.find boundArg.Ref arguments.Types
                        getDefaultArgumentAttributes resultExpr argInfo
                    | _ -> Seq.empty

                let myMeta =
                    [ inheritedAttrColumns; defaultAttrColumns; punColumns ]
                    |> Seq.concat
                    |> Map.ofSeq

                { Domains = Some newDomains
                  MetaColumns = [ systemColumns; subqueryDomainColumns ] |> Seq.concat |> Map.ofSeq
                  Column =
                    { resultColumn with
                        Meta = Map.unionUnique resultColumn.Meta myMeta } }
            // Argument references without paths.
            | FERef({ Ref = { Ref = VRArgument arg
                              AsRoot = asRoot } }) as argExpr when addMetaColumns ->
                let metaColumns =
                    if not flags.IsTopLevel then
                        Seq.empty
                    else
                        let argInfo = Map.find arg arguments.Types

                        let punColumns =
                            match argInfo.FieldType with
                            | FTScalar(SFTReference(newEntityRef, opts)) ->
                                let mainArrow = { Name = funMain; AsRoot = false }

                                let selectExpr =
                                    compileReferenceArgument
                                        ObjectMap.empty
                                        ctx
                                        argInfo
                                        asRoot
                                        [| mainArrow |]
                                        [| newEntityRef |]

                                let punCol =
                                    { Expression = SQL.VESubquery selectExpr
                                      Info = emptyColumnMetaInfo }

                                Seq.singleton (CCPun, punCol)
                            | _ -> Seq.empty

                        let attributeColumns = getDefaultArgumentAttributes argExpr argInfo

                        Seq.concat [ punColumns; attributeColumns ]

                let myMeta = Map.ofSeq metaColumns

                { Domains = None
                  MetaColumns = Map.empty
                  Column =
                    { resultColumn with
                        Meta = Map.unionUnique resultColumn.Meta myMeta } }
            | _ ->
                { Domains = None
                  MetaColumns = Map.empty
                  Column = resultColumn }

        let getResultEntry (i: int) : ResolvedQueryResult -> ResultColumn =
            function
            | QRAll alias -> failwith "Impossible QRAll"
            | QRExpr result -> getResultColumnEntry i result

        let resultEntries = Array.mapi getResultEntry select.Results
        let resultColumns = resultEntries |> Array.map (fun x -> x.Column)
        let emptyDomains = DSingle(ownDomainId, Map.empty)

        let checkSameMeta (a: ResultMetaColumn) (b: ResultMetaColumn) =
            assert (string a.Expression = string b.Expression)
            b

        let metaColumns =
            resultEntries
            |> Seq.map (fun c -> c.MetaColumns)
            |> Seq.fold (Map.unionWith checkSameMeta) Map.empty

        let (newDomains, metaColumns) =
            if not addMetaColumns then
                (emptyDomains, metaColumns)
            else
                let newDomains =
                    resultEntries
                    |> Seq.mapMaybe (fun entry -> entry.Domains)
                    |> Seq.fold mergeDomains emptyDomains

                let mainIdColumns =
                    match flags.MainEntity with
                    | None -> Seq.empty
                    | Some mainRef ->
                        let findMainValue
                            (getValueName: FromInfo -> SQL.ColumnName option)
                            (name, info: FromInfo)
                            : SQL.ColumnRef option =
                            match getValueName info with
                            | Some id ->
                                Some
                                    { Table = Some { Schema = None; Name = name }
                                      Name = id }
                            | None -> None

                        let mainId =
                            Map.toSeq fromMap
                            |> Seq.mapMaybe (findMainValue (fun x -> x.MainId))
                            |> Seq.exactlyOne

                        let idCol = (CMMainId, resultMetaColumn <| SQL.VEColumn mainId)

                        let subEntityCols =
                            let mainEntity = getEntityByRef mainRef

                            if Map.isEmpty mainEntity.Children then
                                Seq.empty
                            else
                                let mainSubEntity =
                                    Map.toSeq fromMap
                                    |> Seq.mapMaybe (findMainValue (fun x -> x.MainSubEntity))
                                    |> Seq.exactlyOne

                                let subEntityCol = (CMMainSubEntity, resultMetaColumn <| SQL.VEColumn mainSubEntity)
                                Seq.singleton subEntityCol

                        Seq.append (Seq.singleton idCol) subEntityCols

                let newMetaColumns = [ mainIdColumns; attributeColumns ] |> Seq.concat |> Map.ofSeq
                let metaColumns = Map.unionUnique metaColumns newMetaColumns
                (newDomains, metaColumns)

        let orderLimit =
            let (newPaths, ret) = compileOrderLimitClause ctx paths select.OrderLimit
            paths <- newPaths
            ret

        assert (Map.isEmpty paths.Map || Option.isSome from)

        let (fromInfo, from) =
            match from with
            | None -> (None, None)
            | Some from ->
                let newJoins = Map.difference paths.Map fromPaths.Map

                let (entitiesMap, from) =
                    buildJoins layout (fromToEntitiesMap fromMap) from (joinsToSeq newJoins)

                let info =
                    { Entities = entitiesMap
                      Joins = paths
                      WhereWithoutSubentities = whereWithoutSubentities }

                (Some info, Some from)

        let info =
            { Domains = newDomains
              MetaColumns = metaColumns
              Columns = resultColumns
              FromInfo = fromInfo }
            : HalfCompiledSingleSelect

        let query =
            { Columns = [||] // We fill columns after processing UNIONs, so that ordering and number of columns and meta-columns is the same everywhere.
              From = from
              Where = where
              GroupBy = groupBy
              OrderLimit = orderLimit
              Locking = None
              Extra = info }
            : SQL.SingleSelectExpr

        (info, query)

    and compileColumnResult
        (ctx: ExprContext)
        (paths0: JoinPaths)
        (isTopLevel: bool)
        (result: ResolvedQueryColumnResult)
        : JoinPaths * SelectColumn =
        let mutable paths = paths0

        let (newPaths, newExpr) =
            match result.Result with
            | FERef ref when not isTopLevel ->
                // When used in sub-select, we don't replace subentity with its JSON representation.
                compileLinkedFieldRef { ctx with RefContext = RCTypeExpr } paths ref
            | _ -> compileFieldExpr ctx paths result.Result

        paths <- newPaths

        let compileAttr (attrName, attr: ResolvedBoundAttribute) =
            let attrCol = CCCellAttribute attrName

            let (newPaths, ret) =
                compileBoundAttributeExpr ctx paths result.Result attr.Expression

            let mapping = boundAttributeToMapping attr.Expression

            let info =
                { Mapping = mapping
                  Dependency = attr.Dependency
                  Internal = attr.Internal
                  SingleRow = if attr.Dependency = DSPerRow then None else Some ret
                  ValueType = None }

            paths <- newPaths
            let ret = { Expression = ret; Info = info }
            (attrCol, ret)

        let attrs = result.Attributes |> Map.toSeq |> Seq.map compileAttr |> Map.ofSeq

        let name =
            match result.TryToName() with
            | None -> newTempFieldName ()
            | Some name -> TName name

        let ret =
            { Name = name
              Column = newExpr
              Meta = attrs }

        (paths, ret)

    and compileFromTableExpr
        (ctx: ExprContext)
        (nextJoinId: int)
        (mainEntity: ResolvedEntityRef option)
        (isInner: bool)
        (tableExpr: ResolvedFromTableExpr)
        : FromResult * SQL.FromExpr =
        let compileSubSelect (alias: EntityAlias) (subsel: ResolvedSelectExpr) =
            let flags =
                { MainEntity = mainEntity
                  IsTopLevel = false
                  MetaColumns = true }

            let (info, expr) = compileSelectExpr flags ctx None subsel

            let (info, fields) =
                match alias.Fields with
                | None -> (finalSelectInfo info, None)
                | Some fields ->
                    let info = renameSelectInfo fields info
                    let fields = info.Columns |> Array.map (fun col -> columnName col.Type)
                    (info, Some fields)

            let compiledAlias =
                { Name = compileName alias.Name
                  Columns = fields }
                : SQL.TableAlias

            let compiledSubsel =
                { Alias = Some compiledAlias
                  Expression = SQL.TESelect expr
                  Lateral = tableExpr.Lateral }
                : SQL.FromTableExpr

            let ret = SQL.FTableExpr compiledSubsel
            let fromInfo = subentityFromInfo mainEntity info

            let res =
                { Tables = Map.singleton compiledAlias.Name fromInfo
                  Joins =
                    { emptyJoinPaths with
                        NextJoinId = nextJoinId } }

            (res, ret)

        let compileDomainValues (fieldType: ResolvedFieldType) (flags: DomainExprInfo) =
            match fieldType with
            | FTScalar(SFTEnum vals) ->
                let compileEnumValue = FString >> FEValue >> VVExpr >> Array.singleton
                let values = vals |> OrderedSet.toArray |> Array.map compileEnumValue |> SValues

                let alias =
                    { tableExpr.Alias with
                        Fields = Some [| enumDomainValuesColumn |] }

                compileSubSelect alias (selectExpr values)
            | _ -> failwith "Invalid domain type"

        match tableExpr.Expression with
        | TESelect subsel -> compileSubSelect tableExpr.Alias subsel
        | TEDomain(ref, flags) ->
            let fieldType = resolvedRefType layout ref |> Option.get
            compileDomainValues fieldType flags
        | TEFieldDomain(entityRef, fieldName, flags) ->
            let resRef = getResolvedEntityRef entityRef
            let entity = layout.FindEntity resRef |> Option.get
            let field = entity.FindField fieldName |> Option.get
            let fieldType = resolvedFieldType field.Field
            compileDomainValues fieldType flags
        | TETypeDomain(fieldType, flags) -> compileDomainValues (getResolvedFieldType fieldType) flags

    // See description of `RealEntityAnnotation.IsInner`.
    and compileFromExpr
        (ctx: ExprContext)
        (nextJoinId: int)
        (mainEntity: ResolvedEntityRef option)
        (isInner: bool)
        : ResolvedFromExpr -> FromResult * SQL.FromExpr =
        function
        | FEntity({ Ref = { Schema = Some schema; Name = name } } as from) ->
            let entityRef = { Schema = schema; Name = name }
            let entity = getEntityByRef entityRef

            let makeDomainEntry name field =
                { Ref = { Entity = entityRef; Name = name }
                  RootEntity = entity.Root
                  IdColumn = idDefault
                  AsRoot = false }

            let domain = mapAllFields makeDomainEntry entity

            let mainEntry =
                { Ref =
                    { Entity = entityRef
                      Name = entity.MainField }
                  RootEntity = entity.Root
                  IdColumn = idDefault
                  AsRoot = false }

            let domain = Map.add funMain mainEntry domain
            let newAlias = compileAliasFromEntity entityRef from.Alias

            let ann =
                { RealEntity = entityRef
                  FromPath = false
                  IsInner = isInner
                  AsRoot = false }
                : RealEntityAnnotation

            let tableRef = compileResolvedEntityRef entity.Root

            let (fromExpr, where) =
                if isInner then
                    let fTable =
                        { Extra = ann
                          Alias = Some newAlias
                          Table = tableRef }
                        : SQL.FromTable

                    let fromExpr = SQL.FTable fTable

                    let subEntityCol =
                        SQL.VEColumn
                            { Table = Some { Schema = None; Name = newAlias.Name }
                              Name = sqlFunSubEntity }

                    let checkExpr = makeEntityCheckExpr subEntityCol from layout entityRef
                    (fromExpr, checkExpr)
                else
                    let fTable = SQL.fromTable tableRef
                    let subEntityCol = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }
                    let checkExpr = makeEntityCheckExpr subEntityCol from layout entityRef

                    let select =
                        { SQL.emptySingleSelectExpr with
                            Columns = [| SQL.SCAll None |]
                            From = Some <| SQL.FTable fTable
                            Where = checkExpr }

                    let expr =
                        { Extra = ann
                          CTEs = None
                          Tree = SQL.SSelect select }
                        : SQL.SelectExpr

                    let subsel = SQL.subSelectExpr newAlias expr
                    let subExpr = SQL.FTableExpr subsel
                    (subExpr, None)

            let entityInfo =
                { Ref = entityRef
                  IsInner = isInner
                  AsRoot = from.AsRoot
                  Check = where }

            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let subEntity =
                        if Map.isEmpty entity.Children then
                            None
                        else
                            Some sqlFunSubEntity

                    (Some sqlFunId, subEntity)

            let fromInfo =
                { FromType = FTEntity(newGlobalDomainId (), domain)
                  Entity = Some entityInfo
                  MainId = mainId
                  MainSubEntity = mainSubEntity
                  Attributes = emptyEntityAttributes }

            usedDatabase <- addUsedEntityRef entityRef usedEntitySelect usedDatabase

            let res =
                { Tables = Map.singleton newAlias.Name fromInfo
                  Joins =
                    { emptyJoinPaths with
                        NextJoinId = nextJoinId } }

            (res, fromExpr)
        | FEntity { Ref = { Schema = None; Name = name }
                    Alias = pun } ->
            let name' = compileName name

            let selectSig =
                match Map.tryFind name' ctx.CTEs with
                | None ->
                    // Most likely a temporary table.
                    emptySelectInfo
                | Some info -> info

            let compiledAlias = Option.map compileAliasFromName pun
            let newName = Option.defaultValue name pun
            let fromInfo = subentityFromInfo mainEntity selectSig

            let fTable =
                { Extra = NoEntityAnnotation
                  Alias = compiledAlias
                  Table = { Schema = None; Name = name' } }
                : SQL.FromTable

            let from = SQL.FTable fTable

            let res =
                { Tables = Map.singleton (compileName newName) fromInfo
                  Joins =
                    { emptyJoinPaths with
                        NextJoinId = nextJoinId } }

            (res, from)
        | FTableExpr expr -> compileFromTableExpr ctx nextJoinId mainEntity isInner expr
        | FJoin join ->
            // We should redo main entities; this duplication is ugly.
            let main1 =
                match join.Type with
                | Left -> mainEntity
                | _ -> None

            let isInner1 =
                match join.Type with
                | Left
                | Inner -> isInner
                | _ -> false

            let (fromRes1, r1) = compileFromExpr ctx nextJoinId main1 isInner1 join.A

            let main2 =
                match join.Type with
                | Right -> mainEntity
                | _ -> None

            let isInner2 =
                match join.Type with
                | Right
                | Inner -> isInner
                | _ -> false

            let (fromRes2, r2) =
                compileFromExpr ctx fromRes1.Joins.NextJoinId main2 isInner2 join.B

            let fromMap = Map.unionUnique fromRes1.Tables fromRes2.Tables

            let joinPaths =
                { Map = Map.unionUnique fromRes1.Joins.Map fromRes2.Joins.Map
                  NextJoinId = fromRes2.Joins.NextJoinId
                  Namespace = ctx.JoinNamespace }

            let (newJoinPaths, joinExpr) = compileFieldExpr ctx joinPaths join.Condition
            let augmentedJoinPaths = Map.difference newJoinPaths.Map joinPaths.Map
            // Split augmented joins to joins for left and right part of the expression.
            let (newJoinPathsA, newJoinPathsB) =
                augmentedJoinPaths
                |> Map.toSeq
                |> Seq.partition (fun (key, join) -> Map.containsKey key.Table fromRes1.Tables)

            assert
                (newJoinPathsB
                 |> Seq.forall (fun (key, join) -> Map.containsKey key.Table fromRes2.Tables))

            let (fromMap, r1) =
                buildInternalJoins layout fromMap r1 (Seq.collect joinToSeq newJoinPathsA)

            let (fromMap, r2) =
                buildInternalJoins layout fromMap r2 (Seq.collect joinToSeq newJoinPathsB)

            let join =
                { Type = compileJoin join.Type
                  A = r1
                  B = r2
                  Condition = joinExpr }
                : SQL.JoinExpr

            let res =
                { Tables = fromMap
                  Joins = newJoinPaths }

            (res, SQL.FJoin join)

    and compileInsertValue (ctx: ExprContext) (paths0: JoinPaths) : ResolvedValuesValue -> JoinPaths * SQL.InsertValue =
        function
        | VVDefault -> (paths0, SQL.IVDefault)
        | VVExpr expr ->
            let (paths, newExpr) = compileFieldExpr ctx paths0 expr
            assert (Map.isEmpty paths.Map)
            (paths, SQL.IVExpr newExpr)

    and compileInsertExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (insert: ResolvedInsertExpr)
        : TempSelectInfo * SQL.InsertExpr =
        let (ctx, ctes) =
            match insert.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (newCtx, newCtes) = compileCommonTableExprs flags ctx ctes
                (newCtx, Some newCtes)

        let entityRef = getResolvedEntityRef insert.Entity.Ref
        let entity = getEntityByRef entityRef
        let (opAlias, opInfo, opTable) = compileOperationEntity insert.Entity

        let fields =
            Array.map (fun fieldName -> Map.find fieldName entity.ColumnFields) insert.Fields

        let (subEntityCol, subEntityExpr) =
            if hasSubType entity then
                let expr = SQL.VEValue(SQL.VString entity.TypeName)
                (Seq.singleton (null, sqlFunSubEntity), Some expr)
            else
                (Seq.empty, None)

        let usedFields =
            insert.Fields
            |> Seq.map (fun fieldName -> (fieldName, usedFieldInsert))
            |> Map.ofSeq

        let usedEntity =
            { usedEntityInsert with
                Fields = usedFields }

        usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase

        let compileField fieldName (field: ResolvedColumnField) =
            let extra = { Name = fieldName }: RealFieldAnnotation
            (extra :> obj, field.ColumnName)

        let columns =
            Seq.append subEntityCol (Seq.map2 compileField insert.Fields fields)
            |> Seq.toArray

        let compileInsertValues vals =
            let newVals = Seq.map (compileInsertValue ctx emptyJoinPaths >> snd) vals

            match subEntityExpr with
            | None -> Array.ofSeq newVals
            | Some subExpr -> Seq.append (Seq.singleton (SQL.IVExpr subExpr)) newVals |> Array.ofSeq

        let source =
            match insert.Source with
            | ISDefaultValues ->
                match subEntityExpr with
                | None -> SQL.ISDefaultValues
                | Some expr ->
                    let defVals = Seq.replicate (Array.length insert.Fields) SQL.IVDefault
                    let row = Seq.append (Seq.singleton (SQL.IVExpr expr)) defVals |> Seq.toArray
                    SQL.ISValues [| row |]
            | ISSelect { CTEs = None; Tree = SValues allVals } ->
                let values = Array.map compileInsertValues allVals
                SQL.ISValues values
            | ISSelect select ->
                let (info, newSelect) = compileSelectExpr subSelectFlags ctx None select

                let newSelect =
                    match subEntityExpr with
                    | None -> newSelect
                    | Some expr ->
                        let subEntityCol = Seq.singleton <| SQL.SCExpr(None, expr)
                        prependColumnsToSelect subEntityCol newSelect

                SQL.ISSelect newSelect

        let ret =
            { CTEs = ctes
              Table = opTable
              Columns = columns
              Source = source
              OnConflict = None
              Returning = [||]
              Extra = null }
            : SQL.InsertExpr

        (emptySelectInfo, ret)

    and compileUpdateAssignExpr
        (ctx: ExprContext)
        (entityRef: ResolvedEntityRef)
        (entity: ResolvedEntity)
        (paths0: JoinPaths)
        (assignExpr: ResolvedUpdateAssignExpr)
        : JoinPaths * SQL.UpdateAssignExpr =
        let compileAssignField name =
            let field = Map.find name entity.ColumnFields
            let extra = { Name = name }: RealFieldAnnotation

            { Name = field.ColumnName
              Extra = extra }
            : SQL.UpdateColumnName

        match assignExpr with
        | UAESet(name, expr) ->
            let (paths, newExpr) = compileInsertValue ctx paths0 expr

            let usedEntity =
                { usedEntityUpdate with
                    Fields = Map.singleton name usedFieldUpdate }

            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
            (paths, SQL.UAESet(compileAssignField name, newExpr))
        | UAESelect(cols, select) ->
            let newCols = Array.map compileAssignField cols
            let (info, newSelect) = compileSelectExpr subSelectFlags ctx None select

            let usedEntity =
                { usedEntityUpdate with
                    Fields = cols |> Seq.map (fun name -> (name, usedFieldUpdate)) |> Map.ofSeq }

            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
            (paths0, SQL.UAESelect(newCols, newSelect))

    and compileGenericUpdateExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (update: ResolvedUpdateExpr)
        : TempSelectInfo * SQL.UpdateExpr =
        let (ctx, ctes) =
            match update.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (newCtx, newCtes) = compileCommonTableExprs flags ctx ctes
                (newCtx, Some newCtes)

        let entityRef = getResolvedEntityRef update.Entity.Ref
        let entity = getEntityByRef entityRef
        let (opAlias, opInfo, opTable) = compileOperationEntity update.Entity
        let fromMap = Map.singleton opAlias opInfo

        let (fromMap, fromPaths, from) =
            match update.From with
            | Some from ->
                let (fromRes, newFrom) = compileFromExpr ctx 0 flags.MainEntity true from
                let fromMap = Map.unionUnique fromMap fromRes.Tables
                (fromMap, fromRes.Joins, Some newFrom)
            | None -> (fromMap, emptyJoinPaths, None)

        let mutable paths = emptyJoinPaths

        let compileField fieldName expr =
            let field = Map.find fieldName entity.ColumnFields
            let extra = { Name = fieldName }: RealFieldAnnotation
            let (newPaths, newExpr) = compileFieldExpr ctx paths expr
            paths <- newPaths
            (field.ColumnName, (extra :> obj, newExpr))

        let compileField fieldName expr =
            let field = Map.find fieldName entity.ColumnFields
            let extra = { Name = fieldName }: RealFieldAnnotation
            let (newPaths, newExpr) = compileFieldExpr ctx paths expr
            paths <- newPaths
            (field.ColumnName, (extra :> obj, newExpr))

        let compileOneAssign assign =
            let (newPaths, newAssign) =
                compileUpdateAssignExpr ctx entityRef entity paths assign

            paths <- newPaths
            newAssign

        let assigns = Array.map compileOneAssign update.Assignments

        let where =
            match update.Where with
            | None -> None
            | Some whereExpr ->
                let (newPaths, newWhere) = compileFieldExpr ctx paths whereExpr
                paths <- newPaths
                Some newWhere

        let newPathsMap = Map.difference paths.Map fromPaths.Map
        // If there are joins with updated table, we need to join an intermediate table first, because there is no way to do a left join with updated table in UPDATEs.

        let mutable selfJoinNeeded = false

        let remapPath (key: JoinKey) (tree: JoinTree) =
            if key.Table = opAlias then
                selfJoinNeeded <- true
                ({ key with Table = selfTableName }, tree)
            else
                (key, tree)

        let remappedPathsMap = Map.mapWithKeys remapPath newPathsMap

        let (where, from) =
            if not selfJoinNeeded then
                (where, from)
            else
                let joinedUpdateId =
                    { Table = Some selfTableRef
                      Name = sqlFunId }
                    : SQL.ColumnRef

                let updateId =
                    { Table = Some { Schema = None; Name = opAlias }
                      Name = sqlFunId }
                    : SQL.ColumnRef

                let joinSame =
                    SQL.VEBinaryOp(SQL.VEColumn updateId, SQL.BOEq, SQL.VEColumn joinedUpdateId)

                let fromTable =
                    { Table = opTable.Table
                      Alias = Some { Name = selfTableName; Columns = None }
                      Extra = NoEntityAnnotation }
                    : SQL.FromTable

                let (where, fromExpr) =
                    match from with
                    | None ->
                        let whereExpr =
                            match where with
                            | None -> joinSame
                            | Some whereExpr -> SQL.VEAnd(whereExpr, joinSame)

                        (Some whereExpr, SQL.FTable fromTable)
                    | Some fromExpr ->
                        let joinExpr =
                            { A = fromExpr
                              B = SQL.FTable fromTable
                              Type = SQL.Full
                              Condition = joinSame }
                            : SQL.JoinExpr

                        (where, SQL.FJoin joinExpr)

                (where, Some fromExpr)

        let entitiesMap = fromToEntitiesMap fromMap

        let from =
            if Map.isEmpty remappedPathsMap then
                from
            else
                let (entitiesMap, fromExpr) =
                    buildJoins layout entitiesMap (Option.get from) (joinsToSeq remappedPathsMap)

                Some fromExpr

        let extra = { WhereWithoutSubentities = where }: UpdateFromInfo
        let whereWithChecks = addEntityChecks entitiesMap where

        let ret =
            { CTEs = ctes
              Table = opTable
              Assignments = assigns
              Where = whereWithChecks
              From = from
              Returning = [||]
              Extra = extra }
            : SQL.UpdateExpr

        (emptySelectInfo, ret)

    and useUpdateAssignExpr (entityRef: ResolvedEntityRef) =
        function
        | UAESet(name, expr) ->
            let usedEntity =
                { usedEntityUpdate with
                    Fields = Map.singleton name usedFieldUpdate }

            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        | UAESelect(cols, expr) ->
            let usedEntity =
                { usedEntityUpdate with
                    Fields = cols |> Seq.map (fun name -> (name, usedFieldUpdate)) |> Map.ofSeq }

            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase

    and compileUpdateExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (update: ResolvedUpdateExpr)
        : TempSelectInfo * SQL.UpdateExpr =
        let (selectInfo, ret) = compileGenericUpdateExpr flags ctx update
        let entityRef = getResolvedEntityRef update.Entity.Ref
        usedDatabase <- addUsedEntityRef entityRef usedEntityUpdate usedDatabase
        (selectInfo, ret)

    and compileDeleteExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (delete: ResolvedDeleteExpr)
        : TempSelectInfo * SQL.DeleteExpr =
        // We compile it as an UPDATE statement -- compilation code is complex and we don't want to repeat that.
        let update =
            { CTEs = delete.CTEs
              Entity = delete.Entity
              Assignments = [||]
              From = delete.Using
              Where = delete.Where
              Extra = delete.Extra }
            : ResolvedUpdateExpr

        let (selectInfo, updateRet) = compileGenericUpdateExpr flags ctx update
        let entityRef = getResolvedEntityRef update.Entity.Ref
        usedDatabase <- addUsedEntityRef entityRef usedEntityDelete usedDatabase

        let ret =
            { CTEs = updateRet.CTEs
              Table = updateRet.Table
              Where = updateRet.Where
              Using = updateRet.From
              Returning = updateRet.Returning
              Extra = updateRet.Extra }
            : SQL.DeleteExpr

        (selectInfo, ret)

    and compileDataExpr
        (flags: SelectFlags)
        (ctx: ExprContext)
        (dataExpr: ResolvedDataExpr)
        : TempSelectInfo * SQL.DataExpr =
        match dataExpr with
        | DESelect select ->
            let (fields, newSelect) = compileSelectExpr flags ctx None select
            (fields, SQL.DESelect newSelect)
        | DEInsert insert ->
            let (fields, newInsert) = compileInsertExpr flags ctx insert
            (fields, SQL.DEInsert newInsert)
        | DEUpdate update ->
            let (fields, newUpdate) = compileUpdateExpr flags ctx update
            (fields, SQL.DEUpdate newUpdate)
        | DEDelete delete ->
            let (fields, newDelete) = compileDeleteExpr flags ctx delete
            (fields, SQL.DEDelete newDelete)

    member this.CompileSingleFromExpr (from: ResolvedFromExpr) (where: ResolvedFieldExpr option) : CompiledSingleFrom =
        let emptyContext = 1
        let (fromRes, compiledFrom) = compileFromExpr rootExprContext 0 None true from

        let (newPaths, whereWithoutSubentities) =
            match where with
            | None -> (emptyJoinPaths, None)
            | Some where ->
                let (newPaths, ret) = compileFieldExpr rootExprContext fromRes.Joins where
                (newPaths, Some ret)

        let (entitiesMap, compiledFrom) =
            buildJoins layout (fromToEntitiesMap fromRes.Tables) compiledFrom (joinsToSeq newPaths.Map)

        let finalWhere = addEntityChecks entitiesMap whereWithoutSubentities

        { From = compiledFrom
          WhereWithoutSubentities = whereWithoutSubentities
          Where = finalWhere
          Entities = entitiesMap
          Joins = newPaths }

    member this.CompileSingleFieldExpr (flags: ExprCompilationFlags) (expr: ResolvedFieldExpr) =
        let ctx = { rootExprContext with Flags = flags }
        let (newPaths, ret) = compileFieldExpr ctx emptyJoinPaths expr

        if not <| Map.isEmpty newPaths.Map then
            failwith "Unexpected join paths in single field expression"

        ret

    member this.CompileSelectExpr
        (mainEntity: ResolvedEntityRef option)
        (metaColumns: bool)
        (select: ResolvedSelectExpr)
        =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns }

        compileSelectExpr flags rootExprContext None select

    member this.CompileDataExpr
        (mainEntity: ResolvedEntityRef option)
        (metaColumns: bool)
        (dataExpr: ResolvedDataExpr)
        =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns }

        compileDataExpr flags rootExprContext dataExpr

    member this.CompileArgumentAttributes(args: ResolvedArgumentsMap) : (CompiledColumnInfo * SQL.ValueExpr)[] =
        let compileArgAttr argName (name, attr: ResolvedBoundAttribute) =
            if attr.Internal then
                None
            else
                let linkedRef = PLocal argName |> VRArgument |> resolvedRefFieldExpr

                let (newPaths, colExpr) =
                    compileBoundAttributeExpr rootExprContext emptyJoinPaths linkedRef attr.Expression

                if not <| Map.isEmpty newPaths.Map then
                    failwith "Unexpected join paths in argument atribute expression"

                let colType = CTMeta <| CMArgAttribute(argName, name)

                let info =
                    { Mapping = boundAttributeToMapping attr.Expression
                      Dependency = attr.Dependency
                      Internal = attr.Internal
                      SingleRow = Some colExpr
                      ValueType = None }

                let column =
                    { Name = columnName colType
                      Type = colType
                      Info = info }

                Some(column, colExpr)

        let compileArg (pl: ArgumentRef, arg: ResolvedArgument) =
            match pl with
            | PLocal name -> arg.Attributes |> Map.toSeq |> Seq.mapMaybe (compileArgAttr name)
            | PGlobal name -> Seq.empty

        args |> OrderedMap.toSeq |> Seq.collect compileArg |> Seq.toArray

    member this.ColumnName name = columnName name

    member this.UsedDatabase = usedDatabase
    member this.Arguments = arguments

let rec private findSelectExprSingleRows
    (f: int -> SQL.ColumnName option -> SQL.ValueExpr -> 'b option)
    : SQL.SelectTreeExpr -> 'b seq =
    function
    | SQL.SSelect query ->
        let assignPure i res =
            match res with
            | SQL.SCAll _ -> None
            | SQL.SCExpr(name, expr) -> f i name expr

        Seq.mapiMaybe assignPure query.Columns
    | SQL.SValues vals -> Seq.empty
    | SQL.SSetOp setOp -> Seq.empty

and private findSelectSingleRows
    (f: int -> SQL.ColumnName option -> SQL.ValueExpr -> 'b option)
    (select: SQL.SelectExpr)
    : 'b seq =
    findSelectExprSingleRows f select.Tree

let rec private filterSelectExprColumns (f: int -> bool) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr =
    function
    | SQL.SSelect query ->
        let checkColumn i _ = f i

        SQL.SSelect
            { query with
                Columns = Array.filteri checkColumn query.Columns }
    | SQL.SValues values ->
        let checkColumn i _ = f i
        let mappedValues = values |> Array.map (Array.filteri checkColumn)
        SQL.SValues mappedValues
    | SQL.SSetOp setOp ->
        SQL.SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = filterSelectColumns f setOp.A
              B = filterSelectColumns f setOp.B
              OrderLimit = setOp.OrderLimit }

and private filterSelectColumns (f: int -> bool) (select: SQL.SelectExpr) : SQL.SelectExpr =
    let tree = filterSelectExprColumns f select.Tree
    { select with Tree = tree }

let rec private flattenDomains: Domains -> FlattenedDomains =
    function
    | DSingle(id, dom) -> Map.singleton id dom
    | DMulti(ns, subdoms) ->
        let mergeEqual k a b =
            if a = b then
                a
            else
                failwithf "Cannot merge equal domains with key %i: %O and %O" k a b

        subdoms
        |> Map.values
        // Duplicate domains are possible here if we flatten a recursive CTE.
        |> Seq.fold (fun m subdoms -> Map.unionWithKey mergeEqual m (flattenDomains subdoms)) Map.empty

type CompiledExprInfo =
    { Arguments: QueryArguments
      UsedDatabase: UsedDatabase }

let compileSingleFromExpr
    (globalFlags: CompilationFlags)
    (layout: Layout)
    (arguments: QueryArguments)
    (from: ResolvedFromExpr)
    (where: ResolvedFieldExpr option)
    : CompiledExprInfo * CompiledSingleFrom =
    let compiler =
        QueryCompiler(globalFlags, layout, emptyMergedDefaultAttributes, arguments)

    let ret = compiler.CompileSingleFromExpr from where

    let info =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase }

    (info, ret)

let compileSingleFieldExpr
    (globalFlags: CompilationFlags)
    (layout: Layout)
    (flags: ExprCompilationFlags)
    (arguments: QueryArguments)
    (expr: ResolvedFieldExpr)
    : CompiledExprInfo * SQL.ValueExpr =
    let compiler =
        QueryCompiler(globalFlags, layout, emptyMergedDefaultAttributes, arguments)

    let newExpr = compiler.CompileSingleFieldExpr flags expr

    let info =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase }

    (info, newExpr)

let compileSelectExpr
    (layout: Layout)
    (arguments: QueryArguments)
    (selectExpr: ResolvedSelectExpr)
    : CompiledExprInfo * SQL.SelectExpr =
    let compiler =
        QueryCompiler(defaultCompilationFlags, layout, emptyMergedDefaultAttributes, arguments)

    let (info, select) = compiler.CompileSelectExpr None false selectExpr

    let retInfo =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase }

    (retInfo, select)

let compileDataExpr
    (layout: Layout)
    (arguments: QueryArguments)
    (dataExpr: ResolvedDataExpr)
    : CompiledExprInfo * SQL.DataExpr =
    let compiler =
        QueryCompiler(defaultCompilationFlags, layout, emptyMergedDefaultAttributes, arguments)

    let (info, delete) = compiler.CompileDataExpr None false dataExpr

    let retInfo =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase }

    (retInfo, delete)

let private columnIsConst (info: CompiledColumnInfo) =
    match info.Info.Dependency with
    | DSConst -> true
    | _ -> false

let private convertTempColumnInfo (compiler: QueryCompiler) (info: TempColumnInfo) : CompiledColumnInfo =
    let finalName = mapColumnTypeFields getFinalName info.Type

    { Type = finalName
      Name = compiler.ColumnName finalName
      Info =
        { info.Info with
            ValueType = columnSQLType finalName } }

let compileViewExpr
    (layout: Layout)
    (defaultAttrs: MergedDefaultAttributes)
    (viewExpr: ResolvedViewExpr)
    : CompiledViewExpr =
    let mainEntityRef =
        viewExpr.MainEntity |> Option.map (fun main -> getResolvedEntityRef main.Entity)

    let arguments = compileArguments viewExpr.Arguments

    let compiler =
        QueryCompiler(defaultCompilationFlags, layout, defaultAttrs, arguments)

    let (info, expr) = compiler.CompileSelectExpr mainEntityRef true viewExpr.Select
    let allColumns = info.Columns |> Array.map (convertTempColumnInfo compiler)
    let argAttrs = compiler.CompileArgumentAttributes viewExpr.Arguments

    let columnIsSingleRow i name col =
        let info = allColumns.[i]

        if info.Info.Dependency <> DSPerRow && not info.Info.Internal then
            Some(info, Option.get info.Info.SingleRow)
        else
            None

    let nonPerRowColumns = findSelectSingleRows columnIsSingleRow expr

    let columnInfoIsPerRow info =
        info.Info.Dependency = DSPerRow && not info.Info.Internal

    let columnIsPerRow i = columnInfoIsPerRow allColumns.[i]
    let perRowExpr = filterSelectColumns columnIsPerRow expr
    let perRowColumns = Array.filter columnInfoIsPerRow allColumns

    let allNonPerRowColumns = Seq.append argAttrs nonPerRowColumns

    let (constColumns, singleRowColumns) =
        Seq.partition (fst >> columnIsConst) allNonPerRowColumns

    let attrQuery =
        { ConstColumns = Array.ofSeq constColumns
          SingleRowColumns = Array.ofSeq singleRowColumns }

    let domains = mapDomainsFields getFinalName info.Domains
    let flattenedDomains = flattenDomains domains

    let compilePragma name v = (compileName name, compileFieldValue v)

    let mainRootEntity =
        match mainEntityRef with
        | None -> None
        | Some mainRef ->
            let mainEntity = layout.FindEntity mainRef |> Option.get
            Some mainEntity.Root

    { Pragmas = Map.mapWithKeys compilePragma viewExpr.Pragmas
      SingleRowQuery = attrQuery
      Query =
        { Expression = perRowExpr
          Arguments = compiler.Arguments }
      UsedDatabase = flattenUsedDatabase layout compiler.UsedDatabase
      Columns = perRowColumns
      Domains = domains
      FlattenedDomains = flattenedDomains
      MainRootEntity = mainEntityRef }

let compileCommandExpr (layout: Layout) (cmdExpr: ResolvedCommandExpr) : CompiledCommandExpr =
    let arguments = compileArguments cmdExpr.Arguments

    let compiler =
        QueryCompiler(defaultCompilationFlags, layout, emptyMergedDefaultAttributes, arguments)

    let (info, expr) = compiler.CompileDataExpr None false cmdExpr.Command
    let argAttrs = compiler.CompileArgumentAttributes cmdExpr.Arguments
    let arguments = compiler.Arguments

    let compilePragma name v = (compileName name, compileFieldValue v)

    { Pragmas = Map.mapWithKeys compilePragma cmdExpr.Pragmas
      Command =
        { Expression = expr
          Arguments = arguments }
      UsedDatabase = flattenUsedDatabase layout compiler.UsedDatabase }
