module FunWithFlags.FunDB.FunQL.Compile

open System
open FSharpPlus
open NodaTime
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Typecheck
open FunWithFlags.FunDB.FunQL.UsedReferences
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Merge
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type DomainIdColumn = int

type CompileException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        CompileException (message, innerException, isUserException innerException)

    new (message : string) = CompileException (message, null, true)

// These are not the same domains as in Layout!
//
// Domains are a way to distinguish rows after set operations so that row types and IDs can be traced back.
// Domain is a map of assigned references to fields to each column.
// Domain key is cross product of all local domain ids i.e. map from domain namespace ids to local domain ids.
// Local domain ids get assigned to all sets in a set operation.
// Each row has a different domain key assigned, so half of rows may come from entity A and half from entity B.
// This way one can make use if IDs assigned to cells to reference and update them.
[<NoEquality; NoComparison>]
type DomainField =
    { Ref : ResolvedFieldRef
      // Needed for fast parsing of subentity names.
      RootEntity : ResolvedEntityRef
      // A field with assigned `IdColumn` of 42 will use id column "__id__42" and sub-entity column "__sub_entity__42"
      IdColumn : DomainIdColumn
      // If this field is not filtered according to user role.
      AsRoot : bool
    }

let private idDefault : DomainIdColumn = 0

type GenericDomain<'e> when 'e : comparison = Map<'e, DomainField>
type GlobalDomainId = int
type DomainNamespaceId = int
type LocalDomainId = int
[<NoEquality; NoComparison>]
type GenericDomains<'e> when 'e : comparison =
    | DSingle of GlobalDomainId * GenericDomain<'e>
    | DMulti of DomainNamespaceId * Map<LocalDomainId, GenericDomains<'e>>

type Domain = GenericDomain<FieldName>
type Domains = GenericDomains<FieldName>

let rec private mapDomain (f : GenericDomain<'a> -> GenericDomain<'b>) : GenericDomains<'a> -> GenericDomains<'b> = function
    | DSingle (id, dom) -> DSingle (id, f dom)
    | DMulti (ns, doms) -> DMulti (ns, Map.map (fun name -> mapDomain f) doms)

let private mapDomainsFields (f : 'e1 -> 'e2) = mapDomain (Map.mapKeys f)

let private renameDomainFields (names : Map<'e1, 'e2>) = mapDomainsFields (fun k -> Map.find k names)

let private tryRenameDomainFields (names : Map<'e, 'e>) = mapDomainsFields (fun k -> Option.defaultValue k (Map.tryFind k names))

let rec private appendDomain (dom : GenericDomain<'e>) (doms : GenericDomains<'e>) : GenericDomains<'e> =
    match doms with
    | DSingle (id, d) -> DSingle (id, Map.unionUnique dom d)
    | DMulti (ns, subdomains) -> DMulti (ns, Map.map (fun name doms -> appendDomain dom doms) subdomains)

let rec private mergeDomains (doms1 : GenericDomains<'e>) (doms2 : GenericDomains<'e>) : GenericDomains<'e> =
    match (doms1, doms2) with
    | (DSingle (oldId, dom), doms)
    | (doms, DSingle (oldId, dom)) -> appendDomain dom doms
    | (DMulti (ns1, subdoms1), (DMulti (ns2, subdoms2))) when ns1 = ns2 ->
        DMulti (ns1,  Map.unionWith (fun nsId -> mergeDomains) subdoms1 subdoms2)
    | (DMulti (ns1, subdoms1), (DMulti _ as doms2)) ->
        DMulti (ns1, Map.map (fun name doms1 -> mergeDomains doms1 doms2) subdoms1)

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

// FIXME: drop when we implement typecheck.
let metaSQLType : MetaType -> SQL.SimpleType option = function
    | CMRowAttribute _ -> None
    | CMArgAttribute _ -> None
    | CMDomain _ -> Some SQL.STInt
    | CMId _ -> Some SQL.STInt
    | CMSubEntity _ -> Some SQL.STString
    | CMMainId -> Some SQL.STInt
    | CMMainSubEntity -> Some SQL.STInt

let private mapColumnTypeFields (f : 'a -> 'b) : GenericColumnType<'a> -> GenericColumnType<'b> = function
    | CTMeta meta -> CTMeta meta
    | CTColumnMeta (name, meta) -> CTColumnMeta (f name, meta)
    | CTColumn name -> CTColumn (f name)

let columnSQLType : GenericColumnType<'a> -> SQL.SimpleType option = function
    | CTMeta meta -> metaSQLType meta
    | CTColumnMeta (name, meta) -> None
    | CTColumn name ->None

type ColumnType = GenericColumnType<FieldName>

type private NameReplacer () =
    let mutable lastIds : Map<string, int> = Map.empty
    let mutable existing : Map<string, SQL.SQLName> = Map.empty

    member this.ConvertName (name : string) =
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
type private TempFieldName = TName of FieldName
                           | TTemp of int

let private unionTempName (a : TempFieldName) (b : TempFieldName) : TempFieldName =
    match (a, b) with
    | (TTemp _, TName name)
    | (TName name, TTemp _) -> TName name
    | (a, b) -> a

let private getFinalName : TempFieldName -> FieldName = function
    | TName name -> name
    | TTemp i -> failwith "Encountered a temporary name where not expected"

type private TempColumnType = GenericColumnType<TempFieldName>
type private TempDomain = GenericDomain<TempFieldName>
type private TempDomains = GenericDomains<TempFieldName>

// Expressions are not fully assembled right away. Instead we build SELECTs with no selected columns
// and this record in `extra`. During first pass through a union expression we collect signatures
// of all sub-SELECTs and build an ordered list of all columns, including meta-. Then we do a second
// pass, filling missing columns in individual sub-SELECTs with NULLs. This ensures the number and order
// of columns is consistent in all subexpressions, and allows user to omit attributes in some
// sub-expressions,
[<NoEquality; NoComparison>]
type private SelectColumn =
    { Name : TempFieldName
      Column : SQL.ValueExpr
      Meta : Map<ColumnMetaType, SQL.ValueExpr>
    }

type FromEntityInfo =
    { Ref : ResolvedEntityRef
      IsInner : bool
      AsRoot : bool
      Check : SQL.ValueExpr option
    }

type FromEntitiesMap = Map<SQL.TableName, FromEntityInfo>

type JoinKey =
    { Table : SQL.TableName
      Column : SQL.ColumnName
      ToRootEntity : ResolvedEntityRef
      AsRoot : bool
    }

type JoinPath =
    { RealEntity : ResolvedEntityRef // Real entity, may differ from `ToRootEntity`.
      Name : SQL.TableName
    }

type JoinTree =
    { Path : JoinPath
      Nested : JoinPathsMap
    }

and JoinPathsMap = Map<JoinKey, JoinTree>

type JoinPathsPair = (JoinKey * JoinPath)

type JoinPaths =
    { NextJoinId : int
      Map : JoinPathsMap
    }

let emptyJoinPaths =
    { NextJoinId = 0
      Map = Map.empty
    }

[<NoEquality; NoComparison>]
type SelectFromInfo =
    { Entities : FromEntitiesMap
      Joins : JoinPaths
      WhereWithoutSubentities : SQL.ValueExpr option
    }

[<NoEquality; NoComparison>]
type UpdateFromInfo =
    { WhereWithoutSubentities : SQL.ValueExpr option
    }

[<NoEquality; NoComparison>]
type private HalfCompiledSingleSelect =
    { Domains : TempDomains
      MetaColumns : Map<MetaType, SQL.ValueExpr>
      Columns : SelectColumn[]
      FromInfo : SelectFromInfo option
    }

[<NoEquality; NoComparison>]
type private SelectColumnSignature =
    { Name : TempFieldName
      Meta : Set<ColumnMetaType>
    }

[<NoEquality; NoComparison>]
type private SelectSignature =
    { MetaColumns : Set<MetaType>
      Columns : SelectColumnSignature[]
    }

[<NoEquality; NoComparison>]
type private GenericSelectInfo<'e> when 'e : comparison =
    { Domains : GenericDomains<'e>
      // PostgreSQL column length is limited to 63 bytes, so we store column types separately.
      Columns : GenericColumnType<'e>[]
    }

let private emptySelectInfo : GenericSelectInfo<'e> =
    { Domains = DSingle (0, Map.empty)
      Columns = [||]
    }

let private mapSelectInfoFields (f : 'e1 -> 'e2) (info : GenericSelectInfo<'e1>) : GenericSelectInfo<'e2> =
    { Domains = mapDomainsFields f info.Domains
      Columns = Array.map (mapColumnTypeFields f) info.Columns
    }

type private SelectInfo = GenericSelectInfo<FieldName>
type private TempSelectInfo = GenericSelectInfo<TempFieldName>

let private finalSelectInfo = mapSelectInfoFields getFinalName

type FlattenedDomains = Map<GlobalDomainId, Domain>

type private EntityAttributes =
    { Entity : Set<AttributeName>
      Fields : Set<FieldName * AttributeName>
    }

type private EntityAttributesMap = Map<SQL.TableName, EntityAttributes>

let private emptyEntityAttributes =
    { Entity = Set.empty
      Fields = Set.empty
    } : EntityAttributes

let private unionEntityAttributes (a : EntityAttributes) (b : EntityAttributes) =
    { Entity = Set.union a.Entity b.Entity
      Fields = Set.union a.Fields b.Fields
    }

let private unionEntityAttributesMap : EntityAttributesMap -> EntityAttributesMap -> EntityAttributesMap = Map.unionWith (fun name -> unionEntityAttributes)

[<NoEquality; NoComparison>]
type private FromType =
    | FTEntity of GlobalDomainId * Domain // Domain ID is used for merging.
    | FTSubquery of SelectInfo

[<NoEquality; NoComparison>]
type private FromInfo =
    { FromType : FromType
      Entity : FromEntityInfo option
      MainId : SQL.ColumnName option
      MainSubEntity : SQL.ColumnName option
      Attributes : EntityAttributes
    }

type private FromMap = Map<SQL.TableName, FromInfo>

type private FromResult =
    { Tables : FromMap
      Joins : JoinPaths
    }

let compileName (FunQLName name) = SQL.SQLName name

let decompileName (SQL.SQLName name) = FunQLName name

let sqlFunId = compileName funId
let sqlFunSubEntity = compileName funSubEntity
let sqlFunView = compileName funView

type private JoinId = int

let compileJoinId (jid : JoinId) : SQL.TableName =
    SQL.SQLName <| sprintf "__join__%i" jid

let private epochDateTime = Instant.FromUnixTimeSeconds(0)
let private epochDate = epochDateTime.InUtc().LocalDateTime.Date

let private defaultCompiledArgumentValue : FieldType<_> -> FieldValue = function
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
    | FTArray (SFTReference (entityRef, opts)) -> FIntArray [||]
    | FTArray (SFTEnum vals) -> FStringArray [||]
    | FTScalar SFTString -> FString ""
    | FTScalar SFTInt -> FInt 0
    | FTScalar SFTDecimal -> FDecimal 0m
    | FTScalar SFTBool -> FBool false
    | FTScalar SFTDateTime -> FDateTime epochDateTime
    | FTScalar SFTDate -> FDate epochDate
    | FTScalar SFTInterval -> FInterval Period.Zero
    | FTScalar SFTJson -> FJson (ComparableJToken (JObject ()))
    | FTScalar SFTUserViewRef -> FUserViewRef { Schema = None; Name = FunQLName "" }
    | FTScalar SFTUuid -> FUuid Guid.Empty
    | FTScalar (SFTReference (entityRef, opts)) -> FInt 0
    | FTScalar (SFTEnum vals) -> vals |> Seq.first |> Option.get |> FString

let defaultCompiledArgument (arg : CompiledArgument) : FieldValue =
    match arg.DefaultValue with
    | Some v -> v
    | None ->
        if arg.Optional then
            FNull
        else
            defaultCompiledArgumentValue arg.FieldType

let private subselectAttributes (info : SelectInfo) : EntityAttributes =
    let getAttribute = function
        | CTMeta (CMRowAttribute name) ->
            Some { emptyEntityAttributes with Entity = Set.singleton name }
        | CTColumnMeta (colName, CCCellAttribute name) ->
            Some { emptyEntityAttributes with Fields = Set.singleton (colName, name) }
        | _ -> None
    info.Columns |> Seq.mapMaybe getAttribute |> Seq.fold unionEntityAttributes emptyEntityAttributes

// Evaluation of column-wise or global attributes
type CompiledAttributesExpr =
    { PureColumns : (ColumnType * SQL.ColumnName * SQL.ValueExpr)[]
      AttributeColumns : (ColumnType * SQL.ColumnName * SQL.ValueExpr)[]
    }

let private emptyAttributesExpr =
    { PureColumns = [||]
      AttributeColumns = [||]
    }

type CompiledPragmasMap = Map<SQL.ParameterName, SQL.Value>

[<NoEquality; NoComparison>]
type CompiledViewExpr =
    { Pragmas : CompiledPragmasMap
      AttributesQuery : CompiledAttributesExpr
      Query : Query<SQL.SelectExpr>
      UsedDatabase : FlatUsedDatabase
      Columns : (ColumnType * SQL.ColumnName)[]
      Domains : Domains
      MainRootEntity : ResolvedEntityRef option
      FlattenedDomains : FlattenedDomains
    }

[<NoEquality; NoComparison>]
type CompiledCommandExpr =
    { Pragmas : CompiledPragmasMap
      Command : Query<SQL.DataExpr>
      UsedDatabase : FlatUsedDatabase
    }

let compileOrder : SortOrder -> SQL.SortOrder = function
    | Asc -> SQL.Asc
    | Desc -> SQL.Desc

let compileNullsOrder : NullsOrder -> SQL.NullsOrder = function
    | NullsFirst -> SQL.NullsFirst
    | NullsLast -> SQL.NullsLast

let private compileJoin : JoinType -> SQL.JoinType = function
    | Left -> SQL.Left
    | Right -> SQL.Right
    | Inner -> SQL.Inner
    | Outer -> SQL.Full

let private compileSetOp : SetOperation -> SQL.SetOperation = function
    | Union -> SQL.Union
    | Except -> SQL.Except
    | Intersect -> SQL.Intersect

let compileEntityRef (entityRef : EntityRef) : SQL.TableRef = { Schema = Option.map compileName entityRef.Schema; Name = compileName entityRef.Name }

// We rename entity references to no-schema prefixed names. This way we guarantee uniqueness between automatically generated sub-entity SELECTs and actual entities.
// For example, `schema2.foo` inherits `schema1.foo`, and they are both used in the query. Without renaming, we would have:
// FROM schema1.foo .... (SELECT * FROM schema2.foo WHERE ...) as foo.
// and a conflict would ensue.
let renameResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.SQLName =
    SQL.SQLName <| sprintf "%s__%s" (string entityRef.Schema) (string entityRef.Name)

let compileRenamedResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef =
    { Schema = None; Name = renameResolvedEntityRef entityRef }

let compileRenamedEntityRef (entityRef : EntityRef) : SQL.TableRef =
    match entityRef.Schema with
    | Some schemaName -> compileRenamedResolvedEntityRef { Schema = schemaName; Name = entityRef.Name }
    | None -> { Schema = None; Name = compileName entityRef.Name }

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { Schema = Some (compileName entityRef.Schema); Name = compileName entityRef.Name }

let compileFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { Table = Option.map compileEntityRef fieldRef.Entity; Name = compileName fieldRef.Name }

let compileResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { Table = Some <| compileResolvedEntityRef fieldRef.Entity; Name = compileName fieldRef.Name }

// Be careful -- one shouldn't use these functions when compiling real field names, only references to sub-entities!
// That's because real fields use `ColumnName`s.
let compileRenamedFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { Table = Option.map compileRenamedEntityRef fieldRef.Entity; Name = compileName fieldRef.Name }

let decompileTableRef (tableRef : SQL.TableRef) : EntityRef =
    { Schema = Option.map decompileName tableRef.Schema; Name = decompileName tableRef.Name }

let compileAliasFromName (name : EntityName) : SQL.TableAlias =
    { Name = compileName name
      Columns = None
    }

let compileNameFromEntity (entityRef : ResolvedEntityRef) (pun : EntityName option) : SQL.TableName =
    match pun with
    | Some punName -> compileName punName
    | None -> renameResolvedEntityRef entityRef

let compileAliasFromEntity (entityRef : ResolvedEntityRef) (pun : EntityName option) : SQL.TableAlias =
    { Name = compileNameFromEntity entityRef pun
      Columns = None
    }

let private composeExhaustingIf (compileTag : 'tag -> SQL.ValueExpr) (options : ('tag * SQL.ValueExpr) array) : SQL.ValueExpr =
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
        SQL.VECase (cases, Some lastExpr)

// We forcibly convert subentity fields to JSON format when they are used in regular expression context.
// In type expressions, however, we use them raw.
type private ReferenceContext =
    | RCExpr
    | RCTypeExpr

let makeCheckExprFor (subEntityColumn : SQL.ValueExpr) (entities : IEntityBits seq) : SQL.ValueExpr =
    let options = entities |> Seq.map (fun x -> x.TypeName |> SQL.VString |> SQL.VEValue) |> Seq.toArray

    if Array.isEmpty options then
        SQL.VEValue (SQL.VBool false)
    else if Array.length options = 1 then
        SQL.VEBinaryOp (subEntityColumn, SQL.BOEq, options.[0])
    else
        SQL.VEIn (subEntityColumn, options)

let makeCheckExpr (subEntityColumn : SQL.ValueExpr) (layout : ILayoutBits) (entityRef : ResolvedEntityRef) : SQL.ValueExpr option =
    match allPossibleEntities layout entityRef with
    // FIXME: return `option` instead
    | PEAny -> None
    | PEList entities -> Some <| makeCheckExprFor subEntityColumn (Seq.map snd entities)

let private compileEntityTag (subEntityColumn : SQL.ValueExpr) (entity : IEntityBits) =
    SQL.VEBinaryOp (subEntityColumn, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))

let private makeEntityCheckExpr (subEntityColumn : SQL.ValueExpr) (entity : ResolvedFromEntity) (layout : ILayoutBits) (entityRef : ResolvedEntityRef) : SQL.ValueExpr option =
    if entity.Only then
        let entity = layout.FindEntity entityRef |> Option.get
        if hasSubType entity then
            Some <| compileEntityTag subEntityColumn entity
        else
            None
    else
        makeCheckExpr subEntityColumn layout entityRef

let private makeSubEntityParseExprFor (layout : ILayoutBits) (subEntityColumn : SQL.ValueExpr) (entities : ResolvedEntityRef seq) : SQL.ValueExpr =
    let getName (ref : ResolvedEntityRef) =
        let entity = layout.FindEntity ref |> Option.get
        if entity.IsAbstract then
            None
        else
            let json = JToken.FromObject ref |> ComparableJToken |> SQL.VJson |> SQL.VEValue
            Some (entity, json)

    let options = entities |> Seq.mapMaybe getName |> Seq.toArray

    composeExhaustingIf (compileEntityTag subEntityColumn) options

type private ColumnPair = ColumnType * SQL.SelectedColumn

// This type is used internally in getResultEntry.
[<NoEquality; NoComparison>]
type private ResultColumn =
    { Domains : TempDomains option
      MetaColumns : Map<MetaType, SQL.ValueExpr>
      Column : SelectColumn
    }

type private SelectFlags =
    { MainEntity : ResolvedEntityRef option
      IsTopLevel : bool
      MetaColumns : bool
    }

let private subSelectFlags : SelectFlags =
    { MainEntity = None
      IsTopLevel = false
      MetaColumns = false
    }

type RealEntityAnnotation =
    { RealEntity : ResolvedEntityRef
      FromPath : bool
      // `IsInner` is `true` when we can filter rows in outer query, not in the subquery. For example,
      // > foo LEFT JOIN bar
      // If we need an additional constraint on `foo`, we can add it to outer WHERE clause.
      // We cannot do the same for `bar` however, because missing rows will be "visible" as NULLs due to LEFT JOIN.
      IsInner : bool
      AsRoot : bool
    }

type RealFieldAnnotation =
    { Name : FieldName
    }

let private fromToEntitiesMap : FromMap -> FromEntitiesMap = Map.mapMaybe (fun name info -> info.Entity)

type private CTEBindings = Map<SQL.TableName, SelectInfo>

type private UpdateRecCTEBindings = SelectSignature -> TempDomains -> CTEBindings

type private ExprContext =
    { CTEs : CTEBindings
      EntityAttributes :  EntityAttributesMap
    }

let private emptyExprContext =
    { CTEs = Map.empty
      EntityAttributes = Map.empty
    } : ExprContext

let private selectSignature (half : HalfCompiledSingleSelect) : SelectSignature =
    { MetaColumns = Map.keysSet half.MetaColumns
      Columns = half.Columns |> Array.map (fun col -> { Name = col.Name; Meta = Map.keysSet col.Meta })
    }

let private mergeSelectSignature (a : SelectSignature) (b : SelectSignature) : Map<TempFieldName, TempFieldName> * SelectSignature =
    let mutable renames = Map.empty
    let mergeOne (a : SelectColumnSignature) (b : SelectColumnSignature) =
        let newName = unionTempName a.Name b.Name
        if newName <> a.Name then
            renames <- Map.add a.Name newName renames
        elif newName <> b.Name then
            renames <- Map.add b.Name newName renames
        { Name = newName; Meta = Set.union a.Meta b.Meta }
    let ret =
        { MetaColumns = Set.union a.MetaColumns b.MetaColumns
          Columns = Array.map2 mergeOne a.Columns b.Columns
        }
    (renames, ret)

// Should be in sync with `signatureColumns`. They are not the same function because `signatureColumnTypes` requires names,
// but `signatureColumns` doesn't.
let private signatureColumnTypes (sign : SelectSignature) : TempColumnType seq =
    seq {
        for metaCol in sign.MetaColumns do
            yield CTMeta metaCol
        for col in sign.Columns do
            for metaCol in col.Meta do
                yield CTColumnMeta (col.Name, metaCol)
            yield CTColumn col.Name
    }

// Used for deduplicating id columns.
// We use `init [entityName, fieldName, ...path]` as key (as the last component doesn't matter).
let rec private idKey (entityName : EntityName) (fieldName : FieldName) (path : FieldName[]) : string =
    Seq.append (Seq.singleton entityName) (Seq.take path.Length (Seq.append (Seq.singleton fieldName) path)) |> Seq.map string |> String.concat "__"

let private infoFromSignature (domains : TempDomains) (signature : SelectSignature) : TempSelectInfo =
    { Columns = Array.ofSeq (signatureColumnTypes signature)
      Domains = domains
    }

let private makeJoinNode (layout : Layout) (joinKey : JoinKey) (join : JoinPath) (from : SQL.FromExpr) : SQL.FromExpr =
    let tableRef = { Schema = None; Name = joinKey.Table } : SQL.TableRef
    let toTableRef = { Schema = None; Name = join.Name } : SQL.TableRef
    let entity = layout.FindEntity join.RealEntity |> Option.get

    let fromColumn = SQL.VEColumn { Table = Some tableRef; Name = joinKey.Column }
    let toColumn = SQL.VEColumn { Table = Some toTableRef; Name = sqlFunId }
    let joinExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
    let alias = { Name = join.Name; Columns = None } : SQL.TableAlias
    let ann =
        { RealEntity = join.RealEntity
          FromPath = true
          IsInner = false
          AsRoot = joinKey.AsRoot
        } : RealEntityAnnotation
    let fTable =
        { Extra = ann
          Alias = Some alias
          Table = compileResolvedEntityRef entity.Root
        } : SQL.FromTable
    let subquery = SQL.FTable fTable
    SQL.FJoin { Type = SQL.Left; A = from; B = subquery; Condition = joinExpr }

let fromTableName (table : SQL.FromTable) =
    table.Alias |> Option.map (fun a -> a.Name) |> Option.defaultValue table.Table.Name

// Join a table by key in existing FROM expression.
let joinPath (layout : Layout) (joinKey : JoinKey) (join : JoinPath) (topFrom : SQL.FromExpr) : SQL.FromExpr option =
    // TODO: this is implemented as such to insert JOINs at proper places considering LATERAL JOINs.
    // However, we don't support outer join paths inside sub-selects anyway, so JOINs are always appended
    // at the upper level.
    let rec findNode = function
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
        | SQL.FTable fTable as from ->
            let realName = fromTableName fTable
            if realName = joinKey.Table then
                (true, true, from)
            else
                (false, false, from)
        | SQL.FSubExpr subsel as from ->
            if subsel.Alias.Name = joinKey.Table then
                (true, true, from)
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

let rec joinsToSeq (paths : JoinPathsMap) : JoinPathsPair seq =
    paths |> Map.toSeq |> Seq.collect joinToSeq

and private joinToSeq (joinKey : JoinKey, tree : JoinTree) : (JoinKey * JoinPath) seq =
    let me = Seq.singleton (joinKey, tree.Path)
    Seq.append me (joinsToSeq tree.Nested)

// Add multiple joins, populating FromEntitiesMap in process.
let buildJoins (layout : Layout) (initialEntitiesMap : FromEntitiesMap) (initialFrom : SQL.FromExpr) (paths : JoinPathsPair seq) : FromEntitiesMap * SQL.FromExpr =
    let foldOne (entitiesMap, from) (joinKey : JoinKey, join : JoinPath) =
        let from = Option.getOrFailWith (fun () -> "Failed to find the table for JOIN") <| joinPath layout joinKey join from
        let entity =
            { Ref = join.RealEntity
              IsInner = false
              AsRoot = joinKey.AsRoot
              Check = None
            }
        let entitiesMap = Map.add join.Name entity entitiesMap
        (entitiesMap, from)
    Seq.fold foldOne (initialEntitiesMap, initialFrom) paths

let private buildInternalJoins (layout : Layout) (initialFromMap : FromMap) (initialFrom : SQL.FromExpr) (paths : JoinPathsPair seq) : FromMap * SQL.FromExpr =
    let (entitiesMap, newFrom) = buildJoins layout Map.empty initialFrom paths

    let makeFromInfo name entityInfo =
        { FromType = FTSubquery emptySelectInfo
          Entity = Some entityInfo
          MainId = None
          MainSubEntity = None
          Attributes = emptyEntityAttributes
        }
    let addedFromMap = Map.map makeFromInfo entitiesMap
    let newFromMap = Map.union initialFromMap addedFromMap
    (newFromMap, newFrom)

type RenamesMap = Map<SQL.TableName, SQL.TableName>

// Returned join paths sequence are only those paths that need to be added to an existing FROM expression
// with `oldPaths` added.
// Also returned are new `JoinPaths` with everything combined.
let augmentJoinPaths (oldPaths : JoinPaths) (newPaths : JoinPaths) : RenamesMap * JoinPathsPair seq * JoinPaths =
    let mutable lastId = oldPaths.NextJoinId
    let mutable addedPaths = []
    let mutable renamesMap = Map.empty

    let rec renameNewPath (newKeyName : SQL.TableName option) (oldMap : JoinPathsMap) (joinKey : JoinKey) (tree : JoinTree) =
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
                    { Name = compileJoinId lastId
                      RealEntity = tree.Path.RealEntity
                    }
                lastId <- lastId + 1
                addedPaths <- (joinKey, newPath) :: addedPaths
                (Map.empty, newPath)
        renamesMap <- Map.add tree.Path.Name newPath.Name renamesMap
        let nestedMap = renameNewPaths (Some newPath.Name) oldNested tree.Nested
        { Path = newPath
          Nested = nestedMap
        }
    and renameNewPaths (newKeyName : SQL.TableName option) (oldMap : JoinPathsMap) (newMap : JoinPathsMap) = Map.map (renameNewPath newKeyName oldMap) newMap

    let renamedNewPaths = renameNewPaths None oldPaths.Map newPaths.Map
    let ret =
        { Map = Map.union oldPaths.Map renamedNewPaths
          NextJoinId = lastId
        }

    (renamesMap, Seq.rev (List.toSeq addedPaths), ret)

let private genericRenameValueExprTables (failOnNoFind : bool) (renamesMap : RenamesMap) : SQL.ValueExpr -> SQL.ValueExpr =
    let mapColumn : SQL.ColumnRef -> SQL.ColumnRef = function
        | { Table = Some { Schema = None; Name = tableName }; Name = colName } as ref ->
            match Map.tryFind tableName renamesMap with
            | None when failOnNoFind -> failwithf "Unknown table name during rename: %O" tableName
            | None -> ref
            | Some newName -> { Table = Some { Schema = None; Name = newName }; Name = colName }
        | ref -> failwithf "Unexpected column ref during rename: %O" ref
    SQL.mapValueExpr { SQL.idValueExprMapper with ColumnReference = mapColumn }

let renameAllValueExprTables = genericRenameValueExprTables true
let renameValueExprTables = genericRenameValueExprTables false

let rec private prependColumnsToSelectTree (cols : SQL.SelectedColumn seq) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
    | SQL.SSelect query ->
        SQL.SSelect { query with Columns = Seq.append cols query.Columns |> Seq.toArray }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp setOp ->
        SQL.SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = prependColumnsToSelect cols setOp.A
              B = prependColumnsToSelect cols setOp.B
              OrderLimit = setOp.OrderLimit
            }

and private prependColumnsToSelect (cols : SQL.SelectedColumn seq) (select : SQL.SelectExpr) : SQL.SelectExpr =
    let tree = prependColumnsToSelectTree cols select.Tree
    { select with Tree = tree }

let addEntityChecks (entitiesMap : FromEntitiesMap) (where : SQL.ValueExpr option) : SQL.ValueExpr option =
    let addWhere where check =
        match where with
        | None -> Some check
        | Some oldWhere -> Some (SQL.VEAnd (oldWhere, check))
    entitiesMap |> Map.values |> Seq.mapMaybe (fun info -> info.Check) |> Seq.fold addWhere where

let private getForcedFieldName (fieldInfo : FieldMeta) (currName : FieldName)  =
    match fieldInfo.ForceSQLName with
    | Some name -> Some name
    | None ->
        // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
        // If it's not we need to keep original naming.
        let isImmediate =
            match fieldInfo.Bound with
            | None -> false
            | Some bound -> bound.Immediate
        if isImmediate then
            None
        else
            Some <| compileName currName

let private attributeToExpr = function
    | AExpr e -> e
    | AMapping (field, cases, elseExpr) ->
        let convCases = cases |> HashMap.toSeq |> Seq.map (fun (matchValue, ret) -> (FEValue matchValue, FEValue ret)) |> Array.ofSeq
        let convElse = Option.map FEValue elseExpr
        FEMatch (FERef field, convCases, convElse)

type ExprCompilationFlags =
    { ForceNoTableRef : bool
      ForceNoMaterialized : bool
    }

let emptyExprCompilationFlags =
    { ForceNoTableRef = false
      ForceNoMaterialized = false
    }

type CompiledSingleFrom =
    { From : SQL.FromExpr
      Where : SQL.ValueExpr option
      WhereWithoutSubentities : SQL.ValueExpr option
      Entities : FromEntitiesMap
      Joins : JoinPaths
    }

let private selfTableName = SQL.SQLName "__this"
let private selfTableRef = { Schema = None; Name = selfTableName } : SQL.TableRef

// Expects metadata:
// * `FieldMeta` for all immediate FERefs in result expressions when meta columns are required;
// * `FieldMeta` with `Bound` filled for all field references with paths;
// * `ReferencePlaceholderMeta` for all placeholders with paths.
type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    // Only compiler can robustly detect used schemas and arguments, accounting for meta columns.
    let mutable arguments = initialArguments
    let mutable usedDatabase = emptyUsedDatabase

    let replacer = NameReplacer ()

    let getArgumentType = function
        | PGlobal globalName as arg ->
            let (argType, newArguments) = addArgument arg (Map.find globalName globalArgumentTypes) arguments
            arguments <- newArguments
            argType
        | PLocal _ as arg -> arguments.Types.[arg]

    let getEntityByRef (entityRef : ResolvedEntityRef) =
        match layout.FindEntity entityRef with
        | None -> raisef CompileException "Failed to find entity %O" entityRef
        | Some e -> e

    let columnName : ColumnType -> SQL.SQLName = function
        | CTMeta (CMRowAttribute (FunQLName name)) -> replacer.ConvertName <| sprintf "__row_attr__%s" name
        | CTColumnMeta (FunQLName field, CCCellAttribute (FunQLName name)) -> replacer.ConvertName <| sprintf "__cell_attr__%s__%s" field name
        | CTColumnMeta (FunQLName field, CCPun) -> replacer.ConvertName <| sprintf "__pun__%s" field
        | CTMeta (CMDomain id) -> replacer.ConvertName <| sprintf "__domain__%i" id
        | CTMeta (CMId field) -> replacer.ConvertName <| sprintf "__id__%O" field
        | CTMeta (CMSubEntity field) -> replacer.ConvertName <| sprintf "__sub_entity__%O" field
        | CTMeta CMMainId -> SQL.SQLName "__main_id"
        | CTMeta CMMainSubEntity -> SQL.SQLName "__main_sub_entity"
        | CTMeta (CMArgAttribute (FunQLName arg, FunQLName name)) -> replacer.ConvertName <| sprintf "__arg_attr__%s__%s" arg name
        | CTColumn (FunQLName column) -> SQL.SQLName column

    let compileEntityAttribute (entityAttributes : EntityAttributesMap) (entityRef : EntityRef) (attrName : AttributeName) : SQL.ValueExpr =
        match Map.tryFind (compileName entityRef.Name) entityAttributes with
        | Some attrs when Set.contains attrName attrs.Entity ->
            let colName = columnName <| CTMeta (CMRowAttribute attrName)
            let colRef = { Table = Some (compileEntityRef entityRef); Name = colName } : SQL.ColumnRef
            SQL.VEColumn colRef
        | _ -> SQL.nullExpr

    let renameSelectInfo (columns : 'f2 seq) (info : GenericSelectInfo<'f1>) : GenericSelectInfo<'f2> =
        let newColumn = columns.GetEnumerator()
        let mutable namesMap = Map.empty

        for column in info.Columns do
            match column with
            | CTColumn name ->
                ignore <| newColumn.MoveNext()
                let newName = newColumn.Current
                namesMap <- Map.add name newName namesMap
            | _ -> ()

        let renameColumns = function
            | CTColumn name -> CTColumn namesMap.[name]
            | CTColumnMeta (name, meta) -> CTColumnMeta (namesMap.[name], meta)
            | CTMeta meta -> CTMeta meta

        let columns = Array.map renameColumns info.Columns
        let domains = renameDomainFields namesMap info.Domains

        { Columns = columns
          Domains = domains
        }

    let signatureColumns (skipNames : bool) (sign : SelectSignature) (half : HalfCompiledSingleSelect) : SQL.SelectedColumn seq =
        seq {
            for metaCol in sign.MetaColumns do
                let name =
                    if not skipNames then
                        CTMeta metaCol |> columnName |> Some
                    else
                        None
                let expr =
                    match Map.tryFind metaCol half.MetaColumns with
                    | Some e -> e
                    | None when skipNames -> SQL.nullExpr
                    | None ->
                        match metaSQLType metaCol with
                        | Some typ -> SQL.VECast (SQL.nullExpr, SQL.VTScalar (typ.ToSQLRawString()))
                        // This will break when current query is a recursive one, because PostgreSQL can't derive
                        // type of column and assumes it as `text`.
                        | None -> SQL.nullExpr
                yield SQL.SCExpr (name, expr)
            for colSig, col in Seq.zip sign.Columns half.Columns do
                for metaCol in colSig.Meta do
                    let name =
                        match col.Name with
                        | TName name when not skipNames -> CTColumnMeta (name, metaCol) |> columnName |> Some
                        | _ -> None
                    let expr =
                        match Map.tryFind metaCol col.Meta with
                        | Some e -> e
                        | None -> SQL.nullExpr
                    yield SQL.SCExpr (name, expr)
                let name =
                    match col.Name with
                    | TName name when not skipNames -> CTColumn name |> columnName |> Some
                    | _ -> None
                yield SQL.SCExpr (name, col.Column)
        }

    let signatureValueColumns (skipNames : bool) (sign : SelectSignature) (valsRow : SQL.ValueExpr array) : SQL.ValueExpr seq =
        seq {
            for metaCol in sign.MetaColumns do
                if skipNames then
                    yield SQL.nullExpr
                else
                    match metaSQLType metaCol with
                    | Some typ -> yield SQL.VECast (SQL.nullExpr, SQL.VTScalar (typ.ToSQLRawString()))
                    | None -> failwithf "Failed to add type to meta column %O" metaCol
            for colSig, col in Seq.zip sign.Columns valsRow do
                for metaCol in colSig.Meta do
                    if skipNames then
                        yield SQL.nullExpr
                    else
                        failwithf "Failed to add type to meta column %O for column %O" metaCol col
                yield col
        }

    let rec setSelectTreeExprColumns (sign : SelectSignature) (skipNames : bool) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
    | SQL.SSelect sel ->
        let half = sel.Extra :?> HalfCompiledSingleSelect
        let columns = signatureColumns skipNames sign half |> Array.ofSeq
        let extra =
            match half.FromInfo with
            | None -> null
            | Some info -> info :> obj
        SQL.SSelect { sel with Columns = columns; Extra = extra }
    | SQL.SSetOp setOp ->
        let a = setSelectExprColumns sign skipNames setOp.A
        let b = setSelectExprColumns sign true setOp.B
        SQL.SSetOp { setOp with A = a; B = b }
    | SQL.SValues vals ->
        let newVals = Array.map (signatureValueColumns skipNames sign >> Array.ofSeq) vals
        SQL.SValues newVals

    and setSelectExprColumns (sign : SelectSignature) (skipNames : bool) (select : SQL.SelectExpr) : SQL.SelectExpr =
        let tree = setSelectTreeExprColumns sign skipNames select.Tree
        { select with Tree = tree }

    let setSelectColumns (sign : SelectSignature) = setSelectExprColumns sign false

    let rec domainExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
        | DSingle (id, dom) -> f dom
        | DMulti (ns, nested) ->
            let makeCase (localId, subcase) =
                match domainExpression tableRef f subcase with
                | SQL.VEValue SQL.VNull -> None
                | subexpr ->
                    let case = SQL.VEBinaryOp (SQL.VEColumn { Table = Some tableRef; Name = columnName (CTMeta (CMDomain ns)) }, SQL.BOEq, SQL.VEValue (SQL.VInt localId))
                    Some (case, subexpr)
            let cases = nested |> Map.toSeq |> Seq.mapMaybe makeCase |> Seq.toArray
            if Array.isEmpty cases then
                SQL.nullExpr
            else
                SQL.VECase (cases, None)

    let fromInfoExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
        | FTEntity (id, dom) -> f dom
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

    let subentityFromInfo (mainEntity : ResolvedEntityRef option) (selectSig : SelectInfo) : FromInfo =
        let (mainId, mainSubEntity) =
            match mainEntity with
            | None -> (None, None)
            | Some mainRef ->
                let mainId = Some (columnName <| CTMeta CMMainId)
                let mainEntityInfo = getEntityByRef mainRef
                let subEntity = if Map.isEmpty mainEntityInfo.Children then None else Some (columnName <| CTMeta CMMainSubEntity)
                (mainId, subEntity)
        { FromType = FTSubquery selectSig
          Entity = None
          MainId = mainId
          MainSubEntity = mainSubEntity
          Attributes = subselectAttributes selectSig
        }

    let compileOperationEntity (opEntity : ResolvedOperationEntity) : SQL.TableName * FromInfo * SQL.OperationTable =
        let entityRef = tryResolveEntityRef opEntity.Ref |> Option.get
        let entity = getEntityByRef entityRef

        let makeDomainEntry name field =
            { Ref = { Entity = entityRef; Name = name }
              RootEntity = entity.Root
              IdColumn = idDefault
              AsRoot = false
            }
        let domain = mapAllFields makeDomainEntry entity
        let mainEntry =
            { Ref = { Entity = entityRef; Name = entity.MainField }
              RootEntity = entity.Root
              IdColumn = idDefault
              AsRoot = false
            }
        let domain = Map.add funMain mainEntry domain
        let newAlias = compileNameFromEntity entityRef opEntity.Alias

        let ann =
            { RealEntity = entityRef
              FromPath = false
              IsInner = true
              AsRoot = false
            } : RealEntityAnnotation

        let tableRef = compileResolvedEntityRef entity.Root
        let fTable =
            { Extra = ann
              Alias = Some newAlias
              Table = tableRef
            } : SQL.OperationTable
        let subEntityCol = SQL.VEColumn { Table = Some { Schema = None; Name = newAlias }; Name = sqlFunSubEntity }
        let checkExpr = makeEntityCheckExpr subEntityCol opEntity layout entityRef

        let entityInfo =
            { Ref = entityRef
              IsInner = true
              AsRoot = opEntity.AsRoot
              Check = checkExpr
            }
        let fromInfo =
            { FromType = FTEntity (newGlobalDomainId (), domain)
              Entity = Some entityInfo
              MainId = None
              MainSubEntity = None
              Attributes = emptyEntityAttributes
            }

        usedDatabase <- addUsedEntityRef entityRef usedEntitySelect usedDatabase

        (newAlias, fromInfo, fTable)

    let rec compileRef (flags : ExprCompilationFlags) (ctx : ReferenceContext) (extra : ObjectMap) (paths0 : JoinPaths) (checkNullExpr : SQL.ValueExpr option) (tableRef : SQL.TableRef option) (fieldRef : ResolvedFieldRef) (forcedName : SQL.ColumnName option) : JoinPaths * SQL.ValueExpr =
        let realColumn name : SQL.ColumnRef =
            let finalName = Option.defaultValue name forcedName
            { Table = tableRef; Name = finalName } : SQL.ColumnRef

        let entity = getEntityByRef fieldRef.Entity
        let fieldInfo =
            match entity.FindField fieldRef.Name with
            | None -> raisef CompileException "Failed to find field of %O" fieldRef
            | Some f -> f

        match fieldInfo.Field with
        | RId ->
            usedDatabase <- addUsedFieldRef fieldRef usedFieldSelect usedDatabase
            (paths0, SQL.VEColumn <| realColumn sqlFunId)
        | RSubEntity ->
            usedDatabase <- addUsedFieldRef fieldRef usedFieldSelect usedDatabase
            match ctx with
            | RCExpr ->
                let newColumn = SQL.VEColumn <| realColumn sqlFunSubEntity
                let entities = entity.Children |> Map.keys |> Seq.append (Seq.singleton fieldRef.Entity)
                let entities =
                    match ObjectMap.tryFindType<PossibleSubtypesMeta> extra with
                    | None -> entities
                    | Some meta -> entities |> Seq.filter (fun ref -> Seq.contains ref meta.PossibleSubtypes)
                let expr = makeSubEntityParseExprFor layout newColumn entities
                (paths0, expr)
            | RCTypeExpr ->
                (paths0, SQL.VEColumn <| realColumn sqlFunSubEntity)
        | RColumnField col ->
            usedDatabase <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedFieldSelect usedDatabase
            (paths0, SQL.VEColumn <| realColumn col.ColumnName)
        | RComputedField comp when comp.IsMaterialized && not flags.ForceNoMaterialized ->
            let rootInfo =
                match comp.Virtual with
                | Some { InheritedFrom = Some rootRef } ->
                    let rootEntity = getEntityByRef rootRef
                    let rootField = Map.find fieldRef.Name rootEntity.ComputedFields |> Result.get
                    Option.get rootField.Root
                | _ -> Option.get comp.Root
            usedDatabase <- unionUsedDatabases rootInfo.UsedDatabase usedDatabase
            (paths0, SQL.VEColumn <| realColumn comp.ColumnName)
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
            assert (Option.isNone forcedName)
            let localRef = tableRef |> Option.map (fun ref -> { Schema = Option.map decompileName ref.Schema; Name = decompileName ref.Name } : EntityRef)
            let mutable paths = paths0

            let possibleEntities =
                match ObjectMap.tryFindType<PossibleSubtypesMeta> extra with
                | None -> None
                | Some meta -> Some meta.PossibleSubtypes

            let compileCase (case : VirtualFieldCase, caseComp : ResolvedComputedField) =
                let entityRefs =
                    match possibleEntities with
                    | None -> case.PossibleEntities
                    | Some possible -> Set.intersect possible case.PossibleEntities
                if Set.isEmpty entityRefs then
                    None
                else
                    let entities = entityRefs |> Seq.map (fun ref -> getEntityByRef ref :> IEntityBits)
                    let (newPaths, newExpr) = compileFieldExpr flags emptyExprContext paths <| replaceEntityRefInExpr localRef caseComp.Expression
                    paths <- newPaths
                    Some (entities, newExpr)

            // It's safe to use even in case when a virtual field exists in an entity with no `sub_entity` column, because
            // in this case `composeExhaustingIf` will omit the check completely.
            let subEntityColumn = SQL.VEColumn { Table = tableRef; Name = sqlFunSubEntity }
            let expr =
                computedFieldCases layout extra { fieldRef with Name = fieldInfo.Name } comp
                    |> Seq.mapMaybe compileCase
                    |> Seq.toArray
                    |> composeExhaustingIf (makeCheckExprFor subEntityColumn)
            let expr =
                match checkNullExpr with
                | None -> expr
                | Some nullExpr ->
                    // We need to check that the innermost reference is not NULL, otherwise computed column can "leak", returning non-NULL result for NULL reference.
                    SQL.VECase ([|(SQL.VEIsNotNull nullExpr, expr)|], None)
            (paths, expr)

    and compilePath (flags : ExprCompilationFlags) (ctx : ReferenceContext) (extra : ObjectMap) (paths : JoinPaths) (checkNullExpr : SQL.ValueExpr option) (tableRef : SQL.TableRef option) (fieldRef : ResolvedFieldRef) (forcedName : SQL.ColumnName option) (asRoot : bool) : (ResolvedEntityRef * PathArrow) list -> JoinPaths * SQL.ValueExpr = function
        | [] ->
            compileRef flags ctx extra paths checkNullExpr tableRef fieldRef forcedName
        | ((newEntityRef, arrow) :: refs) ->
            let entity = getEntityByRef fieldRef.Entity
            let newEntity = getEntityByRef newEntityRef
            let (fieldInfo, column) =
                match entity.FindField fieldRef.Name with
                | Some ({ Field = RColumnField ({ FieldType = FTScalar (SFTReference _) } as column) } as fieldInfo) ->
                    (fieldInfo, column)
                | _ -> failwith "Impossible"

            usedDatabase <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedFieldSelect usedDatabase
            usedDatabase <- addUsedEntityRef newEntityRef usedEntitySelect usedDatabase

            let columnName = Option.defaultValue column.ColumnName forcedName
            let pathKey =
                { Table = (Option.get tableRef).Name
                  Column = columnName
                  ToRootEntity = newEntity.Root
                  AsRoot = asRoot
                }

            let newFieldRef = { Entity = newEntityRef; Name = arrow.Name }
            let newCheckNullExpr = Some <| SQL.VEColumn { Table = Some { Schema = None; Name = pathKey.Table }; Name = pathKey.Column }
            let (newPath, nextJoinId, res) =
                match Map.tryFind pathKey paths.Map with
                | None ->
                    let newRealName = compileJoinId paths.NextJoinId
                    let newTableRef = { Schema = None; Name = newRealName } : SQL.TableRef
                    let bogusPaths =
                        { Map = Map.empty
                          NextJoinId = paths.NextJoinId + 1
                        }
                    let (nestedPaths, res) = compilePath emptyExprCompilationFlags ctx extra bogusPaths newCheckNullExpr (Some newTableRef) newFieldRef None arrow.AsRoot refs
                    let path =
                        { Name = newRealName
                          RealEntity = newEntityRef
                        }
                    let tree =
                        { Path = path
                          Nested = nestedPaths.Map
                        }
                    (tree, nestedPaths.NextJoinId, res)
                | Some tree ->
                    let newTableRef = { Schema = None; Name = tree.Path.Name } : SQL.TableRef
                    let bogusPaths =
                        { Map = tree.Nested
                          NextJoinId = paths.NextJoinId
                        }
                    let (nestedPaths, res) = compilePath emptyExprCompilationFlags ctx extra bogusPaths newCheckNullExpr (Some newTableRef) newFieldRef None arrow.AsRoot refs
                    let newTree = { tree with Nested = nestedPaths.Map }
                    (newTree, nestedPaths.NextJoinId, res)
            let newPaths =
                { Map = Map.add pathKey newPath paths.Map
                  NextJoinId = nextJoinId
                }
            (newPaths, res)

    and compileReferenceArgument (extra : ObjectMap) (ctx : ReferenceContext) (arg : CompiledArgument) (asRoot : bool) (path : PathArrow seq) (boundPath : ResolvedEntityRef seq) : SQL.SelectExpr =
        let (referencedRef, remainingBoundPath) = Seq.snoc boundPath
        let (firstArrow, remainingPath) = Seq.snoc path
        let argTableRef = compileRenamedResolvedEntityRef referencedRef
        let pathWithEntities = Seq.zip remainingBoundPath remainingPath |> List.ofSeq
        let fieldRef = { Entity = referencedRef; Name = firstArrow.Name }
        let fromEntity =
            { Ref = relaxEntityRef referencedRef
              Only = false
              Alias = None
              AsRoot = asRoot
              Extra = ObjectMap.empty
            }
        let (fromRes, from) = compileFromExpr emptyExprContext 0 None true (FEntity fromEntity)
        assert (fromRes.Joins.NextJoinId = 0)
        let argIdRef = SQL.VEColumn { Table = Some argTableRef; Name = sqlFunId }
        let (argPaths, expr) = compilePath emptyExprCompilationFlags ctx extra emptyJoinPaths (Some argIdRef) (Some argTableRef) fieldRef None asRoot pathWithEntities
        let (entitiesMap, from) = buildJoins layout (fromToEntitiesMap fromRes.Tables) from (joinsToSeq argPaths.Map)
        let whereWithoutSubentities = SQL.VEBinaryOp (argIdRef, SQL.BOEq, SQL.VEPlaceholder arg.PlaceholderId)
        let where = addEntityChecks entitiesMap (Some whereWithoutSubentities)
        let extra =
            { Entities = entitiesMap
              Joins = argPaths
              WhereWithoutSubentities = Some whereWithoutSubentities
            } : SelectFromInfo

        // TODO: This SELECT could be moved into a CTE to improve case with multiple usages of the same argument.
        let singleSelect =
            { SQL.emptySingleSelectExpr with
                  Columns = [| SQL.SCExpr (None, expr) |]
                  From = Some from
                  Where = where
                  Extra = extra
            }
        { CTEs = None
          Tree = SQL.SSelect singleSelect
          Extra = null
        }

    and compileLinkedFieldRef (flags : ExprCompilationFlags) (ctx : ReferenceContext) (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
        match linked.Ref.Ref with
        | VRColumn ref ->
            let maybeFieldInfo = ObjectMap.tryFindType<FieldMeta> linked.Extra
            match (linked.Ref.Path, maybeFieldInfo) with
            | (_, Some ({ Bound = Some boundInfo } as fieldInfo)) ->
                let tableRef =
                    if flags.ForceNoTableRef then
                        None
                    else
                        match ref.Entity with
                        | Some renamedTable -> Some <| compileRenamedEntityRef renamedTable
                        | None -> Some <| compileRenamedResolvedEntityRef boundInfo.Ref.Entity
                let newName = getForcedFieldName fieldInfo ref.Name
                let pathWithEntities = Seq.zip boundInfo.Path linked.Ref.Path |> List.ofSeq
                let checkNullRef =
                    if boundInfo.IsInner then
                        None
                    else
                        Some <| SQL.VEColumn { Table = tableRef; Name = sqlFunId }
                compilePath flags ctx linked.Extra paths0 checkNullRef tableRef boundInfo.Ref newName linked.Ref.AsRoot pathWithEntities
            | ([||], _) ->
                let columnRef = compileRenamedFieldRef ref
                let columnRef =
                    match maybeFieldInfo with
                    | Some { ForceSQLName = Some name } -> { columnRef with Name = name }
                    | _ -> columnRef
                (paths0, SQL.VEColumn columnRef)
            | _ -> failwith "Unexpected path with no bound field"
        | VRPlaceholder arg ->
            let argType = getArgumentType arg

            if Array.isEmpty linked.Ref.Path then
                // Explicitly set argument type to avoid ambiguity,
                (paths0, SQL.VECast (SQL.VEPlaceholder argType.PlaceholderId, argType.DbType))
            else
                let argInfo = ObjectMap.findType<ReferencePlaceholderMeta> linked.Extra
                let selectExpr = compileReferenceArgument linked.Extra ctx argType linked.Ref.AsRoot linked.Ref.Path argInfo.Path
                (paths0, SQL.VESubquery selectExpr)

    and compileFieldAttribute (flags : ExprCompilationFlags) (ctx : ExprContext) (paths : JoinPaths) (linked : LinkedBoundFieldRef) (attrName : AttributeName) : JoinPaths * SQL.ValueExpr =
        let getDefaultAttribute (updateEntityRef : LinkedBoundFieldRef -> FieldRef -> LinkedBoundFieldRef) (entityRef : ResolvedEntityRef) (name : FieldName) =
            match defaultAttrs.FindField entityRef name with
            | None -> (paths, SQL.nullExpr)
            | Some attrs ->
                match Map.tryFind attrName attrs with
                | None -> (paths, SQL.nullExpr)
                | Some attr ->
                    let attrExpr = replaceFieldRefInExpr updateEntityRef <| attributeToExpr attr.Attribute
                    compileFieldExpr emptyExprCompilationFlags ctx paths attrExpr

        match linked.Ref.Ref with
        | VRPlaceholder arg ->
            let argType = getArgumentType arg
            if Array.isEmpty linked.Ref.Path then
                match Map.tryFind attrName argType.Attributes with
                | None -> (paths, SQL.nullExpr)
                | Some attr -> compileFieldExpr flags ctx paths <| attributeToExpr attr
            else
                let argInfo = ObjectMap.findType<ReferencePlaceholderMeta> linked.Extra
                let lastEntityRef = Array.last argInfo.Path
                let lastArrow = Array.last linked.Ref.Path
                getDefaultAttribute (replacePathInField layout linked.Ref.Ref linked.Ref.AsRoot linked.Ref.Path linked.Extra true) lastEntityRef lastArrow.Name

        | VRColumn ref ->
            if Array.isEmpty linked.Ref.Path then
                let entityName = (Option.get ref.Entity).Name
                match Map.tryFind (compileName entityName) ctx.EntityAttributes with
                | Some attrs when Set.contains (ref.Name, attrName) attrs.Fields ->
                    let colName = columnName <| CTColumnMeta (ref.Name, CCCellAttribute attrName)
                    let colRef = { Table = Option.map compileEntityRef ref.Entity; Name = colName } : SQL.ColumnRef
                    (paths, SQL.VEColumn colRef)
                | _ ->
                    match ObjectMap.tryFindType<FieldMeta> linked.Extra with
                    | Some { Bound = Some boundInfo } ->
                        getDefaultAttribute (replaceEntityRefInField ref.Entity) boundInfo.Ref.Entity boundInfo.Ref.Name
                    | _ -> (paths, SQL.nullExpr)
            else
                let fieldMeta = ObjectMap.findType<FieldMeta> linked.Extra
                let boundMeta = Option.get fieldMeta.Bound
                let lastEntityRef = Array.last boundMeta.Path
                let lastArrow = Array.last linked.Ref.Path
                getDefaultAttribute (replacePathInField layout linked.Ref.Ref linked.Ref.AsRoot linked.Ref.Path linked.Extra true) lastEntityRef lastArrow.Name

    and compileFieldExpr (flags : ExprCompilationFlags) (ctx : ExprContext) (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0

        let compileLinkedRef ctx linked =
            let (newPaths, ret) = compileLinkedFieldRef flags ctx paths linked
            paths <- newPaths
            ret
        let compileSubSelectExpr =
            snd << compileSelectExpr subSelectFlags ctx None
        let compileFieldAttr fref attr =
            let (newPaths, ret) = compileFieldAttribute flags ctx paths fref attr
            paths <- newPaths
            ret

        let compileTypeCheck includeChildren c (subEntityRef : SubEntityRef) =
            let checkForTypes =
                match ObjectMap.tryFindType<SubEntityMeta> subEntityRef.Extra with
                | Some info -> info.PossibleEntities
                | None ->
                    // Check for everything.
                    let entityRef = tryResolveEntityRef subEntityRef.Ref |> Option.get
                    if includeChildren then
                        allPossibleEntities layout entityRef |> mapPossibleEntities (Seq.map fst >> Set.ofSeq)
                    else
                        let entity = layout.FindEntity entityRef |> Option.get
                        if entity.IsAbstract then
                            PEList Set.empty
                        else
                            PEList <| Set.singleton entityRef

            match checkForTypes with
            | PEAny ->
                SQL.VEValue (SQL.VBool true)
            | PEList types ->
                let col = compileLinkedRef RCTypeExpr c
                let entities = types |> Seq.map (fun typ -> getEntityByRef typ :> IEntityBits)
                makeCheckExprFor col entities

        let rec traverse = function
            | FEValue v -> SQL.VEValue <| compileFieldValue v
            | FERef c -> compileLinkedRef RCExpr c
            | FEEntityAttr (eref, attr) -> compileEntityAttribute ctx.EntityAttributes eref attr
            | FEFieldAttr (fref, attr) -> compileFieldAttr fref attr
            | FENot a -> SQL.VENot (traverse a)
            | FEAnd (a, b) -> SQL.VEAnd (traverse a, traverse b)
            | FEOr (a, b) -> SQL.VEOr (traverse a, traverse b)
            | FEDistinct (a, b) -> SQL.VEDistinct (traverse a, traverse b)
            | FENotDistinct (a, b) -> SQL.VENotDistinct (traverse a, traverse b)
            | FEBinaryOp (a, op, b) -> SQL.VEBinaryOp (traverse a, compileBinaryOp op, traverse b)
            | FESimilarTo (e, pat) -> SQL.VESimilarTo (traverse e, traverse pat)
            | FENotSimilarTo (e, pat) -> SQL.VENotSimilarTo (traverse e, traverse pat)
            | FEIn (a, arr) -> SQL.VEIn (traverse a, Array.map traverse arr)
            | FENotIn (a, arr) -> SQL.VENotIn (traverse a, Array.map traverse arr)
            | FEInQuery (a, query) -> SQL.VEInQuery (traverse a, compileSubSelectExpr query)
            | FENotInQuery (a, query) -> SQL.VENotInQuery (traverse a, compileSubSelectExpr query)
            | FEAny (e, op, arr) -> SQL.VEAny (traverse e, compileBinaryOp op, traverse arr)
            | FEAll (e, op, arr) -> SQL.VEAll (traverse e, compileBinaryOp op, traverse arr)
            | FECast (e, typ) -> SQL.VECast (traverse e, SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType typ))
            | FEIsNull a -> SQL.VEIsNull (traverse a)
            | FEIsNotNull a -> SQL.VEIsNotNull (traverse a)
            | FECase (es, els) -> SQL.VECase (Array.map (fun (cond, expr) -> (traverse cond, traverse expr)) es, Option.map traverse els)
            | FEMatch (expr, es, els) ->
                let compiledExpr = traverse expr
                SQL.VECase (Array.map (fun (cond, ret) -> (SQL.VEBinaryOp (compiledExpr, SQL.BOEq, traverse cond), traverse ret)) es, Option.map traverse els)
            | FEJsonArray vals ->
                let compiled = Array.map traverse vals

                let tryExtract = function
                    | SQL.VEValue v -> Some v
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract compiled
                match optimized with
                | Some optimizedVals -> optimizedVals |> Seq.map JToken.FromObject |> jsonArray :> JToken |> ComparableJToken |> SQL.VJson |> SQL.VEValue
                | None -> SQL.VEFunc (SQL.SQLName "jsonb_build_array", Array.map traverse vals)
            | FEJsonObject obj ->
                let compiled = Map.map (fun name -> traverse) obj

                let tryExtract = function
                    | (FunQLName name, SQL.VEValue v) -> Some (name, v)
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract (Map.toSeq compiled)
                match optimized with
                | Some optimizedVals -> optimizedVals |> Seq.map (fun (name, v) -> (name, JToken.FromObject v)) |> jsonObject :> JToken |> ComparableJToken |> SQL.VJson |> SQL.VEValue
                | None ->
                    let args = obj |> Map.toSeq |> Seq.collect (fun (FunQLName name, v) -> [SQL.VEValue <| SQL.VString name; traverse v]) |> Seq.toArray
                    SQL.VEFunc (SQL.SQLName "jsonb_build_object", args)
            | FEFunc (name,  args) ->
                let compArgs = Array.map traverse args
                match Map.find name allowedFunctions with
                | FRFunction name -> SQL.VEFunc (name, compArgs)
                | FRSpecial special -> SQL.VESpecialFunc (special, compArgs)
            | FEAggFunc (name,  args) -> SQL.VEAggFunc (Map.find name allowedAggregateFunctions, compileAggExpr args)
            | FESubquery query -> SQL.VESubquery (compileSubSelectExpr query)
            | FEInheritedFrom (c, subEntityRef) -> compileTypeCheck true c subEntityRef
            | FEOfType (c, subEntityRef) -> compileTypeCheck false c subEntityRef

        and compileAggExpr : ResolvedAggExpr -> SQL.AggExpr = function
            | AEAll exprs -> SQL.AEAll (Array.map traverse exprs)
            | AEDistinct expr -> SQL.AEDistinct (traverse expr)
            | AEStar -> SQL.AEStar

        let ret = traverse expr
        (paths, ret)

    and compileOrderColumn (ctx : ExprContext) (paths : JoinPaths)  (ord : ResolvedOrderColumn) : JoinPaths * SQL.OrderColumn =
        let (paths, expr) = compileFieldExpr emptyExprCompilationFlags ctx paths ord.Expr
        let ret =
            { Expr = expr
              Order = Option.map compileOrder ord.Order
              Nulls = Option.map compileNullsOrder ord.Nulls
            } : SQL.OrderColumn
        (paths, ret)

    and compileOrderLimitClause (ctx : ExprContext) (paths0 : JoinPaths) (clause : ResolvedOrderLimitClause) : JoinPaths * SQL.OrderLimitClause =
        let mutable paths = paths0
        let compileOrderColumn' ord =
            let (newPaths, ret) = compileOrderColumn ctx paths ord
            paths <- newPaths
            ret
        let compileFieldExpr' expr =
            let (newPaths, ret) = compileFieldExpr emptyExprCompilationFlags ctx paths expr
            paths <- newPaths
            ret
        let ret =
            { OrderBy = Array.map compileOrderColumn' clause.OrderBy
              Limit = Option.map compileFieldExpr' clause.Limit
              Offset = Option.map compileFieldExpr' clause.Offset
            } : SQL.OrderLimitClause
        (paths, ret)

    and compileInsideSelectExpr (flags : SelectFlags) (compileTree : ExprContext -> ResolvedSelectTreeExpr -> SelectSignature * 'a * SQL.SelectTreeExpr) (ctx : ExprContext) (select : ResolvedSelectExpr) : SelectSignature * 'a * SQL.SelectExpr =
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
              Extra = null
            } : SQL.SelectExpr
        (signature, domains, ret)

    and compileValues (ctx : ExprContext) (values : ResolvedFieldExpr[][]) : SelectSignature * TempDomains * SQL.SelectTreeExpr =
        let compiledValues = values |> Array.map (Array.map (compileFieldExpr emptyExprCompilationFlags ctx emptyJoinPaths >> snd))
        let newColumn () =
            { Name = newTempFieldName ()
              Meta = Set.empty
            }
        let info =
            { MetaColumns = Set.empty
              Columns = Array.init (Array.length values.[0]) (fun _ -> newColumn ())
            }
        let emptyDomains = DSingle (newGlobalDomainId (), Map.empty)
        (info, emptyDomains, SQL.SValues compiledValues)

    // `updateBindings` is used to update CTEBindings after non-recursive part of recursive CTE expression is compiled.
    and compileSelectExpr (flags : SelectFlags) (ctx : ExprContext) (initialUpdateBindings : UpdateRecCTEBindings option) (expr : ResolvedSelectExpr) : TempSelectInfo * SQL.SelectExpr =
        let (signature, domains, ret) =
            match expr.Tree with
            // We assemble a new domain when SetOp is found, and we need a different tree traversal for that.
            | SSetOp _ when flags.MetaColumns ->
                let ns = newDomainNamespaceId ()
                let domainColumn = CMDomain ns
                let mutable lastId = 0
                let rec compileTreeExpr (updateBindings : UpdateRecCTEBindings option) (ctx : ExprContext) : ResolvedSelectTreeExpr -> SelectSignature * Map<LocalDomainId, TempDomains> * SQL.SelectTreeExpr = function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags ctx query
                        let id = lastId
                        lastId <- lastId + 1
                        let metaColumns = Map.add domainColumn (SQL.VEValue <| SQL.VInt id) info.MetaColumns
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
                            | Some update -> { ctx with CTEs = update sig1 (DMulti (ns, domainsMap1)) }
                        let (sig2, domainsMap2, expr2) = compileExpr None ctx setOp.B
                        let (limitPaths, compiledLimits) = compileOrderLimitClause ctx emptyJoinPaths setOp.OrderLimit
                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        let domainsMap1 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap1
                        let domainsMap2 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap2
                        assert Map.isEmpty limitPaths.Map
                        let ret =
                            { Operation = compileSetOp setOp.Operation
                              AllowDuplicates = setOp.AllowDuplicates
                              A = expr1
                              B = expr2
                              OrderLimit = compiledLimits
                            } : SQL.SetOperationExpr
                        (newSig, Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp ret)
                and compileExpr (updateBindings : UpdateRecCTEBindings option) = compileInsideSelectExpr flags (compileTreeExpr updateBindings)

                let (signature, domainsMap, expr) = compileExpr initialUpdateBindings ctx expr
                (signature, DMulti (ns, domainsMap), expr)
            | _ ->
                let rec compileTreeExpr (ctx : ExprContext) = function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags ctx query
                        (selectSignature info, info.Domains, SQL.SSelect expr)
                    | SValues values -> compileValues ctx values
                    | SSetOp setOp ->
                        let (sig1, doms1, expr1) = compileExpr ctx setOp.A
                        let (sig2, doms2, expr2) = compileExpr ctx setOp.B
                        let (limitPaths, compiledLimits) = compileOrderLimitClause ctx emptyJoinPaths setOp.OrderLimit
                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        assert Map.isEmpty limitPaths.Map
                        let ret =
                            { Operation = compileSetOp setOp.Operation
                              AllowDuplicates = setOp.AllowDuplicates
                              A = expr1
                              B = expr2
                              OrderLimit = compiledLimits
                            } : SQL.SetOperationExpr
                        // We don't use domains when `MetaColumns = false`, so we return a random value (`doms1` in this case).
                        (newSig, doms1, SQL.SSetOp ret)

                and compileExpr = compileInsideSelectExpr flags compileTreeExpr

                compileExpr ctx expr

        let ret = setSelectColumns signature ret
        let info = infoFromSignature domains signature
        (info, ret)

    and compileCommonTableExpr (flags : SelectFlags) (ctx : ExprContext) (name : SQL.TableName) (cte : ResolvedCommonTableExpr) : SelectInfo * SQL.CommonTableExpr =
        let extra = ObjectMap.findType<ResolvedCommonTableExprInfo> cte.Extra
        let flags = { flags with MainEntity = if extra.MainEntity then flags.MainEntity else None }
        let updateCteBindings (signature : SelectSignature) (domains : TempDomains) =
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
                let retFields = Array.map columnName info.Columns
                (info, Some retFields)
        let ret =
            { Fields = fields
              Expr = SQL.DESelect expr
              Materialized = Some <| Option.defaultValue false cte.Materialized
            } : SQL.CommonTableExpr
        (info, ret)

    and compileCommonTableExprs (flags : SelectFlags) (ctx : ExprContext) (ctes : ResolvedCommonTableExprs) : ExprContext * SQL.CommonTableExprs =
        let mutable ctx = ctx

        let compileOne (name, cte) =
            let name' = compileName name
            let (info, expr) = compileCommonTableExpr flags ctx name' cte
            ctx <- { ctx with CTEs = Map.add name' info ctx.CTEs }
            (name', expr)

        let exprs = Array.map compileOne ctes.Exprs
        let ret =
            { Recursive = ctes.Recursive
              Exprs = exprs
            } : SQL.CommonTableExprs
        (ctx, ret)

    and compileSingleSelectExpr (flags : SelectFlags) (ctx : ExprContext) (select : ResolvedSingleSelectExpr) : HalfCompiledSingleSelect * SQL.SingleSelectExpr =
        let extra =
            match ObjectMap.tryFindType<ResolvedSingleSelectMeta> select.Extra with
            | None ->
                { HasAggregates = false
                }
            | Some extra -> extra

        let (fromMap, fromPaths, from) =
            match select.From with
            | Some from ->
                let (fromMap, newFrom) = compileFromExpr ctx 0 flags.MainEntity true from
                (fromMap.Tables, fromMap.Joins, Some newFrom)
            | None -> (Map.empty, emptyJoinPaths, None)

        let ctx = { ctx with EntityAttributes = fromMap |> Map.map (fun name fromInfo -> fromInfo.Attributes) }
        let mutable paths = fromPaths

        let whereWithoutSubentities =
            match select.Where with
            | None -> None
            | Some where ->
                let (newPaths, ret) = compileFieldExpr emptyExprCompilationFlags ctx paths where
                paths <- newPaths
                Some ret
        let entitiesMap = fromToEntitiesMap fromMap
        let where = addEntityChecks entitiesMap whereWithoutSubentities

        let compileGroupBy expr =
            let (newPaths, compiled) = compileFieldExpr emptyExprCompilationFlags ctx paths expr
            paths <- newPaths
            compiled
        let groupBy = Array.map compileGroupBy select.GroupBy

        let compileRowAttr (name, attr) =
            let (newPaths, col) = compileFieldExpr emptyExprCompilationFlags ctx paths <| attributeToExpr attr
            paths <- newPaths
            (CMRowAttribute name, col)
        let attributeColumns =
            select.Attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        let addMetaColumns = flags.MetaColumns && not extra.HasAggregates

        let mutable idColumns = Map.empty : Map<FromFieldKey, DomainIdColumn>
        let mutable lastIdColumn = 1
        let getIdColumn (key : FromFieldKey) : bool * DomainIdColumn =
            match Map.tryFind key idColumns with
            | Some id -> (false, id)
            | None ->
            let idCol = lastIdColumn
            lastIdColumn <- lastIdColumn + 1
            idColumns <- Map.add key idCol idColumns
            (true, idCol)

        let getResultColumnEntry (i : int) (result : ResolvedQueryColumnResult) : ResultColumn =
            let (newPaths, resultColumn) = compileColumnResult ctx paths flags.IsTopLevel result
            paths <- newPaths

            let getDefaultAttributes (updateEntityRef : LinkedBoundFieldRef -> FieldRef -> LinkedBoundFieldRef) (fieldRef : ResolvedFieldRef) =
                match defaultAttrs.FindField fieldRef.Entity fieldRef.Name with
                | None -> Seq.empty
                | Some attrs ->
                    let makeDefaultAttr (name, attr) =
                        if Map.containsKey name result.Attributes then
                            None
                        else
                            let expr = replaceFieldRefInExpr updateEntityRef <| attributeToExpr attr.Attribute
                            let attrCol = CCCellAttribute name
                            let (newPaths, compiled) = compileFieldExpr emptyExprCompilationFlags ctx paths expr
                            paths <- newPaths
                            Some (attrCol, compiled)
                    attrs |> Map.toSeq |> Seq.mapMaybe makeDefaultAttr

            match result.Result with
            // References with paths.
            | FERef resultRef when not (Array.isEmpty resultRef.Ref.Path) && addMetaColumns ->
                // `replacePath` is used to get system and pun columns with the same paths.
                let (boundPath, fromId, replacePath) =
                    match resultRef.Ref.Ref with
                    | VRColumn col ->
                        let fieldInfo = ObjectMap.findType<FieldMeta> resultRef.Extra
                        let boundInfo = Option.get fieldInfo.Bound
                        let replacePath path boundPath =
                            let newBoundInfo = { boundInfo with Path = boundPath }
                            let newFieldInfo = { fieldInfo with Bound = Some newBoundInfo }
                            { Ref = { resultRef.Ref with Path = path }; Extra = ObjectMap.singleton newFieldInfo }
                        (boundInfo.Path, FIEntity fieldInfo.FromEntityId, replacePath)
                    | VRPlaceholder arg ->
                        let argInfo = ObjectMap.findType<ReferencePlaceholderMeta> resultRef.Extra
                        let replacePath path boundPath =
                            let newArgInfo = { argInfo with Path = boundPath }
                            { Ref = { resultRef.Ref with Path = path }; Extra = ObjectMap.singleton newArgInfo }
                        (argInfo.Path, FIPlaceholder arg, replacePath)

                let finalEntityRef = Array.last boundPath
                let finalEntity = getEntityByRef finalEntityRef
                let finalArrow = Array.last resultRef.Ref.Path
                let finalField = finalEntity.FindField finalArrow.Name |> Option.get
                let finalRef = { Entity = finalEntityRef; Name = finalArrow.Name }

                // Add system columns (id or sub_entity - this is a generic function).
                let makeSystemColumn (columnConstr : int -> MetaType) (systemName : FieldName) =
                    let systemArrow = { finalArrow with Name = systemName }
                    let systemPath = Seq.append (Seq.skipLast 1 resultRef.Ref.Path) (Seq.singleton systemArrow) |> Array.ofSeq
                    let systemRef = replacePath systemPath boundPath
                    let (newPaths, systemExpr) = compileLinkedFieldRef emptyExprCompilationFlags RCTypeExpr paths systemRef
                    paths <- newPaths
                    systemExpr

                let key =
                    { FromId = fromId
                      Path = resultRef.Ref.Path |> Seq.map (fun a -> a.Name) |> Seq.toList
                    }

                let (systemName, systemColumns) =
                    let (appendNew, idCol) = getIdColumn key
                    if not appendNew then
                        (idCol, Seq.empty)
                    else
                        let idColumn = (CMId idCol, makeSystemColumn CMId funId)
                        let subEntityColumns =
                            // We don't need to select entity if there are no possible children.
                            if Seq.length (allPossibleEntitiesList layout finalEntityRef) <= 1 then
                                Seq.empty
                            else
                                Seq.singleton (CMSubEntity idCol, makeSystemColumn CMSubEntity funSubEntity)
                        let cols = Seq.append (Seq.singleton idColumn) subEntityColumns
                        (idCol, cols)

                let newDomains =
                    let newInfo =
                        { Ref = finalRef
                          RootEntity = finalEntity.Root
                          IdColumn = systemName
                          AsRoot = finalArrow.AsRoot
                        }
                    DSingle (newGlobalDomainId (), Map.singleton resultColumn.Name newInfo )

                let punColumns =
                    if not flags.IsTopLevel then
                        Seq.empty
                    else
                        match finalField.Field with
                        | RColumnField { FieldType = FTScalar (SFTReference (newEntityRef, opts)) } ->
                            let mainArrow = { Name = funMain; AsRoot = false }
                            let punRef = replacePath (Array.append resultRef.Ref.Path [|mainArrow|]) (Array.append boundPath [|newEntityRef|])
                            let (newPaths, punExpr) = compileLinkedFieldRef emptyExprCompilationFlags RCExpr paths punRef
                            paths <- newPaths
                            Seq.singleton (CCPun, punExpr)
                        | _ -> Seq.empty

                let attrColumns = getDefaultAttributes (replacePathInField layout resultRef.Ref.Ref resultRef.Ref.AsRoot resultRef.Ref.Path resultRef.Extra true) finalRef
                let myMeta = [ attrColumns; punColumns ] |> Seq.concat |> Map.ofSeq

                { Domains = Some newDomains
                  MetaColumns = Map.ofSeq systemColumns
                  Column = { resultColumn with Meta = Map.unionUnique resultColumn.Meta myMeta }
                }
            // Column references without paths.
            | FERef ({ Ref = { Ref = VRColumn fieldRef } } as resultRef) when addMetaColumns ->
                // Entity ref always exists here after resolution step.
                let entityRef = Option.get fieldRef.Entity
                let tableRef = compileRenamedEntityRef entityRef
                let fromInfo = Map.find tableRef.Name fromMap
                let fieldInfo = ObjectMap.findType<FieldMeta> resultRef.Extra
                let newName = getForcedFieldName fieldInfo fieldRef.Name

                // Add system columns (id or sub_entity - this is a generic function).
                let makeMaybeSystemColumn (needColumn : ResolvedFieldRef -> bool) (columnConstr : int -> MetaType) (systemName : FieldName) =
                    let getSystemColumn (domain : Domain) =
                        match Map.tryFind fieldRef.Name domain with
                        | None -> SQL.nullExpr
                        | Some info ->
                            if needColumn info.Ref then
                                let colName =
                                    if info.IdColumn = idDefault then
                                        compileName systemName
                                    else
                                        columnName (CTMeta (columnConstr info.IdColumn))
                                SQL.VEColumn { Table = Some tableRef; Name = colName }
                            else
                                SQL.nullExpr

                    match fromInfoExpression tableRef getSystemColumn fromInfo.FromType with
                    | SQL.VEValue SQL.VNull -> None
                    | systemExpr -> Some systemExpr

                let key =
                    { FromId = FIEntity fieldInfo.FromEntityId
                      Path = []
                    }

                let (idCol, systemColumns) =
                    let (appendNew, idCol) = getIdColumn key
                    if not appendNew then
                        (idCol, Seq.empty)
                    else
                        let needsSubEntity (ref : ResolvedFieldRef) =
                            Seq.length (allPossibleEntitiesList layout ref.Entity) > 1

                        let maybeIdCol = Option.map (fun x -> (CMId idCol, x)) <| makeMaybeSystemColumn (fun _ -> true) CMId funId
                        let maybeSubEntityCol = Option.map (fun x -> (CMSubEntity idCol, x)) <|makeMaybeSystemColumn needsSubEntity CMSubEntity funSubEntity
                        let cols = Seq.append (Option.toSeq maybeIdCol) (Option.toSeq maybeSubEntityCol)
                        (idCol, cols)

                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldRef.Name domain with
                    | Some info ->
                        let newInfo =
                            { info with
                                IdColumn = idCol
                                AsRoot = info.AsRoot || resultRef.Ref.AsRoot
                            }
                        Map.singleton resultColumn.Name newInfo
                    | None -> Map.empty
                let rec getNewDomains = function
                    | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                    | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))

                let newDomains =
                    match fromInfo.FromType with
                    | FTEntity (domainId, domain) -> DSingle (domainId, getNewDomain domain)
                    | FTSubquery info -> getNewDomains info.Domains

                let rec getDomainColumns = function
                    | DSingle (id, domain) -> Seq.empty
                    | DMulti (ns, nested) ->
                        let colName = CMDomain ns
                        let col = (colName, SQL.VEColumn { Table = Some tableRef; Name = columnName (CTMeta colName) })
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
                        let getPunColumn (domain : Domain) =
                            match Map.tryFind fieldRef.Name domain with
                            | None -> SQL.nullExpr
                            | Some info ->
                                match layout.FindField info.Ref.Entity info.Ref.Name |> Option.get with
                                | { Field = RColumnField { FieldType = FTScalar (SFTReference (newEntityRef, opts)) } } ->
                                    let mainArrow =
                                        { Name = funMain
                                          AsRoot = false
                                        }
                                    let asRoot = info.AsRoot || resultRef.Ref.AsRoot
                                    let (newPaths, expr) = compilePath emptyExprCompilationFlags RCExpr ObjectMap.empty paths None (Some tableRef) info.Ref newName asRoot [(newEntityRef, mainArrow)]
                                    paths <- newPaths
                                    expr
                                | _ -> SQL.nullExpr

                        match fromInfoExpression tableRef getPunColumn fromInfo.FromType with
                        | SQL.VEValue SQL.VNull -> Seq.empty
                        | punExpr ->
                            let col = (CCPun, punExpr)
                            Seq.singleton col

                // Nested and default attributes.
                let (attrColumns, attributes) =
                    match fromInfo.FromType with
                    | FTEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find fieldRef.Name domain
                        let attrColumns = getDefaultAttributes (replaceEntityRefInField (Some entityRef)) info.Ref
                        (attrColumns, Some info.Ref)
                    | FTSubquery queryInfo ->
                        // Inherit column and cell attributes from subquery.
                        let makeInheritedAttr = function
                        | CTColumnMeta (colName, CCCellAttribute name) when colName = fieldRef.Name && not (Map.containsKey name result.Attributes) ->
                            let attrCol = CCCellAttribute name
                            Some (attrCol, SQL.VEColumn { Table = Some tableRef; Name = columnName (CTColumnMeta (fieldRef.Name, attrCol)) })
                        | _ -> None
                        let attrColumns = queryInfo.Columns |> Seq.mapMaybe makeInheritedAttr
                        (attrColumns, None)

                let myMeta = [ attrColumns; punColumns ] |> Seq.concat |> Map.ofSeq

                { Domains = Some newDomains
                  MetaColumns = [ systemColumns; subqueryDomainColumns ] |> Seq.concat |> Map.ofSeq
                  Column = { resultColumn with Meta = Map.unionUnique resultColumn.Meta myMeta }
                }
            // Argument references without paths.
            | FERef ({ Ref = { Ref = VRPlaceholder arg; AsRoot = asRoot } }) when addMetaColumns ->
                let punColumns =
                    if not flags.IsTopLevel then
                        Seq.empty
                    else
                        let argInfo = Map.find arg arguments.Types
                        match argInfo.FieldType with
                        | FTScalar (SFTReference (newEntityRef, opts)) ->
                            let mainArrow =
                                { Name = funMain
                                  AsRoot = false
                                }
                            let selectExpr = compileReferenceArgument ObjectMap.empty RCExpr argInfo asRoot [|mainArrow|] [|newEntityRef|]
                            Seq.singleton (CCPun, SQL.VESubquery selectExpr)
                        | _ -> Seq.empty

                let myMeta = Map.ofSeq punColumns

                { Domains = None
                  MetaColumns = Map.empty
                  Column = { resultColumn with Meta = Map.unionUnique resultColumn.Meta myMeta }
                }
            | _ ->
                { Domains = None
                  MetaColumns = Map.empty
                  Column = resultColumn
                }

        let getResultEntry (i : int) : ResolvedQueryResult -> ResultColumn = function
            | QRAll alias -> failwith "Impossible QRAll"
            | QRExpr result -> getResultColumnEntry i result

        let resultEntries = Array.mapi getResultEntry select.Results
        let resultColumns = resultEntries |> Array.map (fun x -> x.Column)
        let emptyDomains = DSingle (newGlobalDomainId (), Map.empty)

        let checkSame (name : MetaType) (exprA : SQL.ValueExpr) (exprB : SQL.ValueExpr) =
            assert (string exprA = string exprB)
            exprB

        let metaColumns = resultEntries |> Seq.map (fun c -> c.MetaColumns) |> Seq.fold (Map.unionWith checkSame) Map.empty
        let (newDomains, metaColumns) =
            if addMetaColumns then
                let newDomains = resultEntries |> Seq.mapMaybe (fun entry -> entry.Domains) |> Seq.fold mergeDomains emptyDomains

                let mainIdColumns =
                    match flags.MainEntity with
                    | None -> Seq.empty
                    | Some mainRef ->
                        let findMainValue (getValueName : FromInfo -> SQL.ColumnName option) (name, info : FromInfo) : SQL.ColumnRef option =
                            match getValueName info with
                            | Some id -> Some { Table = Some { Schema = None; Name = name }; Name = id }
                            | None -> None
                        let mainId = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.MainId)) |> Seq.exactlyOne
                        let idCol = (CMMainId, SQL.VEColumn mainId)
                        let subEntityCols =
                            let mainEntity = getEntityByRef mainRef
                            if Map.isEmpty mainEntity.Children then
                                Seq.empty
                            else
                                let mainSubEntity = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.MainSubEntity)) |> Seq.exactlyOne
                                let subEntityCol = (CMMainSubEntity, SQL.VEColumn mainSubEntity)
                                Seq.singleton subEntityCol
                        Seq.append (Seq.singleton idCol) subEntityCols

                let newMetaColumns = [ mainIdColumns; attributeColumns ] |> Seq.concat |> Map.ofSeq
                let metaColumns = Map.unionUnique metaColumns newMetaColumns
                (newDomains, metaColumns)
            else
                (emptyDomains, metaColumns)
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
                let (entitiesMap, from) = buildJoins layout (fromToEntitiesMap fromMap) from (joinsToSeq newJoins)
                let info =
                    { Entities = entitiesMap
                      Joins = paths
                      WhereWithoutSubentities = whereWithoutSubentities
                    }
                (Some info, Some from)

        let info =
            { Domains = newDomains
              MetaColumns = metaColumns
              Columns = resultColumns
              FromInfo = fromInfo
            } : HalfCompiledSingleSelect

        let query =
            { Columns = [||] // We fill columns after processing UNIONs, so that ordering and number of columns and meta-columns is the same everywhere.
              From = from
              Where = where
              GroupBy = groupBy
              OrderLimit = orderLimit
              Locking = None
              Extra = info
            } : SQL.SingleSelectExpr

        (info, query)

    and compileColumnResult (ctx : ExprContext) (paths0 : JoinPaths) (isTopLevel : bool) (result : ResolvedQueryColumnResult) : JoinPaths * SelectColumn =
        let mutable paths = paths0

        let (newPaths, newExpr) =
            match result.Result with
            | FERef ref when not isTopLevel ->
                // When used in sub-select, we don't replace subenitty with its JSON representation.
                compileLinkedFieldRef emptyExprCompilationFlags RCTypeExpr paths ref
            | _ -> compileFieldExpr emptyExprCompilationFlags ctx paths result.Result
        paths <- newPaths

        let compileAttr (attrName, attr) =
            let attrCol = CCCellAttribute attrName
            let (newPaths, ret) = compileFieldExpr emptyExprCompilationFlags ctx paths <| attributeToExpr attr
            paths <- newPaths
            (attrCol, ret)

        let attrs = result.Attributes |> Map.toSeq |> Seq.map compileAttr |> Map.ofSeq
        let name =
             match result.TryToName () with
             | None -> newTempFieldName ()
             | Some name -> TName name
        let ret =
            { Name = name
              Column = newExpr
              Meta = attrs
            }
        (paths, ret)

    // See description of `RealEntityAnnotation.IsInner`.
    and compileFromExpr (ctx : ExprContext) (nextJoinId : int) (mainEntity : ResolvedEntityRef option) (isInner : bool) : ResolvedFromExpr -> FromResult * SQL.FromExpr = function
        | FEntity ({ Ref = { Schema = Some schema; Name = name } } as from) ->
            let entityRef = { Schema = schema; Name = name }
            let entity = getEntityByRef entityRef

            let makeDomainEntry name field =
                { Ref = { Entity = entityRef; Name = name }
                  RootEntity = entity.Root
                  IdColumn = idDefault
                  AsRoot = false
                }
            let domain = mapAllFields makeDomainEntry entity
            let mainEntry =
                { Ref = { Entity = entityRef; Name = entity.MainField }
                  RootEntity = entity.Root
                  IdColumn = idDefault
                  AsRoot = false
                }
            let domain = Map.add funMain mainEntry domain
            let newAlias = compileAliasFromEntity entityRef from.Alias

            let ann =
                { RealEntity = entityRef
                  FromPath = false
                  IsInner = isInner
                  AsRoot = false
                } : RealEntityAnnotation

            let tableRef = compileResolvedEntityRef entity.Root
            let (fromExpr, where) =
                if isInner then
                    let fTable =
                        { Extra = ann
                          Alias = Some newAlias
                          Table = tableRef
                        } : SQL.FromTable
                    let fromExpr = SQL.FTable fTable
                    let subEntityCol = SQL.VEColumn { Table = Some { Schema = None; Name = newAlias.Name }; Name = sqlFunSubEntity }
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
                              Where = checkExpr
                        }
                    let expr = { Extra = ann; CTEs = None; Tree = SQL.SSelect select } : SQL.SelectExpr
                    let subsel = SQL.subSelectExpr newAlias expr
                    let subExpr = SQL.FSubExpr subsel
                    (subExpr, None)
            let entityInfo =
                { Ref = entityRef
                  IsInner = isInner
                  AsRoot = from.AsRoot
                  Check = where
                }
            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let subEntity = if Map.isEmpty entity.Children then None else Some sqlFunSubEntity
                    (Some sqlFunId, subEntity)
            let fromInfo =
                { FromType = FTEntity (newGlobalDomainId (), domain)
                  Entity = Some entityInfo
                  MainId = mainId
                  MainSubEntity = mainSubEntity
                  Attributes = emptyEntityAttributes
                }

            usedDatabase <- addUsedEntityRef entityRef usedEntitySelect usedDatabase

            let res =
                { Tables = Map.singleton newAlias.Name fromInfo
                  Joins = { emptyJoinPaths with NextJoinId = nextJoinId }
                }
            (res, fromExpr)
        | FEntity { Ref = { Schema = None; Name = name }; Alias = pun } ->
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
                { Extra = null
                  Alias = compiledAlias
                  Table = { Schema = None; Name = name' }
                } : SQL.FromTable
            let from = SQL.FTable fTable
            let res =
                { Tables = Map.singleton (compileName newName) fromInfo
                  Joins = { emptyJoinPaths with NextJoinId = nextJoinId }
                }
            (res, from)
        | FJoin join ->
            // We should redo main entities; this duplication is ugly.
            let main1 =
                match join.Type with
                | Left -> mainEntity
                | _ -> None
            let isInner1 =
                match join.Type with
                | Left | Inner -> isInner
                | _ -> false
            let (fromRes1, r1) = compileFromExpr ctx nextJoinId main1 isInner1 join.A
            let main2 =
                match join.Type with
                | Right -> mainEntity
                | _ -> None
            let isInner2 =
                match join.Type with
                | Right | Inner -> isInner
                | _ -> false
            let (fromRes2, r2) = compileFromExpr ctx fromRes1.Joins.NextJoinId main2 isInner2 join.B
            let fromMap = Map.unionUnique fromRes1.Tables fromRes2.Tables
            let joinPaths =
                { Map = Map.unionUnique fromRes1.Joins.Map fromRes2.Joins.Map
                  NextJoinId = fromRes2.Joins.NextJoinId
                }
            let (newJoinPaths, joinExpr) = compileFieldExpr emptyExprCompilationFlags ctx joinPaths join.Condition
            let augmentedJoinPaths = Map.difference newJoinPaths.Map joinPaths.Map
            // Split augmented joins to joins for left and right part of the expression.
            let (newJoinPathsA, newJoinPathsB) = augmentedJoinPaths |> Map.toSeq |> Seq.partition (fun (key, join) -> Map.containsKey key.Table fromRes1.Tables)
            assert (newJoinPathsB |> Seq.forall (fun (key, join) -> Map.containsKey key.Table fromRes2.Tables))
            let (fromMap, r1) = buildInternalJoins layout fromMap r1 (Seq.collect joinToSeq newJoinPathsA)
            let (fromMap, r2) = buildInternalJoins layout fromMap r2 (Seq.collect joinToSeq newJoinPathsB)
            let join =
                { Type = compileJoin join.Type
                  A = r1
                  B = r2
                  Condition = joinExpr
                } : SQL.JoinExpr
            let res =
                { Tables = fromMap
                  Joins = newJoinPaths
                }
            (res, SQL.FJoin join)
        | FSubExpr subsel ->
            let flags =
                { MainEntity = mainEntity
                  IsTopLevel = false
                  MetaColumns = true
                }
            let (info, expr) = compileSelectExpr flags ctx None subsel.Select
            let (info, fields) =
                match subsel.Alias.Fields with
                | None -> (finalSelectInfo info, None)
                | Some fields ->
                    let info = renameSelectInfo fields info
                    let fields = Array.map columnName info.Columns
                    (info, Some fields)
            let compiledAlias =
                { Name = compileName subsel.Alias.Name
                  Columns = fields
                } : SQL.TableAlias
            let compiledSubsel =
                { Alias = compiledAlias
                  Select = expr
                  Lateral = subsel.Lateral
                } : SQL.SubSelectExpr
            let ret = SQL.FSubExpr compiledSubsel
            let fromInfo = subentityFromInfo mainEntity info
            let res =
                { Tables = Map.singleton compiledAlias.Name fromInfo
                  Joins = { emptyJoinPaths with NextJoinId = nextJoinId }
                }
            (res, ret)

    and compileInsertValue (ctx : ExprContext) (paths0 : JoinPaths) : ResolvedInsertValue -> JoinPaths * SQL.InsertValue = function
        | IVDefault -> (paths0, SQL.IVDefault)
        | IVValue expr ->
            let (paths, newExpr) = compileFieldExpr emptyExprCompilationFlags ctx paths0 expr
            assert (Map.isEmpty paths.Map)
            (paths, SQL.IVValue newExpr)

    and compileInsertExpr (flags : SelectFlags) (ctx : ExprContext) (insert : ResolvedInsertExpr) : TempSelectInfo * SQL.InsertExpr =
        let (ctx, ctes) =
            match insert.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (newCtx, newCtes) = compileCommonTableExprs flags ctx ctes
                (newCtx, Some newCtes)

        let entityRef = tryResolveEntityRef insert.Entity.Ref |> Option.get
        let entity = getEntityByRef entityRef
        let (opAlias, opInfo, opTable) = compileOperationEntity insert.Entity

        let fields = Array.map (fun fieldName -> Map.find fieldName entity.ColumnFields) insert.Fields

        let (subEntityCol, subEntityExpr) =
            if hasSubType entity then
                let expr = SQL.VEValue (SQL.VString entity.TypeName)
                (Seq.singleton (null, sqlFunSubEntity), Some expr)
            else
                (Seq.empty, None)

        let usedFields = insert.Fields |> Seq.map (fun fieldName -> (fieldName, usedFieldInsert)) |> Map.ofSeq
        let usedEntity = { usedEntityInsert with Fields = usedFields }
        usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase

        let compileField fieldName (field : ResolvedColumnField) =
            let extra = { Name = fieldName } : RealFieldAnnotation
            (extra :> obj, field.ColumnName)

        let columns = Seq.append subEntityCol (Seq.map2 compileField insert.Fields fields) |> Seq.toArray

        let compileInsertValues vals =
            let newVals = Seq.map (compileInsertValue ctx emptyJoinPaths >> snd) vals
            match subEntityExpr with
            | None ->
                Array.ofSeq newVals
            | Some subExpr ->
                Seq.append (Seq.singleton (SQL.IVValue subExpr)) newVals |> Array.ofSeq

        let source =
            match insert.Source with
            | ISDefaultValues ->
                match subEntityExpr with
                | None -> SQL.ISDefaultValues
                | Some expr ->
                    let defVals = Seq.replicate (Array.length insert.Fields) SQL.IVDefault
                    let row = Seq.append (Seq.singleton (SQL.IVValue expr)) defVals |> Seq.toArray
                    SQL.ISValues [| row |]
            | ISValues allVals ->
                let values = Array.map compileInsertValues allVals
                SQL.ISValues values
            | ISSelect select ->
                let (info, newSelect) = compileSelectExpr subSelectFlags ctx None select
                let newSelect =
                    match subEntityExpr with
                    | None -> newSelect
                    | Some expr ->
                        let subEntityCol = Seq.singleton <| SQL.SCExpr (None, expr)
                        prependColumnsToSelect subEntityCol newSelect
                SQL.ISSelect newSelect

        let ret =
            { CTEs = ctes
              Table = opTable
              Columns = columns
              Source = source
              OnConflict = None
              Returning = [||]
              Extra = null
            } : SQL.InsertExpr
        (emptySelectInfo, ret)

    and compileUpdateAssignExpr (ctx : ExprContext) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (paths0 : JoinPaths) (assignExpr : ResolvedUpdateAssignExpr) : JoinPaths * SQL.UpdateAssignExpr =
        let compileAssignField name =
            let field = Map.find name entity.ColumnFields
            let extra = { Name = name } : RealFieldAnnotation
            { Name = field.ColumnName
              Extra = extra
            } : SQL.UpdateColumnName

        match assignExpr with
        | UAESet (name, expr) ->
            let (paths, newExpr) = compileInsertValue ctx paths0 expr
            let usedEntity = { usedEntityUpdate with Fields = Map.singleton name usedFieldUpdate }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
            (paths, SQL.UAESet (compileAssignField name, newExpr))
        | UAESelect (cols, select) ->
            let newCols = Array.map compileAssignField cols
            let (info, newSelect) = compileSelectExpr subSelectFlags ctx None select
            let usedEntity = { usedEntityUpdate with Fields = cols |> Seq.map (fun name -> (name, usedFieldUpdate)) |> Map.ofSeq }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
            (paths0, SQL.UAESelect (newCols, newSelect))

    and compileGenericUpdateExpr (flags : SelectFlags) (ctx : ExprContext) (update : ResolvedUpdateExpr) : TempSelectInfo * SQL.UpdateExpr =
        let (ctx, ctes) =
            match update.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (newCtx, newCtes) = compileCommonTableExprs flags ctx ctes
                (newCtx, Some newCtes)

        let entityRef = tryResolveEntityRef update.Entity.Ref |> Option.get
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
            let extra = { Name = fieldName } : RealFieldAnnotation
            let (newPaths, newExpr) = compileFieldExpr emptyExprCompilationFlags ctx paths expr
            paths <- newPaths
            (field.ColumnName, (extra :> obj, newExpr))

        let compileField fieldName expr =
            let field = Map.find fieldName entity.ColumnFields
            let extra = { Name = fieldName } : RealFieldAnnotation
            let (newPaths, newExpr) = compileFieldExpr emptyExprCompilationFlags ctx paths expr
            paths <- newPaths
            (field.ColumnName, (extra :> obj, newExpr))

        let compileOneAssign assign =
            let (newPaths, newAssign) = compileUpdateAssignExpr ctx entityRef entity paths assign
            paths <- newPaths
            newAssign

        let assigns = Array.map compileOneAssign update.Assignments

        let where =
            match update.Where with
            | None -> None
            | Some whereExpr ->
                let (newPaths, newWhere) = compileFieldExpr emptyExprCompilationFlags ctx paths whereExpr
                paths <- newPaths
                Some newWhere

        let newPathsMap = Map.difference paths.Map fromPaths.Map
        // If there are joins with updated table, we need to join an intermediate table first, because there is no way to do a left join with updated table in UPDATEs.

        let mutable selfJoinNeeded = false
        let remapPath (key : JoinKey) (tree : JoinTree) =
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
                let joinedUpdateId = { Table = Some selfTableRef; Name = sqlFunId } : SQL.ColumnRef
                let updateId = { Table = Some { Schema = None; Name = opAlias }; Name = sqlFunId } : SQL.ColumnRef
                let joinSame = SQL.VEBinaryOp (SQL.VEColumn updateId, SQL.BOEq, SQL.VEColumn joinedUpdateId)
                let fromTable =
                    { Table = opTable.Table
                      Alias = Some { Name = selfTableName; Columns = None }
                      Extra = null
                    } : SQL.FromTable
                let (where, fromExpr) =
                    match from with
                    | None ->
                        let whereExpr =
                            match where with
                            | None -> joinSame
                            | Some whereExpr -> SQL.VEAnd (whereExpr, joinSame)
                        (Some whereExpr, SQL.FTable fromTable)
                    | Some fromExpr ->
                        let joinExpr =
                            { A = fromExpr
                              B = SQL.FTable fromTable
                              Type = SQL.Full
                              Condition = joinSame
                            } : SQL.JoinExpr
                        (where, SQL.FJoin joinExpr)
                (where, Some fromExpr)

        let entitiesMap = fromToEntitiesMap fromMap
        let from =
            if Map.isEmpty remappedPathsMap then
                from
            else
                let (entitiesMap, fromExpr) = buildJoins layout entitiesMap (Option.get from) (joinsToSeq remappedPathsMap)
                Some fromExpr

        let extra =
            { WhereWithoutSubentities = where
            } : UpdateFromInfo
        let whereWithChecks = addEntityChecks entitiesMap where

        let ret =
            { CTEs = ctes
              Table = opTable
              Assignments = assigns
              Where = whereWithChecks
              From = from
              Returning = [||]
              Extra = extra
            } : SQL.UpdateExpr
        (emptySelectInfo, ret)

    and useUpdateAssignExpr (entityRef : ResolvedEntityRef) = function
        | UAESet (name, expr) ->
            let usedEntity = { usedEntityUpdate with Fields = Map.singleton name usedFieldUpdate }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        | UAESelect (cols, expr) ->
            let usedEntity = { usedEntityUpdate with Fields = cols |> Seq.map (fun name -> (name, usedFieldUpdate)) |> Map.ofSeq }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase

    and compileUpdateExpr (flags : SelectFlags) (ctx : ExprContext) (update : ResolvedUpdateExpr) : TempSelectInfo * SQL.UpdateExpr =
        let (selectInfo, ret) = compileGenericUpdateExpr flags ctx update
        let entityRef = tryResolveEntityRef update.Entity.Ref |> Option.get
        usedDatabase <- addUsedEntityRef entityRef usedEntityUpdate usedDatabase
        (selectInfo, ret)

    and compileDeleteExpr (flags : SelectFlags) (ctx : ExprContext) (delete : ResolvedDeleteExpr) : TempSelectInfo * SQL.DeleteExpr =
        // We compile it as an UPDATE statement -- compilation code is complex and we don't want to repeat that.
        let update =
            { CTEs = delete.CTEs
              Entity = delete.Entity
              Assignments = [||]
              From = delete.Using
              Where = delete.Where
              Extra = delete.Extra
            } : ResolvedUpdateExpr
        let (selectInfo, updateRet) = compileGenericUpdateExpr flags ctx update
        let entityRef = tryResolveEntityRef update.Entity.Ref |> Option.get
        usedDatabase <- addUsedEntityRef entityRef usedEntityDelete usedDatabase
        let ret =
            { CTEs = updateRet.CTEs
              Table = updateRet.Table
              Where = updateRet.Where
              Using = updateRet.From
              Returning = updateRet.Returning
              Extra = updateRet.Extra
            } : SQL.DeleteExpr
        (selectInfo, ret)

    and compileDataExpr (flags : SelectFlags) (ctx : ExprContext) (dataExpr : ResolvedDataExpr) : TempSelectInfo * SQL.DataExpr =
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

    member this.CompileSingleFromExpr (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : CompiledSingleFrom =
        let (fromRes, compiledFrom) = compileFromExpr emptyExprContext 0 None true from
        let (newPaths, whereWithoutSubentities) =
            match where with
            | None -> (emptyJoinPaths, None)
            | Some where ->
                let (newPaths, ret) = compileFieldExpr emptyExprCompilationFlags emptyExprContext fromRes.Joins where
                (newPaths, Some ret)
        let (entitiesMap, compiledFrom) = buildJoins layout (fromToEntitiesMap fromRes.Tables) compiledFrom (joinsToSeq newPaths.Map)
        let finalWhere = addEntityChecks entitiesMap whereWithoutSubentities
        { From = compiledFrom
          WhereWithoutSubentities = whereWithoutSubentities
          Where = finalWhere
          Entities = entitiesMap
          Joins = newPaths
        }

    member this.CompileSingleFieldExpr (flags : ExprCompilationFlags) (expr : ResolvedFieldExpr) =
        let (newPaths, ret) = compileFieldExpr flags emptyExprContext emptyJoinPaths expr
        if not <| Map.isEmpty newPaths.Map then
            failwith "Unexpected join paths in single field expression"
        ret

    member this.CompileSelectExpr (mainEntity : ResolvedEntityRef option) (metaColumns : bool) (select : ResolvedSelectExpr) =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns
            }
        compileSelectExpr flags emptyExprContext None select

    member this.CompileDataExpr (mainEntity : ResolvedEntityRef option) (metaColumns : bool) (dataExpr : ResolvedDataExpr) =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns
            }
        compileDataExpr flags emptyExprContext dataExpr

    member this.CompileArgumentAttributes (args : ResolvedArgumentsMap) : (ColumnType * SQL.ColumnName * SQL.ValueExpr)[] =
        let compileArgAttr argName (name, attr) =
            let (newPaths, col) = compileFieldExpr emptyExprCompilationFlags emptyExprContext emptyJoinPaths <| attributeToExpr attr
            if not <| Map.isEmpty newPaths.Map then
                failwith "Unexpected join paths in argument atribute expression"
            let colType = CTMeta <| CMArgAttribute (argName, name)
            (colType, columnName colType, col)
        let compileArg (pl : Placeholder, arg : ResolvedArgument) =
            match pl with
            | PLocal name -> arg.Attributes |> Map.toSeq |> Seq.map (compileArgAttr name)
            | PGlobal name -> Seq.empty

        args |> OrderedMap.toSeq |> Seq.collect compileArg |> Seq.toArray

    member this.ColumnName name = columnName name

    member this.UsedDatabase = usedDatabase
    member this.Arguments = arguments

type private PurityStatus = Pure | RowPure

let private addPurity (a : PurityStatus) (b : PurityStatus) : PurityStatus =
    match (a, b) with
    | (Pure, Pure) -> Pure
    | (RowPure, _) -> RowPure
    | (_, RowPure) -> RowPure

let private checkPureExpr (expr : SQL.ValueExpr) : PurityStatus option =
    let mutable noReferences = true
    let mutable noRowReferences = true
    let foundReference column =
        noReferences <- false
    let foundPlaceholder placeholder =
        noRowReferences <- false
    let foundQuery query =
        // FIXME: make sure we check for unbound references here when we add support for external references in subexpressions.
        noRowReferences <- false
    SQL.iterValueExpr
        { SQL.idValueExprIter with
              ColumnReference = foundReference
              Placeholder = foundPlaceholder
              Query = foundQuery
        }
        expr
    if not noReferences then
        None
    else if not noRowReferences then
        Some RowPure
    else
        Some Pure

[<NoEquality; NoComparison>]
type private PureColumn =
    { ColumnType : ColumnType
      Purity : PurityStatus
      Name : SQL.ColumnName
      Expression : SQL.ValueExpr
    }

let rec private findPureTreeExprAttributes (columnTypes : ColumnType[]) : SQL.SelectTreeExpr -> (PureColumn option)[] = function
    | SQL.SSelect query ->
        let assignPure colType res =
            match res with
            | SQL.SCAll _ -> None
            | SQL.SCExpr (name, expr) ->
                match checkPureExpr expr with
                | Some purity ->
                    let isGood =
                        match colType with
                        | CTMeta (CMRowAttribute attrName) -> true
                        | CTColumnMeta (colName, CCCellAttribute attrName) -> true
                        | _ -> false
                    if isGood then
                        let info =
                            { ColumnType = colType
                              Purity = purity
                              Name = Option.get name
                              Expression = expr
                            }
                        Some info
                    else
                        None
                | _ -> None
        Array.map2 assignPure columnTypes query.Columns
    | SQL.SValues vals -> Array.create (Array.length vals.[0]) None
    | SQL.SSetOp setOp -> Array.create (findPureExprAttributes columnTypes setOp.A |> Array.length) None

and private findPureExprAttributes (columnTypes : ColumnType[]) (select : SQL.SelectExpr) : (PureColumn option)[] =
    findPureTreeExprAttributes columnTypes select.Tree

let rec private filterNonpureTreeExprColumns (cols : (PureColumn option)[]) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
    | SQL.SSelect query ->
        let checkColumn i _ = Option.isNone cols.[i]
        SQL.SSelect { query with Columns = Seq.filteri checkColumn query.Columns |> Seq.toArray }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp setOp ->
        SQL.SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = filterNonpureExprColumns cols setOp.A
              B = filterNonpureExprColumns cols setOp.B
              OrderLimit = setOp.OrderLimit
            }

and private filterNonpureExprColumns (cols : (PureColumn option)[]) (select : SQL.SelectExpr) : SQL.SelectExpr =
    let tree = filterNonpureTreeExprColumns cols select.Tree
    { select with Tree = tree }

let rec private flattenDomains : Domains -> FlattenedDomains = function
    | DSingle (id, dom) -> Map.singleton id dom
    | DMulti (ns, subdoms) -> subdoms |> Map.values |> Seq.fold (fun m subdoms -> Map.union m (flattenDomains subdoms)) Map.empty

type CompiledExprInfo =
    { Arguments : QueryArguments
      UsedDatabase : UsedDatabase
    }

let compileSingleFromExpr (layout : Layout) (arguments : QueryArguments) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : CompiledExprInfo * CompiledSingleFrom =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let ret = compiler.CompileSingleFromExpr from where
    let info =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase
        }
    (info, ret)

let compileSingleFieldExpr (layout : Layout) (flags : ExprCompilationFlags) (arguments : QueryArguments) (expr : ResolvedFieldExpr) : CompiledExprInfo * SQL.ValueExpr =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let newExpr = compiler.CompileSingleFieldExpr flags expr
    let info =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase
        }
    (info, newExpr)

let compileSelectExpr (layout : Layout) (arguments : QueryArguments) (selectExpr : ResolvedSelectExpr) : CompiledExprInfo * SQL.SelectExpr =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let (info, select) = compiler.CompileSelectExpr None false selectExpr
    let retInfo =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase
        }
    (retInfo, select)

let compileDataExpr (layout : Layout) (arguments : QueryArguments) (dataExpr : ResolvedDataExpr) : CompiledExprInfo * SQL.DataExpr =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let (info, delete) = compiler.CompileDataExpr None false dataExpr
    let retInfo =
        { Arguments = compiler.Arguments
          UsedDatabase = compiler.UsedDatabase
        }
    (retInfo, delete)

let private convertPureColumns (infos : PureColumn seq) = infos |> Seq.map (fun info -> (info.ColumnType, info.Name, info.Expression)) |> Seq.toArray

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let mainEntityRef = viewExpr.MainEntity |> Option.map (fun main -> main.Entity)
    let arguments = compileArguments viewExpr.Arguments
    let compiler = QueryCompiler (layout, defaultAttrs, arguments)
    let (info, expr) = compiler.CompileSelectExpr mainEntityRef true viewExpr.Select
    let argAttrs = compiler.CompileArgumentAttributes viewExpr.Arguments
    let columns = Array.map (mapColumnTypeFields getFinalName) info.Columns

    let allPureAttrs = findPureExprAttributes columns expr
    let expr = filterNonpureExprColumns allPureAttrs expr

    let checkColumn i _ = Option.isNone allPureAttrs.[i]
    let newColumns = Seq.filteri checkColumn columns |> Seq.toArray

    let onlyPureAttrs = Seq.catMaybes allPureAttrs |> Seq.toArray
    let attrQuery =
        if Array.isEmpty onlyPureAttrs then
            emptyAttributesExpr
        else
            let filterPureColumn (info : PureColumn) =
                match info.ColumnType with
                | CTMeta (CMRowAttribute name) when info.Purity = Pure -> true
                | CTColumnMeta (colName, CCCellAttribute name) when info.Purity = Pure -> true
                | _ -> false
            let (pureColumns, attributeColumns) = onlyPureAttrs |> Seq.partition filterPureColumn
            { PureColumns = convertPureColumns pureColumns
              AttributeColumns = convertPureColumns attributeColumns
            }

    let attrQuery =
        { attrQuery with PureColumns = Array.append attrQuery.PureColumns argAttrs }

    let domains = mapDomainsFields getFinalName info.Domains
    let flattenedDomains = flattenDomains domains

    let columnsWithNames = Array.map (fun name -> (name, compiler.ColumnName name)) newColumns

    let compilePragma name v = (compileName name, compileFieldValue v)

    let mainRootEntity =
        match mainEntityRef with
        | None -> None
        | Some mainRef ->
            let mainEntity = layout.FindEntity mainRef |> Option.get
            Some mainEntity.Root

    { Pragmas = Map.mapWithKeys compilePragma viewExpr.Pragmas
      AttributesQuery = attrQuery
      Query = { Expression = expr; Arguments = compiler.Arguments }
      UsedDatabase = flattenUsedDatabase layout compiler.UsedDatabase
      Columns = columnsWithNames
      Domains = domains
      FlattenedDomains = flattenedDomains
      MainRootEntity = mainEntityRef
    }

let compileCommandExpr (layout : Layout) (cmdExpr : ResolvedCommandExpr) : CompiledCommandExpr =
    let arguments = compileArguments cmdExpr.Arguments
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let (info, expr) = compiler.CompileDataExpr None false cmdExpr.Command
    let argAttrs = compiler.CompileArgumentAttributes cmdExpr.Arguments
    let arguments = compiler.Arguments

    let compilePragma name v = (compileName name, compileFieldValue v)

    { Pragmas = Map.mapWithKeys compilePragma cmdExpr.Pragmas
      Command = { Expression = expr; Arguments = arguments }
      UsedDatabase = flattenUsedDatabase layout compiler.UsedDatabase
    }
