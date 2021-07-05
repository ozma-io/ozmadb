module FunWithFlags.FunDB.FunQL.Compile

open System
open NpgsqlTypes
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Typecheck
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Merge
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type DomainIdColumn = int

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
      // A field with assigned idColumn of 42 will use id column "__id__42" and sub-entity column "__sub_entity__42"
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
    | (DMulti (ns1, subdoms1), (DMulti _ as doms2)) ->
        DMulti (ns1, Map.map (fun name doms1 -> mergeDomains doms1 doms2) subdoms1)

type MetaType =
    | CMRowAttribute of AttributeName
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
    | CMDomain _ -> Some SQL.STInt
    | CMId _ -> Some SQL.STInt
    | CMSubEntity _ -> Some SQL.STString
    | CMMainId -> Some SQL.STInt
    | CMMainSubEntity -> Some SQL.STInt

let private mapColumnType (f : 'a -> 'b) : GenericColumnType<'a> -> GenericColumnType<'b> = function
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
                let num = Map.findWithDefault trimmed (fun () -> 0) lastIds + 1
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

type JoinPathsSeq = (JoinKey * JoinPath) seq

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
type private SelectInfo =
    { Domains : TempDomains
      // PostgreSQL column length is limited to 63 bytes, so we store column types separately.
      Columns : TempColumnType[]
    }

type FlattenedDomains = Map<GlobalDomainId, Domain>

[<NoEquality; NoComparison>]
type private FromType =
    | FTEntity of GlobalDomainId * TempDomain // Domain ID is used for merging.
    | FTSubquery of SelectInfo

[<NoEquality; NoComparison>]
type private FromInfo =
    { FromType : FromType
      Entity : FromEntityInfo option
      MainId : SQL.ColumnName option
      MainSubEntity : SQL.ColumnName option
    }

type private FromMap = Map<SQL.TableName, FromInfo>

let compileName (FunQLName name) = SQL.SQLName name

let decompileName (SQL.SQLName name) = FunQLName name

let sqlFunId = compileName funId
let sqlFunSubEntity = compileName funSubEntity
let sqlFunView = compileName funView

type private JoinId = int

let compileJoinId (jid : JoinId) : SQL.TableName =
    SQL.SQLName <| sprintf "__join__%i" jid

let defaultCompiledExprArgument : FieldExprType -> FieldValue = function
    | FETArray SFTString -> FStringArray [||]
    | FETArray SFTInt -> FIntArray [||]
    | FETArray SFTDecimal -> FDecimalArray [||]
    | FETArray SFTBool -> FBoolArray [||]
    | FETArray SFTDateTime -> FDateTimeArray [||]
    | FETArray SFTDate -> FDateArray [||]
    | FETArray SFTInterval -> FIntervalArray [||]
    | FETArray SFTJson -> FJsonArray [||]
    | FETArray SFTUserViewRef -> FUserViewRefArray [||]
    | FETArray SFTUuid -> FUuidArray [||]
    | FETScalar SFTString -> FString ""
    | FETScalar SFTInt -> FInt 0
    | FETScalar SFTDecimal -> FDecimal 0m
    | FETScalar SFTBool -> FBool false
    | FETScalar SFTDateTime -> FDateTime NpgsqlDateTime.Epoch
    | FETScalar SFTDate -> FDate NpgsqlDate.Epoch
    | FETScalar SFTInterval -> FInterval NpgsqlTimeSpan.Zero
    | FETScalar SFTJson -> FJson (JObject ())
    | FETScalar SFTUserViewRef -> FUserViewRef { Schema = None; Name = FunQLName "" }
    | FETScalar SFTUuid -> FUuid Guid.Empty

let defaultCompiledArgument : ResolvedFieldType -> FieldValue = function
    | FTType feType -> defaultCompiledExprArgument feType
    | FTReference entityRef -> FInt 0
    | FTEnum values -> values |> Set.toSeq |> Seq.first |> Option.get |> FString

// Evaluation of column-wise or global attributes
type CompiledAttributesExpr =
    { PureColumns : (ColumnType * SQL.ColumnName * SQL.ValueExpr)[]
      AttributeColumns : (ColumnType * SQL.ColumnName * SQL.ValueExpr)[]
    }

type CompiledPragmasMap = Map<SQL.ParameterName, SQL.Value>

[<NoEquality; NoComparison>]
type CompiledViewExpr =
    { Pragmas : CompiledPragmasMap
      AttributesQuery : CompiledAttributesExpr option
      Query : Query<SQL.SelectExpr>
      UsedSchemas : UsedSchemas
      Columns : (ColumnType * SQL.ColumnName)[]
      Domains : Domains
      MainEntity : ResolvedEntityRef option
      FlattenedDomains : FlattenedDomains
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

let compileAliasFromName (name : EntityName) : SQL.TableAlias =
    { Name = compileName name
      Columns = None
    }

let compileAliasFromEntity (entityRef : ResolvedEntityRef) (pun : EntityName option) : SQL.TableAlias =
    let newName =
        match pun with
        | Some punName -> compileName punName
        | None -> renameResolvedEntityRef entityRef

    { Name = newName
      Columns = None
    }

let private composeExhaustingIf (compileTag : 'tag -> SQL.ValueExpr) (options : ('tag * SQL.ValueExpr) array) : SQL.ValueExpr =
    if Array.isEmpty options then
        SQL.VEValue SQL.VNull
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

let makeCheckExpr (subEntityColumn : SQL.ValueExpr) (layout : ILayoutBits) (entityRef : ResolvedEntityRef) = makeCheckExprFor subEntityColumn (allPossibleEntities layout entityRef |> Seq.map snd)

let private compileEntityTag (subEntityColumn : SQL.ValueExpr) (entity : IEntityBits) =
    SQL.VEBinaryOp (subEntityColumn, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))

let private makeSubEntityParseExprFor (layout : ILayoutBits) (subEntityColumn : SQL.ValueExpr) (entities : ResolvedEntityRef seq) : SQL.ValueExpr =
    let getName (ref : ResolvedEntityRef) =
        let entity = layout.FindEntity ref |> Option.get
        if entity.IsAbstract then
            None
        else
            let json = JToken.FromObject ref |> SQL.VJson |> SQL.VEValue
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

let private fromToEntitiesMap : FromMap -> FromEntitiesMap = Map.mapMaybe (fun name info -> info.Entity)

type private CTEBindings = Map<SQL.TableName, SelectInfo>

type private UpdateRecCTEBindings = SelectSignature -> TempDomains -> CTEBindings

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

let private infoFromSignature (domains : TempDomains) (signature : SelectSignature) : SelectInfo =
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
    let subquery = SQL.FTable (ann, Some alias, compileResolvedEntityRef entity.Root)
    SQL.FJoin { Type = SQL.Left; A = from; B = subquery; Condition = joinExpr }

let joinPath (layout : Layout) (joinKey : JoinKey) (join : JoinPath) (topFrom : SQL.FromExpr) : SQL.FromExpr =
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
        | from ->
            let realName =
                match from with
                | SQL.FTable (extra, alias, ref) ->
                    alias |> Option.map (fun a -> a.Name) |> Option.defaultValue ref.Name
                | SQL.FSubExpr subsel -> subsel.Alias.Name
                | _ -> failwith "Impossible"
            if realName = joinKey.Table then
                (true, true, from)
            else
                (false, false, from)

    let (found, insert, newFrom) = findNode topFrom
    assert found
    let newFrom =
        if insert then
            makeJoinNode layout joinKey join newFrom
        else
            newFrom
    newFrom

let rec joinsToSeq (paths : JoinPathsMap) : JoinPathsSeq =
    paths |> Map.toSeq |> Seq.collect joinToSeq

and private joinToSeq (joinKey : JoinKey, tree : JoinTree) : (JoinKey * JoinPath) seq =
    let me = Seq.singleton (joinKey, tree.Path)
    Seq.append me (joinsToSeq tree.Nested)

let buildJoins (layout : Layout) (initialEntitiesMap : FromEntitiesMap) (initialFrom : SQL.FromExpr) (paths : JoinPathsSeq) : FromEntitiesMap * SQL.FromExpr =
    let foldOne (entitiesMap, from) (joinKey : JoinKey, join : JoinPath) =
        let from = joinPath layout joinKey join from
        let entity =
            { Ref = join.RealEntity
              IsInner = false
              AsRoot = joinKey.AsRoot
              Check = None
            }
        let entitiesMap = Map.add join.Name entity entitiesMap
        (entitiesMap, from)
    Seq.fold foldOne (initialEntitiesMap, initialFrom) paths

type RenamesMap = Map<SQL.TableName, SQL.TableName>

// Returned join paths sequence are only those paths that need to be added to an existing FROM expression
// with `oldPaths` added.
// Also returned are new `JoinPaths` with everything combined.
let augmentJoinPaths (oldPaths : JoinPaths) (newPaths : JoinPaths) : RenamesMap * JoinPathsSeq * JoinPaths =
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
      Entities : FromEntitiesMap
      Joins : JoinPaths
    }

// Expects metadata:
// * `FieldMeta` for all immediate FERefs in result expressions when meta columns are required;
// * `FieldMeta` with `Bound` filled for all field references with paths;
// * `ReferencePlaceholderMeta` for all placeholders with paths.
type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    // Only compiler can robustly detect used schemas and arguments, accounting for meta columns.
    let mutable arguments = initialArguments
    let mutable usedSchemas = Map.empty

    let replacer = NameReplacer ()

    let columnName : ColumnType -> SQL.SQLName = function
        | CTMeta (CMRowAttribute (FunQLName name)) -> replacer.ConvertName <| sprintf "__row_attr__%s" name
        | CTColumnMeta (FunQLName field, CCCellAttribute (FunQLName name)) -> replacer.ConvertName <| sprintf "__cell_attr__%s__%s" field name
        | CTColumnMeta (FunQLName field, CCPun) -> replacer.ConvertName <| sprintf "__pun__%s" field
        | CTMeta (CMDomain id) -> replacer.ConvertName <| sprintf "__domain__%i" id
        | CTMeta (CMId field) -> replacer.ConvertName <| sprintf "__id__%O" field
        | CTMeta (CMSubEntity field) -> replacer.ConvertName <| sprintf "__sub_entity__%O" field
        | CTMeta CMMainId -> SQL.SQLName "__main_id"
        | CTMeta CMMainSubEntity -> SQL.SQLName "__main_sub_entity"
        | CTColumn (FunQLName column) -> SQL.SQLName column

    let renameSelectInfo (columns : FieldName[]) (info : SelectInfo) : SelectInfo =
        let mutable columnI = 0
        let mutable namesMap = Map.empty

        for column in info.Columns do
            match column with
            | CTColumn name ->
                let newName = TName columns.[columnI]
                columnI <- columnI + 1
                namesMap <- Map.add name newName namesMap
            | _ -> ()

        let renameColumns = function
            | CTColumn name -> CTColumn namesMap.[name]
            | CTColumnMeta (name, meta) -> CTColumnMeta (namesMap.[name], meta)
            | a -> a

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
                    | None when skipNames -> SQL.VEValue SQL.VNull
                    | None ->
                        match metaSQLType metaCol with
                        | Some typ -> SQL.VECast (SQL.VEValue SQL.VNull, SQL.VTScalar (typ.ToSQLRawString()))
                        // This will break when current query is a recursive one, because PostgreSQL can't derive
                        // type of column and assumes it as `text`.
                        | None -> SQL.VEValue SQL.VNull
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
                        | None -> SQL.VEValue SQL.VNull
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
                    yield SQL.VEValue SQL.VNull
                else
                    match metaSQLType metaCol with
                    | Some typ -> yield SQL.VECast (SQL.VEValue SQL.VNull, SQL.VTScalar (typ.ToSQLRawString()))
                    | None -> failwithf "Failed to add type to meta column %O" metaCol
            for colSig, col in Seq.zip sign.Columns valsRow do
                for metaCol in colSig.Meta do
                    if skipNames then
                        yield SQL.VEValue SQL.VNull
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

    let rec domainExpression (tableRef : SQL.TableRef) (f : TempDomain -> SQL.ValueExpr) = function
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
                SQL.VEValue SQL.VNull
            else
                SQL.VECase (cases, None)

    let fromInfoExpression (tableRef : SQL.TableRef) (f : TempDomain -> SQL.ValueExpr) = function
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
                let mainEntityInfo = layout.FindEntity mainRef |> Option.get
                let subEntity = if Map.isEmpty mainEntityInfo.Children then None else Some (columnName <| CTMeta CMMainSubEntity)
                (mainId, subEntity)
        { FromType = FTSubquery selectSig
          Entity = None
          MainId = mainId
          MainSubEntity = mainSubEntity
        }

    let rec compileRef (flags : ExprCompilationFlags) (ctx : ReferenceContext) (extra : ObjectMap) (paths0 : JoinPaths) (tableRef : SQL.TableRef option) (fieldRef : ResolvedFieldRef) (forcedName : SQL.ColumnName option) : JoinPaths * SQL.ValueExpr =
        let realColumn name : SQL.ColumnRef =
            let finalName = Option.defaultValue name forcedName
            { Table = tableRef; Name = finalName } : SQL.ColumnRef

        let entity = layout.FindEntity fieldRef.Entity |> Option.get
        let fieldInfo = entity.FindField fieldRef.Name |> Option.get

        match fieldInfo.Field with
        | RId ->
            usedSchemas <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedSchemas
            (paths0, SQL.VEColumn <| realColumn sqlFunId)
        | RSubEntity ->
            usedSchemas <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedSchemas

            match ctx with
            | RCExpr ->
                let newColumn = SQL.VEColumn <| realColumn sqlFunSubEntity
                let children = entity.Children |> Map.keys
                let children =
                    match ObjectMap.tryFindType<PossibleSubtypesMeta> extra with
                    | None -> children
                    | Some meta -> children |> Seq.filter (fun ref -> Seq.contains ref meta.PossibleSubtypes)
                let expr = makeSubEntityParseExprFor layout newColumn children
                (paths0, expr)
            | RCTypeExpr ->
                (paths0, SQL.VEColumn <| realColumn sqlFunSubEntity)
        | RColumnField col ->
            usedSchemas <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedSchemas
            (paths0, SQL.VEColumn <| realColumn col.ColumnName)
        | RComputedField comp when comp.IsMaterialized && not flags.ForceNoMaterialized ->
            let rootInfo =
                match comp.Virtual with
                | Some { InheritedFrom = Some rootRef } ->
                    let rootEntity = layout.FindEntity rootRef |> Option.get
                    let rootField = Map.find fieldRef.Name rootEntity.ComputedFields |> Result.get
                    Option.get rootField.Root
                | _ -> Option.get comp.Root
            usedSchemas <- mergeUsedSchemas rootInfo.UsedSchemas usedSchemas
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
                    let entities = entityRefs |> Seq.map (fun ref -> layout.FindEntity ref |> Option.get :> IEntityBits)
                    let (newPaths, newExpr) = compileLinkedFieldExpr flags Map.empty paths <| replaceEntityRefInExpr localRef caseComp.Expression
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
            (paths, expr)

    and compilePath (flags : ExprCompilationFlags) (ctx : ReferenceContext) (extra : ObjectMap) (paths : JoinPaths) (tableRef : SQL.TableRef option) (fieldRef : ResolvedFieldRef) (forcedName : SQL.ColumnName option) (asRoot : bool) : (ResolvedEntityRef * PathArrow) list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef flags ctx extra paths tableRef fieldRef forcedName
        | ((newEntityRef, arrow) :: refs) ->
            let entity = layout.FindEntity fieldRef.Entity |> Option.get
            let newEntity = layout.FindEntity newEntityRef |> Option.get
            let (fieldInfo, column) =
                match entity.FindField fieldRef.Name with
                | Some ({ Field = RColumnField ({ FieldType = FTReference _ } as column) } as fieldInfo) ->
                    (fieldInfo, column)
                | _ -> failwith "Impossible"

            usedSchemas <- addUsedField fieldRef.Entity.Schema fieldRef.Entity.Name fieldInfo.Name usedSchemas
            usedSchemas <- addUsedEntityRef newEntityRef usedSchemas

            let columnName = Option.defaultValue column.ColumnName forcedName
            let pathKey =
                { Table = (Option.get tableRef).Name
                  Column = columnName
                  ToRootEntity = newEntity.Root
                  AsRoot = asRoot
                }

            let newFieldRef = { Entity = newEntityRef; Name = arrow.Name }
            let (newPath, nextJoinId, res) =
                match Map.tryFind pathKey paths.Map with
                | None ->
                    let newRealName = compileJoinId paths.NextJoinId
                    let newTableRef = { Schema = None; Name = newRealName } : SQL.TableRef
                    let bogusPaths =
                        { Map = Map.empty
                          NextJoinId = paths.NextJoinId + 1
                        }
                    let (nestedPaths, res) = compilePath emptyExprCompilationFlags ctx extra bogusPaths (Some newTableRef) newFieldRef None arrow.AsRoot refs
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
                    let (nestedPaths, res) = compilePath emptyExprCompilationFlags ctx extra bogusPaths (Some newTableRef) newFieldRef None arrow.AsRoot refs
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
        let (argPaths, expr) = compilePath emptyExprCompilationFlags ctx extra emptyJoinPaths (Some argTableRef) fieldRef None asRoot pathWithEntities
        let fromEntity =
            { Ref = relaxEntityRef referencedRef
              Alias = None
              AsRoot = asRoot
            }
        let (fromMap, from) = compileFromExpr Map.empty None true (FEntity fromEntity)
        let (entitiesMap, from) = buildJoins layout (fromToEntitiesMap fromMap) from (joinsToSeq argPaths.Map)
        let whereWithoutSubentities = SQL.VEBinaryOp (SQL.VEColumn { Table = Some argTableRef; Name = sqlFunId }, SQL.BOEq, SQL.VEPlaceholder arg.PlaceholderId)
        let where = addEntityChecks entitiesMap (Some whereWithoutSubentities)
        let extra =
            { Entities = entitiesMap
              Joins = argPaths
              WhereWithoutSubentities = Some whereWithoutSubentities
            } : SelectFromInfo

        // TODO: This SELECT could be moved into a subquery to improve case with multiple usages of the same argument.
        let singleSelect =
            { Columns = [| SQL.SCExpr (None, expr) |]
              From = Some from
              Where = where
              GroupBy = [||]
              OrderLimit = SQL.emptyOrderLimitClause
              Extra = extra
            } : SQL.SingleSelectExpr
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
                compilePath flags ctx linked.Extra paths0 tableRef boundInfo.Ref newName linked.Ref.AsRoot pathWithEntities
            | ([||], _) ->
                let columnRef = compileRenamedFieldRef ref
                let columnRef =
                    match maybeFieldInfo with
                    | Some { ForceSQLName = Some name } -> { columnRef with Name = name }
                    | _ -> columnRef
                (paths0, SQL.VEColumn columnRef)
            | _ -> failwith "Unexpected path with no bound field"
        | VRPlaceholder arg ->
            let argType =
                match arg with
                | PGlobal globalName ->
                    let (argType, newArguments) = addArgument arg (Map.find globalName globalArgumentTypes) arguments
                    arguments <- newArguments
                    argType
                | PLocal _ -> arguments.Types.[arg]

            if Array.isEmpty linked.Ref.Path then
                // Explicitly set argument type to avoid ambiguity,
                (paths0, SQL.VECast (SQL.VEPlaceholder argType.PlaceholderId, argType.DbType))
            else
                let argInfo = ObjectMap.findType<ReferencePlaceholderMeta> linked.Extra
                let selectExpr = compileReferenceArgument linked.Extra ctx argType linked.Ref.AsRoot linked.Ref.Path argInfo.Path
                (paths0, SQL.VESubquery selectExpr)

    and compileLinkedFieldExpr (flags : ExprCompilationFlags) (cteBindings : CTEBindings) (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0

        let compileLinkedRef ctx linked =
            let (newPaths, ret) = compileLinkedFieldRef flags ctx paths linked
            paths <- newPaths
            ret
        let compileSubSelectExpr =
            let flags =
                { MainEntity = None
                  IsTopLevel = false
                  MetaColumns = false
                }
            snd << compileSelectExpr flags cteBindings None

        let rec traverse = function
            | FEValue v -> SQL.VEValue <| compileFieldValue v
            | FERef c -> compileLinkedRef RCExpr c
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
            | FECast (e, typ) -> SQL.VECast (traverse e, SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldExprType typ))
            | FEIsNull a -> SQL.VEIsNull (traverse a)
            | FEIsNotNull a -> SQL.VEIsNotNull (traverse a)
            | FECase (es, els) -> SQL.VECase (Array.map (fun (cond, expr) -> (traverse cond, traverse expr)) es, Option.map traverse els)
            | FEJsonArray vals ->
                let compiled = Array.map traverse vals

                let tryExtract = function
                    | SQL.VEValue v -> Some v
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract compiled
                match optimized with
                | Some optimizedVals -> optimizedVals |> Seq.map JToken.FromObject |> jsonArray :> JToken |> SQL.VJson |> SQL.VEValue
                | None -> SQL.VEFunc (SQL.SQLName "jsonb_build_array", Array.map traverse vals)
            | FEJsonObject obj ->
                let compiled = Map.map (fun name -> traverse) obj

                let tryExtract = function
                    | (FunQLName name, SQL.VEValue v) -> Some (name, v)
                    | _ -> None

                // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
                let optimized = Seq.traverseOption tryExtract (Map.toSeq compiled)
                match optimized with
                | Some optimizedVals -> optimizedVals |> Seq.map (fun (name, v) -> (name, JToken.FromObject v)) |> jsonObject :> JToken |> SQL.VJson |> SQL.VEValue
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
            | FEInheritedFrom (c, subEntityRef)
            | FEOfType (c, subEntityRef) ->
                let checkForTypes =
                    match ObjectMap.tryFindType<SubEntityMeta> subEntityRef.Extra with
                    | Some info -> info.CheckForTypes
                    | None ->
                        // Check for everything.
                        let entityRef = tryResolveEntityRef subEntityRef.Ref |> Option.get
                        allPossibleEntities layout entityRef |> Seq.map fst |> Set.ofSeq |> Some
                match checkForTypes with
                | None ->
                    SQL.VEValue (SQL.VBool true)
                | Some types ->
                    let col = compileLinkedRef RCTypeExpr c
                    let entities = types |> Seq.map (fun typ -> layout.FindEntity typ |> Option.get :> IEntityBits)
                    makeCheckExprFor col entities

        and compileAggExpr : ResolvedAggExpr -> SQL.AggExpr = function
            | AEAll exprs -> SQL.AEAll (Array.map traverse exprs)
            | AEDistinct expr -> SQL.AEDistinct (traverse expr)
            | AEStar -> SQL.AEStar

        let ret = traverse expr
        (paths, ret)

    and compileOrderColumn (cteBindings : CTEBindings) (paths : JoinPaths)  (ord : ResolvedOrderColumn) : JoinPaths * SQL.OrderColumn =
        let (paths, expr) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths ord.Expr
        let ret =
            { Expr = expr
              Order = Option.map compileOrder ord.Order
              Nulls = Option.map compileNullsOrder ord.Nulls
            } : SQL.OrderColumn
        (paths, ret)

    and compileOrderLimitClause (cteBindings : CTEBindings) (paths0 : JoinPaths) (clause : ResolvedOrderLimitClause) : JoinPaths * SQL.OrderLimitClause =
        let mutable paths = paths0
        let compileOrderColumn' ord =
            let (newPaths, ret) = compileOrderColumn cteBindings paths ord
            paths <- newPaths
            ret
        let compileFieldExpr' expr =
            let (newPaths, ret) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths expr
            paths <- newPaths
            ret
        let ret =
            { OrderBy = Array.map compileOrderColumn' clause.OrderBy
              Limit = Option.map compileFieldExpr' clause.Limit
              Offset = Option.map compileFieldExpr' clause.Offset
            } : SQL.OrderLimitClause
        (paths, ret)

    and compileInsideSelectExpr (flags : SelectFlags) (compileTree : CTEBindings -> ResolvedSelectTreeExpr -> SelectSignature * 'a * SQL.SelectTreeExpr) (cteBindings : CTEBindings) (select : ResolvedSelectExpr) : SelectSignature * 'a * SQL.SelectExpr =
        let (cteBindings, ctes) =
            match select.CTEs with
            | None -> (cteBindings, None)
            | Some ctes ->
                let (bindings, newCtes) = compileCommonTableExprs flags cteBindings ctes
                (bindings, Some newCtes)
        let (signature, domains, tree) = compileTree cteBindings select.Tree
        let ret =
            { CTEs = ctes
              Tree = tree
              Extra = null
            } : SQL.SelectExpr
        (signature, domains, ret)

    and compileValues (cteBindings : CTEBindings) (values : ResolvedFieldExpr[][]) : SelectSignature * TempDomains * SQL.SelectTreeExpr =
        let compiledValues = values |> Array.map (Array.map (compileLinkedFieldExpr emptyExprCompilationFlags cteBindings emptyJoinPaths >> snd))
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
    and compileSelectExpr (flags : SelectFlags) (cteBindings : CTEBindings) (initialUpdateBindings : UpdateRecCTEBindings option) (expr : ResolvedSelectExpr) : SelectInfo * SQL.SelectExpr =
        let (signature, domains, ret) =
            match expr.Tree with
            // We assemble a new domain when SetOp is found, and we need a different tree traversal for that.
            | SSetOp _ when flags.MetaColumns ->
                let ns = newDomainNamespaceId ()
                let domainColumn = CMDomain ns
                let mutable lastId = 0
                let rec compileTreeExpr (updateBindings : UpdateRecCTEBindings option) (cteBindings : CTEBindings) : ResolvedSelectTreeExpr -> SelectSignature * Map<LocalDomainId, TempDomains> * SQL.SelectTreeExpr = function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags cteBindings query
                        let id = lastId
                        lastId <- lastId + 1
                        let metaColumns = Map.add domainColumn (SQL.VEValue <| SQL.VInt id) info.MetaColumns
                        let info = { info with MetaColumns = metaColumns }
                        let expr = { expr with Extra = info }
                        (selectSignature info, Map.singleton id info.Domains, SQL.SSelect expr)
                    | SValues values ->
                        let (info, domains, ret) = compileValues cteBindings values
                        let id = lastId
                        lastId <- lastId + 1
                        (info, Map.singleton id domains, ret)
                    | SSetOp setOp ->
                        let (sig1, domainsMap1, expr1) = compileExpr None cteBindings setOp.A
                        let cteBindings =
                            match updateBindings with
                            | None -> cteBindings
                            | Some update -> update sig1 (DMulti (ns, domainsMap1))
                        let (sig2, domainsMap2, expr2) = compileExpr None cteBindings setOp.B
                        let (limitPaths, compiledLimits) = compileOrderLimitClause cteBindings emptyJoinPaths setOp.OrderLimit
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

                let (signature, domainsMap, expr) = compileExpr initialUpdateBindings cteBindings expr
                (signature, DMulti (ns, domainsMap), expr)
            | _ ->
                let rec compileTreeExpr (cteBindings : CTEBindings) = function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags cteBindings query
                        (selectSignature info, info.Domains, SQL.SSelect expr)
                    | SValues values -> compileValues cteBindings values
                    | SSetOp setOp ->
                        let (sig1, doms1, expr1) = compileExpr cteBindings setOp.A
                        let (sig2, doms2, expr2) = compileExpr cteBindings setOp.B
                        let (limitPaths, compiledLimits) = compileOrderLimitClause cteBindings emptyJoinPaths setOp.OrderLimit
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

                compileExpr cteBindings expr

        let ret = setSelectColumns signature ret
        let info = infoFromSignature domains signature
        (info, ret)

    and compileCommonTableExpr (flags : SelectFlags) (cteBindings : CTEBindings) (name : SQL.TableName) (cte : ResolvedCommonTableExpr) : SelectInfo * SQL.CommonTableExpr =
        let extra = ObjectMap.findType<ResolvedCommonTableExprInfo> cte.Extra
        let flags = { flags with MainEntity = if extra.MainEntity then flags.MainEntity else None }
        let updateCteBindings (signature : SelectSignature) (domains : TempDomains) =
            let info = infoFromSignature domains signature
            let info =
                match cte.Fields with
                | None -> info
                | Some fields -> renameSelectInfo fields info
            Map.add name info cteBindings
        let (info, expr) = compileSelectExpr flags cteBindings (Some updateCteBindings) cte.Expr
        let (info, fields) =
            match cte.Fields with
            | None -> (info, None)
            | Some fields ->
                let info = renameSelectInfo fields info
                let retFields = info.Columns |> Array.map (mapColumnType getFinalName >> columnName)
                (info, Some retFields)
        let ret =
            { Fields = fields
              Expr = expr
              Materialized = Some <| Option.defaultValue false cte.Materialized
            } : SQL.CommonTableExpr
        (info, ret)

    and compileCommonTableExprs (flags : SelectFlags) (cteBindings : CTEBindings) (ctes : ResolvedCommonTableExprs) : CTEBindings * SQL.CommonTableExprs =
        let mutable cteBindings = cteBindings

        let compileOne (name, cte) =
            let name' = compileName name
            let (info, expr) = compileCommonTableExpr flags cteBindings name' cte
            cteBindings <- Map.add name' info cteBindings
            (name', expr)

        let exprs = Array.map compileOne ctes.Exprs
        let ret =
            { Recursive = ctes.Recursive
              Exprs = exprs
            } : SQL.CommonTableExprs
        (cteBindings, ret)

    and compileSingleSelectExpr (flags : SelectFlags) (cteBindings : CTEBindings) (select : ResolvedSingleSelectExpr) : HalfCompiledSingleSelect * SQL.SingleSelectExpr =
        let mutable paths = emptyJoinPaths

        let extra =
            match ObjectMap.tryFindType<ResolvedSingleSelectMeta> select.Extra with
            | None ->
                { HasAggregates = false
                }
            | Some extra -> extra 

        let (fromMap, from) =
            match select.From with
            | Some from ->
                let (fromMap, newFrom) = compileFromExpr cteBindings flags.MainEntity true from
                (fromMap, Some newFrom)
            | None -> (Map.empty, None)

        let whereWithoutSubentities =
            match select.Where with
            | None -> None
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths where
                paths <- newPaths
                Some ret
        let entitiesMap = fromToEntitiesMap fromMap
        let where = addEntityChecks entitiesMap whereWithoutSubentities

        let compileGroupBy expr =
            let (newPaths, compiled) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths expr
            paths <- newPaths
            compiled
        let groupBy = Array.map compileGroupBy select.GroupBy

        let compileRowAttr (name, expr) =
            let (newPaths, col) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths expr
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

        // Get meta columns for result columns with paths.
        // `replacePath` is used to get system and pun columns with the same paths.
        let getPathColumns (replacePath : PathArrow[] -> ResolvedEntityRef[] -> LinkedBoundFieldRef) (fromId : FromId) (path : PathArrow[]) (boundPath : ResolvedEntityRef[]) (fieldName : TempFieldName) =
            assert (Array.length path = Array.length boundPath)
            let finalEntityRef = Array.last boundPath
            let finalEntity = layout.FindEntity finalEntityRef |> Option.get
            let finalArrow = Array.last path
            let finalField = finalEntity.FindField finalArrow.Name |> Option.get
            let finalRef = { Entity = finalEntityRef; Name = finalArrow.Name }

            // Add system columns (id or sub_entity - this is a generic function).
            let makeSystemColumn (columnConstr : int -> MetaType) (systemName : FieldName) =
                let systemArrow = { finalArrow with Name = systemName }
                let systemPath = Seq.append (Seq.skipLast 1 path) (Seq.singleton systemArrow) |> Array.ofSeq
                let systemRef = replacePath systemPath boundPath
                let (newPaths, systemExpr) = compileLinkedFieldRef emptyExprCompilationFlags RCTypeExpr paths systemRef
                paths <- newPaths
                systemExpr

            let key =
                { FromId = fromId
                  Path = path |> Seq.map (fun a -> a.Name) |> Seq.toList
                }

            let (systemName, systemColumns) =
                let (appendNew, idCol) = getIdColumn key
                if not appendNew then
                    (idCol, Seq.empty)
                else
                    let idColumn = (CMId idCol, makeSystemColumn CMId funId)
                    let subEntityColumns =
                        // We don't need to select entity if there are no possible children.
                        if Seq.length (allPossibleEntities layout finalEntityRef) <= 1 then
                            Seq.empty
                        else
                            Seq.singleton (CMSubEntity idCol, makeSystemColumn CMSubEntity funSubEntity)
                    let cols = Seq.append (Seq.singleton idColumn) subEntityColumns
                    (idCol, cols)

            let newDomains =
                let newInfo =
                    { Ref = finalRef
                      IdColumn = systemName
                      AsRoot = finalArrow.AsRoot
                    }
                DSingle (newGlobalDomainId (), Map.singleton fieldName newInfo )

            let punColumns =
                if not flags.IsTopLevel then
                    Seq.empty
                else
                    match finalField.Field with
                    | RColumnField { FieldType = FTReference newEntityRef } ->
                        let mainArrow = { Name = funMain; AsRoot = false }
                        let punRef = replacePath (Array.append path [|mainArrow|]) (Array.append boundPath [|newEntityRef|])
                        let (newPaths, punExpr) = compileLinkedFieldRef emptyExprCompilationFlags RCExpr paths punRef
                        paths <- newPaths
                        let col = (CCPun, punExpr)
                        Seq.singleton col
                    | _ -> Seq.empty

            (newDomains, Map.ofSeq systemColumns, Map.ofSeq punColumns)

        let getResultColumnEntry (i : int) (result : ResolvedQueryColumnResult) : ResultColumn =
            let currentAttrs = Map.keysSet result.Attributes

            let (newPaths, resultColumn) = compileColumnResult cteBindings paths flags.IsTopLevel result
            paths <- newPaths

            match result.Result with
            // References with paths.
            | FERef resultRef when not (Array.isEmpty resultRef.Ref.Path) && addMetaColumns ->
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
                        (argInfo.Path,  FIPlaceholder arg, replacePath)
                let (newDomains, metaColumns, colMetaColumns) = getPathColumns replacePath fromId resultRef.Ref.Path boundPath resultColumn.Name
                { Domains = Some newDomains
                  MetaColumns = metaColumns
                  Column = { resultColumn with Meta = Map.unionUnique resultColumn.Meta colMetaColumns }
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
                    let getSystemColumn (domain : TempDomain) =
                        match Map.tryFind (TName fieldRef.Name) domain with
                        | None -> SQL.VEValue SQL.VNull
                        | Some info ->
                            if needColumn info.Ref then
                                let colName =
                                    if info.IdColumn = idDefault then
                                        compileName systemName
                                    else
                                        columnName (CTMeta (columnConstr info.IdColumn))
                                SQL.VEColumn { Table = Some tableRef; Name = colName }
                            else
                                SQL.VEValue SQL.VNull

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
                            Seq.length (allPossibleEntities layout ref.Entity) > 1

                        let maybeIdCol = Option.map (fun x -> (CMId idCol, x)) <| makeMaybeSystemColumn (fun _ -> true) CMId funId
                        let maybeSubEntityCol = Option.map (fun x -> (CMSubEntity idCol, x)) <|makeMaybeSystemColumn needsSubEntity CMSubEntity funSubEntity
                        let cols = Seq.append (Option.toSeq maybeIdCol) (Option.toSeq maybeSubEntityCol)
                        (idCol, cols)

                let getNewDomain (domain : TempDomain) =
                    match Map.tryFind (TName fieldRef.Name) domain with
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
                        let getPunColumn (domain : TempDomain) =
                            match Map.tryFind (TName fieldRef.Name) domain with
                            | None -> SQL.VEValue SQL.VNull
                            | Some info ->
                                match layout.FindField info.Ref.Entity info.Ref.Name |> Option.get with
                                | { Field = RColumnField { FieldType = FTReference newEntityRef } } ->
                                    let mainArrow =
                                        { Name = funMain
                                          AsRoot = false
                                        }
                                    let asRoot = info.AsRoot || resultRef.Ref.AsRoot
                                    let (newPaths, expr) = compilePath emptyExprCompilationFlags RCExpr ObjectMap.empty paths (Some tableRef) info.Ref newName asRoot [(newEntityRef, mainArrow)]
                                    paths <- newPaths
                                    expr
                                | _ -> SQL.VEValue SQL.VNull

                        match fromInfoExpression tableRef getPunColumn fromInfo.FromType with
                        | SQL.VEValue SQL.VNull -> Seq.empty
                        | punExpr ->
                            let col = (CCPun, punExpr)
                            Seq.singleton col

                // Nested and default attributes.
                let attrColumns =
                    match fromInfo.FromType with
                    | FTEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find (TName fieldRef.Name) domain
                        match defaultAttrs.FindField info.Ref.Entity info.Ref.Name with
                        | None -> Seq.empty
                        | Some attrs ->
                            let makeDefaultAttr name =
                                let attr = Map.find name attrs
                                let expr = replaceEntityRefInExpr (Some entityRef) attr.Expression
                                let attrCol = CCCellAttribute name
                                let (newPaths, compiled) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths expr
                                paths <- newPaths
                                (attrCol, compiled)
                            let defaultSet = Map.keysSet attrs
                            let inheritedAttrs = Set.difference defaultSet currentAttrs
                            inheritedAttrs |> Set.toSeq |> Seq.map makeDefaultAttr
                    | FTSubquery queryInfo ->
                        // Inherit column and cell attributes from subquery.
                        let filterColumnAttr = function
                        | CTColumnMeta (colName, CCCellAttribute name) when colName = TName fieldRef.Name -> Some name
                        | _ -> None
                        let oldAttrs = queryInfo.Columns |> Seq.mapMaybe filterColumnAttr |> Set.ofSeq
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrCol = CCCellAttribute name
                            (attrCol, SQL.VEColumn { Table = Some tableRef; Name = columnName (CTColumnMeta (fieldRef.Name, attrCol)) })
                        inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr

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
                        | FTReference newEntityRef ->
                            let mainArrow =
                                { Name = funMain
                                  AsRoot = false
                                }
                            let selectExpr = compileReferenceArgument ObjectMap.empty RCExpr argInfo asRoot [|mainArrow|] [|newEntityRef|]
                            Seq.singleton (CCPun, SQL.VESubquery selectExpr)
                        | _ -> Seq.empty
                { Domains = None
                  MetaColumns = Map.empty
                  Column = { resultColumn with Meta = Map.ofSeq punColumns }
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
                            let mainEntity = layout.FindEntity mainRef |> Option.get
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
            let (newPaths, ret) = compileOrderLimitClause cteBindings paths select.OrderLimit
            paths <- newPaths
            ret

        assert (Map.isEmpty paths.Map || Option.isSome from)
        let (fromInfo, newFrom) =
            match from with
            | None -> (None, None)
            | Some from ->
                let (entitiesMap, from) = buildJoins layout (fromToEntitiesMap fromMap) from (joinsToSeq paths.Map)
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
              From = newFrom
              Where = where
              GroupBy = groupBy
              OrderLimit = orderLimit
              Extra = info
            } : SQL.SingleSelectExpr

        (info, query)

    and compileColumnResult (cteBindings : CTEBindings) (paths0 : JoinPaths) (isTopLevel : bool) (result : ResolvedQueryColumnResult) : JoinPaths * SelectColumn =
        let mutable paths = paths0

        let (newPaths, newExpr) =
            match result.Result with
            | FERef ref when not isTopLevel ->
                // When used in sub-select, we don't replace subenitty with its JSON representation.
                compileLinkedFieldRef emptyExprCompilationFlags RCTypeExpr paths ref
            | _ -> compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths result.Result
        paths <- newPaths

        let compileAttr (attrName, expr) =
            let attrCol = CCCellAttribute attrName
            let (newPaths, ret) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings paths expr
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
    and compileFromExpr (cteBindings : CTEBindings) (mainEntity : ResolvedEntityRef option) (isInner : bool) : ResolvedFromExpr -> FromMap * SQL.FromExpr = function
        | FEntity ({ Ref = { Schema = Some schema; Name = name } } as from) ->
            let entityRef = { Schema = schema; Name = name }
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef

            let makeDomainEntry name field =
                { Ref = { Entity = entityRef; Name = name }
                  IdColumn = idDefault
                  AsRoot = false
                }
            let domain = mapAllFields makeDomainEntry entity |> Map.mapKeys TName
            let mainEntry =
                { Ref = { Entity = entityRef; Name = entity.MainField }
                  IdColumn = idDefault
                  AsRoot = false
                }
            let domain = Map.add (TName funMain) mainEntry domain
            let newAlias = compileAliasFromEntity entityRef from.Alias

            let ann =
                { RealEntity = entityRef
                  FromPath = false
                  IsInner = isInner
                  AsRoot = false
                } : RealEntityAnnotation

            let tableRef = compileResolvedEntityRef entity.Root
            let (fromExpr, where) =
                match entity.Parent with
                | None ->
                    let fromExpr = SQL.FTable (ann, Some newAlias, tableRef)
                    (fromExpr, None)
                | Some parent when isInner ->
                    let fromExpr = SQL.FTable (ann, Some newAlias, tableRef)
                    let subEntityCol = SQL.VEColumn { Table = Some { Schema = None; Name = newAlias.Name }; Name = sqlFunSubEntity }
                    let checkExpr = makeCheckExpr subEntityCol layout entityRef
                    (fromExpr, Some checkExpr)
                | Some parent ->
                    let subEntityCol = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }
                    let checkExpr = makeCheckExpr subEntityCol layout entityRef
                    let select =
                        { Columns = [| SQL.SCAll None |]
                          From = Some <| SQL.FTable (null, None, tableRef)
                          Where = Some checkExpr
                          GroupBy = [||]
                          OrderLimit = SQL.emptyOrderLimitClause
                          Extra = null
                        } : SQL.SingleSelectExpr
                    let expr = { Extra = ann; CTEs = None; Tree = SQL.SSelect select } : SQL.SelectExpr
                    let subsel =
                        { Select = expr
                          Alias = newAlias
                          Lateral = false
                        } : SQL.SubSelectExpr
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
                }

            usedSchemas <- addUsedEntityRef entityRef usedSchemas

            (Map.singleton newAlias.Name fromInfo, fromExpr)
        | FEntity { Ref = { Schema = None; Name = name }; Alias = pun } ->
            let name' = compileName name
            let selectSig = Map.find name' cteBindings
            let compiledAlias = Option.map compileAliasFromName pun
            let newName = Option.defaultValue name pun
            let fromInfo = subentityFromInfo mainEntity selectSig
            let ret = SQL.FTable (null, compiledAlias, { Schema = None; Name = name' })
            (Map.singleton (compileName newName) fromInfo, ret)
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
            let (fromMap1, r1) = compileFromExpr cteBindings main1 isInner1 join.A
            let main2 =
                match join.Type with
                | Right -> mainEntity
                | _ -> None
            let isInner2 =
                match join.Type with
                | Right | Inner -> isInner
                | _ -> false
            let (fromMap2, r2) = compileFromExpr cteBindings main2 isInner2 join.B
            let fromMap = Map.unionUnique fromMap1 fromMap2
            let (joinPaths, joinExpr) = compileLinkedFieldExpr emptyExprCompilationFlags cteBindings emptyJoinPaths join.Condition
            assert (Map.isEmpty joinPaths.Map)
            let ret =
                { Type = compileJoin join.Type
                  A = r1
                  B = r2
                  Condition = joinExpr
                } : SQL.JoinExpr
            (fromMap, SQL.FJoin ret)
        | FSubExpr subsel ->
            let flags =
                { MainEntity = mainEntity
                  IsTopLevel = false
                  MetaColumns = true
                }
            let (info, expr) = compileSelectExpr flags cteBindings None subsel.Select
            let (info, fields) =
                match subsel.Alias.Fields with
                | None -> (info, None)
                | Some fields ->
                    let info = renameSelectInfo fields info
                    let fields = info.Columns |> Array.map (mapColumnType getFinalName >> columnName)
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
            (Map.singleton compiledAlias.Name fromInfo, ret)

    member this.CompileSingleFromExpr (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : CompiledSingleFrom =
        let (fromMap, compiledFrom) = compileFromExpr Map.empty None true from
        let (newPaths, where) =
            match where with
            | None -> (emptyJoinPaths, None)
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr emptyExprCompilationFlags Map.empty emptyJoinPaths where
                (newPaths, Some ret)
        let (entitiesMap, compiledFrom) = buildJoins layout (fromToEntitiesMap fromMap) compiledFrom (joinsToSeq newPaths.Map)
        { From = compiledFrom
          Where = where
          Entities = entitiesMap
          Joins = newPaths
        }

    member this.CompileSingleFieldExpr (flags : ExprCompilationFlags) (expr : ResolvedFieldExpr) =
        let (newPaths, ret) = compileLinkedFieldExpr flags Map.empty emptyJoinPaths expr
        if not <| Map.isEmpty newPaths.Map then
            failwith "Unexpected join paths in single field expression"
        ret

    member this.CompileSelectExpr (mainEntity : ResolvedEntityRef option) (metaColumns : bool) (select : ResolvedSelectExpr) =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns
            }
        compileSelectExpr flags Map.empty None select

    member this.ColumnName name = columnName name

    member this.UsedSchemas = usedSchemas
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
      UsedSchemas : UsedSchemas
    }

let compileSingleFromExpr (layout : Layout) (arguments : QueryArguments) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : CompiledExprInfo * CompiledSingleFrom =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let ret = compiler.CompileSingleFromExpr from where
    let info =
        { Arguments = compiler.Arguments
          UsedSchemas = compiler.UsedSchemas
        }
    (info, ret)

let compileSingleFieldExpr (layout : Layout) (flags : ExprCompilationFlags) (arguments : QueryArguments) (expr : ResolvedFieldExpr) : CompiledExprInfo * SQL.ValueExpr =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let newExpr = compiler.CompileSingleFieldExpr flags expr
    let info =
        { Arguments = compiler.Arguments
          UsedSchemas = compiler.UsedSchemas
        }
    (info, newExpr)

let compileSelectExpr (layout : Layout) (arguments : QueryArguments) (viewExpr : ResolvedSelectExpr) : CompiledExprInfo * SQL.SelectExpr =
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, arguments)
    let (info, select) = compiler.CompileSelectExpr None false viewExpr
    let retInfo =
        { Arguments = compiler.Arguments
          UsedSchemas = compiler.UsedSchemas
        }
    (retInfo, select)

let private convertPureColumns (infos : PureColumn seq) = infos |> Seq.map (fun info -> (info.ColumnType, info.Name, info.Expression)) |> Seq.toArray

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let mainEntityRef = viewExpr.MainEntity |> Option.map (fun main -> main.Entity)
    let arguments = compileArguments viewExpr.Arguments
    let compiler = QueryCompiler (layout, defaultAttrs, arguments)
    let (info, expr) = compiler.CompileSelectExpr mainEntityRef true viewExpr.Select
    let arguments = compiler.Arguments
    let columns = Array.map (mapColumnType getFinalName) info.Columns

    let allPureAttrs = findPureExprAttributes columns expr
    let newExpr = filterNonpureExprColumns allPureAttrs expr

    let checkColumn i _ = Option.isNone allPureAttrs.[i]
    let newColumns = Seq.filteri checkColumn columns |> Seq.toArray

    let onlyPureAttrs = Seq.catMaybes allPureAttrs |> Seq.toArray
    let attrQuery =
        if Array.isEmpty onlyPureAttrs then
            None
        else
            let filterPureColumn (info : PureColumn) =
                match info.ColumnType with
                | CTMeta (CMRowAttribute name) when info.Purity = Pure -> true
                | CTColumnMeta (colName, CCCellAttribute name) when info.Purity = Pure -> true
                | _ -> false
            let (pureColumns, attributeColumns) = onlyPureAttrs |> Seq.partition filterPureColumn
            Some
                { PureColumns = convertPureColumns pureColumns
                  AttributeColumns = convertPureColumns attributeColumns
                }

    let domains = mapDomainsFields getFinalName info.Domains
    let flattenedDomains = flattenDomains domains

    let columnsWithNames = Array.map (fun name -> (name, compiler.ColumnName name)) newColumns

    let compilePragma name v = (compileName name, compileFieldValue v)

    { Pragmas = Map.mapWithKeys compilePragma viewExpr.Pragmas
      AttributesQuery = attrQuery
      Query = { Expression = newExpr; Arguments = arguments }
      UsedSchemas = compiler.UsedSchemas
      Columns = columnsWithNames
      Domains = domains
      FlattenedDomains = flattenedDomains
      MainEntity = mainEntityRef
    }
