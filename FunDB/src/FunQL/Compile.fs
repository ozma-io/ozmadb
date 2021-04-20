module FunWithFlags.FunDB.FunQL.Compile

open System
open NpgsqlTypes
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
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

[<NoEquality; NoComparison>]
type private HalfCompiledSelect =
    { Domains : TempDomains
      MetaColumns : Map<MetaType, SQL.ValueExpr>
      Columns : SelectColumn[]
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

type JoinKey =
    { Table : SQL.TableName
      Column : SQL.ColumnName
      ToEntity : ResolvedEntityRef // Real entity
    }
type JoinPath =
    { Name : SQL.TableName
      Nested : JoinPaths
    }
and JoinPaths = Map<JoinKey, JoinPath>

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
    | FETScalar SFTUserViewRef -> FUserViewRef { schema = None; name = FunQLName "" }
    | FETScalar SFTUuid -> FUuid Guid.Empty

let defaultCompiledArgument : ResolvedFieldType -> FieldValue = function
    | FTType feType -> defaultCompiledExprArgument feType
    | FTReference entityRef -> FInt 0
    | FTEnum values -> values |> Set.toSeq |> Seq.first |> Option.get |> FString

// Evaluation of column-wise or global attributes
type CompiledAttributesExpr =
    { Query : string
      Columns : ColumnType[]
      PureAttributes : Set<AttributeName>
      PureColumnAttributes : Map<FieldName, Set<AttributeName>>
    }

[<NoEquality; NoComparison>]
type CompiledViewExpr =
    { AttributesQuery : CompiledAttributesExpr option
      Query : Query<SQL.SelectExpr>
      Columns : (ColumnType * SQL.ColumnName)[]
      Domains : Domains
      MainEntity : ResolvedEntityRef option
      FlattenedDomains : FlattenedDomains
      UsedSchemas : UsedSchemas
    }

let private compileOrder : SortOrder -> SQL.SortOrder = function
    | Asc -> SQL.Asc
    | Desc -> SQL.Desc

let private compileJoin : JoinType -> SQL.JoinType = function
    | Left -> SQL.Left
    | Right -> SQL.Right
    | Inner -> SQL.Inner
    | Outer -> SQL.Full

let private compileSetOp : SetOperation -> SQL.SetOperation = function
    | Union -> SQL.Union
    | Except -> SQL.Except
    | Intersect -> SQL.Intersect

let compileEntityRef (entityRef : EntityRef) : SQL.TableRef = { schema = Option.map compileName entityRef.schema; name = compileName entityRef.name }

// We rename entity references to no-schema prefixed names. This way we guarantee uniqueness between automatically generated sub-entity SELECTs and actual entities.
// For example, `schema2.foo` inherits `schema1.foo`, and they are both used in the query. Without renaming, we would have:
// FROM schema1.foo .... (SELECT * FROM schema2.foo WHERE ...) as foo.
// and a conflict would ensue.
let renameResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.SQLName =
    SQL.SQLName <| sprintf "%s__%s" (string entityRef.schema) (string entityRef.name)

let compileRenamedResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef =
    { schema = None; name = renameResolvedEntityRef entityRef }

let compileRenamedEntityRef (entityRef : EntityRef) : SQL.TableRef =
    match entityRef.schema with
    | Some schemaName -> compileRenamedResolvedEntityRef { schema = schemaName; name = entityRef.name }
    | None -> { schema = None; name = compileName entityRef.name }

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = Some (compileName entityRef.schema); name = compileName entityRef.name }

let compileFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

// Be careful -- one shouldn't use these functions when compiling real field names, only references to sub-entities!
// That's because real fields use `ColumnName`s.
let compileRenamedFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileRenamedEntityRef fieldRef.entity; name = compileName fieldRef.name }

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

let composeExhaustingIf (compileTag : 'tag -> SQL.ValueExpr) (options : ('tag * SQL.ValueExpr) array) : SQL.ValueExpr =
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

let private rewriteSubEntityCheck (subEntity : SQL.ValueExpr) : SQL.ValueExpr -> SQL.ValueExpr =
    SQL.genericMapValueExpr
        { SQL.idValueExprGenericMapper with
              ColumnReference = fun _ -> subEntity
        }

type ReferenceContext =
    | RCExpr
    | RCTypeExpr

let private compileBinaryOp = function
    | BOLess -> SQL.BOLess
    | BOLessEq -> SQL.BOLessEq
    | BOGreater -> SQL.BOGreater
    | BOGreaterEq -> SQL.BOGreaterEq
    | BOEq -> SQL.BOEq
    | BONotEq -> SQL.BONotEq
    | BOConcat -> SQL.BOConcat
    | BOLike -> SQL.BOLike
    | BOILike -> SQL.BOILike
    | BONotLike -> SQL.BONotLike
    | BONotILike -> SQL.BONotILike
    | BOMatchRegex -> SQL.BOMatchRegex
    | BOMatchRegexCI -> SQL.BOMatchRegexCI
    | BONotMatchRegex -> SQL.BONotMatchRegex
    | BONotMatchRegexCI -> SQL.BONotMatchRegexCI
    | BOPlus -> SQL.BOPlus
    | BOMinus -> SQL.BOMinus
    | BOMultiply -> SQL.BOMultiply
    | BODivide -> SQL.BODivide
    | BOJsonArrow -> SQL.BOJsonArrow
    | BOJsonTextArrow -> SQL.BOJsonTextArrow

let rec genericCompileFieldExpr (layout : Layout) (refFunc : ReferenceContext -> 'f -> SQL.ValueExpr) (queryFunc : SelectExpr<'e, 'f> -> SQL.SelectExpr) : FieldExpr<'e, 'f> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> SQL.VEValue <| compileFieldValue v
        | FERef c -> refFunc RCExpr c
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
        | FEInQuery (a, query) -> SQL.VEInQuery (traverse a, queryFunc query)
        | FENotInQuery (a, query) -> SQL.VENotInQuery (traverse a, queryFunc query)
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
        | FEAggFunc (name,  args) -> SQL.VEAggFunc (Map.find name allowedAggregateFunctions, genericCompileAggExpr traverse args)
        | FESubquery query -> SQL.VESubquery (queryFunc query)
        | FEInheritedFrom (c, subEntityRef) ->
            let info = subEntityRef.Extra :?> ResolvedSubEntityInfo
            if info.AlwaysTrue then
                SQL.VEValue (SQL.VBool true)
            else
                let col = refFunc RCTypeExpr c
                let entity = layout.FindEntity (tryResolveEntityRef subEntityRef.Ref |> Option.get) |> Option.get
                let inheritance = entity.Inheritance |> Option.get
                rewriteSubEntityCheck col inheritance.CheckExpr
        | FEOfType (c, subEntityRef) ->
            let info = subEntityRef.Extra :?> ResolvedSubEntityInfo
            if info.AlwaysTrue then
                SQL.VEValue (SQL.VBool true)
            else
                let col = refFunc RCTypeExpr c
                let entity = layout.FindEntity (tryResolveEntityRef subEntityRef.Ref |> Option.get) |> Option.get
                SQL.VEBinaryOp (col, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))
    traverse

and genericCompileAggExpr (func : FieldExpr<'e, 'f> -> SQL.ValueExpr) : AggExpr<'e, 'f> -> SQL.AggExpr = function
    | AEAll exprs -> SQL.AEAll (Array.map func exprs)
    | AEDistinct expr -> SQL.AEDistinct (func expr)
    | AEStar -> SQL.AEStar

let replaceColumnRefs (columnRef : SQL.ColumnRef) : SQL.ValueExpr -> SQL.ValueExpr =
    let mapper =
        { SQL.idValueExprMapper with
              ColumnReference = fun _ -> columnRef
        }
    SQL.mapValueExpr mapper

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
    }

type private CTEBindings = Map<SQL.TableName, SelectInfo>

type private UpdateRecCTEBindings = SelectSignature -> TempDomains -> CTEBindings

let private selectSignature (half : HalfCompiledSelect) : SelectSignature =
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

let private infoFromSignature (flags : SelectFlags) (domains : TempDomains) (signature : SelectSignature) : SelectInfo =
    { Columns = Array.ofSeq (signatureColumnTypes signature)
      Domains = domains
    }

type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments
    let mutable usedSchemas : UsedSchemas = Map.empty
    let replacer = NameReplacer ()
    let mutable lastRenamedTableId = 0

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

    let signatureColumns (skipNames : bool) (sign : SelectSignature) (half : HalfCompiledSelect) : SQL.SelectedColumn seq =
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
        let half = sel.Extra :?> HalfCompiledSelect
        let columns = signatureColumns skipNames sign half |> Array.ofSeq
        SQL.SSelect { sel with Columns = columns; Extra = null }
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
                let case = SQL.VEBinaryOp (SQL.VEColumn { table = Some tableRef; name = columnName (CTMeta (CMDomain ns)) }, SQL.BOEq, SQL.VEValue (SQL.VInt localId))
                (case, domainExpression tableRef f subcase)
            SQL.VECase (nested |> Map.toSeq |> Seq.map makeCase |> Seq.toArray, None)

    let fromInfoExpression (tableRef : SQL.TableRef) (f : TempDomain -> SQL.ValueExpr) = function
        | FTEntity (id, dom) -> f dom
        | FTSubquery info -> domainExpression tableRef f info.Domains

    let convertLinkedLocalExpr (localRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
        let onGlobalArg (name : ArgumentName) =
            let (argPlaceholder, newArguments) = addArgument (PGlobal name) (Map.find name globalArgumentTypes) arguments
            arguments <- newArguments
        setRelatedExprEntity onGlobalArg localRef

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

    let mutable lastJoinId = 0
    let newJoinId () =
        let id = lastJoinId
        lastJoinId <- lastJoinId + 1
        compileJoinId id

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
          MainId = mainId
          MainSubEntity = mainSubEntity
        }

    let rec compileRef (ctx : ReferenceContext) (paths0 : JoinPaths) (tableRef : SQL.TableRef) (fieldRef : ResolvedFieldRef) (forcedName : FieldName option) : JoinPaths * SQL.ValueExpr =
        let realColumn name : SQL.ColumnRef =
            let finalName =
                match forcedName with
                | Some n -> compileName n
                | None -> name
            { table = Some tableRef; name = finalName }

        let entity = layout.FindEntity fieldRef.entity |> Option.get
        let fieldInfo = entity.FindField fieldRef.name |> Option.get

        match fieldInfo.Field with
        | RId ->
            usedSchemas <- addUsedEntityRef fieldRef.entity usedSchemas
            (paths0, SQL.VEColumn <| realColumn sqlFunId)
        | RSubEntity ->
            usedSchemas <- addUsedEntityRef fieldRef.entity usedSchemas
            match ctx with
            | RCExpr ->
                let newColumn = realColumn sqlFunSubEntity
                (paths0, replaceColumnRefs newColumn entity.SubEntityParseExpr)
            | RCTypeExpr ->
                (paths0, SQL.VEColumn <| realColumn sqlFunSubEntity)
        | RColumnField col ->
            usedSchemas <- addUsedField fieldRef.entity.schema fieldRef.entity.name fieldInfo.Name usedSchemas
            (paths0, SQL.VEColumn  <| realColumn col.ColumnName)
        | RComputedField comp ->
            let localRef = { schema = Option.map decompileName tableRef.schema; name = decompileName tableRef.name } : EntityRef
            usedSchemas <- mergeUsedSchemas comp.UsedSchemas usedSchemas
            match comp.VirtualCases with
            | None -> compileLinkedFieldExpr Map.empty paths0 <| convertLinkedLocalExpr localRef comp.Expression
            | Some cases ->
                let subEntityRef = { table = Some tableRef; name = sqlFunSubEntity } : SQL.ColumnRef
                let mutable paths = paths0

                let compileCase (case : VirtualFieldCase) =
                    let (newPaths, compiled) = compileLinkedFieldExpr Map.empty paths <| convertLinkedLocalExpr localRef case.Expression
                    paths <- newPaths
                    (case, compiled)
                let options = Array.map compileCase cases
                assert not (Array.isEmpty options)

                let compileTag (case : VirtualFieldCase) =
                    replaceColumnRefs subEntityRef case.Check
                let compiled = composeExhaustingIf compileTag options
                (paths, compiled)

    and compilePath (ctx : ReferenceContext) (paths : JoinPaths) (tableRef : SQL.TableRef) (fieldRef : ResolvedFieldRef) (forcedName : FieldName option) : FieldName list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef ctx paths tableRef fieldRef forcedName
        | (ref :: refs) ->
            let fieldInfo = layout.FindField fieldRef.entity fieldRef.name |> Option.get
            match fieldInfo.Field with
            | RColumnField ({ FieldType = FTReference newEntityRef } as col) ->
                let newFieldRef = { entity = newEntityRef; name = ref }
                let column =
                    match forcedName with
                    | None -> col.ColumnName
                    | Some n -> compileName n
                let pathKey =
                    { Table = tableRef.name
                      Column = column
                      ToEntity = newEntityRef
                    }
                let (newPath, res) =
                    match Map.tryFind pathKey paths with
                    | None ->
                        let newRealName = newJoinId ()
                        let newTableRef = { schema = None; name = newRealName } : SQL.TableRef
                        let (nested, res) = compilePath ctx Map.empty newTableRef newFieldRef None refs
                        let path =
                            { Name = newRealName
                              Nested = nested
                            }
                        (path, res)
                    | Some path ->
                        let newTableRef = { schema = None; name = path.Name } : SQL.TableRef
                        let (nested, res) = compilePath ctx path.Nested newTableRef newFieldRef None refs
                        let newPath = { path with Nested = nested }
                        (newPath, res)
                usedSchemas <- addUsedField fieldRef.entity.schema fieldRef.entity.name fieldInfo.Name usedSchemas
                (Map.add pathKey newPath paths, res)
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    and compileLinkedFieldRef (ctx : ReferenceContext) (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
        match linked.Ref with
        | VRColumn ref ->
            match (linked.Path, ref.Bound) with
            | ([||], None) ->
                let columnRef = compileRenamedFieldRef ref.Ref
                (paths0, SQL.VEColumn columnRef)
            | (_, Some boundRef) ->
                let tableRef =
                    match ref.Ref.entity with
                    | Some renamedTable -> compileRenamedEntityRef renamedTable
                    | None -> compileRenamedResolvedEntityRef boundRef.Ref.entity
                // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
                // If it's not we need to keep original naming.
                let newName =
                    if boundRef.Immediate then None else Some ref.Ref.name
                compilePath ctx paths0 tableRef boundRef.Ref newName (Array.toList linked.Path)
            | _ -> failwith "Unexpected path with no bound field"
        | VRPlaceholder name ->
            if Array.isEmpty linked.Path then
                // Explicitly set argument type to avoid ambiguity,
                let arg = arguments.Types.[name]
                (paths0, SQL.VECast (SQL.VEPlaceholder arg.PlaceholderId, arg.DbType))
            else
                let argInfo = Map.find name initialArguments.Types
                match argInfo.FieldType with
                | FTReference argEntityRef ->
                    let firstName = linked.Path.[0]
                    let remainingPath = Array.skip 1 linked.Path
                    let argEntityRef' = relaxEntityRef argEntityRef

                    // Subquery
                    let makeColumn name path =
                        let bound =
                            { Ref = { entity = argEntityRef; name = name }
                              Immediate = true
                            }
                        let col = VRColumn { Ref = ({ entity = Some argEntityRef'; name = name } : FieldRef); Bound = Some bound }
                        { Ref = col; Path = path }

                    let idColumn = makeColumn funId [||]
                    let arg = { Ref = VRPlaceholder name; Path = [||] }

                    let result =
                        { Attributes = Map.empty
                          Result = FERef <| makeColumn firstName remainingPath
                          Alias = None
                        }
                    let selectClause =
                        { Attributes = Map.empty
                          Results = [| QRExpr result |]
                          From = Some <| FEntity (None, argEntityRef')
                          Where = Some <| FEBinaryOp (FERef idColumn, BOEq, FERef arg)
                          GroupBy = [||]
                          OrderLimit = emptyOrderLimitClause
                          Extra = null
                        } : ResolvedSingleSelectExpr
                    let selectExpr =
                        { CTEs = None
                          Tree = SSelect selectClause
                          Extra = null
                        }
                    let flags =
                        { MainEntity = None
                          IsTopLevel = false
                          MetaColumns = false
                        }
                    let (info, subquery) = compileSelectExpr flags Map.empty None selectExpr
                    (paths0, SQL.VESubquery subquery)
                | typ -> failwith <| sprintf "Argument is not a reference: %O" name

    and compileLinkedFieldExpr (cteBindings : CTEBindings) (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0
        let compileLinkedRef ctx linked =
            let (newPaths, ret) = compileLinkedFieldRef ctx paths linked
            paths <- newPaths
            ret
        let compileSubSelectExpr =
            let flags =
                { MainEntity = None
                  IsTopLevel = false
                  MetaColumns = false
                }
            snd << compileSelectExpr flags cteBindings None
        let ret = genericCompileFieldExpr layout compileLinkedRef compileSubSelectExpr expr
        (paths, ret)

    and compileOrderLimitClause (cteBindings : CTEBindings) (paths0 : JoinPaths) (clause : ResolvedOrderLimitClause) : JoinPaths * SQL.OrderLimitClause =
        let mutable paths = paths0
        let compileFieldExpr' expr =
            let (newPaths, ret) = compileLinkedFieldExpr cteBindings paths expr
            paths <- newPaths
            ret
        let ret =
            { OrderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr' expr)) clause.OrderBy
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
        let compiledValues = values |> Array.map (Array.map (compileLinkedFieldExpr cteBindings Map.empty >> snd))
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
                        let (limitPaths, compiledLimits) = compileOrderLimitClause cteBindings Map.empty setOp.OrderLimit
                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        let domainsMap1 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap1
                        let domainsMap2 = Map.map (fun name -> tryRenameDomainFields renames) domainsMap2
                        assert Map.isEmpty limitPaths
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
                        let (limitPaths, compiledLimits) = compileOrderLimitClause cteBindings Map.empty setOp.OrderLimit
                        let (renames, newSig) = mergeSelectSignature sig1 sig2
                        assert Map.isEmpty limitPaths
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
        let info = infoFromSignature flags domains signature
        (info, ret)

    and compileCommonTableExpr (flags : SelectFlags) (cteBindings : CTEBindings) (name : SQL.TableName) (cte : ResolvedCommonTableExpr) : SelectInfo * SQL.CommonTableExpr =
        let extra = cte.Extra :?> ResolvedCommonTableExprInfo
        let flags = { flags with MainEntity = if extra.MainEntity then flags.MainEntity else None }
        let updateCteBindings (signature : SelectSignature) (domains : TempDomains) =
            let info = infoFromSignature flags domains signature
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
              Materialized = Some false
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

    and compileSingleSelectExpr (flags : SelectFlags) (cteBindings : CTEBindings) (select : ResolvedSingleSelectExpr) : HalfCompiledSelect * SQL.SingleSelectExpr =
        let mutable paths = Map.empty

        let extra =
            if isNull select.Extra then
                { HasAggregates = false
                }
            else
                select.Extra :?> ResolvedSingleSelectInfo

        let (fromMap, from) =
            match select.From with
            | Some from ->
                let (fromMap, newFrom) = compileFromExpr cteBindings flags.MainEntity from
                (fromMap, Some newFrom)
            | None -> (Map.empty, None)

        let where =
            match select.Where with
            | None -> None
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr cteBindings paths where
                paths <- newPaths
                Some ret

        let compileGroupBy expr =
            let (newPaths, compiled) = compileLinkedFieldExpr cteBindings paths expr
            paths <- newPaths
            compiled
        let groupBy = Array.map compileGroupBy select.GroupBy

        let compileRowAttr (name, expr) =
            let (newPaths, col) = compileLinkedFieldExpr cteBindings paths expr
            paths <- newPaths
            (CMRowAttribute name, col)
        let attributeColumns =
            select.Attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        let addMetaColumns = flags.MetaColumns && not extra.HasAggregates

        let mutable idCols = Map.empty : Map<string, DomainIdColumn>
        let mutable lastIdCol = 1

        let getResultColumnEntry (i : int) (result : ResolvedQueryColumnResult) : ResultColumn =
            let currentAttrs = Map.keysSet result.Attributes

            let (newPaths, resultColumn) = compileColumnResult cteBindings paths result
            paths <- newPaths

            match resultFieldRef result with
            | Some ({ Ref = { Ref = { entity = Some ({ name = entityName } as entityRef); name = fieldName } } } as resultRef) when addMetaColumns ->
                // Add columns for tracking (id, sub_entity etc.)
                let tableRef = compileRenamedEntityRef entityRef
                let fromInfo = Map.find tableRef.name fromMap

                let finalRef = resultRef.Ref.Bound |> Option.map (fun bound -> followPath layout bound.Ref (List.ofArray resultRef.Path))

                // Add system columns (id or sub_entity - this is a generic function).
                let makeMaybeSystemColumn (needColumn : ResolvedFieldRef -> bool) (columnConstr : int -> MetaType) (name : FieldName) =
                    if Array.isEmpty resultRef.Path
                    then
                        let mutable foundSystem = false

                        let getSystemColumn (domain : TempDomain) =
                            match Map.tryFind (TName fieldName) domain with
                            | None -> SQL.VEValue SQL.VNull
                            | Some info ->
                                if needColumn info.Ref then
                                    let colName =
                                        if info.IdColumn = idDefault then
                                            compileName name
                                        else
                                            columnName (CTMeta (columnConstr info.IdColumn))
                                    foundSystem <- true
                                    SQL.VEColumn { table = Some tableRef; name = colName }
                                else
                                    SQL.VEValue SQL.VNull

                        let systemExpr = fromInfoExpression tableRef getSystemColumn fromInfo.FromType
                        if foundSystem then
                            Some systemExpr
                        else
                            None
                    else
                        let newRef = Option.get finalRef
                        if needColumn newRef then
                            let systemPath = Seq.append (Seq.take (Array.length resultRef.Path - 1) resultRef.Path) (Seq.singleton name) |> Array.ofSeq
                            let systemRef = { Ref = VRColumn resultRef.Ref; Path = systemPath }
                            let (newPaths, systemExpr) = compileLinkedFieldRef RCTypeExpr paths systemRef
                            paths <- newPaths
                            Some systemExpr
                        else
                            None

                let needsSubEntity (ref : ResolvedFieldRef) =
                    let entity = layout.FindEntity ref.entity |> Option.get
                    not <| Map.isEmpty entity.Children

                let maybeIdExpr = makeMaybeSystemColumn (fun _ -> true) CMId funId
                let maybeSubEntityExpr = makeMaybeSystemColumn needsSubEntity CMSubEntity funSubEntity

                // Maybe we already have a fitting `id` column added for a similar column.
                let (maybeSystemName, systemColumns) =
                    match maybeIdExpr with
                    | None -> (None, Seq.empty)
                    | Some idExpr ->
                        let name = idKey entityName fieldName resultRef.Path
                        match Map.tryFind name idCols with
                        | None ->
                            let idCol = lastIdCol
                            lastIdCol <- lastIdCol + 1
                            let makeSubEntityColumn expr = (CMSubEntity idCol, expr)

                            let column = (CMId idCol, idExpr)
                            let subEntityColumn = Option.map makeSubEntityColumn maybeSubEntityExpr

                            idCols <- Map.add name idCol idCols

                            (Some idCol, Seq.append (Seq.singleton column) (Option.toSeq subEntityColumn))
                        | Some idCol -> (Some idCol, Seq.empty)

                // These are used only when `Path` is empty, hence we use `fieldName`.
                let getNewDomain (domain : TempDomain) =
                    match Map.tryFind (TName fieldName) domain with
                    | Some info ->
                        Map.singleton resultColumn.Name { info with IdColumn = Option.get maybeSystemName }
                    | None -> Map.empty
                let rec getNewDomains = function
                | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))

                let (pathRef, newDomains) =
                    if Array.isEmpty resultRef.Path
                    then
                        let newDomains =
                            match fromInfo.FromType with
                            | FTEntity (domainId, domain) -> DSingle (domainId, getNewDomain domain)
                            | FTSubquery info -> getNewDomains info.Domains
                        (None, newDomains)
                    else
                        // Pathed refs always have bound fields.
                        let newRef = Option.get finalRef
                        let newInfo =
                            { Ref = newRef
                              IdColumn = Option.get maybeSystemName
                            }
                        let newDomains = DSingle (newGlobalDomainId (), Map.singleton resultColumn.Name newInfo )
                        (Some newRef, newDomains)

                let rec getDomainColumns = function
                | DSingle (id, domain) -> Seq.empty
                | DMulti (ns, nested) ->
                    let colName = CMDomain ns
                    let col = (colName, SQL.VEColumn { table = Some tableRef; name = columnName (CTMeta colName) })
                    Seq.append (Seq.singleton col) (nested |> Map.values |> Seq.collect getDomainColumns)

                // Propagate domain columns from subquery, It only makes sense without a path, when
                // rows might be from different sources.
                let subqueryDomainColumns =
                    if Array.isEmpty resultRef.Path
                    then
                        match fromInfo.FromType with
                        | FTEntity _ -> Seq.empty
                        | FTSubquery info -> getDomainColumns info.Domains
                    else
                        Seq.empty

                let punColumns =
                    if flags.IsTopLevel
                    then
                        match pathRef with
                        | None ->
                            let mutable foundPun = false

                            let getPunColumn (domain : TempDomain) =
                                match Map.tryFind (TName fieldName) domain with
                                | None -> SQL.VEValue SQL.VNull
                                | Some info ->
                                    match layout.FindField info.Ref.entity info.Ref.name |> Option.get with
                                    | { Field = RColumnField { FieldType = FTReference newEntityRef } } ->
                                        let fieldRef = { entity = info.Ref.entity; name = fieldName }
                                        let (newPaths, expr) = compilePath RCExpr paths tableRef fieldRef None [funMain]
                                        paths <- newPaths
                                        foundPun <- true
                                        expr
                                    | _ -> SQL.VEValue SQL.VNull

                            let punExpr = fromInfoExpression tableRef getPunColumn fromInfo.FromType
                            if foundPun then
                                let col = (CCPun, punExpr)
                                Seq.singleton col
                            else
                                Seq.empty
                        | Some endRef ->
                            let endFieldInfo = layout.FindField endRef.entity endRef.name |> Option.get
                            match endFieldInfo.Field with
                            | RColumnField { FieldType = FTReference newEntityRef } ->
                                let punPath = Seq.append (Seq.take (Array.length resultRef.Path - 1) resultRef.Path) (Seq.singleton funMain) |> Array.ofSeq
                                let punRef = { Ref = VRColumn resultRef.Ref; Path = punPath }
                                let (newPaths, punExpr) = compileLinkedFieldRef RCExpr paths punRef
                                paths <- newPaths
                                let col = (CCPun, punExpr)
                                Seq.singleton col
                            | _ -> Seq.empty
                    else
                        Seq.empty

                // Nested and default attributes.
                let attrColumns =
                    match fromInfo.FromType with
                    | FTEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find (TName fieldName) domain
                        match defaultAttrs.FindField info.Ref.entity info.Ref.name with
                        | None -> Seq.empty
                        | Some attrs ->
                            let makeDefaultAttr name =
                                let attr = Map.find name attrs
                                let expr = convertLinkedLocalExpr entityRef attr.Expression
                                let attrCol = CCCellAttribute name
                                let (newPaths, compiled) = compileLinkedFieldExpr cteBindings paths expr
                                paths <- newPaths
                                (attrCol, compiled)
                            let defaultSet = Map.keysSet attrs
                            let inheritedAttrs = Set.difference defaultSet currentAttrs
                            inheritedAttrs |> Set.toSeq |> Seq.map makeDefaultAttr
                    | FTSubquery queryInfo ->
                        // Inherit column and cell attributes from subquery.
                        let filterColumnAttr = function
                        | CTColumnMeta (colName, CCCellAttribute name) when colName = TName fieldName -> Some name
                        | _ -> None
                        let oldAttrs = queryInfo.Columns |> Seq.mapMaybe filterColumnAttr |> Set.ofSeq
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrCol = CCCellAttribute name
                            (attrCol, SQL.VEColumn { table = Some tableRef; name = columnName (CTColumnMeta (fieldName, attrCol)) })
                        inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr

                let myMeta = [ attrColumns; punColumns ] |> Seq.concat |> Map.ofSeq

                { Domains = Some newDomains
                  MetaColumns = [ systemColumns; subqueryDomainColumns ] |> Seq.concat |> Map.ofSeq
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
                            | Some id -> Some { table = Some { schema = None; name = name }; name = id }
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

        // At this point we need to ensure there are no lazy sequences left that could mutate paths when computed ;)
        let newFrom =
            if Map.isEmpty paths then
                from
            else
                let fromVal = Option.get from
                Some <| buildJoins fromVal paths

        let info =
            { Domains = newDomains
              MetaColumns = metaColumns
              Columns = resultColumns
            } : HalfCompiledSelect

        let query =
            { Columns = [||] // We fill columns after processing UNIONs, so that ordering and number of columns and meta-columns is the same everywhere.
              From = newFrom
              Where = where
              GroupBy = groupBy
              OrderLimit = orderLimit
              Extra = info
            } : SQL.SingleSelectExpr

        (info, query)

    and compileColumnResult (cteBindings : CTEBindings) (paths0 : JoinPaths) (result : ResolvedQueryColumnResult) : JoinPaths * SelectColumn =
        let mutable paths = paths0

        let (newPaths, newExpr) = compileLinkedFieldExpr cteBindings paths result.Result
        paths <- newPaths

        let compileAttr (attrName, expr) =
            let attrCol = CCCellAttribute attrName
            let (newPaths, ret) = compileLinkedFieldExpr cteBindings paths expr
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

    and buildJoins (from : SQL.FromExpr) (paths : JoinPaths) : SQL.FromExpr =
        Map.fold joinPath from paths

    and joinPath (from : SQL.FromExpr) (joinKey : JoinKey) (path : JoinPath) : SQL.FromExpr =
        let tableRef = { schema = None; name = joinKey.Table } : SQL.TableRef
        let toTableRef = { schema = None; name = path.Name } : SQL.TableRef
        let entity = layout.FindEntity joinKey.ToEntity |> Option.get

        let fromColumn = SQL.VEColumn { table = Some tableRef; name = joinKey.Column }
        let toColumn = SQL.VEColumn { table = Some toTableRef; name = sqlFunId }
        let joinExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
        let alias = { Name = path.Name; Columns = None } : SQL.TableAlias
        let subquery = SQL.FTable ({ RealEntity = joinKey.ToEntity }, Some alias, compileResolvedEntityRef entity.Root)
        let currJoin = SQL.FJoin { Type = SQL.Left; A = from; B = subquery; Condition = joinExpr }
        usedSchemas <- addUsedEntityRef joinKey.ToEntity usedSchemas
        buildJoins currJoin path.Nested

    and compileFromExpr (cteBindings : CTEBindings) (mainEntity : ResolvedEntityRef option) : ResolvedFromExpr -> FromMap * SQL.FromExpr = function
        | FEntity (pun, { schema = Some schema; name = name }) ->
            let entityRef = { schema = schema; name = name }
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef

            let makeDomainEntry name field =
                { Ref = { entity = entityRef; name = name }
                  IdColumn = idDefault
                }
            let domain = mapAllFields makeDomainEntry entity |> Map.mapKeys TName
            let mainEntry =
                { Ref = { entity = entityRef; name = entity.MainField }
                  IdColumn = idDefault
                }
            let domain = Map.add (TName funMain) mainEntry domain
            let newAlias = compileAliasFromEntity entityRef pun

            let subquery =
                match entity.Inheritance with
                | None ->
                    SQL.FTable ({ RealEntity = entityRef }, Some newAlias, compileResolvedEntityRef entityRef)
                | Some inheritance ->
                    let select =
                        { Columns = [| SQL.SCAll None |]
                          From = Some <| SQL.FTable (null, None, compileResolvedEntityRef entity.Root)
                          Where = Some inheritance.CheckExpr
                          GroupBy = [||]
                          OrderLimit = SQL.emptyOrderLimitClause
                          Extra = null
                        } : SQL.SingleSelectExpr
                    let expr = { Extra = { RealEntity = entityRef }; CTEs = None; Tree = SQL.SSelect select } : SQL.SelectExpr
                    SQL.FSubExpr (newAlias, expr)
            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let subEntity = if Map.isEmpty entity.Children then None else Some sqlFunSubEntity
                    (Some sqlFunId, subEntity)
            let fromInfo =
                { FromType = FTEntity (newGlobalDomainId (), domain)
                  MainId = mainId
                  MainSubEntity = mainSubEntity
                }

            usedSchemas <- addUsedEntityRef entityRef usedSchemas
            (Map.singleton newAlias.Name fromInfo, subquery)
        | FEntity (pun, { schema = None; name = name }) ->
            let name' = compileName name
            let selectSig = Map.find name' cteBindings
            let compiledAlias = Option.map compileAliasFromName pun
            let newName = Option.defaultValue name pun
            let fromInfo = subentityFromInfo mainEntity selectSig
            let ret = SQL.FTable (null, compiledAlias, { schema = None; name = name' })
            (Map.singleton (compileName newName) fromInfo, ret)
        | FJoin join ->
            let main1 =
                match join.Type with
                | Left -> mainEntity
                | _ -> None
            let (fromMap1, r1) = compileFromExpr cteBindings main1 join.A
            let main2 =
                match join.Type with
                | Right -> mainEntity
                | _ -> None
            let (fromMap2, r2) = compileFromExpr cteBindings main2 join.B
            let fromMap = Map.unionUnique fromMap1 fromMap2
            let (joinPaths, joinExpr) = compileLinkedFieldExpr cteBindings Map.empty join.Condition
            if not <| Map.isEmpty joinPaths then
                failwith <| sprintf "Unexpected dereference in join expression: %O" join.Condition
            let ret =
                { Type = compileJoin join.Type
                  A = r1
                  B = r2
                  Condition = joinExpr
                } : SQL.JoinExpr
            (fromMap, SQL.FJoin ret)
        | FSubExpr (alias, q) ->
            let flags =
                { MainEntity = mainEntity
                  IsTopLevel = false
                  MetaColumns = true
                }
            let (info, expr) = compileSelectExpr flags cteBindings None q
            let (info, fields) =
                match alias.Fields with
                | None -> (info, None)
                | Some fields ->
                    let info = renameSelectInfo fields info
                    let fields = info.Columns |> Array.map (mapColumnType getFinalName >> columnName)
                    (info, Some fields)
            let compiledAlias =
                { Name = compileName alias.Name
                  Columns = fields
                } : SQL.TableAlias
            let ret = SQL.FSubExpr (compiledAlias, expr)
            let fromInfo = subentityFromInfo mainEntity info
            (Map.singleton compiledAlias.Name fromInfo, ret)

    member this.CompileSingleFromClause (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) =
        let (fromMap, compiledFrom) = compileFromExpr Map.empty None from
        let (newPaths, where) =
            match where with
            | None -> (Map.empty, None)
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr Map.empty Map.empty where
                (newPaths, Some ret)
        let builtFrom = buildJoins compiledFrom newPaths
        (builtFrom, where)

    member this.CompileSelectExpr (mainEntity : ResolvedEntityRef option) (metaColumns : bool) (select : ResolvedSelectExpr) =
        let flags =
            { MainEntity = mainEntity
              IsTopLevel = true
              MetaColumns = metaColumns
            }
        compileSelectExpr flags Map.empty None select

    member this.Arguments = arguments
    member this.UsedSchemas = usedSchemas
    member this.ColumnName name = columnName name

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

let private checkPureColumn : SQL.SelectedColumn -> PurityStatus option = function
    | SQL.SCAll _ -> None
    | SQL.SCExpr (name, expr) -> checkPureExpr expr

[<NoEquality; NoComparison>]
type private PureColumn =
    { ColumnType : ColumnType
      Purity : PurityStatus
      Result : SQL.SelectedColumn
    }

let rec private findPureTreeExprAttributes (columnTypes : ColumnType[]) : SQL.SelectTreeExpr -> (PureColumn option)[] = function
    | SQL.SSelect query ->
        let assignPure colType res =
            match checkPureColumn res with
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
                          Result = res
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

let compileSingleFromClause (layout : Layout) (argumentsMap : CompiledArgumentsMap) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : SQL.FromExpr * SQL.ValueExpr option =
    let bogusArguments =
        { emptyArguments with Types = argumentsMap }
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, bogusArguments)
    compiler.CompileSingleFromClause from where

let compileSelectExpr (layout : Layout) (argumentsMap : CompiledArgumentsMap) (viewExpr : ResolvedSelectExpr) : UsedSchemas * SQL.SelectExpr =
    let bogusArguments =
        { emptyArguments with Types = argumentsMap }
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, bogusArguments)
    let (info, select) = compiler.CompileSelectExpr None false viewExpr
    (compiler.UsedSchemas, select)

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let mainEntityRef = viewExpr.MainEntity |> Option.map (fun main -> main.Entity)
    let compiler = QueryCompiler (layout, defaultAttrs, compileArguments viewExpr.Arguments)
    let (info, expr) = compiler.CompileSelectExpr mainEntityRef true viewExpr.Select
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
            let query = SQL.SSelect {
                    Columns = Array.map (fun info -> info.Result) onlyPureAttrs
                    From = None
                    Where = None
                    GroupBy = [||]
                    OrderLimit = SQL.emptyOrderLimitClause
                    Extra = null
                }

            let getPureAttribute (info : PureColumn) =
                match info.ColumnType with
                | CTMeta (CMRowAttribute name) when info.Purity = Pure -> Some name
                | _ -> None
            let pureAttrs = onlyPureAttrs |> Seq.mapMaybe getPureAttribute |> Set.ofSeq
            let getPureColumnAttribute (info : PureColumn) =
                match info.ColumnType with
                | CTColumnMeta (colName, CCCellAttribute name) when info.Purity = Pure -> Some (colName, Set.singleton name)
                | _ -> None
            let pureColAttrs = onlyPureAttrs |> Seq.mapMaybe getPureColumnAttribute |> Map.ofSeqWith (fun name -> Set.union)
            Some { Query = query.ToString()
                   Columns = Array.map (fun info -> info.ColumnType) onlyPureAttrs
                   PureAttributes = pureAttrs
                   PureColumnAttributes = pureColAttrs
                 }

    let domains = mapDomainsFields getFinalName info.Domains
    let flattenedDomains = flattenDomains domains

    let columnsWithNames = Array.map (fun name -> (name, compiler.ColumnName name)) newColumns

    { AttributesQuery = attrQuery
      Query = { Expression = newExpr; Arguments = compiler.Arguments }
      Columns = columnsWithNames
      Domains = domains
      FlattenedDomains = flattenedDomains
      UsedSchemas = compiler.UsedSchemas
      MainEntity = mainEntityRef
    }