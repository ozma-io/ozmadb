module FunWithFlags.FunDB.FunQL.Compile

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

// Domains is a way to distinguish rows after set operations so that row types and IDs can be traced back.
// Domain is a map of assigned references to fields to each column.
// Domain key is cross product of all local domain ids i.e. map from domain namespace ids to local domain ids.
// Local domain ids get assigned to all sets in a set operation.
// Each row has a different domain key assigned, so half of rows may come from entity A and half from entity B.
// This way one can make use if IDs assigned to cells to reference and update them.
[<NoEquality; NoComparison>]
type DomainField =
    { ref : ResolvedFieldRef
      // A field with assigned idColumn of "foo" will use id column "__id__foo" and sub-entity column "__sub_entity__foo"
      idColumn : FieldName
    }

type Domain = Map<FieldName, DomainField>
type GlobalDomainId = int
type DomainNamespaceId = int
type LocalDomainId = int
[<NoEquality; NoComparison>]
type Domains =
    | DSingle of GlobalDomainId * Domain
    | DMulti of DomainNamespaceId * Map<LocalDomainId, Domains>

type MetaType =
    | CMRowAttribute of AttributeName
    | CMDomain of DomainNamespaceId
    // Columns with entity IDs/subentities are named for the first encountered column for this entity.
    // This is used to get domains for them later, to decode subentities.
    | CMId of FieldName
    | CMSubEntity of FieldName
    | CMMainId
    | CMMainSubEntity

type ColumnMetaType =
    | CCCellAttribute of AttributeName
    | CCPun

type ColumnType =
    | CTMeta of MetaType
    | CTColumnMeta of FunQLName * ColumnMetaType
    | CTColumn of FunQLName

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

// Expressions are not fully assembled right away. Instead we build SELECTs with no selected columns
// and this record in `extra`. During first pass through a union expression we collect signatures
// of all sub-SELECTs and build an ordered list of all columns, including meta-. Then we do a second
// pass, filling missing columns in individual sub-SELECTs with NULLs. This ensures number and order
// of columns if consistent in all subexpressions, and allows user to omit attributes in some
// sub-expressions,
[<NoEquality; NoComparison>]
type private SelectColumn =
    { name : FunQLName option
      column : SQL.ValueExpr
      meta : Map<ColumnMetaType, SQL.ValueExpr>
    }

[<NoEquality; NoComparison>]
type private HalfCompiledSelect =
    { domains : Domains
      metaColumns : Map<MetaType, SQL.ValueExpr>
      columns : SelectColumn[]
    }

[<NoEquality; NoComparison>]
type private SelectColumnSignature =
    { name : FunQLName option
      meta : Set<ColumnMetaType>
    }

[<NoEquality; NoComparison>]
type private SelectSignature =
    { metaColumns : Set<MetaType>
      columns : SelectColumnSignature[]
    }

[<NoEquality; NoComparison>]
type private SelectInfo =
    { domains : Domains
      // PostgreSQL column length is limited to 63 bytes, so we store column types separately.
      columns : ColumnType[]
    }

type FlattenedDomains = Map<GlobalDomainId, Domain>

[<NoEquality; NoComparison>]
type private FromType =
    | FTEntity of GlobalDomainId * Domain // Domain ID is used for merging.
    | FTSubquery of SelectInfo

[<NoEquality; NoComparison>]
type private FromInfo =
    { fromType : FromType
      mainId : SQL.ColumnName option
      mainSubEntity : SQL.ColumnName option
    }

type private FromMap = Map<EntityName, FromInfo>

let rec private appendDomain (dom : Domain) (doms : Domains) : Domains =
    match doms with
    | DSingle (id, d) -> DSingle (id, Map.unionUnique dom d)
    | DMulti (ns, subdomains) -> DMulti (ns, Map.map (fun name doms -> appendDomain dom doms) subdomains)

let rec private mergeDomains (doms1 : Domains) (doms2 : Domains) : Domains =
    match (doms1, doms2) with
    | (DSingle (oldId, dom), doms) -> appendDomain dom doms
    | (doms, DSingle (oldId, dom)) -> appendDomain dom doms
    | (DMulti (ns1, subdoms1), (DMulti _ as doms2)) ->
        DMulti (ns1, Map.map (fun name doms1 -> mergeDomains doms1 doms2) subdoms1)

let compileName (FunQLName name) = SQL.SQLName name

let decompileName (SQL.SQLName name) = FunQLName name

let sqlFunId = compileName funId
let sqlFunSubEntity = compileName funSubEntity
let sqlFunView = compileName funView
let private funEmpty = FunQLName ""

type private JoinId = int

let compileJoinId (jid : JoinId) : SQL.TableName =
    SQL.SQLName <| sprintf "__join__%i" jid

type JoinKey =
    { table : SQL.TableName
      column : SQL.ColumnName
      toEntity : ResolvedEntityRef // Real entity
    }
type JoinPath =
    { name : SQL.TableName
      nested : JoinPaths
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
    | FETScalar SFTString -> FString ""
    | FETScalar SFTInt -> FInt 0
    | FETScalar SFTDecimal -> FDecimal 0m
    | FETScalar SFTBool -> FBool false
    | FETScalar SFTDateTime -> FDateTime NpgsqlDateTime.Epoch
    | FETScalar SFTDate -> FDate NpgsqlDate.Epoch
    | FETScalar SFTInterval -> FInterval NpgsqlTimeSpan.Zero
    | FETScalar SFTJson -> FJson (JObject ())
    | FETScalar SFTUserViewRef -> FUserViewRef { schema = None; name = FunQLName "" }

let defaultCompiledArgument : ArgumentFieldType -> FieldValue = function
    | FTType feType -> defaultCompiledExprArgument feType
    | FTReference (entityRef, None) -> FInt 0
    | FTReference (entityRef, Some where) -> failwith "Reference with a condition in an argument"
    | FTEnum values -> values |> Set.toSeq |> Seq.first |> Option.get |> FString

// Evaluation of column-wise or global attributes
type CompiledAttributesExpr =
    { query : string
      columns : ColumnType[]
      pureAttributes : Set<AttributeName>
      pureColumnAttributes : Map<FieldName, Set<AttributeName>>
    }

[<NoEquality; NoComparison>]
type CompiledViewExpr =
    { attributesQuery : CompiledAttributesExpr option
      query : Query<SQL.SelectExpr>
      columns : ColumnType[]
      domains : Domains
      mainEntity : ResolvedEntityRef option
      flattenedDomains : FlattenedDomains
      usedSchemas : UsedSchemas
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

let compileNoSchemaEntityRef (entityRef : EntityRef) : SQL.TableRef = { schema = None; name = compileName entityRef.name }

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = Some (compileName entityRef.schema); name = compileName entityRef.name }

let compileNoSchemaResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = None; name = compileName entityRef.name }

let compileFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileNoSchemaFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileNoSchemaEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileNoSchemaResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileNoSchemaResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileFieldValue (v : FieldValue) : SQL.ValueExpr =
    let ret = compileFieldValueSingle v
    match v with
    // PostgreSQL cannot deduce text's type on its own
    | FString _ ->SQL.VECast (SQL.VEValue ret, SQL.VTScalar (SQL.STString.ToSQLRawString()))
    | _ -> SQL.VEValue ret

let inline sqlJsonArray< ^a, ^b when ^b :> JToken and ^b : (static member op_Implicit : ^a -> ^b) > (initialVals : SQL.ValueArray< ^a >) : JToken =
    let rec traverseOne = function
        | SQL.AVArray ss -> traverse ss
        | SQL.AVValue v -> (^b : (static member op_Implicit : ^a -> ^b) v) :> JToken
        | SQL.AVNull -> JValue.CreateNull() :> JToken
    and traverse vals =
        vals |> Seq.map traverseOne |> jsonArray :> JToken
    traverse initialVals

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

let private valueToJson : SQL.Value -> JToken = function
    | SQL.VInt i -> JToken.op_Implicit i
    | SQL.VDecimal d -> JToken.op_Implicit d
    | SQL.VString s -> JToken.op_Implicit s
    | SQL.VBool b -> JToken.op_Implicit b
    | SQL.VJson j -> j
    | SQL.VStringArray ss -> sqlJsonArray ss
    | SQL.VIntArray ss -> sqlJsonArray ss
    | SQL.VBoolArray ss -> sqlJsonArray ss
    | SQL.VDecimalArray ss -> sqlJsonArray ss
    | SQL.VJsonArray initialVals ->
        let rec traverse vals =
            let arr = JArray()
            for v in vals do
                let newV =
                    match v with
                    | SQL.AVArray ss -> traverse ss
                    | SQL.AVValue v -> v
                    | SQL.AVNull -> JValue.CreateNull() :> JToken
                arr.Add(newV)
            arr :> JToken
        traverse initialVals
    | SQL.VNull -> JValue.CreateNull() :> JToken
    | (SQL.VDate _ as v)
    | (SQL.VDateTime _ as v)
    | (SQL.VInterval _ as v)
    | (SQL.VDateArray _ as v)
    | (SQL.VDateTimeArray _ as v)
    | (SQL.VIntervalArray _ as v)
    | (SQL.VRegclass _ as v)
    | (SQL.VRegclassArray _ as v) ->
        failwith <| sprintf "Encountered impossible value %O while converting to JSON in parser" v

let private validJsonValue = function
    // Keep in sync with valueToJson!
    | SQL.VDate _
    | SQL.VDateTime _
    | SQL.VDateArray _
    | SQL.VDateTimeArray _ -> true
    | _ -> false

let private rewriteSubEntityCheck (subEntity : SQL.ValueExpr) : SQL.ValueExpr -> SQL.ValueExpr =
    SQL.genericMapValueExpr
        { SQL.idValueExprGenericMapper with
              columnReference = fun _ -> subEntity
        }

type ReferenceContext =
    | RCExpr
    | RCTypeExpr

let rec genericCompileFieldExpr (layout : Layout) (refFunc : ReferenceContext -> 'f -> SQL.ValueExpr) (queryFunc : SelectExpr<'e, 'f> -> SQL.SelectExpr) : FieldExpr<'e, 'f> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FERef c -> refFunc RCExpr c
        | FENot a -> SQL.VENot (traverse a)
        | FEAnd (a, b) -> SQL.VEAnd (traverse a, traverse b)
        | FEOr (a, b) -> SQL.VEOr (traverse a, traverse b)
        | FEConcat (a, b) -> SQL.VEConcat (traverse a, traverse b)
        | FEDistinct (a, b) -> SQL.VEDistinct (traverse a, traverse b)
        | FENotDistinct (a, b) -> SQL.VENotDistinct (traverse a, traverse b)
        | FEEq (a, b) -> SQL.VEEq (traverse a, traverse b)
        | FENotEq (a, b) -> SQL.VENotEq (traverse a, traverse b)
        | FELike (e, pat) -> SQL.VELike (traverse e, traverse pat)
        | FENotLike (e, pat) -> SQL.VENotLike (traverse e, traverse pat)
        | FELess (a, b) -> SQL.VELess (traverse a, traverse b)
        | FELessEq (a, b) -> SQL.VELessEq (traverse a, traverse b)
        | FEGreater (a, b) -> SQL.VEGreater (traverse a, traverse b)
        | FEGreaterEq (a, b) -> SQL.VEGreaterEq (traverse a, traverse b)
        | FEIn (a, arr) -> SQL.VEIn (traverse a, Array.map traverse arr)
        | FENotIn (a, arr) -> SQL.VENotIn (traverse a, Array.map traverse arr)
        | FEInQuery (a, query) -> SQL.VEInQuery (traverse a, queryFunc query)
        | FENotInQuery (a, query) -> SQL.VENotInQuery (traverse a, queryFunc query)
        | FECast (e, typ) -> SQL.VECast (traverse e, SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldExprType typ))
        | FEIsNull a -> SQL.VEIsNull (traverse a)
        | FEIsNotNull a -> SQL.VEIsNotNull (traverse a)
        | FECase (es, els) -> SQL.VECase (Array.map (fun (cond, expr) -> (traverse cond, traverse expr)) es, Option.map traverse els)
        | FECoalesce arr -> SQL.VECoalesce (Array.map traverse arr)
        | FEJsonArray vals ->
            let compiled = Array.map traverse vals

            let tryExtract = function
                | SQL.VEValue v when validJsonValue v -> Some v
                | _ -> None

            // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
            let optimized = Seq.traverseOption tryExtract compiled
            match optimized with
            | Some optimizedVals -> optimizedVals |> Seq.map valueToJson |> jsonArray :> JToken |> SQL.VJson |> SQL.VEValue
            | None -> SQL.VEFunc (SQL.SQLName "jsonb_build_array", Array.map traverse vals)
        | FEJsonObject obj ->
            let compiled = Map.map (fun name -> traverse) obj

            let tryExtract = function
                | (FunQLName name, SQL.VEValue v) when validJsonValue v -> Some (name, v)
                | _ -> None

            // Recheck if all values can be represented as JSON value; e.g. user view references are now valid values.
            let optimized = Seq.traverseOption tryExtract (Map.toSeq compiled)
            match optimized with
            | Some optimizedVals -> optimizedVals |> Seq.map (fun (name, v) -> (name, valueToJson v)) |> jsonObject :> JToken |> SQL.VJson |> SQL.VEValue
            | None ->
                let args = obj |> Map.toSeq |> Seq.collect (fun (FunQLName name, v) -> [SQL.VEValue <| SQL.VString name; traverse v]) |> Seq.toArray
                SQL.VEFunc (SQL.SQLName "jsonb_build_object", args)
        | FEJsonArrow (a, b) -> SQL.VEJsonArrow (traverse a, traverse b)
        | FEJsonTextArrow (a, b) -> SQL.VEJsonTextArrow (traverse a, traverse b)
        | FEPlus (a, b) -> SQL.VEPlus (traverse a, traverse b)
        | FEMinus (a, b) -> SQL.VEMinus (traverse a, traverse b)
        | FEMultiply (a, b) -> SQL.VEMultiply (traverse a, traverse b)
        | FEDivide (a, b) -> SQL.VEDivide (traverse a, traverse b)
        | FEFunc (name,  args) -> SQL.VEFunc (compileName name, Array.map traverse args)
        | FEAggFunc (name,  args) -> SQL.VEAggFunc (compileName name, genericCompileAggExpr traverse args)
        | FESubquery query -> SQL.VESubquery (queryFunc query)
        | FEInheritedFrom (c, subEntityRef) ->
            let info = subEntityRef.extra :?> ResolvedSubEntityInfo
            if info.alwaysTrue then
                SQL.VEValue (SQL.VBool true)
            else
                let col = refFunc RCTypeExpr c
                let entity = layout.FindEntity (tryResolveEntityRef subEntityRef.ref |> Option.get) |> Option.get
                let inheritance = entity.inheritance |> Option.get
                rewriteSubEntityCheck col inheritance.checkExpr
        | FEOfType (c, subEntityRef) ->
            let info = subEntityRef.extra :?> ResolvedSubEntityInfo
            if info.alwaysTrue then
                SQL.VEValue (SQL.VBool true)
            else
                let col = refFunc RCTypeExpr c
                let entity = layout.FindEntity (tryResolveEntityRef subEntityRef.ref |> Option.get) |> Option.get
                SQL.VEEq (col, SQL.VEValue (SQL.VString entity.typeName))
    traverse

and genericCompileAggExpr (func : FieldExpr<'e, 'f> -> SQL.ValueExpr) : AggExpr<'e, 'f> -> SQL.AggExpr = function
    | AEAll exprs -> SQL.AEAll (Array.map func exprs)
    | AEDistinct expr -> SQL.AEDistinct (func expr)
    | AEStar -> SQL.AEStar

let replaceColumnRefs (columnRef : SQL.ColumnRef) : SQL.ValueExpr -> SQL.ValueExpr =
    let mapper =
        { SQL.idValueExprMapper with
              columnReference = fun _ -> columnRef
        }
    SQL.mapValueExpr mapper

type private ColumnPair = ColumnType * SQL.SelectedColumn

// This type is used internally in getResultEntry.
[<NoEquality; NoComparison>]
type private ResultColumn =
    { domains : Domains option
      metaColumns : Map<MetaType, SQL.ValueExpr>
      column : SelectColumn
    }

type private SelectFlags =
    { mainEntity : ResolvedEntityRef option
      isTopLevel : bool
      metaColumns : bool
    }

type RealEntityAnnotation = { realEntity : ResolvedEntityRef }

let private selectSignature (half : HalfCompiledSelect) : SelectSignature =
    { metaColumns = Map.keysSet half.metaColumns
      columns = half.columns |> Array.map (fun col -> { name = col.name; meta = Map.keysSet col.meta })
    }

let private mergeSelectSignature (a : SelectSignature) (b : SelectSignature) : SelectSignature =
    { metaColumns = Set.union a.metaColumns b.metaColumns
      columns = Array.map2 (fun (a : SelectColumnSignature) b -> { name = a.name; meta = Set.union a.meta b.meta }) a.columns b.columns
    }

// Should be in sync with `signatureColumns`. They are not the same function because `signatureColumnTypes` requires names,
// but `signatureColumns` doesn't.
let private signatureColumnTypes (sign : SelectSignature) : ColumnType seq =
    seq {
        for metaCol in sign.metaColumns do
            yield CTMeta metaCol
        for col in sign.columns do
            let name = Option.get col.name
            for metaCol in col.meta do
                yield CTColumnMeta (name, metaCol)
            yield CTColumn name
    }

let rec private leftmostNames : ResolvedSelectExpr -> FieldName[] = function
    | SSelect sel -> sel.results |> Array.map (fun res -> res.result.TryToName () |> Option.get)
    | SSetOp (op, a, b, limits) -> leftmostNames a

// Used for deduplicating id columns.
// We use FROM entity name and dereference path as key
let rec private idKey (from : EntityName) (path : FieldName[]) =
    if Array.isEmpty path then
        string from
    else
        sprintf "%O_%s" from (path |> Seq.map string |> String.concat "_")

type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments
    let mutable usedSchemas : UsedSchemas = Map.empty
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

    let signatureColumns (skipNames : bool) (sign : SelectSignature) (half : HalfCompiledSelect) : SQL.SelectedColumn seq =
        seq {
            for metaCol in sign.metaColumns do
                let name =
                    if not skipNames then
                        CTMeta metaCol |> columnName |> Some
                    else
                        None
                let expr =
                    match Map.tryFind metaCol half.metaColumns with
                    | Some e -> e
                    | None -> SQL.VEValue SQL.VNull
                yield SQL.SCExpr (name, expr)
            for colSig, col in Seq.zip sign.columns half.columns do
                for metaCol in colSig.meta do
                    let name =
                        if not skipNames then
                            CTColumnMeta (Option.get col.name, metaCol) |> columnName |> Some
                        else
                            None
                    let expr =
                        match Map.tryFind metaCol col.meta with
                        | Some e -> e
                        | None -> SQL.VEValue SQL.VNull
                    yield SQL.SCExpr (name, expr)
                let name =
                    if not skipNames then
                        Option.map (CTColumn >> columnName) col.name
                    else
                        None
                yield SQL.SCExpr (name, col.column)
        }

    let setSelectColumns (sign : SelectSignature) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr =
        let rec traverse (skipNames : bool) = function
        | SQL.SSelect sel ->
            let half = sel.extra :?> HalfCompiledSelect
            let columns = signatureColumns skipNames sign half |> Array.ofSeq
            SQL.SSelect { sel with columns = columns; extra = null }
        | SQL.SSetOp (op, a, b, limits) ->
            let a = traverse skipNames a
            let b = traverse true b
            SQL.SSetOp (op, a, b, limits)
        | SQL.SValues vals -> SQL.SValues vals

        traverse false

    let rec domainExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
        | DSingle (id, dom) -> f dom
        | DMulti (ns, nested) ->
            let makeCase (localId, subcase) =
                let case = SQL.VEEq (SQL.VEColumn { table = Some tableRef; name = columnName (CTMeta (CMDomain ns)) }, SQL.VEValue (SQL.VInt localId))
                (case, domainExpression tableRef f subcase)
            SQL.VECase (nested |> Map.toSeq |> Seq.map makeCase |> Seq.toArray, None)

    let fromInfoExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
        | FTEntity (id, dom) -> f dom
        | FTSubquery info -> domainExpression tableRef f info.domains

    let convertLinkedLocalExpr (localRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
        let resolveReference (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
            let newRef =
                match ref.ref with
                | VRColumn col ->
                    VRColumn { col with ref = { col.ref with entity = Some localRef } }
                | VRPlaceholder (PLocal name) -> failwith <| sprintf "Unexpected local argument: %O" name
                | VRPlaceholder ((PGlobal name) as arg) ->
                    let (argPlaceholder, newArguments) = addArgument arg (Map.find name globalArgumentTypes) arguments
                    arguments <- newArguments
                    VRPlaceholder arg
            { ref = newRef; path = ref.path }
        let mapper = idFieldExprMapper resolveReference id
        mapFieldExpr mapper

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

    let rec compileRef (ctx : ReferenceContext) (paths0 : JoinPaths) (tableRef : SQL.TableRef) (fieldRef : ResolvedFieldRef) (forcedName : FieldName option) : JoinPaths * SQL.ValueExpr =
        let realColumn name : SQL.ColumnRef =
            let finalName =
                match forcedName with
                | Some n -> compileName n
                | None -> name
            { table = Some tableRef; name = finalName }

        let entity = layout.FindEntity fieldRef.entity |> Option.get
        let (realName, field) = entity.FindField fieldRef.name |> Option.get

        match field with
        | RId ->
            usedSchemas <- addUsedEntityRef fieldRef.entity usedSchemas
            (paths0, SQL.VEColumn <| realColumn sqlFunId)
        | RSubEntity ->
            usedSchemas <- addUsedEntityRef fieldRef.entity usedSchemas
            match ctx with
            | RCExpr ->
                let newColumn = realColumn sqlFunSubEntity
                (paths0, replaceColumnRefs newColumn entity.subEntityParseExpr)
            | RCTypeExpr ->
                (paths0, SQL.VEColumn <| realColumn sqlFunSubEntity)
        | RColumnField col ->
            usedSchemas <- addUsedField fieldRef.entity.schema fieldRef.entity.name realName usedSchemas
            (paths0, SQL.VEColumn  <| realColumn col.columnName)
        | RComputedField comp ->
            let localRef = { schema = Option.map decompileName tableRef.schema; name = decompileName tableRef.name } : EntityRef
            usedSchemas <- mergeUsedSchemas comp.usedSchemas usedSchemas
            match comp.virtualCases with
            | None -> compileLinkedFieldExpr paths0 <| convertLinkedLocalExpr localRef comp.expression
            | Some cases ->
                let subEntityRef = { table = Some tableRef; name = sqlFunSubEntity } : SQL.ColumnRef
                let mutable paths = paths0

                let compileCase (case : VirtualFieldCase) =
                    let (newPaths, compiled) = compileLinkedFieldExpr paths <| convertLinkedLocalExpr localRef case.expression
                    paths <- newPaths
                    (case, compiled)
                let options = Array.map compileCase cases
                assert not (Array.isEmpty options)

                let compileTag (case : VirtualFieldCase) =
                    replaceColumnRefs subEntityRef case.check
                let compiled = composeExhaustingIf compileTag options
                (paths, compiled)

    and compilePath (ctx : ReferenceContext) (paths : JoinPaths) (tableRef : SQL.TableRef) (fieldRef : ResolvedFieldRef) (forcedName : FieldName option) : FieldName list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef ctx paths tableRef fieldRef forcedName
        | (ref :: refs) ->
            let (realName, field) = layout.FindField fieldRef.entity fieldRef.name |> Option.get
            match field with
            | RColumnField ({ fieldType = FTReference (newEntityRef, _) } as col) ->
                let newFieldRef = { entity = newEntityRef; name = ref }
                let column =
                    match forcedName with
                    | None -> col.columnName
                    | Some n -> compileName n
                let pathKey =
                    { table = tableRef.name
                      column = column
                      toEntity = newEntityRef
                    }
                let (newPath, res) =
                    match Map.tryFind pathKey paths with
                    | None ->
                        let newRealName = newJoinId ()
                        let newTableRef = { schema = None; name = newRealName } : SQL.TableRef
                        let (nested, res) = compilePath ctx Map.empty newTableRef newFieldRef None refs
                        let path =
                            { name = newRealName
                              nested = nested
                            }
                        (path, res)
                    | Some path ->
                        let newTableRef = { schema = None; name = path.name } : SQL.TableRef
                        let (nested, res) = compilePath ctx path.nested newTableRef newFieldRef None refs
                        let newPath = { path with nested = nested }
                        (newPath, res)
                usedSchemas <- addUsedField fieldRef.entity.schema fieldRef.entity.name realName usedSchemas
                (Map.add pathKey newPath paths, res)
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    and compileLinkedFieldRef (ctx : ReferenceContext) (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
        match linked.ref with
        | VRColumn ref ->
            match (linked.path, ref.bound) with
            | ([||], None) ->
                let columnRef = compileNoSchemaFieldRef ref.ref
                (paths0, SQL.VEColumn columnRef)
            | (_, Some boundRef) ->
                let tableRef =
                    match ref.ref.entity with
                    | Some renamedTable -> compileNoSchemaEntityRef renamedTable
                    | None -> compileNoSchemaResolvedEntityRef boundRef.ref.entity
                // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
                // If it's not we need to keep original naming.
                let newName =
                    if boundRef.immediate then None else Some ref.ref.name
                compilePath ctx paths0 tableRef boundRef.ref newName (Array.toList linked.path)
            | _ -> failwith "Unexpected path with no bound field"
        | VRPlaceholder name ->
            if Array.isEmpty linked.path then
                // Explicitly set argument type to avoid ambiguity,
                let arg = arguments.types.[name]
                (paths0, SQL.VECast (SQL.VEPlaceholder arg.placeholderId, arg.dbType))
            else
                let argInfo = Map.find name initialArguments.types
                match argInfo.fieldType with
                | FTReference (argEntityRef, where) ->
                    let firstName = linked.path.[0]
                    let remainingPath = Array.skip 1 linked.path
                    let argEntityRef' = { schema = Some argEntityRef.schema; name = argEntityRef.name } : EntityRef

                    // Subquery
                    let makeColumn name path =
                        let bound =
                            { ref = { entity = argEntityRef; name = name }
                              immediate = true
                            }
                        let col = VRColumn { ref = ({ entity = Some argEntityRef'; name = name } : FieldRef); bound = Some bound }
                        { ref = col; path = path }

                    let idColumn = makeColumn funId [||]
                    let arg = { ref = VRPlaceholder name; path = [||] }

                    let result =
                        { attributes = Map.empty
                          result = QRExpr (None, FERef <| makeColumn firstName remainingPath)
                        }
                    let selectClause =
                        { attributes = Map.empty
                          results = [| result |]
                          from = Some <| FEntity (None, argEntityRef)
                          where = Some <| FEEq (FERef idColumn, FERef arg)
                          groupBy = [||]
                          orderLimit = emptyOrderLimitClause
                          extra = null
                        } : ResolvedSingleSelectExpr
                    let flags =
                        { mainEntity = None
                          isTopLevel = false
                          metaColumns = false
                        }
                    let (info, subquery) = compileSelectExpr flags (SSelect selectClause)
                    (paths0, SQL.VESubquery subquery)
                | typ -> failwith <| sprintf "Argument is not a reference: %O" name

    and compileLinkedFieldExpr (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0
        let compileLinkedRef ctx linked =
            let (newPaths, ret) = compileLinkedFieldRef ctx paths linked
            paths <- newPaths
            ret
        let compileSubSelectExpr =
            let flags =
                { mainEntity = None
                  isTopLevel = false
                  metaColumns = false
                }
            snd << compileSelectExpr flags
        let ret = genericCompileFieldExpr layout compileLinkedRef compileSubSelectExpr expr
        (paths, ret)

    and compileOrderLimitClause (paths0 : JoinPaths) (clause : ResolvedOrderLimitClause) : JoinPaths * SQL.OrderLimitClause =
        let mutable paths = paths0
        let compileFieldExpr' expr =
            let (newPaths, ret) = compileLinkedFieldExpr paths expr
            paths <- newPaths
            ret
        let ret =
            { orderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr' expr)) clause.orderBy
              limit = Option.map compileFieldExpr' clause.limit
              offset = Option.map compileFieldExpr' clause.offset
            } : SQL.OrderLimitClause
        (paths, ret)

    and compileSelectExpr (flags : SelectFlags) (expr : ResolvedSelectExpr) : SelectInfo * SQL.SelectExpr =
        let names =
            if flags.metaColumns then leftmostNames expr else [||]
        let (signature, domains, tree) = compileSelectTreeExpr flags names expr
        let tree = setSelectColumns signature tree
        let ret = { ctes = Map.empty; tree = tree } : SQL.SelectExpr
        let info =
            { columns = if flags.metaColumns then Array.ofSeq (signatureColumnTypes signature) else [||]
              domains = domains
            } : SelectInfo
        (info, ret)

    and compileSelectTreeExpr (flags : SelectFlags) (names : FieldName[]) : ResolvedSelectExpr -> SelectSignature * Domains * SQL.SelectTreeExpr = function
        | SSelect query ->
            let (info, expr) = compileSingleSelectExpr flags names query
            (selectSignature info, info.domains, SQL.SSelect expr)
        | SSetOp (startOp, startA, startB, startLimits) as select ->
            if not flags.metaColumns then
                let (sig1, doms1, expr1) = compileSelectTreeExpr flags names startA
                let (sig2, doms2, expr2) = compileSelectTreeExpr flags names startB
                let (limitPaths, compiledLimits) = compileOrderLimitClause Map.empty startLimits
                assert Map.isEmpty limitPaths
                (mergeSelectSignature sig1 sig2, doms1, SQL.SSetOp (compileSetOp startOp, expr1, expr2, compiledLimits))
            else
                let ns = newDomainNamespaceId ()
                let domainColumn = CMDomain ns
                let mutable lastId = 0
                let rec compileDomained = function
                    | SSelect query ->
                        let (info, expr) = compileSingleSelectExpr flags names query
                        let id = lastId
                        lastId <- lastId + 1
                        let metaColumns = Map.add domainColumn (SQL.VEValue <| SQL.VInt id) info.metaColumns
                        let info = { info with metaColumns = metaColumns }
                        let expr = { expr with extra = info }
                        (selectSignature info, Map.singleton id info.domains, SQL.SSelect expr)
                    | SSetOp (op, a, b, limits) ->
                        let (sig1, domainsMap1, expr1) = compileDomained a
                        let (sig2, domainsMap2, expr2) = compileDomained b
                        let (limitPaths, compiledLimits) = compileOrderLimitClause Map.empty limits
                        assert Map.isEmpty limitPaths
                        (mergeSelectSignature sig1 sig2, Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp (compileSetOp op, expr1, expr2, compiledLimits))
                let (signature, domainsMap, expr) = compileDomained select
                (signature, DMulti (ns, domainsMap), expr)

    and compileSingleSelectExpr (flags : SelectFlags) (names : FieldName[]) (select : ResolvedSingleSelectExpr) : HalfCompiledSelect * SQL.SingleSelectExpr =
        let mutable paths = Map.empty

        let extra =
            if isNull select.extra then
                { hasAggregates = false
                }
            else
                select.extra :?> ResolvedSelectInfo

        let (fromMap, from) =
            match select.from with
            | Some from ->
                let (fromMap, newFrom) = compileFromExpr flags.mainEntity from
                (fromMap, Some newFrom)
            | None -> (Map.empty, None)

        let where =
            match select.where with
            | None -> None
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr paths where
                paths <- newPaths
                Some ret

        let compileGroupBy expr =
            let (newPaths, compiled) = compileLinkedFieldExpr paths expr
            paths <- newPaths
            compiled
        let groupBy = Array.map compileGroupBy select.groupBy

        let compileRowAttr (name, expr) =
            let (newPaths, col) = compileLinkedFieldExpr paths expr
            paths <- newPaths
            (CMRowAttribute name, col)
        let attributeColumns =
            select.attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        let addMetaColumns = flags.metaColumns && not extra.hasAggregates

        let mutable idCols = Map.empty : Map<string, FieldName>

        let getResultEntry (i : int) (result : ResolvedQueryResult) : ResultColumn =
            let currentAttrs = Map.keysSet result.attributes

            let (newPaths, resultColumn) = compileResult paths result
            paths <- newPaths

            match resultFieldRef result.result with
            | Some ({ ref = { ref = { entity = Some ({ name = entityName } as entityRef); name = fieldName } } } as resultRef) when addMetaColumns ->
                let colName = names.[i]
                // Add columns for tracking (id, sub_entity etc.)
                let fromInfo = Map.find entityName fromMap
                let tableRef : SQL.TableRef = { schema = None; name = compileName entityName }

                let finalRef = resultRef.ref.bound |> Option.map (fun bound -> followPath layout bound.ref (List.ofArray resultRef.path))

                // Add system columns (id or sub_entity - this is a generic function).
                let makeMaybeSystemColumn (needColumn : ResolvedFieldRef -> bool) (columnConstr : FieldName -> MetaType) (name : FieldName) =
                    let sqlName = compileName name
                    if Array.isEmpty resultRef.path
                    then
                        let mutable foundSystem = false

                        let getSystemColumn (domain : Domain) =
                            match Map.tryFind fieldName domain with
                            | None -> SQL.VEValue SQL.VNull
                            | Some info ->
                                if needColumn info.ref then
                                    let colName =
                                        if info.idColumn = funEmpty then
                                            sqlName
                                        else
                                            columnName (CTMeta (columnConstr info.idColumn))
                                    foundSystem <- true
                                    SQL.VEColumn { table = Some tableRef; name = colName }
                                else
                                    SQL.VEValue SQL.VNull

                        let systemExpr = fromInfoExpression tableRef getSystemColumn fromInfo.fromType
                        if foundSystem then
                            Some systemExpr
                        else
                            None
                    else
                        let newRef = Option.get finalRef
                        if needColumn newRef then
                            let systemPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton name) |> Array.ofSeq
                            let systemRef = { ref = VRColumn resultRef.ref; path = systemPath }
                            let (newPaths, systemExpr) = compileLinkedFieldRef RCTypeExpr paths systemRef
                            paths <- newPaths
                            Some systemExpr
                        else
                            None

                let needsSubEntity (ref : ResolvedFieldRef) =
                    let entity = layout.FindEntity ref.entity |> Option.get
                    not <| Map.isEmpty entity.children

                let maybeIdExpr = makeMaybeSystemColumn (fun _ -> true) CMId funId
                let maybeSubEntityExpr = makeMaybeSystemColumn needsSubEntity CMSubEntity funSubEntity

                // Maybe we already have a fitting `id` column added for a similar column.
                let (maybeSystemName, systemColumns) =
                    match maybeIdExpr with
                    | None -> (None, Seq.empty)
                    | Some idExpr ->
                        let name = idKey entityName resultRef.path
                        match Map.tryFind name idCols with
                        | None ->
                            let makeSubEntityColumn expr = (CMSubEntity colName, expr)

                            let idCol = CMId colName
                            let column = (idCol, idExpr)
                            let subEntityColumn = Option.map makeSubEntityColumn maybeSubEntityExpr

                            idCols <- Map.add name colName idCols

                            (Some colName, Seq.append (Seq.singleton column) (Option.toSeq subEntityColumn))
                        | Some idCol -> (Some idCol, Seq.empty)

                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | Some info -> Map.singleton colName { info with idColumn = Option.get maybeSystemName }
                    | None -> Map.empty
                let rec getNewDomains = function
                | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))

                let (pathRef, newDomains) =
                    if Array.isEmpty resultRef.path
                    then
                        let newDomains =
                            match fromInfo.fromType with
                            | FTEntity (domainId, domain) -> DSingle (domainId, getNewDomain domain)
                            | FTSubquery info -> getNewDomains info.domains
                        (None, newDomains)
                    else
                        // Pathed refs always have bound fields
                        let newRef = Option.get finalRef
                        let newInfo =
                            { ref = newRef
                              idColumn = Option.get maybeSystemName
                            }
                        let newDomains = DSingle (newGlobalDomainId (), Map.singleton colName newInfo )
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
                    if Array.isEmpty resultRef.path
                    then
                        match fromInfo.fromType with
                        | FTEntity _ -> Seq.empty
                        | FTSubquery info -> getDomainColumns info.domains
                    else
                        Seq.empty

                let punColumns =
                    if flags.isTopLevel
                    then
                        match pathRef with
                        | None ->
                            let mutable foundPun = false

                            let getPunColumn (domain : Domain) =
                                match Map.tryFind fieldName domain with
                                | None -> SQL.VEValue SQL.VNull
                                | Some info ->
                                    match layout.FindField info.ref.entity info.ref.name |> Option.get with
                                    | (_, RColumnField { fieldType = FTReference (newEntityRef, _) }) ->
                                        let fieldRef = { entity = info.ref.entity; name = fieldName }
                                        let (newPaths, expr) = compilePath RCExpr paths tableRef fieldRef None [funMain]
                                        paths <- newPaths
                                        foundPun <- true
                                        expr
                                    | _ -> SQL.VEValue SQL.VNull

                            let punExpr = fromInfoExpression tableRef getPunColumn fromInfo.fromType
                            if foundPun then
                                let col = (CCPun, punExpr)
                                Seq.singleton col
                            else
                                Seq.empty
                        | Some endRef ->
                            let endField = layout.FindField endRef.entity endRef.name |> Option.get
                            match endField with
                            | (_, RColumnField { fieldType = FTReference (newEntityRef, _) }) ->
                                let punPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton funMain) |> Array.ofSeq
                                let punRef = { ref = VRColumn resultRef.ref; path = punPath }
                                let (newPaths, punExpr) = compileLinkedFieldRef RCExpr paths punRef
                                paths <- newPaths
                                let col = (CCPun, punExpr)
                                Seq.singleton col
                            | _ -> Seq.empty
                    else
                        Seq.empty

                // Nested and default attributes.
                let attrColumns =
                    match fromInfo.fromType with
                    | FTEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find fieldName domain
                        match defaultAttrs.FindField info.ref.entity info.ref.name with
                        | None -> Seq.empty
                        | Some attrs ->
                            let makeDefaultAttr name =
                                let attr = Map.find name attrs
                                let expr = convertLinkedLocalExpr entityRef attr.Expression
                                let attrCol = CCCellAttribute name
                                let (newPaths, compiled) = compileLinkedFieldExpr paths expr
                                paths <- newPaths
                                (attrCol, compiled)
                            let defaultSet = Map.keysSet attrs
                            let inheritedAttrs = Set.difference defaultSet currentAttrs
                            inheritedAttrs |> Set.toSeq |> Seq.map makeDefaultAttr
                    | FTSubquery queryInfo ->
                        // Inherit column and cell attributes from subquery.
                        let filterColumnAttr = function
                        | CTColumnMeta (colName, CCCellAttribute name) when colName = fieldName -> Some name
                        | _ -> None
                        let oldAttrs = queryInfo.columns |> Seq.mapMaybe filterColumnAttr |> Set.ofSeq
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrCol = CCCellAttribute name
                            (attrCol, SQL.VEColumn { table = Some tableRef; name = columnName (CTColumnMeta (fieldName, attrCol)) })
                        inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr

                let myMeta = [ attrColumns; punColumns ] |> Seq.concat |> Map.ofSeq

                { domains = Some newDomains
                  metaColumns = [ systemColumns; subqueryDomainColumns ] |> Seq.concat |> Map.ofSeq
                  column = { resultColumn with meta = Map.unionUnique resultColumn.meta myMeta }
                }
            | _ ->
                { domains = None
                  metaColumns = Map.empty
                  column = resultColumn
                }

        let resultEntries = Array.mapi getResultEntry select.results
        let resultColumns = resultEntries |> Array.map (fun x -> x.column)
        let emptyDomains = DSingle (newGlobalDomainId (), Map.empty)

        let checkSame (name : MetaType) (exprA : SQL.ValueExpr) (exprB : SQL.ValueExpr) =
            assert (string exprA = string exprB)
            exprB

        let metaColumns = resultEntries |> Seq.map (fun c -> c.metaColumns) |> Seq.fold (Map.unionWith checkSame) Map.empty
        let (newDomains, metaColumns) =
            if addMetaColumns then
                let newDomains = resultEntries |> Seq.mapMaybe (fun entry -> entry.domains) |> Seq.fold mergeDomains emptyDomains

                let mainIdColumns =
                    match flags.mainEntity with
                    | None -> Seq.empty
                    | Some mainRef ->
                        let findMainValue (getValueName : FromInfo -> SQL.ColumnName option) (name, info : FromInfo) : SQL.ColumnRef option =
                            match getValueName info with
                            | Some id -> Some { table = Some { schema = None; name = compileName name }; name = id }
                            | None -> None
                        let mainId = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.mainId)) |> Seq.exactlyOne
                        let idCol = (CMMainId, SQL.VEColumn mainId)
                        let subEntityCols =
                            let mainEntity = layout.FindEntity mainRef |> Option.get
                            if Map.isEmpty mainEntity.children then
                                Seq.empty
                            else
                                let mainSubEntity = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.mainSubEntity)) |> Seq.exactlyOne
                                let subEntityCol = (CMMainSubEntity, SQL.VEColumn mainSubEntity)
                                Seq.singleton subEntityCol
                        Seq.append (Seq.singleton idCol) subEntityCols

                let newMetaColumns = [ mainIdColumns; attributeColumns ] |> Seq.concat |> Map.ofSeq
                let metaColumns = Map.unionUnique metaColumns newMetaColumns
                (newDomains, metaColumns)
            else
                (emptyDomains, metaColumns)
        let orderLimit =
            let (newPaths, ret) = compileOrderLimitClause paths select.orderLimit
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
            { domains = newDomains
              metaColumns = metaColumns
              columns = resultColumns
            } : HalfCompiledSelect

        let query =
            { columns = [||] // We fill columns after processing UNIONs, so that ordering and number of columns and meta-columns is the same everywhere.
              from = newFrom
              where = where
              groupBy = groupBy
              orderLimit = orderLimit
              extra = info
            } : SQL.SingleSelectExpr

        (info, query)

    and compileResult (paths0 : JoinPaths) (result : ResolvedQueryResult) : JoinPaths * SelectColumn =
        let mutable paths = paths0

        let newExpr =
            match result.result with
            | QRExpr (name, expr) ->
                let (newPaths, ret) = compileLinkedFieldExpr paths expr
                paths <- newPaths
                ret

        let compileAttr (attrName, expr) =
            let attrCol = CCCellAttribute attrName
            let (newPaths, ret) = compileLinkedFieldExpr paths expr
            paths <- newPaths
            (attrCol, ret)

        let attrs = result.attributes |> Map.toSeq |> Seq.map compileAttr |> Map.ofSeq
        let ret =
            { name = result.result.TryToName ()
              column = newExpr
              meta = attrs
            }
        (paths, ret)

    and buildJoins (from : SQL.FromExpr) (paths : JoinPaths) : SQL.FromExpr =
        Map.fold joinPath from paths

    and joinPath (from : SQL.FromExpr) (joinKey : JoinKey) (path : JoinPath) : SQL.FromExpr =
        let tableRef = { schema = None; name = joinKey.table } : SQL.TableRef
        let toTableRef = { schema = None; name = path.name } : SQL.TableRef
        let entity = layout.FindEntity joinKey.toEntity |> Option.get

        let fromColumn = SQL.VEColumn { table = Some tableRef; name = joinKey.column }
        let toColumn = SQL.VEColumn { table = Some toTableRef; name = sqlFunId }
        let joinExpr = SQL.VEEq (fromColumn, toColumn)
        let subquery = SQL.FTable ({ realEntity = joinKey.toEntity }, Some path.name, compileResolvedEntityRef entity.root)
        let currJoin = SQL.FJoin (SQL.Left, from, subquery, joinExpr)
        usedSchemas <- addUsedEntityRef joinKey.toEntity usedSchemas
        buildJoins currJoin path.nested

    and compileFromExpr (mainEntity : ResolvedEntityRef option) : ResolvedFromExpr -> FromMap * SQL.FromExpr = function
        | FEntity (pun, entityRef) ->
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef

            let makeDomainEntry name field =
                { ref = { entity = entityRef; name = name }
                  // Special value which means "use defaults"
                  idColumn = funEmpty
                }
            let domain = mapAllFields makeDomainEntry entity

            let compiledPun = Option.map compileName pun
            let newName = Option.defaultValue entityRef.name pun
            let compiledName = Option.defaultValue (compileName entityRef.name) compiledPun
            let subquery =
                match entity.inheritance with
                | None ->
                    SQL.FTable ({ realEntity = entityRef }, compiledPun, compileResolvedEntityRef entityRef)
                | Some inheritance ->
                    let select =
                        { columns = [| SQL.SCAll None |]
                          from = Some <| SQL.FTable (null, None, compileResolvedEntityRef entity.root)
                          where = Some inheritance.checkExpr
                          groupBy = [||]
                          orderLimit = SQL.emptyOrderLimitClause
                          extra = { realEntity = entityRef }
                        } : SQL.SingleSelectExpr
                    let expr = { ctes = Map.empty; tree = SQL.SSelect select } : SQL.SelectExpr
                    SQL.FSubExpr (compiledName, None, expr)
            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let subEntity = if Map.isEmpty entity.children then None else Some sqlFunSubEntity
                    (Some sqlFunId, subEntity)
            let fromInfo =
                { fromType = FTEntity (newGlobalDomainId (), domain)
                  mainId = mainId
                  mainSubEntity = mainSubEntity
                }

            usedSchemas <- addUsedEntityRef entityRef usedSchemas
            (Map.singleton newName fromInfo, subquery)
        | FJoin (jt, e1, e2, where) ->
            let main1 =
                match jt with
                | Left -> mainEntity
                | _ -> None
            let (fromMap1, r1) = compileFromExpr main1 e1
            let main2 =
                match jt with
                | Right -> mainEntity
                | _ -> None
            let (fromMap2, r2) = compileFromExpr main2 e2
            let fromMap = Map.unionUnique fromMap1 fromMap2
            let (joinPaths, joinExpr) = compileLinkedFieldExpr Map.empty where
            if not <| Map.isEmpty joinPaths then
                failwith <| sprintf "Unexpected dereference in join expression: %O" where
            let ret = SQL.FJoin (compileJoin jt, r1, r2, joinExpr)
            (fromMap, ret)
        | FSubExpr (name, q) ->
            let flags =
                { mainEntity = mainEntity
                  isTopLevel = false
                  metaColumns = true
                }
            let (selectSig, expr) = compileSelectExpr flags q
            let ret = SQL.FSubExpr (compileName name, None, expr)
            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let mainId = Some (columnName <| CTMeta CMMainId)
                    let mainEntityInfo = layout.FindEntity mainRef |> Option.get
                    let subEntity = if Map.isEmpty mainEntityInfo.children then None else Some (columnName <| CTMeta CMMainSubEntity)
                    (mainId, subEntity)
            let fromInfo =
                { fromType = FTSubquery selectSig
                  mainId = mainId
                  mainSubEntity = mainSubEntity
                }
            (Map.singleton name fromInfo, ret)
        | FValues (name, fieldNames, values) ->
            assert Option.isNone mainEntity

            let domainsMap = DSingle (newDomainNamespaceId (), Map.empty)
            let selectSig =
                { columns = Array.map CTColumn fieldNames
                  domains = domainsMap
                } : SelectInfo
            let compiledValues = values |> Array.map (Array.map (compileLinkedFieldExpr Map.empty >> snd))
            let select = { ctes = Map.empty; tree = SQL.SValues compiledValues } : SQL.SelectExpr
            let ret = SQL.FSubExpr (compileName name, Some (Array.map compileName fieldNames), select)
            let fromInfo =
                { fromType = FTSubquery selectSig
                  mainId = None
                  mainSubEntity = None
                }
            (Map.singleton name fromInfo, ret)

    member this.CompileSingleFromClause (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) =
        let (fromMap, from) = compileFromExpr None from
        let (newPaths, where) =
            match where with
            | None -> (Map.empty, None)
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr Map.empty where
                (newPaths, Some ret)
        let builtFrom = buildJoins from newPaths
        (builtFrom, where)

    member this.CompileSelectExpr (mainEntity : ResolvedEntityRef option) =
        let flags =
            { mainEntity = mainEntity
              isTopLevel = true
              metaColumns = true
            }
        compileSelectExpr flags

    member this.Arguments = arguments
    member this.UsedSchemas = usedSchemas

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
              columnReference = foundReference
              placeholder = foundPlaceholder
              query = foundQuery
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

let rec private findPureAttributes (columnTypes : ColumnType[]) : SQL.SelectTreeExpr -> (PureColumn option)[] = function
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
        Array.map2 assignPure columnTypes query.columns
    | SQL.SValues vals -> Array.create (Array.length vals.[0]) None
    | SQL.SSetOp (op, a, b, limits) -> Array.create (findPureAttributes columnTypes a |> Array.length) None

let rec private filterExprColumns (cols : (PureColumn option)[]) : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
    | SQL.SSelect query ->
        let checkColumn i _ = Option.isNone cols.[i]
        SQL.SSelect { query with columns = Seq.filteri checkColumn query.columns |> Seq.toArray }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp (op, a, b, limits) ->
        SQL.SSetOp (op, filterExprColumns cols a, filterExprColumns cols b, limits)

let rec private flattenDomains : Domains -> FlattenedDomains = function
    | DSingle (id, dom) -> Map.singleton id dom
    | DMulti (ns, subdoms) -> subdoms |> Map.values |> Seq.fold (fun m subdoms -> Map.union m (flattenDomains subdoms)) Map.empty

let compileSingleFromClause (layout : Layout) (argumentsMap : CompiledArgumentsMap) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : SQL.FromExpr * SQL.ValueExpr option =
    let bogusArguments =
        { types = argumentsMap
          lastPlaceholderId = 0
        }
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, bogusArguments)
    compiler.CompileSingleFromClause from where

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let mainEntityRef = viewExpr.mainEntity |> Option.map (fun main -> main.entity)
    let compiler = QueryCompiler (layout, defaultAttrs, compileArguments viewExpr.arguments)
    let (info, expr) = compiler.CompileSelectExpr mainEntityRef viewExpr.select

    let allPureAttrs = findPureAttributes info.columns expr.tree
    let newExpr = { expr with tree = filterExprColumns allPureAttrs expr.tree }

    let checkColumn i _ = Option.isNone allPureAttrs.[i]
    let newColumns = Seq.filteri checkColumn info.columns |> Seq.toArray

    let onlyPureAttrs = Seq.catMaybes allPureAttrs |> Seq.toArray
    let attrQuery =
        if Array.isEmpty onlyPureAttrs then
            None
        else
            let query = SQL.SSelect {
                    columns = Array.map (fun info -> info.Result) onlyPureAttrs
                    from = None
                    where = None
                    groupBy = [||]
                    orderLimit = SQL.emptyOrderLimitClause
                    extra = null
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
            Some { query = query.ToString()
                   columns = Array.map (fun info -> info.ColumnType) onlyPureAttrs
                   pureAttributes = pureAttrs
                   pureColumnAttributes = pureColAttrs
                 }

    { attributesQuery = attrQuery
      query = { expression = newExpr; arguments = compiler.Arguments }
      columns = newColumns
      domains = info.domains
      flattenedDomains = flattenDomains info.domains
      usedSchemas = compiler.UsedSchemas
      mainEntity = mainEntityRef
    }