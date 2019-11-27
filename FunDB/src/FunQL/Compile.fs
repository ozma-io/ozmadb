module FunWithFlags.FunDB.FunQL.Compile

open System
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Merge
module SQL = FunWithFlags.FunDB.SQL.AST

// Domains is a way to distinguish rows after set operations so that row types and IDs can be traced back.
// Domain is a map of assigned references to fields to each column.
// Domain key is cross product of all local domain ids i.e. map from domain namespace ids to local domain ids.
// Local domain ids get assigned to all sets in a set operation.
// Each row has a different domain key assigned, so half of rows may come from entity A and half from entity B.
// This way one can make use if IDs assigned to cells to reference and update them.
[<NoComparison>]
type DomainField =
    { ref : ResolvedFieldRef
      // A field with assigned idColumn of Foo will use ID column "__Id__Foo" and SubEntity column "__SubEntity__Foo"
      idColumn : EntityName
    }

type Domain = Map<FieldName, DomainField>
type GlobalDomainId = int
type DomainNamespaceId = int
type LocalDomainId = int
[<NoComparison>]
type Domains =
    | DSingle of GlobalDomainId * Domain
    | DMulti of DomainNamespaceId * Map<LocalDomainId, Domains>

type ColumnType =
    | CTRowAttribute of AttributeName
    | CTCellAttribute of FieldName * AttributeName
    | CTPunAttribute of FieldName
    | CTDomainColumn of DomainNamespaceId
    | CTIdColumn of EntityName
    | CTSubEntityColumn of EntityName
    | CTMainIdColumn
    | CTMainSubEntityColumn
    | CTColumn of FunQLName

let columnName : ColumnType -> SQL.SQLName = function
    | CTRowAttribute (FunQLName name) -> SQL.SQLName (sprintf "__RowAttribute__%s" name)
    | CTCellAttribute (FunQLName field, FunQLName name) -> SQL.SQLName (sprintf "__CellAttribute__%s__%s" field name)
    | CTPunAttribute (FunQLName field) -> SQL.SQLName (sprintf "__Pun__%s" field)
    | CTDomainColumn id -> SQL.SQLName (sprintf "__Domain__%i" id)
    | CTIdColumn (FunQLName entity) -> SQL.SQLName (sprintf "__Id__%s" entity)
    | CTSubEntityColumn (FunQLName entity) -> SQL.SQLName (sprintf "__SubEntity__%s" entity)
    | CTMainIdColumn -> SQL.SQLName "__MainId"
    | CTMainSubEntityColumn -> SQL.SQLName "__MainSubEntity"
    | CTColumn (FunQLName column) -> SQL.SQLName column

[<NoComparison>]
type private SelectInfo =
    { attributes : Map<FieldName, Set<AttributeName>>
      domains : Domains
      // PostgreSQL column length is limited to 63 bytes, so we store column types separately.
      columns : ColumnType[]
    }

type FlattenedDomains = Map<GlobalDomainId, Domain>

[<NoComparison>]
type private FromType = FTEntity of GlobalDomainId * Domain // Domain ID is used for merging.
                      | FTSubquery of SelectInfo

[<NoComparison>]
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
    SQL.SQLName <| sprintf "__Join__%i" jid

let rec private domainExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
    | DSingle (id, dom) -> f dom
    | DMulti (ns, nested) ->
        let makeCase (localId, subcase) =
            let case = SQL.VEEq (SQL.VEColumn { table = Some tableRef; name = columnName (CTDomainColumn ns) }, SQL.VEValue (SQL.VInt localId))
            (case, domainExpression tableRef f subcase)
        SQL.VECase (nested |> Map.toSeq |> Seq.map makeCase |> Seq.toArray, None)

let private fromInfoExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
    | FTEntity (id, dom) -> f dom
    | FTSubquery info -> domainExpression tableRef f info.domains

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
    | FETArray SFTJson -> FJsonArray [||]
    | FETArray SFTUserViewRef -> FUserViewRefArray [||]
    | FETScalar SFTString -> FString ""
    | FETScalar SFTInt -> FInt 0
    | FETScalar SFTDecimal -> FDecimal 0m
    | FETScalar SFTBool -> FBool false
    | FETScalar SFTDateTime -> FDateTime DateTimeOffset.UnixEpoch
    | FETScalar SFTDate -> FDateTime DateTimeOffset.UnixEpoch
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

[<NoComparison>]
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

let private compileEntityRef (entityRef : EntityRef) : SQL.TableRef = { schema = Option.map compileName entityRef.schema; name = compileName entityRef.name }

let private compileNoSchemaEntityRef (entityRef : EntityRef) : SQL.TableRef = { schema = None; name = compileName entityRef.name }

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = Some (compileName entityRef.schema); name = compileName entityRef.name }

let private compileNoSchemaResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = None; name = compileName entityRef.name }

let private compileFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileNoSchemaEntityRef fieldRef.entity; name = compileName fieldRef.name }

let private compileResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

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
    | (SQL.VDateArray _ as v)
    | (SQL.VDateTimeArray _ as v)
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

let private replaceColumnRefs (columnRef : SQL.ColumnRef) : SQL.ValueExpr -> SQL.ValueExpr =
    let mapper =
        { SQL.idValueExprMapper with
              columnReference = fun _ -> columnRef
        }
    SQL.mapValueExpr mapper

[<NoComparison>]
type private ResultColumn =
    { domains : Domains option
      attributes : Set<AttributeName>
      columns : (ColumnType * SQL.SelectedColumn) array
    }

type private SelectFlags =
    { mainEntity : ResolvedEntityRef option
      isTopLevel : bool
      metaColumns : bool
    }

type RealEntityAnnotation = { realEntity : ResolvedEntityRef }

type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let convertLinkedLocalExpr (localRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
        let resolveReference (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
            let newRef =
                match ref.ref with
                | VRColumn col ->
                    VRColumn { col with ref = { col.ref with entity = Some localRef } }
                | VRPlaceholder (PLocal name) -> failwith <| sprintf "Unexpected local argument: %O" name
                | VRPlaceholder ((PGlobal name) as arg) ->
                    arguments <- addArgument arg (Map.find name globalArgumentTypes) arguments
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

    let rec followPath (fieldRef : ResolvedFieldRef) (field : ResolvedField) : FieldName list -> ResolvedFieldRef = function
        | [] -> fieldRef
        | (ref :: refs) ->
            match field with
            | RColumnField { fieldType = FTReference (entityRef, _) } ->
                let newEntity = Option.get <| layout.FindEntity entityRef
                let (_, newField) = Option.get <| newEntity.FindField ref
                let newFieldRef = { entity = entityRef; name = ref }
                followPath newFieldRef newField refs
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    let rec compileRef (ctx : ReferenceContext) (paths : JoinPaths) (tableRef : SQL.TableRef) (field : ResolvedFieldRef) (forcedName : FieldName option) : JoinPaths * SQL.ValueExpr =
        let realColumn name : SQL.ColumnRef =
            let finalName =
                match forcedName with
                | Some n -> compileName n
                | None -> name
            { table = Some tableRef; name = finalName }

        let entity = layout.FindEntity field.entity |> Option.get
        let (_, field) = entity.FindField field.name |> Option.get

        match field with
        | RId -> (paths, SQL.VEColumn <| realColumn sqlFunId)
        | RSubEntity ->
            match ctx with
            | RCExpr ->
                let newColumn = realColumn sqlFunSubEntity
                (paths, replaceColumnRefs newColumn entity.subEntityParseExpr)
            | RCTypeExpr ->
                (paths, SQL.VEColumn <| realColumn sqlFunSubEntity)
        | RColumnField col -> (paths, SQL.VEColumn  <| realColumn col.columnName)
        | RComputedField comp ->
            let localRef = { schema = Option.map decompileName tableRef.schema; name = decompileName tableRef.name } : EntityRef
            compileLinkedFieldExpr paths <| convertLinkedLocalExpr localRef comp.expression

    and compilePath (ctx : ReferenceContext) (paths : JoinPaths) (tableRef : SQL.TableRef) (fieldRef : ResolvedFieldRef) (forcedName : FieldName option) : FieldName list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef ctx paths tableRef fieldRef forcedName
        | (ref :: refs) ->
            let (_, field) = layout.FindField fieldRef.entity fieldRef.name |> Option.get
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
                (Map.add pathKey newPath paths, res)
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    and compileLinkedFieldRef (ctx : ReferenceContext) (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
        match linked.ref with
        | VRColumn ref ->
            match (linked.path, ref.bound) with
            | ([||], None) ->
                let columnRef = compileFieldRef ref.ref
                (paths0, SQL.VEColumn columnRef)
            | (_, Some boundRef) ->
                let tableRef =
                    match ref.ref.entity with
                    | Some renamedTable -> compileEntityRef renamedTable
                    | None -> compileNoSchemaResolvedEntityRef boundRef.ref.entity
                // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
                // If it's not we need to keep original naming.
                let newName =
                    if boundRef.immediate then None else Some ref.ref.name
                compilePath ctx paths0 tableRef boundRef.ref newName (Array.toList linked.path)
            | _ -> failwith "Unexpected path with no bound field"
        | VRPlaceholder name ->
            if Array.isEmpty linked.path then
                (paths0, SQL.VEPlaceholder arguments.types.[name].placeholderId)
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

    and compileAttribute (paths0 : JoinPaths) (attrType : ColumnType) (expr : ResolvedFieldExpr) : JoinPaths * SQL.SelectedColumn =
        let (newPaths, compiled) = compileLinkedFieldExpr paths0 expr
        (newPaths, SQL.SCExpr (Some <| columnName attrType, compiled))

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

    and compileSelectExpr (flags : SelectFlags) : ResolvedSelectExpr -> SelectInfo * SQL.SelectExpr = function
        | SSelect query ->
            let (info, expr) = compileSingleSelectExpr flags query
            (info, SQL.SSelect expr)
        | SSetOp _ as select ->
            let ns = newDomainNamespaceId ()
            let domainColumn = CTDomainColumn ns
            let domainName = columnName domainColumn
            let mutable lastId = 0
            let rec compileDomained = function
                | SSelect query ->
                    let (info, expr) = compileSingleSelectExpr flags query
                    let id = lastId
                    lastId <- lastId + 1
                    let modifiedExpr =
                        { expr with
                              columns = Array.append [| SQL.SCExpr (Some domainName, SQL.VEValue <| SQL.VInt id) |] expr.columns
                        }
                    (info.attributes, info.columns, Map.singleton id info.domains, SQL.SSelect modifiedExpr)
                | SSetOp (op, a, b, limits) ->
                    let (attrs1, columns1, domainsMap1, expr1) = compileDomained a
                    let (attrs2, columns2, domainsMap2, expr2) = compileDomained b
                    let (limitPaths, compiledLimits) = compileOrderLimitClause Map.empty limits
                    assert Map.isEmpty limitPaths
                    assert (attrs1 = attrs2)
                    (attrs1, columns1, Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp (compileSetOp op, expr1, expr2, compiledLimits))
            let (attrs, columns, domainsMap, expr) = compileDomained select
            let info =
                { attributes = attrs
                  domains = DMulti (ns, domainsMap)
                  columns = Array.append [| domainColumn |] columns
                } : SelectInfo
            (info, expr)

    and compileSingleSelectExpr (flags : SelectFlags) (select : ResolvedSingleSelectExpr) : SelectInfo * SQL.SingleSelectExpr =
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
            let attrCol = CTRowAttribute name
            let (newPaths, col) = compileAttribute paths attrCol expr
            paths <- newPaths
            (attrCol, col)
        let attributeColumns =
            select.attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        let addMetaColumns = flags.metaColumns && not extra.hasAggregates

        // We keep Id columns map to remove duplicates.
        let mutable ids = Map.empty

        let getResultEntry (result : ResolvedQueryResult) =
            let currentAttrs = Map.keysSet result.attributes
            let resultColumns =
                let (newPaths, ret) = compileResult paths result
                paths <- newPaths
                Array.toSeq ret

            match resultFieldRef result.result with
            | Some ({ ref = { ref = { entity = Some ({ name = entityName } as entityRef); name = fieldName } } } as resultRef) when addMetaColumns ->
                let newName = result.result.TryToName () |> Option.get
                let fromInfo = Map.find entityName fromMap
                let tableRef : SQL.TableRef = { schema = None; name = compileName entityName }

                let makeMaybeSystemColumn (needColumn : ResolvedFieldRef -> bool) (columnConstr : EntityName -> ColumnType) (name : FieldName) =
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
                                            columnName (columnConstr info.idColumn)
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
                        let systemPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton name) |> Array.ofSeq
                        let systemRef = { ref = VRColumn resultRef.ref; path = systemPath }
                        let (newPaths, systemExpr) = compileLinkedFieldRef RCTypeExpr paths systemRef
                        paths <- newPaths
                        Some systemExpr

                let needsSubEntity (ref : ResolvedFieldRef) =
                    let entity = layout.FindEntity ref.entity |> Option.get
                    hasSubType entity

                let maybeIdExpr = makeMaybeSystemColumn (fun _ -> true) CTIdColumn funId
                let maybeSubEntityExpr = makeMaybeSystemColumn needsSubEntity CTSubEntityColumn funSubEntity

                let (maybeSystemName, systemColumns) =
                    match maybeIdExpr with
                    | None -> (None, Seq.empty)
                    | Some idExpr ->
                        let idStr = idExpr.ToString()
                        match Map.tryFind idStr ids with
                        | None ->
                            let makeSubEntityColumn expr =
                                let colName = CTSubEntityColumn newName
                                (colName, SQL.SCExpr (Some <| columnName colName, expr))

                            ids <- Map.add idStr newName ids
                            let colName = CTIdColumn newName
                            let column = (colName, SQL.SCExpr (Some <| columnName colName, idExpr))
                            let subEntityColumn = Option.map makeSubEntityColumn maybeSubEntityExpr
                            (Some newName, Seq.append (Seq.singleton column) (Option.toSeq subEntityColumn))
                        | Some idName -> (Some idName, Seq.empty)

                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | Some info -> Map.singleton newName { info with idColumn = Option.get maybeSystemName }
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
                        let oldBound = Option.get resultRef.ref.bound
                        let (_, oldField) = Option.get <| layout.FindField oldBound.ref.entity oldBound.ref.name
                        let newRef = followPath oldBound.ref oldField (List.ofArray resultRef.path)
                        let newInfo =
                            { ref = newRef
                              idColumn = newName
                            }
                        let newDomains = DSingle (newGlobalDomainId (), Map.singleton newName newInfo )
                        (Some newRef, newDomains)

                let rec getDomainColumns = function
                | DSingle (id, domain) -> Seq.empty
                | DMulti (ns, nested) ->
                    let colName = CTDomainColumn ns
                    let col = (colName, SQL.SCExpr (None, SQL.VEColumn { table = Some tableRef; name = columnName colName }))
                    Seq.append (Seq.singleton col) (nested |> Map.values |> Seq.collect getDomainColumns)
                let domainColumns =
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
                                let colName = CTPunAttribute newName
                                let col = (colName, SQL.SCExpr (Some <| columnName colName, punExpr))
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
                                let colName = CTPunAttribute newName
                                let col = (colName, SQL.SCExpr (Some <| columnName colName, punExpr))
                                Seq.singleton col
                            | _ -> Seq.empty
                    else
                        Seq.empty

                // Nested and default attributes.
                let (newAttrs, attrColumns) =
                    match fromInfo.fromType with
                    | FTEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find fieldName domain
                        match defaultAttrs.FindField info.ref.entity info.ref.name with
                        | None -> (currentAttrs, Seq.empty)
                        | Some attrs ->
                            let makeDefaultAttr name =
                                let attr = Map.find name attrs
                                let expr = convertLinkedLocalExpr entityRef attr.expression
                                let attrCol = CTCellAttribute (fieldName, name)
                                let (newPaths, ret) = compileAttribute paths attrCol expr
                                paths <- newPaths
                                (attrCol, ret)
                            let defaultSet = Map.keysSet attrs
                            let inheritedAttrs = Set.difference defaultSet currentAttrs
                            let allAttrs = Set.union defaultSet currentAttrs
                            let defaultCols = inheritedAttrs |> Set.toSeq |> Seq.map makeDefaultAttr
                            (allAttrs, defaultCols)
                    | FTSubquery queryInfo ->
                        let oldAttrs =
                            match Map.tryFind fieldName queryInfo.attributes with
                            | Some attrs -> attrs
                            | None -> Set.empty
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let allAttrs = Set.union oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrCol = CTCellAttribute (fieldName, name)
                            (attrCol, SQL.SCExpr (None, SQL.VEColumn { table = Some tableRef; name = columnName attrCol }))
                        let inheritedCols = inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr
                        (allAttrs, inheritedCols)

                { domains = Some newDomains
                  attributes = newAttrs
                  columns = [ systemColumns; domainColumns; attrColumns; resultColumns; punColumns ] |> Seq.concat |> Seq.toArray
                }
            | _ ->
                { domains = None
                  attributes = currentAttrs
                  columns = Seq.toArray resultColumns
                }

        let resultEntries = select.results |> Seq.map getResultEntry |> Seq.toArray
        let emptyDomains = DSingle (newGlobalDomainId (), Map.empty)
        let (attributes, newDomains, columns) =
            if addMetaColumns then
                let resultColumns = resultEntries |> Seq.collect (fun entry -> entry.columns) |> Seq.distinct
                let newDomains = resultEntries |> Seq.mapMaybe (fun entry -> entry.domains) |> Seq.fold mergeDomains emptyDomains
                let queryAttrs = Seq.fold2 (fun attrsMap result entry -> Map.add (result.result.TryToName () |> Option.get) entry.attributes attrsMap) Map.empty select.results resultEntries

                let mainIdColumns =
                    match flags.mainEntity with
                    | None -> Seq.empty
                    | Some mainRef ->
                        let findMainValue (getValueName : FromInfo -> SQL.ColumnName option) (name, info : FromInfo) : SQL.ColumnRef option =
                            match getValueName info with
                            | Some id -> Some { table = Some { schema = None; name = compileName name }; name = id }
                            | None -> None
                        let mainId = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.mainId)) |> Seq.exactlyOne
                        let idCol = (CTMainIdColumn, SQL.SCExpr (Some <| columnName CTMainIdColumn, SQL.VEColumn mainId))
                        let subEntityCols =
                            let mainEntity = layout.FindEntity mainRef |> Option.get
                            if Map.isEmpty mainEntity.children then
                                Seq.empty
                            else
                                let mainSubEntity = Map.toSeq fromMap |> Seq.mapMaybe (findMainValue (fun x -> x.mainSubEntity)) |> Seq.exactlyOne
                                let subEntityCol = (CTMainSubEntityColumn, SQL.SCExpr (Some <| columnName CTMainSubEntityColumn, SQL.VEColumn mainSubEntity))
                                Seq.singleton subEntityCol
                        Seq.append (Seq.singleton idCol) subEntityCols

                let columns = [ mainIdColumns; attributeColumns; resultColumns ] |> Seq.concat |> Array.ofSeq
                (queryAttrs, newDomains, columns)
            else
                let columns = resultEntries |> Seq.collect (fun entry -> entry.columns) |> Array.ofSeq
                (Map.empty, emptyDomains, columns)
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

        let query =
            { columns = Array.map snd columns
              from = newFrom
              where = where
              groupBy = groupBy
              orderLimit = orderLimit
              extra = null
            } : SQL.SingleSelectExpr

        let info =
            { attributes = attributes
              domains = newDomains
              columns = Array.map fst columns
            } : SelectInfo
        (info, query)

    and compileResult (paths0 : JoinPaths) (result : ResolvedQueryResult) : JoinPaths * (ColumnType * SQL.SelectedColumn) array =
        let resultName =
            match result.result.TryToName () with
            | Some name -> name
            | None -> FunQLName "<unnamed>" // This hack is okay because unnamed results are allowed _only_ in expression queries where there is only one column
        let sqlCol = CTColumn resultName
        let mutable paths = paths0

        let newExpr =
            match result.result with
            | QRExpr (name, expr) ->
                let (newPaths, ret) = compileLinkedFieldExpr paths expr
                paths <- newPaths
                ret

        let resultColumn = (sqlCol, SQL.SCExpr (Some <| columnName sqlCol, newExpr))

        let compileAttr (attrName, expr) =
            let attrCol = CTCellAttribute (resultName, attrName)
            let (newPaths, ret) = compileAttribute paths attrCol expr
            paths <- newPaths
            (attrCol, ret)

        let attrs = result.attributes |> Map.toSeq |> Seq.map compileAttr
        // We need to force computation of columns to calculate paths.
        let cols = Seq.append (Seq.singleton resultColumn) attrs |> Array.ofSeq
        (paths, cols)

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

            let subquery =
                match entity.inheritance with
                | None ->
                    SQL.FTable ({ realEntity = entityRef }, Option.map compileName pun, compileResolvedEntityRef entityRef)
                | Some inheritance ->
                    let select =
                        { columns = [| SQL.SCAll None |]
                          from = Some <| SQL.FTable (null, None, compileResolvedEntityRef entity.root)
                          where = Some inheritance.checkExpr
                          groupBy = [||]
                          orderLimit = SQL.emptyOrderLimitClause
                          extra = { realEntity = entityRef }
                        } : SQL.SingleSelectExpr
                    let expr = SQL.SSelect select
                    SQL.FSubExpr (compileName entityRef.name, None, expr)
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

            let newName = Option.defaultValue entityRef.name pun
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
            let (selectInfo, expr) = compileSelectExpr flags q
            let ret = SQL.FSubExpr (compileName name, None, expr)
            let (mainId, mainSubEntity) =
                match mainEntity with
                | None -> (None, None)
                | Some mainRef ->
                    let mainId = Some (columnName CTMainIdColumn)
                    let mainEntityInfo = layout.FindEntity mainRef |> Option.get
                    let subEntity = if Map.isEmpty mainEntityInfo.children then None else Some (columnName CTMainSubEntityColumn)
                    (mainId, subEntity)
            let fromInfo =
                { fromType = FTSubquery selectInfo
                  mainId = mainId
                  mainSubEntity = mainSubEntity
                }
            (Map.singleton name fromInfo, ret)
        | FValues (name, fieldNames, values) ->
            assert Option.isNone mainEntity

            let domainsMap = DSingle (newDomainNamespaceId (), Map.empty)
            let selectInfo =
                { attributes = fieldNames |> Seq.map (fun name -> (name, Set.empty)) |> Map.ofSeq
                  columns = Array.map CTColumn fieldNames
                  domains = domainsMap
                } : SelectInfo
            let compiledValues = values |> Array.map (Array.map (compileLinkedFieldExpr Map.empty >> snd))
            let ret = SQL.FSubExpr (compileName name, Some (Array.map compileName fieldNames), SQL.SValues compiledValues)
            let fromInfo =
                { fromType = FTSubquery selectInfo
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

type private PurityStatus = Pure | NonArgumentPure

let private addPurity (a : PurityStatus) (b : PurityStatus) : PurityStatus =
    match (a, b) with
    | (Pure, Pure) -> Pure
    | (NonArgumentPure, _) -> NonArgumentPure
    | (_, NonArgumentPure) -> NonArgumentPure

let private checkPureExpr (expr : SQL.ValueExpr) : PurityStatus option =
    let mutable noReferences = true
    let mutable noArgumentReferences = true
    let foundReference column =
        noReferences <- false
    let foundPlaceholder placeholder =
        noArgumentReferences <- false
    let foundQuery query =
        noReferences <- false
    SQL.iterValueExpr
        { SQL.idValueExprIter with
              columnReference = foundReference
              placeholder = foundPlaceholder
              query = foundQuery
        }
        expr
    if not noReferences then
        None
    else if not noArgumentReferences then
        Some NonArgumentPure
    else
        Some Pure

let private checkPureColumn : SQL.SelectedColumn -> (SQL.ColumnName * PurityStatus) option = function
    | SQL.SCAll _ -> None
    | SQL.SCExpr (name, expr) -> Option.map (fun purity -> (Option.get name, purity)) (checkPureExpr expr)

[<NoComparison>]
type private PureColumn =
    { columnType : ColumnType
      purity : PurityStatus
      result : SQL.SelectedColumn
    }

let rec private findPureAttributes (columnTypes : ColumnType[]) : SQL.SelectExpr -> (PureColumn option)[] = function
    | SQL.SSelect query ->
        let assignPure colType res =
            match checkPureColumn res with
            | Some (name, purity) ->
                let isGood =
                    match colType with
                    | CTRowAttribute attrName -> true
                    | CTCellAttribute (colName, attrName) -> true
                    | _ -> false
                if isGood then
                    let info =
                        { columnType = colType
                          purity = purity
                          result = res
                        }
                    Some info
                else
                    None
            | _ -> None
        Array.map2 assignPure columnTypes query.columns
    | SQL.SValues vals -> Array.create (Array.length vals.[0]) None
    | SQL.SSetOp (op, a, b, limits) ->
        let addPure minfo1 minfo2 =
            match (minfo1, minfo2) with
            | (Some info1, Some info2) when info1.result = info2.result ->
                Some { columnType = info1.columnType
                       purity = addPurity info1.purity info2.purity
                       result = info1.result
                     }
            | _ -> None
        Array.map2 addPure (findPureAttributes columnTypes a) (findPureAttributes columnTypes b)

let rec private filterExprColumns (cols : (PureColumn option)[]) : SQL.SelectExpr -> SQL.SelectExpr = function
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

    let allPureAttrs = findPureAttributes info.columns expr
    let newExpr = filterExprColumns allPureAttrs expr

    let checkColumn i _ = Option.isNone allPureAttrs.[i]
    let newColumns = Seq.filteri checkColumn info.columns |> Seq.toArray

    let onlyPureAttrs = Seq.catMaybes allPureAttrs |> Seq.toArray
    let attrQuery =
        if Array.isEmpty onlyPureAttrs then
            None
        else
            let query = SQL.SSelect {
                    columns = Array.map (fun info -> info.result) onlyPureAttrs
                    from = None
                    where = None
                    groupBy = [||]
                    orderLimit = SQL.emptyOrderLimitClause
                    extra = null
                }

            let getPureAttribute (info : PureColumn) =
                match info.columnType with
                | CTRowAttribute name when info.purity = Pure -> Some name
                | _ -> None
            let pureAttrs = onlyPureAttrs |> Seq.mapMaybe getPureAttribute |> Set.ofSeq
            let getPureColumnAttribute (info : PureColumn) =
                match info.columnType with
                | CTCellAttribute (colName, name) when info.purity = Pure -> Some (colName, Set.singleton name)
                | _ -> None
            let pureColAttrs = onlyPureAttrs |> Seq.mapMaybe getPureColumnAttribute |> Map.ofSeqWith (fun name -> Set.union)
            Some { query = query.ToString()
                   columns = Array.map (fun info -> info.columnType) onlyPureAttrs
                   pureAttributes = pureAttrs
                   pureColumnAttributes = pureColAttrs
                 }

    { attributesQuery = attrQuery
      query = { expression = newExpr; arguments = compiler.Arguments }
      columns = newColumns
      domains = info.domains
      flattenedDomains = flattenDomains info.domains
      usedSchemas = viewExpr.usedSchemas
      mainEntity = mainEntityRef
    }