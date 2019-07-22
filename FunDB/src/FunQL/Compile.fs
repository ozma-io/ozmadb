module FunWithFlags.FunDB.FunQL.Compile

open System
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Types
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
      field : ResolvedField
      // A field with assigned idEntity of Foo will use ID column "__Id__Foo"
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
    | CTMainIdColumn
    | CTColumn of FunQLName

let columnName : ColumnType -> SQL.SQLName = function
    | CTRowAttribute (FunQLName name) -> SQL.SQLName (sprintf "__RowAttribute__%s" name)
    | CTCellAttribute (FunQLName field, FunQLName name) -> SQL.SQLName (sprintf "__CellAttribute__%s__%s" field name)
    | CTPunAttribute (FunQLName field) -> SQL.SQLName (sprintf "__Pun__%s" field)
    | CTDomainColumn id -> SQL.SQLName (sprintf "__Domain__%i" id)
    | CTIdColumn (FunQLName entity) -> SQL.SQLName (sprintf "__Id__%s" entity)
    | CTMainIdColumn -> SQL.SQLName "__MainId"
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
      toTable : ResolvedEntityRef
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

let genericCompileFieldExpr (refFunc : 'f -> SQL.ValueExpr) (queryFunc : SelectExpr<'e, 'f> -> SQL.SelectExpr) : FieldExpr<'e, 'f> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FERef c -> refFunc c
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
    traverse

let rec compileLocalComputedField (tableRef : SQL.TableRef) (entity : ResolvedEntity) (expr : LinkedLocalFieldExpr) : SQL.ValueExpr =
        let compileReference (ref : LinkedFieldName) =
            let fieldRef =
                match ref.ref with
                | VRColumn c -> c
                | VRPlaceholder p -> failwith <| sprintf "Unexpected placeholder: %O" p
            // This is checked during resolve already.
            assert (Array.isEmpty ref.path)
            let (_, field) = entity.FindField fieldRef |> Option.get
            match field with
            | RId
            | RColumnField _ -> SQL.VEColumn { table = Some tableRef; name = compileName fieldRef }
            | RComputedField comp -> compileLocalComputedField tableRef entity comp.expression
        let voidQuery q = failwith <| sprintf "Unexpected query in local computed field expression: %O" q
        genericCompileFieldExpr compileReference voidQuery expr

let compileLocalFieldExpr (arguments : CompiledArgumentsMap) (tableRef : SQL.TableRef) (entity : ResolvedEntity) (expr : LocalFieldExpr) : SQL.ValueExpr =
        let compileReference = function
            | VRColumn name ->
                let (_, field) = entity.FindField name |> Option.get
                match field with
                | RId
                | RColumnField _ -> SQL.VEColumn { table = Some tableRef; name = compileName name }
                | RComputedField comp -> compileLocalComputedField tableRef entity comp.expression
            | VRPlaceholder name -> SQL.VEPlaceholder arguments.[name].placeholderId
        let voidQuery q = failwith <| sprintf "Unexpected query in local field expression: %O" q
        genericCompileFieldExpr compileReference voidQuery expr

[<NoComparison>]
type private ResultColumn =
    { domains : Domains option
      attributes : Set<AttributeName>
      columns : (ColumnType * SQL.SelectedColumn) array
    }

type private SelectFlags =
    { hasMainEntity : bool
      isTopLevel : bool
      metaColumns : bool
    }

type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let convertLinkedLocalExpr (entityRef : ResolvedEntityRef) (localRef : EntityRef) : LinkedLocalFieldExpr -> ResolvedFieldExpr =
        let resolveReference (ref : LinkedFieldName) : LinkedBoundFieldRef =
            let newRef =
                match ref.ref with
                | VRColumn fieldName ->
                    let bound =
                        { ref = { entity = entityRef; name = fieldName }
                          immediate = true
                        }
                    VRColumn { ref = ({ entity = Some localRef; name = fieldName } : FieldRef); bound = Some bound }
                | VRPlaceholder (PLocal name) -> failwith <| sprintf "Unexpected local argument: %O" name
                | VRPlaceholder ((PGlobal name) as arg) ->
                    arguments <- addArgument arg (Map.find name globalArgumentTypes) arguments
                    VRPlaceholder arg
            { ref = newRef; path = ref.path }
        // FIXME: why so? allow it!
        let foundQuery query = failwith <| sprintf "Unexpected query: %O" query
        mapFieldExpr id resolveReference foundQuery

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

    let rec followPath (fieldRef : ResolvedFieldRef) (field : ResolvedField) : FieldName list -> ResolvedFieldRef * ResolvedField = function
        | [] -> (fieldRef, field)
        | (ref :: refs) ->
            match field with
            | RColumnField { fieldType = FTReference (entityRef, _) } ->
                let newEntity = Option.get <| layout.FindEntity entityRef
                let (_, newField) = Option.get <| newEntity.FindField ref
                let newFieldRef = { entity = entityRef; name = ref }
                followPath newFieldRef newField refs
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    let rec compileRef (paths : JoinPaths) (tableRef : SQL.TableRef) (entityRef : ResolvedEntityRef) (field : ResolvedField) (ref : FieldName) : JoinPaths * SQL.ValueExpr =
        match field with
        | RId
        | RColumnField _ -> (paths, SQL.VEColumn { table = Some tableRef; name = compileName ref })
        | RComputedField comp ->
            let localRef = { schema = Option.map decompileName tableRef.schema; name = decompileName tableRef.name } : EntityRef
            compileLinkedFieldExpr paths <| convertLinkedLocalExpr entityRef localRef comp.expression

    and compilePath (paths : JoinPaths) (tableRef : SQL.TableRef) (entityRef : ResolvedEntityRef) (field : ResolvedField) (name : FieldName) : FieldName list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef paths tableRef entityRef field name
        | (ref :: refs) ->
            match field with
            | RColumnField { fieldType = FTReference (newEntityRef, _) } ->
                let (realName, newField) = Option.get <| layout.FindField newEntityRef ref
                let pathKey =
                    { table = tableRef.name
                      column = compileName name
                      toTable = newEntityRef
                    }
                let (newPath, res) =
                    match Map.tryFind pathKey paths with
                    | None ->
                        let newRealName = newJoinId ()
                        let newTableRef = { schema = None; name = newRealName } : SQL.TableRef
                        let (nested, res) = compilePath Map.empty newTableRef newEntityRef newField realName refs
                        let path =
                            { name = newRealName
                              nested = nested
                            }
                        (path, res)
                    | Some path ->
                        let newTableRef = { schema = None; name = path.name } : SQL.TableRef
                        let (nested, res) = compilePath path.nested newTableRef newEntityRef newField realName refs
                        let newPath = { path with nested = nested }
                        (newPath, res)
                (Map.add pathKey newPath paths, res)
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    and compileLinkedFieldRef (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
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
                    | None -> compileResolvedEntityRef boundRef.ref.entity
                let (realName, field) = layout.FindField boundRef.ref.entity boundRef.ref.name |> Option.get
                // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
                // If it's not we need to keep original naming.
                let newName =
                    if boundRef.immediate then realName else ref.ref.name
                compilePath paths0 tableRef boundRef.ref.entity field newName (Array.toList linked.path)
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
                    let argEntity = layout.FindEntity argEntityRef |> Option.get

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
                    let fromClause =
                        { from = FEntity (None, argEntityRef)
                          where = Some <| FEEq (FERef idColumn, FERef arg)
                        }

                    let result =
                        { attributes = Map.empty
                          result = QRField <| makeColumn firstName remainingPath
                        }
                    let selectClause =
                        { attributes = Map.empty
                          results = [| result |]
                          clause = Some fromClause
                          orderLimit = emptyOrderLimitClause
                        } : ResolvedSingleSelectExpr
                    let flags =
                        { hasMainEntity = false
                          isTopLevel = false
                          metaColumns = false
                        }
                    let (info, subquery) = compileSelectExpr flags (SSelect selectClause)
                    (paths0, SQL.VESubquery subquery)
                | typ -> failwith <| sprintf "Argument is not a reference: %O" name

    and compileLinkedFieldExpr (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0
        let compileLinkedRef linked =
            let (newPaths, ret) = compileLinkedFieldRef paths linked
            paths <- newPaths
            ret
        let compileSubSelectExpr =
            let flags =
                { hasMainEntity = false
                  isTopLevel = false
                  metaColumns = false
                }
            snd << compileSelectExpr flags
        let ret = genericCompileFieldExpr compileLinkedRef compileSubSelectExpr expr
        (paths, ret)

    and compileAttribute (paths0 : JoinPaths) (attrType : ColumnType) (expr : ResolvedFieldExpr) : JoinPaths * SQL.SelectedColumn =
        let (newPaths, compiled) = compileLinkedFieldExpr paths0 expr
        (newPaths, SQL.SCExpr (columnName attrType, compiled))

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
                              columns = Array.append [| SQL.SCExpr (domainName, SQL.VEValue <| SQL.VInt id) |] expr.columns
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

        let (fromMap, queryClause) =
            match select.clause with
            | Some clause ->
                let (newPaths, domainsMap, ret) = compileFromClause flags.hasMainEntity paths clause
                paths <- newPaths
                (domainsMap, Some ret)
            | None -> (Map.empty, None)

        let compileRowAttr (name, expr) =
            let attrCol = CTRowAttribute name
            let (newPaths, col) = compileAttribute paths attrCol expr
            paths <- newPaths
            (attrCol, col)
        let attributeColumns =
            select.attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        // We keep Id columns map to remove duplicates.
        let mutable ids = Map.empty

        let getResultEntry (result : ResolvedQueryResult) =
            let currentAttrs = Map.keysSet result.attributes
            let (newPaths, resultColumns) = compileResult paths result
            paths <- newPaths

            match resultFieldRef result.result with
            | Some ({ ref = { ref = { entity = Some ({ name = entityName } as entityRef); name = fieldName } } } as resultRef) when flags.metaColumns ->
                let newName = result.result.ToName ()
                let fromInfo = Map.find entityName fromMap
                let tableRef : SQL.TableRef = { schema = None; name = compileName entityName }

                let maybeIdColumn =
                    if Array.isEmpty resultRef.path
                    then
                        let mutable foundId = false

                        let getIdColumn (domain : Domain) =
                            match Map.tryFind fieldName domain with
                            | None -> SQL.VEValue SQL.VNull
                            | Some info ->
                                let colName =
                                    if info.idColumn = funEmpty then
                                        sqlFunId
                                    else
                                        columnName (CTIdColumn info.idColumn)
                                foundId <- true
                                SQL.VEColumn { table = Some tableRef; name = colName }

                        let idExpr = fromInfoExpression tableRef getIdColumn fromInfo.fromType
                        if foundId then
                            Some idExpr
                        else
                            None
                    else
                        let idPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton funId) |> Array.ofSeq
                        let idRef = { ref = VRColumn resultRef.ref; path = idPath }
                        let (newPaths, idExpr) = compileLinkedFieldRef paths idRef
                        paths <- newPaths
                        Some idExpr

                let (maybeIdName, idColumns) =
                    match maybeIdColumn with
                    | None -> (None, Seq.empty)
                    | Some idExpr ->
                        let idStr = idExpr.ToString()
                        match Map.tryFind idStr ids with
                        | None ->
                            ids <- Map.add idStr newName ids
                            let colName = CTIdColumn newName
                            let column = (colName, SQL.SCExpr (columnName colName, idExpr))
                            (Some newName, Seq.singleton column)
                        | Some idName -> (Some idName, Seq.empty)

                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | Some info -> Map.singleton newName { info with idColumn = Option.get maybeIdName }
                    | None -> Map.empty
                let rec getNewDomains = function
                | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))
                let newDomains =
                    if Array.isEmpty resultRef.path
                    then
                        match fromInfo.fromType with
                        | FTEntity (domainId, domain) -> DSingle (domainId, getNewDomain domain)
                        | FTSubquery info -> getNewDomains info.domains
                    else
                        // Pathed refs always have bound fields
                        let oldBound = Option.get resultRef.ref.bound
                        let (_, oldField) = Option.get <| layout.FindField oldBound.ref.entity oldBound.ref.name
                        let (newRef, newField) = followPath oldBound.ref oldField (List.ofArray resultRef.path)
                        let newInfo =
                            { ref = newRef
                              field = newField
                              idColumn = newName
                            }
                        DSingle (newGlobalDomainId (), Map.singleton newName newInfo )

                let rec getDomainColumns = function
                | DSingle (id, domain) -> Seq.empty
                | DMulti (ns, nested) ->
                    let colName = CTDomainColumn ns
                    let col = (colName, SQL.SCColumn { table = Some tableRef; name = columnName colName })
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
                        // TODO: algorithm is similar to the one for Ids; generalize?
                        if Array.isEmpty resultRef.path
                        then
                            let mutable foundPun = false

                            let getPunColumn (domain : Domain) =
                                match Map.tryFind fieldName domain with
                                | None -> SQL.VEValue SQL.VNull
                                | Some info ->
                                    let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity: %O" info.ref.entity) <| layout.FindEntity info.ref.entity
                                    match info.field with
                                    | RColumnField { fieldType = FTReference (newEntityRef, _) } ->
                                        let (_, field) = entity.FindField fieldName |> Option.get
                                        let (newPaths, expr) = compilePath paths tableRef info.ref.entity field fieldName [funMain]
                                        paths <- newPaths
                                        foundPun <- true
                                        expr
                                    | _ -> SQL.VEValue SQL.VNull

                            let punExpr = fromInfoExpression tableRef getPunColumn fromInfo.fromType
                            if foundPun then
                                let colName = CTPunAttribute newName
                                let col = (colName, SQL.SCExpr (columnName colName, punExpr))
                                Seq.singleton col
                            else
                                Seq.empty
                        else
                            let punPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton funMain) |> Array.ofSeq
                            let punRef = { ref = VRColumn resultRef.ref; path = punPath }
                            let (newPaths, punExpr) = compileLinkedFieldRef paths punRef
                            paths <- newPaths
                            let colName = CTPunAttribute newName
                            let col = (colName, SQL.SCExpr (columnName colName, punExpr))
                            Seq.singleton col
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
                                let expr = convertLinkedLocalExpr info.ref.entity entityRef attr.expression
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
                        let oldAttrs = Map.find fieldName queryInfo.attributes
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let allAttrs = Set.union oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrCol = CTCellAttribute (fieldName, name)
                            (attrCol, SQL.SCColumn { table = Some tableRef; name = columnName attrCol })
                        let inheritedCols = inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr
                        (allAttrs, inheritedCols)

                { domains = Some newDomains
                  attributes = newAttrs
                  columns = [ idColumns; domainColumns; attrColumns; resultColumns; punColumns ] |> Seq.concat |> Seq.toArray
                }
            | _ ->
                { domains = None
                  attributes = currentAttrs
                  columns = Seq.toArray resultColumns
                }

        let resultEntries = select.results |> Seq.map getResultEntry |> Seq.toArray
        let emptyDomains = DSingle (newGlobalDomainId (), Map.empty)
        let (attributes, newDomains, columns) =
            if flags.metaColumns then
                let resultColumns = resultEntries |> Seq.collect (fun entry -> entry.columns) |> Seq.distinct
                let newDomains = resultEntries |> Seq.mapMaybe (fun entry -> entry.domains) |> Seq.fold mergeDomains emptyDomains
                let queryAttrs = Seq.fold2 (fun attrsMap result entry -> Map.add (result.result.ToName ()) entry.attributes attrsMap) Map.empty select.results resultEntries

                let mainIdColumns =
                    if not flags.hasMainEntity then
                        Seq.empty
                    else
                        let findMainId (name, info : FromInfo) : SQL.ColumnRef option =
                            match info.mainId with
                            | Some id -> Some { table = Some { schema = None; name = compileName name }; name = id }
                            | None -> None
                        let mainId = Map.toSeq fromMap |> Seq.mapMaybe findMainId |> Seq.exactlyOne
                        let col = (CTMainIdColumn, SQL.SCExpr (columnName CTMainIdColumn, SQL.VEColumn mainId))
                        Seq.singleton col

                let columns = [ mainIdColumns; attributeColumns; resultColumns ] |> Seq.concat |> Array.ofSeq
                (queryAttrs, newDomains, columns)
            else
                let columns = resultEntries |> Seq.collect (fun entry -> entry.columns) |> Array.ofSeq
                (Map.empty, emptyDomains, columns)
        let (newOrderLimitPaths, orderLimit) = compileOrderLimitClause paths select.orderLimit

        let newClause =
            if Map.isEmpty newOrderLimitPaths then
                queryClause
            else
                let clause = Option.get queryClause
                Some
                    { clause with
                          from = buildJoins clause.from newOrderLimitPaths
                    }

        let query =
            { columns = Array.map snd columns
              clause = newClause
              orderLimit = orderLimit
            } : SQL.SingleSelectExpr

        let info =
            { attributes = attributes
              domains = newDomains
              columns = Array.map fst columns
            } : SelectInfo
        (info, query)

    and compileResult (paths0 : JoinPaths) (result : ResolvedQueryResult) : JoinPaths * (ColumnType * SQL.SelectedColumn) seq =
        let resultName = result.result.ToName ()
        let sqlCol = CTColumn resultName
        let mutable paths = paths0

        let (newPaths, newExpr) =
            match result.result with
            | QRField field -> compileLinkedFieldRef paths field
            | QRExpr (name, expr) -> compileLinkedFieldExpr paths expr

        paths <- newPaths
        let resultColumn = (sqlCol, SQL.SCExpr (columnName sqlCol, newExpr))

        let compileAttr (attrName, expr) =
            let attrCol = CTCellAttribute (resultName, attrName)
            let (newPaths, ret) = compileAttribute paths attrCol expr
            paths <- newPaths
            (attrCol, ret)

        let attrs = result.attributes |> Map.toSeq |> Seq.map compileAttr
        let cols = Seq.append (Seq.singleton resultColumn) attrs |> Seq.toArray
        (paths, Array.toSeq cols)

    and compileFromClause (hasMainEntity : bool) (paths0 : JoinPaths) (clause : ResolvedFromClause) : JoinPaths * FromMap * SQL.FromClause =
        let (fromMap, from) = compileFromExpr hasMainEntity clause.from
        let (newPaths, where) =
            match clause.where with
            | None -> (paths0, None)
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr paths0 where
                (newPaths, Some ret)
        (newPaths, fromMap, { from = from; where = where })

    and buildJoins (from : SQL.FromExpr) (paths : JoinPaths) : SQL.FromExpr =
        Map.fold joinPath from paths

    and joinPath (from : SQL.FromExpr) (joinKey : JoinKey) (path : JoinPath) : SQL.FromExpr =
        let tableRef = { schema = None; name = joinKey.table } : SQL.TableRef
        let toTableRef = { schema = None; name = path.name } : SQL.TableRef
        let entity = layout.FindEntity joinKey.toTable |> Option.get

        let fromColumn = SQL.VEColumn { table = Some tableRef; name = joinKey.column }
        let toColumn = SQL.VEColumn { table = Some toTableRef; name = sqlFunId }
        let joinExpr = SQL.VEEq (fromColumn, toColumn)
        let subquery = SQL.FTable (Some path.name, compileResolvedEntityRef joinKey.toTable)
        let currJoin = SQL.FJoin (SQL.Left, from, subquery, joinExpr)
        buildJoins currJoin path.nested

    and compileFromExpr (hasMainEntity : bool) : ResolvedFromExpr -> FromMap * SQL.FromExpr = function
        | FEntity (pun, entityRef) ->
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef

            let makeDomainEntry name field =
                { ref = { entity = entityRef; name = name }
                  field = field
                  // Special value which means "use Id"
                  idColumn = funEmpty
                }
            let domain = mapAllFields makeDomainEntry entity

            let subquery = SQL.FTable (Option.map compileName pun, compileResolvedEntityRef entityRef)
            let fromInfo =
                { fromType = FTEntity (newGlobalDomainId (), domain)
                  mainId = if hasMainEntity then Some sqlFunId else None
                }

            (Map.singleton entityRef.name fromInfo, subquery)
        | FJoin (jt, e1, e2, where) ->
            let hasMain1 =
                match jt with
                | Left -> true
                | _ -> false
            let (fromMap1, r1) = compileFromExpr hasMain1 e1
            let hasMain2 =
                match jt with
                | Right -> true
                | _ -> false
            let (fromMap2, r2) = compileFromExpr hasMain2 e2
            let fromMap = Map.unionUnique fromMap1 fromMap2
            let (joinPaths, joinExpr) = compileLinkedFieldExpr Map.empty where
            if not <| Map.isEmpty joinPaths then
                failwith <| sprintf "Unexpected dereference in join expression: %O" where
            let ret = SQL.FJoin (compileJoin jt, r1, r2, joinExpr)
            (fromMap, ret)
        | FSubExpr (name, q) ->
            let flags =
                { hasMainEntity = hasMainEntity
                  isTopLevel = false
                  metaColumns = true
                }
            let (selectInfo, expr) = compileSelectExpr flags q
            let ret = SQL.FSubExpr (compileName name, None, expr)
            let fromInfo =
                { fromType = FTSubquery selectInfo
                  mainId = Some (columnName CTMainIdColumn)
                }
            (Map.singleton name fromInfo, ret)
        | FValues (name, fieldNames, values) ->
            assert not hasMainEntity

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
                }
            (Map.singleton name fromInfo, ret)

    member this.CompileSelectExpr (hasMainEntity : bool) =
        let flags =
            { hasMainEntity = hasMainEntity
              isTopLevel = true
              metaColumns = true
            }
        compileSelectExpr flags

    member this.CompileSingleFromClause (clause : ResolvedFromClause) =
        let (paths, domainsMap, compiled) = compileFromClause false Map.empty clause
        { compiled with
              from = buildJoins compiled.from paths
        }

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
    SQL.iterValueExpr foundReference ignore foundQuery expr
    if not noReferences then
        None
    else if not noArgumentReferences then
        Some NonArgumentPure
    else
        Some Pure

let private checkPureColumn : SQL.SelectedColumn -> (SQL.ColumnName * PurityStatus) option = function
    | SQL.SCAll _ -> None
    | SQL.SCColumn _ -> None
    | SQL.SCExpr (name, expr) -> Option.map (fun purity -> (name, purity)) (checkPureExpr expr)

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

let compileSingleFromClause (layout : Layout) (argumentsMap : CompiledArgumentsMap) (clause : ResolvedFromClause) : SQL.FromClause =
    let bogusArguments =
        { types = argumentsMap
          lastPlaceholderId = 0
        }
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, bogusArguments)
    compiler.CompileSingleFromClause clause

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let compiler = QueryCompiler (layout, defaultAttrs, compileArguments viewExpr.arguments)
    let (info, expr) = compiler.CompileSelectExpr (Option.isSome viewExpr.mainEntity) viewExpr.select

    let allPureAttrs = findPureAttributes info.columns expr
    let newExpr = filterExprColumns allPureAttrs expr

    let checkColumn i _ = Option.isNone allPureAttrs.[i]
    let newColumns = Seq.filteri checkColumn info.columns |> Seq.toArray

    let onlyPureAttrs = Seq.catMaybes allPureAttrs |> Seq.toArray
    let attrQuery =
        if Array.isEmpty onlyPureAttrs then
            None
        else
            let query = SQL.SSelect { columns = Array.map (fun info -> info.result) onlyPureAttrs; clause = None; orderLimit = SQL.emptyOrderLimitClause }

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
    }