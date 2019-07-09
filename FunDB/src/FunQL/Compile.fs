module FunWithFlags.FunDB.FunQL.Compile

open System
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Utils
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

[<NoComparison>]
type private SelectInfo =
    { attributes : Map<FieldName, Set<AttributeName>>
      domains : Domains
    }

type FlattenedDomains = Map<GlobalDomainId, Domain>

[<NoComparison>]
type private FromInfo = FIEntity of GlobalDomainId * Domain // Domain ID is used for merging.
                      | FISubquery of SelectInfo

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

let compilePun (name : FieldName) : SQL.ColumnName =
    SQL.SQLName <| sprintf "__Pun__%O" name

let compileDomain (ns : DomainNamespaceId) : SQL.ColumnName =
    SQL.SQLName <| sprintf "__Domain__%i" ns

let compileId (entity : EntityName) : SQL.ColumnName =
    SQL.SQLName <| sprintf "__Id__%O" entity

let rec private domainExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
    | DSingle (id, dom) -> f dom
    | DMulti (ns, nested) ->
        let makeCase (localId, subcase) =
            let case = SQL.VEEq (SQL.VEColumn { table = Some tableRef; name = compileDomain ns }, SQL.VEValue (SQL.VInt localId))
            (case, domainExpression tableRef f subcase)
        SQL.VECase (nested |> Map.toSeq |> Seq.map makeCase |> Seq.toArray, None)

let private fromInfoExpression (tableRef : SQL.TableRef) (f : Domain -> SQL.ValueExpr) = function
    | FIEntity (id, dom) -> f dom
    | FISubquery info -> domainExpression tableRef f info.domains

type private JoinId = int

let compileJoinId (jid : JoinId) : SQL.TableName =
    SQL.SQLName <| sprintf "__Join__%i" jid

type ColumnType =
    | CTRowAttribute of AttributeName
    | CTCellAttribute of FieldName * AttributeName
    | CTPunAttribute of FieldName
    | CTDomainColumn of DomainNamespaceId
    | CTIdColumn of EntityName
    | CTColumn of FunQLName

let parseColumnName (name : SQL.SQLName) =
    match name.ToString().Split("__") with
    | [|""; "RowAttribute"; name|] -> CTRowAttribute (FunQLName name)
    | [|""; "CellAttribute"; columnName; name|] -> CTCellAttribute (FunQLName columnName, FunQLName name)
    | [|""; "Pun"; name|] -> CTPunAttribute (FunQLName name)
    | [|""; "Domain"; ns|] -> CTDomainColumn (Int32.Parse ns)
    | [|""; "Id"; entity|] -> CTIdColumn (FunQLName entity)
    | [|name|] -> CTColumn (FunQLName name)
    | _ -> failwith <| sprintf "Cannot parse column name: %O" name

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
      pureAttributes : Set<AttributeName>
      pureColumnAttributes : Map<FieldName, Set<AttributeName>>
    }

[<NoComparison>]
type CompiledViewExpr =
    { attributesQuery : CompiledAttributesExpr option
      query : Query<SQL.SelectExpr>
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

let genericCompileFieldExpr (columnFunc : 'f -> SQL.ValueExpr) (placeholderFunc : Placeholder -> SQL.ValueExpr) (queryFunc : SelectExpr<'e, 'f> -> SQL.SelectExpr) : FieldExpr<'e, 'f> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FEColumn c -> columnFunc c
        | FEPlaceholder name -> placeholderFunc name
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
        | FEJsonArray vals -> SQL.VEFunc (SQL.SQLName "jsonb_build_array", Array.map traverse vals)
        | FEJsonObject obj -> SQL.VEFunc (SQL.SQLName "jsonb_build_object", obj |> Map.toSeq |> Seq.collect (fun (FunQLName name, v) -> [SQL.VEValue <| SQL.VString name; traverse v]) |> Seq.toArray)
        | FEJsonArrow (a, b) -> SQL.VEJsonArrow (traverse a, traverse b)
        | FEJsonTextArrow (a, b) -> SQL.VEJsonTextArrow (traverse a, traverse b)
    traverse

let rec compileLocalComputedField (tableRef : SQL.TableRef) (entity : ResolvedEntity) (expr : LinkedLocalFieldExpr) : SQL.ValueExpr =
        let compileRef (ref : LinkedFieldName) =
            // This is checked during resolve already.
            assert (Array.isEmpty ref.path)
            let (_, field) = entity.FindField ref.ref |> Option.get
            match field with
            | RId
            | RColumnField _ -> SQL.VEColumn { table = Some tableRef; name = compileName ref.ref }
            | RComputedField comp -> compileLocalComputedField tableRef entity comp.expression
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in local computed field expression: %O" c
        let voidQuery q = failwith <| sprintf "Unexpected query in local computed field expression: %O" q
        genericCompileFieldExpr compileRef voidPlaceholder voidQuery expr

let compileLocalFieldExpr (arguments : CompiledArgumentsMap) (tableRef : SQL.TableRef) (entity : ResolvedEntity) (expr : LocalFieldExpr) : SQL.ValueExpr =
        let compileRef (name : FieldName) =
            let (_, field) = entity.FindField name |> Option.get
            match field with
            | RId
            | RColumnField _ -> SQL.VEColumn { table = Some tableRef; name = compileName name }
            | RComputedField comp -> compileLocalComputedField tableRef entity comp.expression
        let compilePlaceholder name = SQL.VEPlaceholder arguments.[name].placeholderId
        let voidQuery q = failwith <| sprintf "Unexpected query in local field expression: %O" q
        genericCompileFieldExpr compileRef compilePlaceholder voidQuery expr

type private QueryCompiler (layout : Layout, defaultAttrs : MergedDefaultAttributes, mainEntity : ResolvedMainEntity option, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let convertDefaultAttributeExpr (entityRef : ResolvedEntityRef) (entityName : EntityName) : DefaultAttributeFieldExpr -> ResolvedFieldExpr =
        let resolveReference : DefaultFieldRef -> LinkedBoundFieldRef = function
            | ThisRef fieldName ->
                let bound =
                    { ref = { entity = entityRef; name = fieldName }
                      immediate = true
                    }
                { ref = { ref = { entity = Some { schema = None; name = entityName }; name = fieldName }; bound = Some bound }; path = [||] }
        let resolvePlaceholder = function
            | PLocal name -> failwith <| sprintf "Unexpected local argument in default attribute: %O" name
            | (PGlobal name) as arg ->
                arguments <- addArgument arg (Map.find name globalArgumentTypes) arguments    
                arg            
        let foundQuery query = failwith <| sprintf "Unexpected query in default attribute: %O" query
        mapFieldExpr id resolveReference resolvePlaceholder foundQuery

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

    let rec compileRef (paths : JoinPaths) (tableRef : SQL.TableRef) (entity : ResolvedEntity) (field : ResolvedField) (ref : FieldName) : JoinPaths * SQL.ValueExpr =
        match field with
        | RId
        | RColumnField _ -> (paths, SQL.VEColumn { table = Some { schema = None; name = tableRef.name }; name = compileName ref })
        | RComputedField comp -> compileLinkedLocalFieldExpr paths tableRef entity comp.expression

    and compilePath (paths : JoinPaths) (tableRef : SQL.TableRef) (entity : ResolvedEntity) (field : ResolvedField) (name : FieldName) : FieldName list -> JoinPaths * SQL.ValueExpr = function
        | [] -> compileRef paths tableRef entity field name
        | (ref :: refs) ->
            match field with
            | RColumnField { fieldType = FTReference (entityRef, _) } ->
                let newEntity = Option.get <| layout.FindEntity entityRef
                let (realName, newField) = Option.get <| newEntity.FindField ref
                let pathKey =
                    { table = tableRef.name
                      column = compileName name
                      toTable = entityRef
                    }
                let (newPath, res) =
                    match Map.tryFind pathKey paths with
                    | None ->
                        let newRealName = newJoinId ()
                        let newTableRef = { schema = None; name = newRealName } : SQL.TableRef
                        let (nested, res) = compilePath Map.empty newTableRef newEntity newField realName refs
                        let path =
                            { name = newRealName
                              nested = nested
                            }
                        (path, res)
                    | Some path ->
                        let newTableRef = { schema = None; name = path.name } : SQL.TableRef
                        let (nested, res) = compilePath path.nested newTableRef newEntity newField realName refs
                        let newPath = { path with nested = nested }
                        (newPath, res)
                (Map.add pathKey newPath paths, res)
            | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

    and compileLinkedLocalFieldExpr (paths0 : JoinPaths) (tableRef : SQL.TableRef) (entity : ResolvedEntity) (expr : LinkedLocalFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0
        let compileLinkedName (linked : LinkedFieldName) =
            let (newName, field) = entity.FindField linked.ref |> Option.get
            let (newPaths, ret) = compilePath paths tableRef entity field newName (Array.toList linked.path)
            paths <- newPaths
            ret
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
        let voidQuery q = failwith <| sprintf "Unexpected query in computed field expression: %O" q
        let ret = genericCompileFieldExpr compileLinkedName voidPlaceholder voidQuery expr
        (paths, ret)

    let compileLinkedFieldRef (paths0 : JoinPaths) (linked : LinkedBoundFieldRef) : JoinPaths * SQL.ValueExpr =
        match (linked.path, linked.ref.bound) with
        | ([||], None) ->
            let columnRef = compileFieldRef linked.ref.ref
            (paths0, SQL.VEColumn columnRef)
        | (_, Some boundRef) ->
            let tableRef =
                match linked.ref.ref.entity with
                | Some renamedTable -> compileEntityRef renamedTable
                | None -> compileResolvedEntityRef boundRef.ref.entity
            let entity = layout.FindEntity boundRef.ref.entity |> Option.get
            let (realName, field) = entity.FindField boundRef.ref.name |> Option.get
            // In case it's an immediate name we need to rename outermost field (i.e. `__main`).
            // If it's not we need to keep original naming.
            let newName =
                if boundRef.immediate then realName else linked.ref.ref.name
            compilePath paths0 tableRef entity field newName (Array.toList linked.path)
        | _ -> failwith "Unexpected path with no bound field"

    let rec compileLinkedFieldExpr (paths0 : JoinPaths) (expr : ResolvedFieldExpr) : JoinPaths * SQL.ValueExpr =
        let mutable paths = paths0
        let compileLinkedRef linked =
            let (newPaths, ret) = compileLinkedFieldRef paths linked
            paths <- newPaths
            ret
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
        let compilePlaceholder name = SQL.VEPlaceholder arguments.types.[name].placeholderId
        let compileSubSelectExpr = snd << compileSelectExpr false
        let ret = genericCompileFieldExpr compileLinkedRef compilePlaceholder compileSubSelectExpr expr
        (paths, ret)

    and compileAttribute (paths0 : JoinPaths) (prefix : string) (FunQLName name) (expr : ResolvedFieldExpr) : JoinPaths * SQL.SelectedColumn =
        let (newPaths, compiled) = compileLinkedFieldExpr paths0 expr
        (newPaths, SQL.SCExpr (SQL.SQLName <| sprintf "__%s__%s" prefix name, compiled))

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

    and compileSelectExpr (isTopLevel : bool) : ResolvedSelectExpr -> SelectInfo * SQL.SelectExpr = function
        | SSelect query ->
            let (info, expr) = compileSingleSelectExpr isTopLevel query
            (info, SQL.SSelect expr)
        | SSetOp _ as select ->
            let ns = newDomainNamespaceId ()
            let domainColumn = compileDomain ns
            let mutable lastId = 0
            let rec compileDomained = function
                | SSelect query ->
                    let (info, expr) = compileSingleSelectExpr isTopLevel query
                    let id = lastId
                    lastId <- lastId + 1
                    let modifiedExpr =
                        { expr with
                              columns = Array.append [| SQL.SCExpr (domainColumn, SQL.VEValue <| SQL.VInt id) |] expr.columns
                        }
                    (info.attributes, Map.singleton id info.domains, SQL.SSelect modifiedExpr)
                | SSetOp (op, a, b, limits) ->
                    let (attrs1, domainsMap1, expr1) = compileDomained a
                    let (attrs2, domainsMap2, expr2) = compileDomained b
                    let (limitPaths, compiledLimits) = compileOrderLimitClause Map.empty limits
                    assert Map.isEmpty limitPaths
                    assert (attrs1 = attrs2)
                    (attrs1, Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp (compileSetOp op, expr1, expr2, compiledLimits))
            let (attrs, domainsMap, expr) = compileDomained select
            let info =
                { attributes = attrs
                  domains = DMulti (ns, domainsMap)
                }
            (info, expr)

    and compileSingleSelectExpr (isTopLevel : bool) (select : ResolvedSingleSelectExpr) : SelectInfo * SQL.SingleSelectExpr =
        let mutable paths = Map.empty

        let (domainsMap, queryClause) =
            match select.clause with
            | Some clause ->
                let (newPaths, domainsMap, ret) = compileFromClause paths clause
                paths <- newPaths
                (domainsMap, Some ret)
            | None -> (Map.empty, None)

        let compileRowAttr (name, expr) =
            let (newPaths, col) = compileAttribute paths "RowAttribute" name expr
            paths <- newPaths
            col
        let attributeColumns =
            select.attributes
            |> Map.toSeq
            |> Seq.map compileRowAttr

        let compileRes res =
            let (newPaths, ret) = compileResult paths res
            paths <- newPaths
            ret
        let resultColumns = select.results |> Seq.collect compileRes

        // When at top level we need to ensure that Id column for the main
        // entity is fetched.
        let mutable foundMainId = false

        let getDomainColumns (result : ResolvedQueryResult) =
            let currentAttrs = Map.keysSet result.attributes
    
            match resultFieldRef result.result with
            | Some ({ ref = { ref = { entity = Some { name = entityName }; name = fieldName } } } as resultRef) ->
                let newName = resultName result.result
                let fromInfo = Map.find entityName domainsMap
                let tableRef : SQL.TableRef = { schema = None; name = compileName entityName }

                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | Some info -> Map.singleton newName { info with idColumn = newName }
                    | None -> Map.empty
                let rec getNewDomains = function
                | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))
                let newDomains =
                    if Array.isEmpty resultRef.path
                    then
                        match fromInfo with
                        | FIEntity (domainId, domain) -> DSingle (domainId, getNewDomain domain)
                        | FISubquery info -> getNewDomains info.domains
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

                let idColumns =
                    if Array.isEmpty resultRef.path
                    then
                        let mutable foundId = false

                        let getIdColumn (domain : Domain) =
                            match Map.tryFind fieldName domain with
                            | None -> SQL.VEValue SQL.VNull
                            | Some info ->
                                let colName =
                                    if isTopLevel then
                                        match mainEntity with
                                        | Some main when info.ref.entity = main.entity ->
                                            // Main entity semantics make this safe because having a main entity forbids
                                            // UNIONs.
                                            foundMainId <- true
                                        | _ -> ()
                                    if info.idColumn = funEmpty then
                                        sqlFunId
                                    else
                                        compileId info.idColumn
                                foundId <- true
                                SQL.VEColumn { table = Some tableRef; name = colName }

                        let idExpr = fromInfoExpression tableRef getIdColumn fromInfo
                        if foundId then
                            Seq.singleton <| SQL.SCExpr (compileId newName, idExpr)
                        else
                            Seq.empty
                    else
                        let idPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton funId) |> Array.ofSeq
                        let idRef = { resultRef with path = idPath }
                        let (newPaths, idExpr) = compileLinkedFieldRef paths idRef
                        paths <- newPaths
                        Seq.singleton <| SQL.SCExpr (compileId newName, idExpr)

                let rec getDomainColumns = function
                | DSingle (id, domain) -> Seq.empty
                | DMulti (ns, nested) ->
                    let col = Seq.singleton <| SQL.SCColumn { table = Some tableRef; name = compileDomain ns }
                    Seq.append col (nested |> Map.values |> Seq.collect getDomainColumns)
                let domainColumns =
                    if Array.isEmpty resultRef.path
                    then       
                        match fromInfo with
                        | FIEntity _ -> Seq.empty
                        | FISubquery info -> getDomainColumns info.domains
                    else
                        Seq.empty

                let punColumns =
                    if isTopLevel
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
                                        let (newPaths, expr) = compilePath paths tableRef entity field fieldName [funMain]
                                        paths <- newPaths
                                        expr
                                    | _ -> SQL.VEValue SQL.VNull

                            let punExpr = fromInfoExpression tableRef getPunColumn fromInfo
                            if foundPun then
                                Seq.singleton <| SQL.SCExpr (compilePun newName, punExpr)
                            else
                                Seq.empty
                        else
                            let punPath = Seq.append (Seq.take (Array.length resultRef.path - 1) resultRef.path) (Seq.singleton funMain) |> Array.ofSeq
                            let punRef = { resultRef with path = punPath }
                            let (newPaths, punExpr) = compileLinkedFieldRef paths punRef
                            paths <- newPaths
                            Seq.singleton <| SQL.SCExpr (compilePun newName, punExpr)
                    else Seq.empty

                // Nested and default attributes.
                let (newAttrs, attrColumns) =
                    match fromInfo with
                    | FIEntity (domainId, domain) ->
                        // All initial fields for given entity are always in a domain.
                        let info = Map.find fieldName domain
                        match defaultAttrs.FindField info.ref.entity info.ref.name with
                        | None -> (currentAttrs, Seq.empty)
                        | Some attrs ->
                            let makeDefaultAttr name =
                                let attr = Map.find name attrs
                                let expr = convertDefaultAttributeExpr info.ref.entity entityName attr.expression
                                let (newPaths, ret) = compileAttribute paths (sprintf "CellAttribute__%O" fieldName) name expr
                                paths <- newPaths
                                ret
                            let defaultSet = Map.keysSet attrs
                            let inheritedAttrs = Set.difference defaultSet currentAttrs
                            let allAttrs = Set.union defaultSet currentAttrs
                            let defaultCols = inheritedAttrs |> Set.toSeq |> Seq.map makeDefaultAttr
                            (allAttrs, defaultCols)
                    | FISubquery queryInfo ->
                        let oldAttrs = Map.find fieldName queryInfo.attributes
                        let inheritedAttrs = Set.difference oldAttrs currentAttrs
                        let allAttrs = Set.union oldAttrs currentAttrs
                        let makeInheritedAttr name =
                            let attrName = SQL.SQLName <| sprintf "__CellAttribute__%O__%O" fieldName name
                            SQL.SCColumn { table = Some tableRef; name = attrName }  
                        let inheritedCols = inheritedAttrs |> Set.toSeq |> Seq.map makeInheritedAttr         
                        (allAttrs, inheritedCols)

                (Some newDomains, newAttrs, [ idColumns; domainColumns; punColumns; attrColumns ] |> Seq.concat |> Seq.toArray)
            | _ -> (None, currentAttrs, [||])

        let domainResults = select.results |> Seq.map getDomainColumns |> Seq.toArray
        let domainColumns = domainResults |> Seq.collect (fun (doms, attrs, cols) -> cols) |> Seq.distinct
        let newDomains = domainResults |> Seq.mapMaybe (fun (doms, attrs, cols) -> doms) |> Seq.fold mergeDomains (DSingle (newGlobalDomainId (), Map.empty))
        let queryAttrs = Seq.fold2 (fun attrsMap result (doms, attrs, cols) -> Map.add (resultName result.result) attrs attrsMap) Map.empty select.results domainResults

        let mainIdColumn =
            match mainEntity with
            | Some main when isTopLevel && not foundMainId ->
                let fromCol = { table = Some { schema = None; name = compileName main.entity.name }; name = sqlFunId }: SQL.ColumnRef
                Seq.singleton <| SQL.SCExpr (compileId main.entity.name, SQL.VEColumn fromCol)
            | _ -> Seq.empty

        let columns = [ mainIdColumn; attributeColumns; resultColumns; domainColumns ] |> Seq.concat |> Array.ofSeq
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
            { columns = columns
              clause = newClause
              orderLimit = orderLimit
            } : SQL.SingleSelectExpr

        let info =
            { attributes = queryAttrs
              domains = newDomains
            }
        (info, query)

    and compileResult (paths0 : JoinPaths) (result : ResolvedQueryResult) : JoinPaths * SQL.SelectedColumn seq =
        let name = resultName result.result
        let mutable paths = paths0

        let resultColumn =
            match result.result with
            | QRField field ->
                let (newPaths, ret) = compileLinkedFieldRef paths field
                paths <- newPaths
                SQL.SCExpr (compileName field.ref.ref.name, ret)
            | QRExpr (name, expr) ->
                let (newPaths, ret) = compileLinkedFieldExpr paths expr
                paths <- newPaths
                SQL.SCExpr (compileName name, ret)

        let compileAttr (attrName, expr) =
            let (newPaths, ret) = compileAttribute paths (sprintf "CellAttribute__%O" name) attrName expr
            paths <- newPaths
            ret

        let attrs = result.attributes |> Map.toSeq |> Seq.map compileAttr
        let cols = Seq.append (Seq.singleton resultColumn) attrs |> Seq.toArray
        (paths, Array.toSeq cols)

    and compileFromClause (paths0 : JoinPaths) (clause : ResolvedFromClause) : JoinPaths * FromMap * SQL.FromClause =
        let (domainsMap, from) = compileFromExpr clause.from
        let (newPaths, where) =
            match clause.where with
            | None -> (paths0, None)
            | Some where ->
                let (newPaths, ret) = compileLinkedFieldExpr paths0 where
                (newPaths, Some ret)
        (newPaths, domainsMap, { from = from; where = where })

    and buildJoins (from : SQL.FromExpr) (paths : JoinPaths) =
        Map.fold joinPath from paths

    and joinPath (from : SQL.FromExpr) (joinKey : JoinKey) (path : JoinPath) =
        let tableRef = { schema = None; name = joinKey.table } : SQL.TableRef
        let toTableRef = { schema = None; name = path.name } : SQL.TableRef
        let entity = layout.FindEntity joinKey.toTable |> Option.get

        let fromColumn = SQL.VEColumn { table = Some tableRef; name = joinKey.column }
        let toColumn = SQL.VEColumn { table = Some toTableRef; name = sqlFunId }
        let joinExpr = SQL.VEEq (fromColumn, toColumn)
        let subquery = SQL.FTable (Some path.name, compileResolvedEntityRef joinKey.toTable)
        let currJoin = SQL.FJoin (SQL.Left, from, subquery, joinExpr)
        buildJoins currJoin path.nested

    and compileFromExpr : ResolvedFromExpr -> FromMap * SQL.FromExpr = function
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

            (Map.singleton entityRef.name (FIEntity (newGlobalDomainId (), domain)), subquery)
        | FJoin (jt, e1, e2, where) ->
            let (fromMap1, r1) = compileFromExpr e1
            let (fromMap2, r2) = compileFromExpr e2
            let fromMap = Map.unionUnique fromMap1 fromMap2
            let (joinPaths, joinExpr) = compileLinkedFieldExpr Map.empty where
            if not <| Map.isEmpty joinPaths then
                failwith <| sprintf "Unexpected dereference in join expression: %O" where
            let ret = SQL.FJoin (compileJoin jt, r1, r2, joinExpr)
            (fromMap, ret)
        | FSubExpr (name, q) ->
            let (info, expr) = compileSelectExpr false q
            let ret = SQL.FSubExpr (compileName name, None, expr)
            (Map.singleton name (FISubquery info), ret)
        | FValues (name, fieldNames, values) ->
            let domainsMap = DSingle (newDomainNamespaceId (), Map.empty)
            let info =
                { attributes = fieldNames |> Seq.map (fun name -> (name, Set.empty)) |> Map.ofSeq
                  domains = domainsMap
                }
            let compiledValues = values |> Array.map (Array.map (compileLinkedFieldExpr Map.empty >> snd))
            let ret = SQL.FSubExpr (compileName name, Some (Array.map compileName fieldNames), SQL.SValues compiledValues)
            (Map.singleton name (FISubquery info), ret)

    member this.CompileSelectExpr = compileSelectExpr true

    member this.CompileSingleFromClause (clause : ResolvedFromClause) =
        let (paths, domainsMap, compiled) = compileFromClause Map.empty clause
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

type private AttributeColumn =
    | ACRowAttribute of AttributeName
    | ACCellAttribute of FieldName * AttributeName

let rec private findPureAttributes : SQL.SelectExpr -> Map<SQL.ColumnName, PurityStatus * SQL.SelectedColumn * AttributeColumn> = function
    | SQL.SSelect query ->
        let assignPure res =
            match checkPureColumn res with
            | Some (name, purity) ->
                match parseColumnName name with
                | CTRowAttribute attrName -> Some (name, (purity, res, ACRowAttribute attrName))
                | CTCellAttribute (colName, attrName) -> Some (name, (purity, res, ACCellAttribute (colName, attrName)))
                | _ -> None
            | _ -> None
        query.columns |> Seq.mapMaybe assignPure |> Map.ofSeq
    | SQL.SValues _ -> Map.empty
    | SQL.SSetOp (op, a, b, limits) ->
        let addPure name (aPurity, aExpr, aAttr) (bPurity, bExpr, bAttr) =
            if aExpr = bExpr then
                Some (addPurity aPurity bPurity, aExpr, aAttr)
            else
                None
        Map.intersectWithMaybe addPure (findPureAttributes a) (findPureAttributes b)

let rec private filterExprColumns (cols : Set<SQL.ColumnName>) : SQL.SelectExpr -> SQL.SelectExpr = function
    | SQL.SSelect query ->
        let checkColumn = function
            | SQL.SCExpr (name, _) -> not <| Set.contains name cols
            | _ -> true
        SQL.SSelect { query with columns = query.columns |> Array.filter checkColumn }
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
    let compiler = QueryCompiler (layout, emptyMergedDefaultAttributes, None, bogusArguments)
    compiler.CompileSingleFromClause clause

let compileViewExpr (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let compiler = QueryCompiler (layout, defaultAttrs, viewExpr.mainEntity, compileArguments viewExpr.arguments)
    let (info, expr) = compiler.CompileSelectExpr viewExpr.select

    let allPureAttrs = findPureAttributes expr
    let newExpr = filterExprColumns (Map.keysSet allPureAttrs) expr
    let attrQuery =
        if Map.isEmpty allPureAttrs then
            None
        else
            let cols = allPureAttrs |> Map.values |> Seq.map (fun (status, r, attrName) -> r) |> Array.ofSeq
            let query = SQL.SSelect { columns = cols; clause = None; orderLimit = SQL.emptyOrderLimitClause }

            let getPureAttribute (status, r, attr) =
                match attr with
                | ACRowAttribute name when status = Pure -> Some name
                | _ -> None
            let pureAttrs = allPureAttrs |> Map.values |> Seq.mapMaybe getPureAttribute |> Set.ofSeq
            let getPureColumnAttribute (status, r, attr) =
                match attr with
                | ACCellAttribute (colName, name) when status = Pure -> Some (colName, Set.singleton name)
                | _ -> None
            let pureColAttrs = allPureAttrs |> Map.values |> Seq.mapMaybe getPureColumnAttribute |> Map.ofSeqWith (fun name -> Set.union)
            Some { query = query.ToSQLString()
                   pureAttributes = pureAttrs
                   pureColumnAttributes = pureColAttrs
                 }

    { attributesQuery = attrQuery
      query = { expression = newExpr; arguments = compiler.Arguments }
      domains = info.domains
      flattenedDomains = flattenDomains info.domains
      usedSchemas = viewExpr.usedSchemas
    }