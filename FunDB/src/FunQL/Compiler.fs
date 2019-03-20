module FunWithFlags.FunDB.FunQL.Compiler

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.AST

// Domains is a way to distinguish rows after set operations so that row types and IDs can be traced back.
// Domain is a map of assigned references to fields to each column.
// Domain key is cross product of all local domain ids i.e. map from domain namespace ids to local domain ids.
// Local domain ids get assigned to all sets in a set operation.
// Each row has a different domain key assigned, so half of rows may come from entity A and half from entity B.
// This way one can make use if IDs assigned to cells to reference and update them.
type DomainField = {
    field : ResolvedFieldRef
    // A field with assigned idEntity of Foo will use ID column "__Id__Foo"
    idColumn : EntityName
}

type Domain = Map<FieldName, DomainField>
type GlobalDomainId = int
type DomainNamespaceId = int
type LocalDomainId = int
type Domains =
    | DSingle of GlobalDomainId * Domain
    | DMulti of DomainNamespaceId * Map<LocalDomainId, Domains>
type FlattenedDomains = Map<GlobalDomainId, Domain>

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

let compileName : FunQLName -> SQL.SQLName = function
    | FunQLName name -> SQL.SQLName name

let sqlFunId = compileName funId
let sqlFunView = compileName funView
let private funEmpty = FunQLName ""

let compilePun (name : FieldName) : SQL.SQLName =
    SQL.SQLName <| sprintf "__Pun__%O" name

let compileDomain (ns : DomainNamespaceId) =
    SQL.SQLName <| sprintf "__Domain__%i" ns

let compileId (entity : EntityName) =
    SQL.SQLName <| sprintf "__Id__%O" entity

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

type CompiledArgument =
    { placeholder : int
      fieldType : ParsedFieldType
      valueType : SQL.SimpleValueType
    }

type private ArgumentsMap = Map<Placeholder, CompiledArgument>

let typecheckArgument (fieldType : FieldType<_, _>) (value : FieldValue) : Result<unit, string> =
    match fieldType with
    | FTEnum vals ->
        match value with
        | FString str when Set.contains str vals -> Ok ()
        | _ -> Error <| sprintf "Argument is not from allowed values of a enum: %O" value
    // Most casting/typechecking will be done by database or Npgsql
    | _ -> Ok ()

let defaultCompiledExprArgument : FieldExprType -> FieldValue = function
    | FETArray SFTString -> FStringArray [||]
    | FETArray SFTInt -> FStringArray [||]
    | FETArray SFTDecimal -> FStringArray [||]
    | FETArray SFTBool -> FStringArray [||]
    | FETArray SFTDateTime -> FStringArray [||]
    | FETArray SFTDate -> FStringArray [||]
    | FETScalar SFTString -> FString ""
    | FETScalar SFTInt -> FInt 0
    | FETScalar SFTDecimal -> FDecimal 0m
    | FETScalar SFTBool -> FBool false
    | FETScalar SFTDateTime -> FDateTime DateTimeOffset.UnixEpoch
    | FETScalar SFTDate -> FDateTime DateTimeOffset.UnixEpoch

let defaultCompiledArgument : ParsedFieldType -> FieldValue = function
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

type CompiledViewExpr =
    { attributesQuery : CompiledAttributesExpr option
      query : SQL.SelectExpr
      arguments : ArgumentsMap
      domains : Domains
      flattenedDomains : FlattenedDomains
    }

let private compileScalarType : ScalarFieldType -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate

let private compileFieldExprType : FieldExprType -> SQL.SimpleValueType = function
    | FETScalar stype -> SQL.VTScalar <| compileScalarType stype
    | FETArray stype -> SQL.VTArray <| compileScalarType stype

let compileFieldType : FieldType<_, _> -> SQL.SimpleValueType = function
    | FTType fetype -> compileFieldExprType fetype
    | FTReference (ent, restriction) -> SQL.VTScalar SQL.STInt
    | FTEnum vals -> SQL.VTScalar SQL.STString

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

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = Some (compileName entityRef.schema); name = compileName entityRef.name }

let compileFieldRef (fieldRef : FieldRef) : SQL.ColumnRef =
    { table = Option.map compileEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileResolvedFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

let compileArray (vals : 'a array) : SQL.ValueArray<'a> = Array.map SQL.AVValue vals

let compileFieldValue : FieldValue -> SQL.ValueExpr = function
    | FInt i -> SQL.VEValue (SQL.VInt i)
    | FDecimal d -> SQL.VEValue (SQL.VDecimal d)
    // PostgreSQL cannot deduce text's type on its own
    | FString s -> SQL.VECast (SQL.VEValue (SQL.VString s), SQL.VTScalar (SQL.STString.ToSQLRawString()))
    | FBool b -> SQL.VEValue (SQL.VBool b)
    | FDateTime dt -> SQL.VEValue (SQL.VDateTime dt)
    | FDate d -> SQL.VEValue (SQL.VDate d)
    | FIntArray vals -> SQL.VEValue (SQL.VIntArray (compileArray vals))
    | FDecimalArray vals -> SQL.VEValue (SQL.VDecimalArray (compileArray vals))
    | FStringArray vals -> SQL.VEValue (SQL.VStringArray (compileArray vals))
    | FBoolArray vals -> SQL.VEValue (SQL.VBoolArray (compileArray vals))
    | FDateTimeArray vals -> SQL.VEValue (SQL.VDateTimeArray (compileArray vals))
    | FDateArray vals -> SQL.VEValue (SQL.VDateArray (compileArray vals))
    | FNull -> SQL.VEValue SQL.VNull

// Differs from compileFieldValue in that it doesn't emit value expressions.
let compileArgument : FieldValue -> SQL.Value = function
    | FInt i -> SQL.VInt i
    | FDecimal d -> SQL.VDecimal d
    | FString s -> SQL.VString s
    | FBool b -> SQL.VBool b
    | FDateTime dt -> SQL.VDateTime dt
    | FDate d -> SQL.VDate d
    | FIntArray vals -> SQL.VIntArray (compileArray vals)
    | FDecimalArray vals -> SQL.VDecimalArray (compileArray vals)
    | FStringArray vals -> SQL.VStringArray (compileArray vals)
    | FBoolArray vals -> SQL.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> SQL.VDateTimeArray (compileArray vals)
    | FDateArray vals -> SQL.VDateArray (compileArray vals)
    | FNull -> SQL.VNull

let genericCompileFieldExpr (columnFunc : 'f -> SQL.ColumnRef) (placeholderFunc : Placeholder -> int) (queryFunc : SelectExpr<'e, 'f> -> SQL.SelectExpr) : FieldExpr<'e, 'f> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FEColumn c -> SQL.VEColumn (columnFunc c)
        | FEPlaceholder name -> SQL.VEPlaceholder (placeholderFunc name)
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
    traverse

let private compileLocalFieldExpr (tableRef : SQL.TableRef option) : LocalFieldExpr -> SQL.ValueExpr =
    let makeFullName name = { table = tableRef; name = compileName name } : SQL.ColumnRef
    let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
    let voidQuery _ = failwith <| sprintf "Unexpected query in computed field expression"
    genericCompileFieldExpr makeFullName voidPlaceholder voidQuery

type private QueryCompiler (layout : Layout, arguments : ArgumentsMap) =
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
    
    let findReferenceMainField (name : FieldName) (field : ResolvedColumnField) : ResolvedFieldRef option =
        match field.fieldType with
        | FTReference (entityRef, where) ->
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef
            Some { entity = entityRef; name = entity.mainField }
        | _ -> None

    let rec compileAttribute prefix (FunQLName name) expr = SQL.SCExpr (SQL.SQLName <| sprintf "__%s__%s" prefix name, compileFieldExpr expr)

    and compileOrderLimitClause (clause : ResolvedOrderLimitClause) : SQL.OrderLimitClause =
        { orderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr expr)) clause.orderBy
          limit = Option.map compileFieldExpr clause.limit
          offset = Option.map compileFieldExpr clause.offset
        }

    and compileSelectExpr : ResolvedSelectExpr -> Domains * SQL.SelectExpr = function
        | SSelect query ->
            let (domains, expr) = compileSingleSelectExpr query
            (domains, SQL.SSelect expr)
        | SSetOp _ as select ->
            let ns = newDomainNamespaceId ()
            let domainColumn = compileDomain ns
            let mutable lastId = 0
            let rec compileDomained = function
                | SSelect query ->
                    let (domains, expr) = compileSingleSelectExpr query
                    let id = lastId
                    lastId <- lastId + 1
                    let modifiedExpr = {
                        expr with
                            columns = Array.append [| SQL.SCExpr (domainColumn, SQL.VEValue <| SQL.VInt id) |] expr.columns
                    }
                    (Map.singleton id domains, SQL.SSelect modifiedExpr)
                | SSetOp (op, a, b, limits) ->            
                    let (domainsMap1, expr1) = compileDomained a
                    let (domainsMap2, expr2) = compileDomained b
                    (Map.unionUnique domainsMap1 domainsMap2, SQL.SSetOp (compileSetOp op, expr1, expr2, compileOrderLimitClause limits))
            let (domainsMap, expr) = compileDomained select
            (DMulti (ns, domainsMap), expr)

    and compileSingleSelectExpr (select : ResolvedSingleSelectExpr) : Domains * SQL.SingleSelectExpr =
        let (domainsMap, queryClause) =
            match select.clause with
            | Some clause ->
                let (domainsMap, ret) = compileFromClause clause
                (domainsMap, Some ret)
            | None -> (Map.empty, None)
        
        let attributeColumns =
            select.attributes
                |> Map.toSeq
                |> Seq.map (fun (name, expr) -> compileAttribute "RowAttribute" name expr)

        let resultColumns = select.results |> Seq.collect compileResult

        let getDomainColumns (result : ResolvedQueryResult) =
            match resultFieldRef result.result with
            | Some { entity = Some { name = entityName }; name = fieldName } ->
                let newName = resultName result.result
                let domains = Map.find entityName domainsMap
                let tableRef : SQL.TableRef = { schema = None; name = compileName entityName }

                let getIdColumn (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | None -> Seq.empty
                    | Some info ->
                        let colName =
                            if info.idColumn = funEmpty then
                                sqlFunId
                            else
                                compileId info.idColumn
                        Seq.singleton <| SQL.SCExpr (compileId entityName, SQL.VEColumn { table = Some tableRef; name = colName })
                let rec getIdColumns = function
                | DSingle (id, domain) -> getIdColumn domain
                | DMulti (ns, nested) -> nested |> Map.values |> Seq.collect getIdColumns
                let idColumns = getIdColumns domains

                let rec getDomainColumns = function
                | DSingle (id, domain) -> Seq.empty
                | DMulti (ns, nested) ->
                    let col = Seq.singleton <| SQL.SCColumn { table = Some tableRef; name = compileDomain ns }
                    Seq.append col (nested |> Map.values |> Seq.collect getDomainColumns)
                let domainColumns = getDomainColumns domains

                let getPunColumn (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | Some info when info.field.name <> funId ->
                        let field = Option.getOrFailWith (fun () -> sprintf "Can't find field: %O" info.field) <| layout.FindField info.field.entity info.field.name
                        match field with
                        | RColumnField { fieldType = FTReference _ } ->
                            let ref : SQL.ColumnRef = { table = Some tableRef; name = compilePun fieldName }
                            Seq.singleton <| SQL.SCExpr (compilePun newName, SQL.VEColumn ref)
                        | _ -> Seq.empty
                    | _ -> Seq.empty   
                let rec getPunColumns = function
                | DSingle (id, domain) -> getPunColumn domain
                | DMulti (ns, nested) -> nested |> Map.values |> Seq.collect getPunColumns
                let punColumns = getPunColumns domains
            
                let getNewDomain (domain : Domain) =
                    match Map.tryFind fieldName domain with
                    | None -> Map.empty
                    | Some info -> Map.singleton newName { info with idColumn = entityName }
                let rec getNewDomains = function
                | DSingle (id, domain) -> DSingle (id, getNewDomain domain)
                | DMulti (ns, nested) -> DMulti (ns, nested |> Map.map (fun key domains -> getNewDomains domains))
                let newDomains = getNewDomains domains

                (Some newDomains, [ idColumns; domainColumns; punColumns ] |> Seq.concat)
            | _ -> (None, Seq.empty)
        
        let domainResults = select.results |> Seq.map getDomainColumns |> Seq.cache
        let domainColumns = domainResults |> Seq.collect snd |> Seq.distinct
        let newDomains = domainResults |> Seq.mapMaybe fst |> Seq.fold mergeDomains (DSingle (newGlobalDomainId (), Map.empty))

        let query =
            { columns = [ attributeColumns; resultColumns; domainColumns ] |> Seq.concat |> Array.ofSeq
              clause = queryClause
              orderLimit = compileOrderLimitClause select.orderLimit
            } : SQL.SingleSelectExpr
        (newDomains, query)

    and compileFieldExpr =
        let compileSubSelectExpr = snd << compileSelectExpr
        genericCompileFieldExpr compileFieldRef (fun name -> arguments.[name].placeholder) compileSubSelectExpr

    and compileResult (result : ResolvedQueryResult) : SQL.SelectedColumn seq =
        let name = resultName result.result
        let resultColumn =
            match result.result with
            | QRField field -> SQL.SCColumn (compileFieldRef field)
            | QRExpr (name, expr) -> SQL.SCExpr (compileName name, compileFieldExpr expr)
        let attrs = result.attributes |> Map.toSeq |> Seq.map (fun (attrName, expr) -> compileAttribute (sprintf "CellAttribute__%O" name) attrName expr)
        Seq.append (Seq.singleton resultColumn) attrs

    and compileFromClause (clause : ResolvedFromClause) : Map<EntityName, Domains> * SQL.FromClause =
        let (domainsMap, from) = compileFromExpr clause.from
        (domainsMap, { from = from; where = Option.map compileFieldExpr clause.where })

    and compileFromExpr : ResolvedFromExpr -> Map<EntityName, Domains> * SQL.FromExpr = function
        | FEntity entityRef ->
            let entity = Option.getOrFailWith (fun () -> sprintf "Can't find entity %O" entityRef) <| layout.FindEntity entityRef
            let tableRef = compileResolvedEntityRef entityRef
            let idColumn = SQL.SCColumn { table = Some tableRef; name = sqlFunId }

            let columnFields = entity.columnFields |> Map.toSeq |> Seq.map (fun (name, field) -> SQL.SCColumn { table = Some tableRef; name = compileName name })
            let computedFields = entity.computedFields |> Map.toSeq |> Seq.map (fun (name, field) -> SQL.SCExpr (compileName name, compileLocalFieldExpr (Some tableRef) field.expression))
            let references = Map.update findReferenceMainField entity.columnFields

            let buildReferences (from : SQL.FromExpr) (fieldName : FieldName) (referenceRef : ResolvedFieldRef) =
                let columnName = compileName fieldName
                let table = compileResolvedEntityRef referenceRef.entity
                let where = SQL.VEEq (SQL.VEColumn { table = Some tableRef; name = columnName }, SQL.VEColumn { table = Some table; name = sqlFunId }) : SQL.ValueExpr
                SQL.FJoin (SQL.Left, from, SQL.FTable table, where)
            let from = Map.fold buildReferences (SQL.FTable tableRef) references

            // Add columns with names of referenced entries
            let makeReferenceName (fieldName : FieldName, referenceRef : ResolvedFieldRef) =
                let refEntity = Option.getOrFailWith (fun () -> sprintf "Can't find entity: %O" referenceRef.entity) <| layout.FindEntity referenceRef.entity
                let expr =
                    if referenceRef.name = funId then
                        SQL.VEColumn <| compileResolvedFieldRef referenceRef
                    else
                        let refField = Option.getOrFailWith (fun () -> sprintf "Can't find field: %O" referenceRef) <| refEntity.FindField referenceRef.name
                        match refField with
                        | RColumnField field -> SQL.VEColumn <| compileResolvedFieldRef referenceRef
                        | RComputedField field -> compileLocalFieldExpr (Some (compileResolvedEntityRef referenceRef.entity)) field.expression
                SQL.SCExpr (compilePun fieldName, expr)
            let referenceNames = references |> Map.toSeq |> Seq.map makeReferenceName

            let subquery =
                SQL.SSelect
                    { columns = Seq.append (Seq.append (Seq.singleton idColumn) (Seq.append columnFields computedFields)) referenceNames |> Seq.toArray 
                      clause = Some { from = from
                                      where = None
                                    }
                      orderLimit = SQL.emptyOrderLimitClause
                    }
            let subExpr = SQL.FSubExpr (tableRef.name, subquery)

            let makeDomainEntry name col =
                { field = { entity = entityRef; name = name }
                  // Special value which means "use Id"
                  idColumn = funEmpty
                }
            let domain = entity.columnFields |> Map.map makeDomainEntry
        
            (Map.singleton entityRef.name (DSingle (newGlobalDomainId (), domain)), subExpr)
        | FJoin (jt, e1, e2, where) ->
            let (domainsMap1, r1) = compileFromExpr e1
            let (domainsMap2, r2) = compileFromExpr e2
            let ret = SQL.FJoin (compileJoin jt, r1, r2, compileFieldExpr where)
            (Map.unionUnique domainsMap1 domainsMap2, ret)
        | FSubExpr (name, q) ->
            let (domainsMap, expr) = compileSelectExpr q
            let ret = SQL.FSubExpr (compileName name, expr)
            (Map.singleton name domainsMap, ret)

    member this.CompileSelectExpr = compileSelectExpr

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
    | SQL.SCAll -> None
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
    | SQL.SSetOp (op, a, b, limits) ->
        SQL.SSetOp (op, filterExprColumns cols a, filterExprColumns cols b, limits)

let rec private flattenDomains : Domains -> FlattenedDomains = function
    | DSingle (id, dom) -> Map.singleton id dom
    | DMulti (ns, subdoms) -> subdoms |> Map.values |> Seq.fold (fun m subdoms -> Map.union m (flattenDomains subdoms)) Map.empty

let rec private ensureMainIdColumn (mainEntity : ResolvedMainEntity) : SQL.SelectExpr -> SQL.SelectExpr = function
    | SQL.SSelect query ->
        // Should be in sync with id columns generation above
        let checkColumn = function
            | SQL.SCAll -> false
            | SQL.SCColumn ref -> false
            | SQL.SCExpr (name, e) -> name = compileId mainEntity.entity.name
        if query.columns |> Seq.exists checkColumn then
            SQL.SSelect query
        else
            let fromCol : SQL.ColumnRef = { table = Some { schema = None; name = compileName mainEntity.entity.name }; name = sqlFunId }
            let res = SQL.SCExpr (compileId mainEntity.entity.name, SQL.VEColumn fromCol)
            SQL.SSelect { query with columns = Array.append [|res|] query.columns }
    | SQL.SSetOp (op, a, b, limits) ->
        failwith "Set operation or no FROM in a view with main entity"    

let compileViewExpr (layout : Layout) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    eprintfn "Compiling %O" (viewExpr.select.ToFunQLString())
    let convertArgument i (name, fieldType) =
        let info = {
            placeholder = i
            fieldType = fieldType
            valueType = compileFieldType fieldType
        }
        (name, info)

    let arguments =
        viewExpr.arguments
            |> Map.toSeq
            |> Seq.mapi convertArgument
            |> Map.ofSeq

    let compiler = QueryCompiler (layout, arguments)
    let (domains, expr) = compiler.CompileSelectExpr viewExpr.select

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

    let ensuredExpr =
        match viewExpr.mainEntity with
        | Some mainEntity -> ensureMainIdColumn mainEntity newExpr
        | None -> newExpr
    
    { attributesQuery = attrQuery
      query = ensuredExpr
      arguments = arguments
      domains = domains
      flattenedDomains = flattenDomains domains
    }