module FunWithFlags.FunDB.FunQL.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.
// ...that is, except checking references to other views.

type ViewResolveException (message : string) =
    inherit Exception(message)

[<NoEquality; NoComparison>]
type private BoundFieldInfo =
    { ref : ResolvedFieldRef
      entity : IEntityFields
      field : ResolvedField
      // Means that field is selected directly from an entity and not from a subexpression.
      immediate : bool
    }

[<NoEquality; NoComparison>]
type private QSubqueryField =
    | QRename of FieldName
    // BoundFieldInfo exists only for fields that are bound to one and only one field.
    // Any set operation which merges fields with different bound fields discards that.
    // It's used for:
    // * checking dereferences;
    // * finding bound columns for main entity.
    // Should not be used for anything else - different cells can be bound to different
    // fields.
    | QField of BoundFieldInfo option

type private QSubqueryFields = Map<FieldName, QSubqueryField>

// None as EntityName is used only for offset/limit expressions in set operations.
type private QMapping = Map<EntityName option, SchemaName option * QSubqueryFields>

type private SomeArgumentsMap = Map<ArgumentName, ParsedFieldType>

[<NoEquality; NoComparison>]
type private QSelectResults =
    { fields : QSubqueryFields
      fieldAttributeNames : (FieldName * Set<FieldName>)[]
      attributes : ResolvedAttributeMap
    }

// If a query has a main entity it satisfies two properties:
// 1. All rows can be bound to some entry of that entity;
// 2. Columns contain all required fields of that entity.
type ResolvedMainEntity =
    { entity : ResolvedEntityRef
      columnsToFields : Map<FieldName, FieldName>
      fieldsToColumns : Map<FieldName, FieldName>
    }

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>

[<NoEquality; NoComparison>]
type ResolvedViewExpr =
    { arguments : ResolvedArgumentsMap
      select : ResolvedSelectExpr
      mainEntity : ResolvedMainEntity option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let selectStr = this.select.ToFunQLString()
            if Map.isEmpty this.arguments
            then selectStr
            else
                let printArgument (name : Placeholder, arg : ResolvedArgument) =
                    match name with
                    | PGlobal _ -> None
                    | PLocal _ -> Some <| sprintf "%s %s" (name.ToFunQLString()) (arg.ToFunQLString())
                let argStr = this.arguments |> Map.toSeq |> Seq.mapMaybe printArgument |> String.concat ", "
                sprintf "(%s): %s" argStr selectStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

let private unboundSubqueryField = QField None

let private mergeSubqueryField (f1 : QSubqueryField) (f2 : QSubqueryField) =
    match (f1, f2) with
    | (QField (Some bf1), QField (Some bf2)) when bf1.ref = bf2.ref -> QField (Some bf1)
    | _ -> unboundSubqueryField

let rec private findRootField (name : FieldName) (fields : QSubqueryFields) : (FieldName * BoundFieldInfo option) option =
    match Map.tryFind name fields with
    | None -> None
    | Some (QRename newName) -> findRootField newName fields
    | Some (QField field) -> Some (name, field)

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ViewResolveException "Invalid name: %s" name

let refField : LinkedRef<ValueRef<'f>> -> LinkedRef<'f> option = function
    | { ref = VRColumn field; path = path } -> Some { ref = field; path = path }
    | _ -> None

let resultFieldRef : QueryResultExpr<'e, LinkedRef<ValueRef<'f>>> -> LinkedRef<'f> option = function
    | QRExpr (name, FERef ref) -> refField ref
    | QRExpr (name, _) -> None

// Copy of that in Layout.Resolve but with a different exception.
let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ViewResolveException "Unspecified schema in name: %O" name

let private resolveArgumentFieldType (layout : Layout) : ParsedFieldType -> ArgumentFieldType = function
    | FTType ft -> FTType ft
    | FTReference (entityRef, Some where) -> raisef ViewResolveException "Restrictions in reference arguments aren't allowed"
    | FTReference (entityRef, None) ->
        let resolvedRef = resolveEntityRef entityRef
        let refEntity =
            match layout.FindEntity(resolvedRef) with
            | None -> raisef ViewResolveException "Cannot find entity %O from reference type" resolvedRef
            | Some refEntity -> refEntity
        FTReference (resolvedRef, None)
    | FTEnum vals ->
        if Set.isEmpty vals then
            raisef ViewResolveException "Enums must not be empty"
        FTEnum vals

let resolveArgument (layout : Layout) (arg : ParsedArgument) : ResolvedArgument =
    { argType = resolveArgumentFieldType layout arg.argType
      optional = arg.optional
    }

let rec private findMainEntityRef : ResolvedFromExpr -> ResolvedEntityRef option = function
    | FEntity (pun, ent) -> Some ent
    | FJoin (ftype, a, b, where) ->
        match ftype with
        | Left -> findMainEntityRef a
        | Right -> findMainEntityRef b
        | _ -> None
    | FSubExpr (name, (SSelect { from = Some from })) -> findMainEntityRef from
    | FSubExpr _ -> None
    | FValues _ -> None

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let rec private findMainEntity (ref : ResolvedEntityRef) (fields : QSubqueryFields) : ResolvedSelectExpr -> Map<FieldName, FieldName> option = function
    | SSelect sel when not (Array.isEmpty sel.groupBy) -> None
    | SSelect { results = results; from = Some from } ->
        if findMainEntityRef from = Some ref then
            let getField (result : ResolvedQueryResult) : (FieldName * FieldName) option =
                let name = result.result.TryToName () |> Option.get
                match Map.find name fields with
                | QField (Some { ref = boundRef; field = RColumnField _ }) when boundRef.entity = ref -> Some (name, boundRef.name)
                | QRename newName -> failwith <| sprintf "Unexpected rename which should have been removed: %O -> %O" name newName
                | _ -> None
            let columnsToFields = results |> Seq.mapMaybe getField |> Map.ofSeqUnique
            Some columnsToFields
        else
            None
    | SSelect _ -> None
    | SSetOp (op, a, b, clause) ->
        findMainEntity ref fields a |> Option.bind (fun columnsA ->
            findMainEntity ref fields b |> Option.map (fun columnsB ->
                // Because we use QSubqueryFields column maps should be equal.
                assert (columnsA = columnsB)
                columnsA
            )
        )

type private ResolvedExprInfo =
    { isLocal : bool
      hasAggregates : bool
    }

type private ResolvedResultInfo =
    { subquery : QSubqueryField
      hasAggregates : bool
    }

type ResolvedSelectInfo =
    { hasAggregates : bool
    }

type ResolvedSubEntityInfo =
    { alwaysTrue : bool
    }

let rec followPath (layout : ILayoutFields) (fieldRef : ResolvedFieldRef) : FieldName list -> ResolvedFieldRef = function
    | [] -> fieldRef
    | (ref :: refs) ->
        let entity = layout.FindEntity fieldRef.entity |> Option.get
        let (_, field) = entity.FindField fieldRef.name |> Option.get
        match field with
        | RColumnField { fieldType = FTReference (entityRef, _) } ->
            let newFieldRef = { entity = entityRef; name = ref }
            followPath layout newFieldRef refs
        | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

let resolveSubEntity (layout : ILayoutFields) (ctx : SubEntityContext) (field : LinkedBoundFieldRef) (subEntityInfo : SubEntityRef) : SubEntityRef =
    let fieldRef =
        let bound =
            match field.ref with
            | VRColumn { bound = Some bound } -> bound
            | _ -> raisef ViewResolveException "Unbound field in a type assertion"
        followPath layout bound.ref (Array.toList field.path)
    let fields = layout.FindEntity fieldRef.entity |> Option.get
    match fields.FindField fieldRef.name with
    | Some (_, RSubEntity) -> ()
    | _ -> raisef ViewResolveException "Bound field in a type assertion is not a SubEntity field"
    let subEntityRef = { schema = Option.defaultValue fieldRef.entity.schema subEntityInfo.ref.schema; name = subEntityInfo.ref.name }
    let info =
        match ctx with
        | SECInheritedFrom ->
            if checkInheritance layout subEntityRef fieldRef.entity then
                { alwaysTrue = true }
            else if checkInheritance layout fieldRef.entity subEntityRef then
                { alwaysTrue = false }
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"
        | SECOfType ->
            let subEntity =
                match layout.FindEntity subEntityRef with
                | None -> raisef ViewResolveException "Couldn't find subentity %O" subEntityInfo.ref
                | Some r -> r
            if subEntity.IsAbstract then
                raisef ViewResolveException "Instances of abstract entity %O do not exist" subEntityRef
            if not <| checkInheritance layout fieldRef.entity subEntityRef then
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"
            { alwaysTrue = not <| hasSubType subEntity }
    { ref = relaxEntityRef subEntityRef; extra = info }

type private SelectFlags =
    { oneColumn : bool
      requireNames : bool
      noAttributes : bool
    }

let private viewExprSelectFlags =
    { noAttributes = false
      oneColumn = false
      requireNames = true
    }

let private subExprSelectFlags =
    { noAttributes = true
      oneColumn = true
      requireNames = false
    }

type private QueryResolver (layout : ILayoutFields, arguments : ResolvedArgumentsMap) =
    let mutable usedArguments : Set<Placeholder> = Set.empty

    // Returns innermost bound field. It has sense only for result expressions in subexpressions,
    // where it will be bound to the result name.
    let rec resolvePath (useMain : bool) (boundField : BoundFieldInfo) : FieldName list -> BoundFieldInfo = function
        | [] -> boundField
        | (ref :: refs) ->
            match boundField.field with
            | RColumnField { fieldType = FTReference (entityRef, _) } ->
                let newEntity = layout.FindEntity entityRef |> Option.get
                match newEntity.FindField ref with
                | Some (newRef, refField) ->
                    let nextBoundField =
                        { ref =
                              { entity = entityRef
                                name = newRef
                              }
                          field = refField
                          entity = newEntity
                          immediate = false
                        }
                    resolvePath useMain nextBoundField refs
                | None -> raisef ViewResolveException "Column not found in dereference: %O" ref
            | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

    // Returns:
    // * Subquery field information in case this field will be used as a result from a subexpression.
    //   It will be added to `mapping` of the resulting SELECT, hence it bounds innermost field in path;
    // * Outer bound field, if exists;
    // * Resulting reference.
    let resolveReference (useMain : bool) (mapping : QMapping) (f : LinkedFieldRef) : QSubqueryField * BoundFieldInfo option * LinkedBoundFieldRef =
        match f.ref with
        | VRColumn ref ->
            let (entityName, (schemaName, fields)) =
                match ref.entity with
                | None ->
                    if mapping.Count = 1 then
                        Map.toSeq mapping |> Seq.head
                    else
                        raisef ViewResolveException "None or more than one possible interpretation of %O in %O" f.ref f
                | Some ename ->
                    let srcName = Some ename.name
                    match Map.tryFind srcName mapping with
                    | Some ((schema, ref) as ret) ->
                        if Option.isSome ename.schema && schema <> ename.schema then
                            raisef ViewResolveException "Invalid entity schema %O in %O" ename f
                        else
                            (srcName, ret)
                    | None ->
                        raisef ViewResolveException "Field entity not found for %O in %O" ename f
            let (newName, outerBoundField) =
                match findRootField ref.name fields with
                | Some r -> r
                | None -> raisef ViewResolveException "Unknown field %O in %O" ref.name f

            let (innerBoundField, outerRef) =
                match outerBoundField with
                | Some field ->
                    // FIXME: why immediate = false here?
                    let inner = resolvePath useMain { field with immediate = false } (Array.toList f.path)
                    (Some inner, Some { ref = field.ref; immediate = field.immediate })
                | None when Array.isEmpty f.path ->
                    (None, None)
                | _ ->
                    raisef ViewResolveException "Dereference on an unbound field in %O" f

            let newRef = { entity = Option.map (fun ename -> { schema = None; name = ename }) entityName; name = newName } : FieldRef
            (QField innerBoundField, outerBoundField, { ref = VRColumn { ref = newRef; bound = outerRef }; path = f.path })
        | VRPlaceholder arg ->
            let argInfo =
                match Map.tryFind arg arguments with
                | None -> raisef ViewResolveException "Unknown argument: %O" arg
                | Some argInfo -> argInfo
            let innerBoundField =
                if Array.isEmpty f.path then
                    None
                else
                    match argInfo.argType with
                    | FTReference (entityRef, where) ->
                        let (name, remainingPath) =
                            match Array.toList f.path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "impossible"
                        let argEntity = layout.FindEntity entityRef |> Option.get
                        let argField =
                            match argEntity.FindField name with
                            | None -> raisef ViewResolveException "Field doesn't exist in %O: %O" entityRef name
                            | Some (_, argField) -> argField
                        let fieldInfo =
                            { ref = { entity = entityRef; name = name }
                              entity = argEntity
                              field = argField
                              immediate = false
                            }
                        let inner = resolvePath useMain fieldInfo remainingPath
                        Some inner
                    | _ -> raisef ViewResolveException "Argument is not a reference: %O" ref
            usedArguments <- Set.add arg usedArguments
            (QField innerBoundField, None, { ref = VRPlaceholder arg; path = f.path })

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { ref = VRPlaceholder name; path = [||] } ->
                if Map.containsKey name arguments then
                    usedArguments <- Set.add name usedArguments
                    { ref = VRPlaceholder name; path = [||] }
                else
                    raisef ViewResolveException "Undefined placeholder: %O" name
            | ref -> raisef ViewResolveException "Invalid reference in LIMIT or OFFSET: %O" ref
        let voidQuery query = raisef ViewResolveException "Forbidden subquery in LIMIT or OFFSET: %O" query
        let voidAggr aggr = raisef ViewResolveException "Forbidden aggregate function in LIMIT or OFFSET"
        let voidSubEntity field subEntity = raisef ViewResolveException "Forbidden type assertion in LIMIT or OFFSET"
        let mapper =
            { idFieldExprMapper resolveReference voidQuery with
                  aggregate = voidAggr
                  subEntity = voidSubEntity
            }
        mapFieldExpr mapper expr

    let rec resolveResult (flags : SelectFlags) (mapping : QMapping) (result : ParsedQueryResult) : ResolvedResultInfo * ResolvedQueryResult =
        if not (Map.isEmpty result.attributes) && flags.noAttributes then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        let (exprInfo, expr) = resolveResultExpr flags mapping result.result
        let ret = {
            attributes = resolveAttributes mapping result.attributes
            result = expr
        }
        (exprInfo, ret)

    // Should be in sync with resultField
    and resolveResultExpr (flags : SelectFlags) (mapping : QMapping) : ParsedQueryResultExpr -> ResolvedResultInfo * ResolvedQueryResultExpr = function
        | QRExpr (name, FERef f) ->
            Option.iter checkName name
            let (boundField, _, f') = resolveReference true mapping f
            let info =
                { subquery = boundField
                  hasAggregates = false
                }
            (info, QRExpr (name, FERef f'))
        | QRExpr (name, e) ->
            match name with
            | Some n -> checkName n
            | None when flags.requireNames -> raisef ViewResolveException "Unnamed results are allowed only inside expression queries"
            | None -> ()
            let (exprInfo, expr) = resolveFieldExpr true mapping e
            let info =
                { subquery = unboundSubqueryField
                  hasAggregates = exprInfo.hasAggregates
                }
            (info, QRExpr (name, expr))

    and resolveAttributes (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveNonaggrFieldExpr mapping expr) attributes

    and resolveNonaggrFieldExpr (mapping : QMapping) (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let (info, res) = resolveFieldExpr false mapping expr
        if info.hasAggregates then
            raisef ViewResolveException "Aggregate functions are not allowed here"
        res

    and resolveFieldExpr (useMain : bool) (mapping : QMapping) (expr : ParsedFieldExpr) : ResolvedExprInfo * ResolvedFieldExpr =
        let mutable isLocal = true
        let mutable hasAggregates = false
        let resolveExprReference col =
            let (_, outer, ret) = resolveReference useMain mapping col

            if not <| Array.isEmpty col.path then
                isLocal <- false
            match outer with
            | Some { field = RComputedField { isLocal = false } } ->
                isLocal <- false
            | _ -> ()

            ret
        let resolveQuery query =
            let (_, res) = resolveSelectExpr subExprSelectFlags query
            res
        let resolveAggr aggr =
            hasAggregates <- true
            aggr
        let mapper =
            { idFieldExprMapper resolveExprReference resolveQuery with
                  aggregate = resolveAggr
                  subEntity = resolveSubEntity layout
            }
        let ret = mapFieldExpr mapper expr
        let info =
            { isLocal = isLocal
              hasAggregates = hasAggregates
            }
        (info, ret)

    and resolveOrderLimitClause (mapping : QMapping) (limits : ParsedOrderLimitClause) : bool * ResolvedOrderLimitClause =
        let mutable isLocal = true
        let resolveOrderBy (ord, expr) =
            let (info, ret) = resolveFieldExpr false mapping expr
            if info.hasAggregates then
                raisef ViewResolveException "Aggregates are not allowed here"
            if not info.isLocal then
                isLocal <- false
            (ord, ret)
        let ret =
            { orderBy = Array.map resolveOrderBy limits.orderBy
              limit = Option.map resolveLimitFieldExpr limits.limit
              offset = Option.map resolveLimitFieldExpr limits.offset
            }
        (isLocal, ret)

    and resolveSelectExpr (flags : SelectFlags) : ParsedSelectExpr -> QSelectResults * ResolvedSelectExpr = function
        | SSelect query ->
            let (fields, res) = resolveSingleSelectExpr flags query
            let results = {
                fields = fields
                attributes = res.attributes
                fieldAttributeNames =
                    if flags.noAttributes then
                        Array.empty
                    else
                        res.results |> Array.map (fun res -> (res.result.TryToName () |> Option.get, Map.keysSet res.attributes))
            }
            (results, SSelect res)
        | SSetOp (op, a, b, limits) ->
            let (results1, a') = resolveSelectExpr flags a
            let (results2, b') = resolveSelectExpr { flags with requireNames = false } b
            if Array.length results1.fieldAttributeNames <> Array.length results2.fieldAttributeNames then
                raisef ViewResolveException "Different number of columns in a set operation expression"
            let newFields = Map.unionWith (fun name -> mergeSubqueryField) results1.fields results2.fields
            let orderLimitMapping = Map.singleton None (None, newFields)
            let (limitsAreLocal, resolvedLimits) = resolveOrderLimitClause orderLimitMapping limits
            if not limitsAreLocal then
                raisef ViewResolveException "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.orderBy
            ({ results1 with fields = newFields }, SSetOp (op, a', b', resolvedLimits))

    and resolveSingleSelectExpr (flags : SelectFlags) (query : ParsedSingleSelectExpr) : QSubqueryFields * ResolvedSingleSelectExpr =
        if flags.noAttributes && not (Map.isEmpty query.attributes) then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        if flags.oneColumn && Array.length query.results <> 1 then
            raisef ViewResolveException "Expression queries must have only one resulting column"
        let (fromMapping, qFrom) =
            match query.from with
            | None -> (Map.empty, None)
            | Some from ->
                let (mapping, res) = resolveFromExpr flags from
                (mapping, Some res)
        let qWhere = Option.map (resolveNonaggrFieldExpr fromMapping) query.where
        let qGroupBy = Array.map (resolveNonaggrFieldExpr fromMapping) query.groupBy
        let rawResults = Array.map (resolveResult flags fromMapping) query.results
        let hasAggregates = Array.exists (fun (info : ResolvedResultInfo, expr) -> info.hasAggregates) rawResults || not (Array.isEmpty qGroupBy)
        let results = Array.map snd rawResults
        let makeField (info, res) =
            let bound = if hasAggregates then unboundSubqueryField else info.subquery
            (res.result.TryToName () |> Option.get, bound)
        let newFields =
            if flags.oneColumn then
                Map.empty
            else
                try
                    rawResults |> Seq.map makeField |> Map.ofSeqUnique
                with
                    | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

        let extra =
            { hasAggregates = hasAggregates
            }

        let newQuery = {
            attributes = resolveAttributes fromMapping query.attributes
            from = qFrom
            where = qWhere
            groupBy = qGroupBy
            results = results
            orderLimit = snd <| resolveOrderLimitClause fromMapping query.orderLimit
            extra = extra :> obj
        }
        (newFields, newQuery)

    and resolveFromExpr (flags : SelectFlags) : ParsedFromExpr -> (QMapping * ResolvedFromExpr) = function
        | FEntity (pun, name) ->
            let resName = resolveEntityRef name
            let entity =
                match layout.FindEntity resName with
                | None -> raisef ViewResolveException "Entity not found: %O" name
                | Some entity -> entity

            let makeBoundField (name : FieldName) (field : ResolvedField) =
                let ref = { entity = resName; name = name } : ResolvedFieldRef
                QField <| Some { ref = ref; entity = entity; field = field; immediate = true }

            let realFields = mapAllFields makeBoundField entity
            let fields = Map.add funMain (QRename entity.MainField) realFields
            let newName = Option.defaultValue name.name pun
            (Map.singleton (Some newName) (Some resName.schema, fields), FEntity (pun, resName))
        | FJoin (jt, e1, e2, where) ->
            let (newMapping1, newE1) = resolveFromExpr flags e1
            let (newMapping2, newE2) = resolveFromExpr flags e2

            let newMapping =
                try
                    Map.unionUnique newMapping1 newMapping2
                with
                | Failure msg -> raisef ViewResolveException "Clashing entity names in a join: %s" msg

            let (info, newFieldExpr) = resolveFieldExpr false newMapping where
            if not info.isLocal then
                raisef ViewResolveException "Cannot use dereferences in join expressions: %O" where
            if info.hasAggregates then
                raisef ViewResolveException "Cannot use aggregate functions in join expression"
            (newMapping, FJoin (jt, newE1, newE2, newFieldExpr))
        | FSubExpr (name, q) ->
            checkName name
            let (info, newQ) = resolveSelectExpr { flags with requireNames = true } q
            (Map.singleton (Some name) (None, info.fields), FSubExpr (name, newQ))
        | FValues (name, fieldNames, values) ->
            checkName name

            let mapEntry entry =
                if Array.length entry <> Array.length fieldNames then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map (resolveFieldExpr true Map.empty >> snd)
            let newValues = values |> Array.map mapEntry

            let mapName name =
                checkName name
                (name, QField None)
            let fields =
                try
                    fieldNames |> Seq.map mapName |> Map.ofSeqUnique
                with
                    | Failure msg -> raisef ViewResolveException "Clashing VALUES names: %s" msg

            let mapping = Map.singleton (Some name) (None, fields)
            (mapping, FValues (name, fieldNames, newValues))

    let resolveMainEntity (fields : QSubqueryFields) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity =
        let ref = resolveEntityRef main.entity
        let entity =
            match layout.FindEntity ref with
            | None -> raisef ViewResolveException "Entity not found: %O" main.entity
            | Some e -> e :?> ResolvedEntity
        if entity.isAbstract then
            raisef ViewResolveException "Entity is abstract: %O" main.entity
        let mappedResults =
            match findMainEntity ref fields query with
            | Some fields -> fields
            | someMain -> raisef ViewResolveException "Cannot map main entity to the expression: %O, possible value: %O" ref someMain

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.defaultValue && not field.isNullable then
                if not (Map.containsKey fieldName mappedResults) then
                    raisef ViewResolveException "Required inserted entity field is not in the view expression: %O" fieldName
        entity.columnFields |> Map.iter checkField

        { entity = ref
          fieldsToColumns = Map.reverse mappedResults
          columnsToFields = mappedResults
        }

    member this.ResolveSelectExpr flags select = resolveSelectExpr flags select
    member this.ResolveMainEntity fields select main = resolveMainEntity fields select main
    member this.UsedArguments = usedArguments

let resolveSelectExpr (layout : ILayoutFields) (select : ParsedSelectExpr) : Set<Placeholder> * ResolvedSelectExpr =
    let qualifier = QueryResolver (layout, globalArgumentsMap)
    let (results, qQuery) = qualifier.ResolveSelectExpr subExprSelectFlags select
    (qualifier.UsedArguments, qQuery)

let resolveViewExpr (layout : Layout) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let arguments = viewExpr.arguments |> Map.map (fun name -> resolveArgument layout)
    let localArguments = Map.mapKeys PLocal arguments
    let allArguments = Map.union localArguments globalArgumentsMap
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr viewExprSelectFlags viewExpr.select
    let mainEntity = Option.map (qualifier.ResolveMainEntity results.fields qQuery) viewExpr.mainEntity

    { arguments = Map.union localArguments (Map.filter (fun name _ -> Set.contains name qualifier.UsedArguments) globalArgumentsMap)
      select = qQuery
      mainEntity = mainEntity
    }
