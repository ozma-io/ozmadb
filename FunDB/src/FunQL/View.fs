module FunWithFlags.FunDB.FunQL.View

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.Layout.Types

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.

exception ViewResolveException of info : string with
    override this.Message = this.info

[<NoComparison>]
type BoundField =
    { ref : ResolvedFieldRef
      field : ResolvedField
    }

[<NoComparison>]
type BoundFieldRef =
    { ref : FieldRef
      bound : BoundField option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = this.ref.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type LinkedResolvedFieldRef = LinkedRef<BoundFieldRef>

[<NoComparison>]
type private QSubqueryField =
    | QRename of FieldName
    | QField of BoundField option

type private QSubqueryFields = Map<FieldName, QSubqueryField>

type private QMapping = Map<EntityName option, SchemaName option * QSubqueryFields>

type private SomeArgumentsMap = Map<string, ParsedFieldType>
type private ArgumentsMap = Map<Placeholder, Argument>

type ResolvedFieldExpr = FieldExpr<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedSelectExpr = SelectExpr<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedSingleSelectExpr = SingleSelectExpr<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedQueryResult = QueryResult<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedQueryResultExpr = QueryResultExpr<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedFromExpr = FromExpr<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedFromClause = FromClause<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedAttributeMap = AttributeMap<ResolvedEntityRef, LinkedResolvedFieldRef>
type ResolvedOrderLimitClause = OrderLimitClause<ResolvedEntityRef, LinkedResolvedFieldRef>

[<NoComparison>]
type private QSelectResults =
    { fields : QSubqueryFields
      fieldAttributeNames : (FieldName * Set<FieldName>)[]
      attributes : ResolvedAttributeMap
    }

type ResolvedMainEntity =
    { entity : ResolvedEntityRef
      name : EntityName
      columnsToFields : Map<FieldName, FieldName>
      fieldsToColumns : Map<FieldName, FieldName>
    }

[<NoComparison>]
type ResolvedViewExpr =
    { arguments : ArgumentsMap
      select : ResolvedSelectExpr
      mainEntity : ResolvedMainEntity option
      columns : FieldName[]
    }

type private MainEntity =
    { entity : ResolvedEntityRef
      name : EntityName
    }

let private unboundSubqueryField = QField None

let rec private findRootField (name : FieldName) (fields : QSubqueryFields) =
    match Map.tryFind name fields with
    | None -> raise (ViewResolveException <| sprintf "Unknown field: %O" name)
    | Some (QRename newName) -> findRootField newName fields
    | Some (QField field) -> (name, field)

let private checkName (name : string) : unit =
    if not (goodName name) then
        raise (ViewResolveException <| sprintf "Invalid name: %s" name)

let resultFieldRef : ResolvedQueryResultExpr -> FieldRef option = function
    | QRField field -> Some field.ref.ref
    | QRExpr (name, FEColumn field) -> Some field.ref.ref
    | QRExpr (name, _) -> None

let resultName : ResolvedQueryResultExpr -> FieldName = function
    | QRField field -> field.ref.ref.name
    | QRExpr (name, _) -> name

let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match name.schema with
    | Some schema -> { schema = schema; name = name.name }
    | None -> raise (ViewResolveException <| sprintf "Unspecified schema in name: %O" name)

let rec private findMainFromEntity : ResolvedFromExpr -> MainEntity option = function
    | FEntity ent -> Some { entity = ent; name = ent.name }
    | FJoin (ftype, a, b, where) ->
        match ftype with
        | Left -> findMainFromEntity a
        | Right -> findMainFromEntity b
        | _ -> None
    | FSubExpr (name, (SSelect { clause = Some expr })) -> Option.map (fun main -> { main with name = name }) <| findMainFromEntity expr.from
    | FSubExpr _ -> None

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private findMainEntity (fields : QSubqueryFields) : ResolvedSelectExpr -> (Map<FieldName, FieldName> * MainEntity) option = function
    | SSelect { results = results; clause = Some clause } ->
        match findMainFromEntity clause.from with
        | None -> None
        | Some main ->
            let getField (result : ResolvedQueryResult) : (FieldName * FieldName) option =
                let name = resultName result.result
                match Map.find name fields with
                | QField (Some { ref = boundRef; field = RColumnField _ }) when boundRef.entity = main.entity -> Some (name, boundRef.name)
                | QRename newName -> failwith <| sprintf "Unexpected rename which should have been removed: %O -> %O" name newName
                | _ -> None
            let fields = results |> Seq.mapMaybe getField |> Map.ofSeqUnique
            Some (fields, main)
    | _ -> None

type private QueryResolver (layout : Layout, arguments : ArgumentsMap) =
    let mutable usedArguments : Set<Placeholder> = Set.empty

    let rec resolvePath (field : ResolvedColumnField) = function
        | [] -> []
        | (ref :: refs) ->
            match field.fieldType with
            | FTReference (entityRef, _) ->
                let newEntity = layout.FindEntity entityRef |> Option.get
                match newEntity.FindField ref with
                | Some (newRef, RColumnField newField) -> newRef :: resolvePath newField refs
                | Some (newRef, RComputedField _)
                | Some (newRef, RId) ->
                    if not <| List.isEmpty refs then
                        raise (ViewResolveException <| sprintf "Invalid dereference: %O" ref)
                    [newRef]
                | None -> raise (ViewResolveException <| sprintf "Column not found in dereference: %O" ref)
            | _ -> raise (ViewResolveException <| sprintf "Invalid dereference in local expression path: %O" ref)

    let resolveField (mapping : QMapping) (f : LinkedFieldRef) : QSubqueryField * LinkedResolvedFieldRef =
        let (entityName, (schemaName, fields)) =
            match f.ref.entity with
            | None ->
                if mapping.Count = 1 then
                    Map.toSeq mapping |> Seq.head
                else
                    raise (ViewResolveException <| sprintf "None or more than one possible interpretation: %O" f.ref)
            | Some ename ->
                let srcName = Some ename.name
                match Map.tryFind srcName mapping with
                | Some ((schema, ref) as ret) ->
                    if Option.isSome ename.schema && schema <> ename.schema then
                        raise (ViewResolveException <| sprintf "Invalid entity schema: %O" ename)
                    else
                        (srcName, ret)
                | None ->
                    raise (ViewResolveException <| sprintf "Field entity not found for: %O" ename)
        let (newName, boundField) = findRootField f.ref.name fields

        let newPath =
            match boundField with
            | Some { field = (RColumnField col) } -> List.toArray <| resolvePath col (Array.toList f.path)
            | _ when Array.isEmpty f.path -> [||]
            | _ -> raise (ViewResolveException <| sprintf "Invalid dereference: %O" f)

        let newRef = { entity = Option.map (fun ename -> { schema = None; name = ename }) entityName; name = newName } : FieldRef
        (QField boundField, { ref = { ref = newRef; bound = boundField }; path = newPath })

    let resolvePlaceholder (name : Placeholder) : Placeholder =
        if Map.containsKey name arguments then
            usedArguments <- Set.add name usedArguments
            name
        else
            raise (ViewResolveException <| sprintf "Undefined placeholder: %O" name)

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let foundReference column = raise (ViewResolveException <| sprintf "Reference in LIMIT or OFFSET: %O" column)
        let foundQuery query = raise (ViewResolveException <| sprintf "Subquery in LIMIT or OFFSET: %O" query)
        mapFieldExpr foundReference resolvePlaceholder foundQuery expr

    let rec resolveResult (mapping : QMapping) (result : ParsedQueryResult) : QSubqueryField * ResolvedQueryResult =
        let (boundField, expr) = resolveResultExpr mapping result.result
        let ret = {
            attributes = resolveAttributes mapping result.attributes
            result = expr
        }
        (boundField, ret)

    and resolveResultExpr (mapping : QMapping) : ParsedQueryResultExpr -> QSubqueryField * ResolvedQueryResultExpr = function
        | QRField f ->
            let (boundField, f') = resolveField mapping f
            (boundField, QRField f')
        | QRExpr (name, e) ->
            checkName (name.ToString())
            let (_, e') = resolveFieldExpr mapping e
            let boundField =
                match e' with
                | FEColumn col ->
                    let (_, fields) = Map.find (Option.map (fun (ename : EntityRef) -> ename.name) col.ref.ref.entity) mapping
                    Map.find col.ref.ref.name fields
                | _ -> unboundSubqueryField
            (boundField, QRExpr (name, e'))

    and resolveAttributes (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> snd <| resolveFieldExpr mapping expr) attributes

    and resolveFieldExpr (mapping : QMapping) (expr : ParsedFieldExpr) : bool * ResolvedFieldExpr =
        let mutable isLocal = true
        let resolveColumn col =
            let (_, ret) = resolveField mapping col
            if not <| Array.isEmpty col.path then
                isLocal <- false
            ret
        let resolveQuery query =
            let (_, res) = resolveSubSelectExpr query
            res
        let ret = mapFieldExpr resolveColumn resolvePlaceholder resolveQuery expr
        (isLocal, ret)

    and resolveOrderLimitClause (mapping : QMapping) (limits : ParsedOrderLimitClause) : bool * ResolvedOrderLimitClause =
        let mutable isLocal = true
        let resolveOrderBy (ord, expr) =
            let (thisIsLocal, ret) = resolveFieldExpr mapping expr
            if not thisIsLocal then
                isLocal <- false
            (ord, ret)
        let ret =
            { orderBy = Array.map resolveOrderBy limits.orderBy
              limit = Option.map resolveLimitFieldExpr limits.limit
              offset = Option.map resolveLimitFieldExpr limits.offset
            }
        (isLocal, ret)

    and resolveSelectExpr : ParsedSelectExpr -> QSelectResults * ResolvedSelectExpr = function
        | SSelect query ->
            let (fields, res) = resolveSingleSelectExpr query
            let results = {
                fields = fields
                attributes = res.attributes
                fieldAttributeNames = res.results |> Array.map (fun res -> (resultName res.result, Map.keysSet res.attributes))
            }
            (results, SSelect res)
        | SSetOp (op, a, b, limits) ->
            let (results1, a') = resolveSelectExpr a
            let (results2, b') = resolveSelectExpr b
            if Array.length results1.fieldAttributeNames <> Array.length results2.fieldAttributeNames then
                raise (ViewResolveException "Different number of columns in a set operation expression")
            for ((name1, attrs1), (name2, attrs2)) in Seq.zip results1.fieldAttributeNames results2.fieldAttributeNames do
                if name1 <> name2 then
                    raise (ViewResolveException <| sprintf "Different column names in a set operation: %O and %O" name1 name2)
                if attrs1 <> attrs2 then
                    raise (ViewResolveException <| sprintf "Different attributes for column %O in a set operation expression" name1)
            let newFields = Map.unionWith (fun name f1 f2 -> unboundSubqueryField) results1.fields results2.fields
            let orderLimitMapping = Map.singleton None (None, newFields)
            let (limitsAreLocal, resolvedLimits) = resolveOrderLimitClause orderLimitMapping limits
            if not limitsAreLocal then
                raise (ViewResolveException <| sprintf "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.orderBy)
            ({ results1 with fields = newFields }, SSetOp (op, a', b', resolvedLimits))

    and resolveSubSelectExpr (query : ParsedSelectExpr) : QSubqueryFields * ResolvedSelectExpr =
        let (results, res) = resolveSelectExpr query
        if not <| Map.isEmpty results.attributes then
            raise (ViewResolveException "Subqueries cannot have attributes")
        for (name, attrs) in results.fieldAttributeNames do
            if not <| Set.isEmpty attrs then
                raise (ViewResolveException <| sprintf "Subquery field %O cannot have attributes" name)
        (results.fields, res)

    and resolveSingleSelectExpr (query : ParsedSingleSelectExpr) : QSubqueryFields * ResolvedSingleSelectExpr =
        let (fromMapping, qClause) =
            match query.clause with
            | None -> (Map.empty, None)
            | Some clause ->
                let (mapping, res) = resolveFromClause clause
                (mapping, Some res)
        let rawResults = Array.map (resolveResult fromMapping) query.results
        let results = Array.map snd rawResults
        let newFields =
            try
                rawResults |> Seq.map (fun (boundField, res) -> (resultName res.result, boundField)) |> Map.ofSeqUnique
            with
                | Failure msg -> raise (ViewResolveException <| sprintf "Clashing result names: %s" msg)

        let newQuery = {
            attributes = resolveAttributes fromMapping query.attributes
            clause = qClause
            results = results
            orderLimit = snd <| resolveOrderLimitClause fromMapping query.orderLimit
        }
        (newFields, newQuery)

    and resolveFromClause (clause : ParsedFromClause) : QMapping * ResolvedFromClause =
        let (newMapping, qFrom) = resolveFromExpr clause.from
        let res = { from = qFrom
                    where = Option.map (snd << resolveFieldExpr newMapping) clause.where
                  }
        (newMapping, res)

    and resolveFromExpr : ParsedFromExpr -> (QMapping * ResolvedFromExpr) = function
        | FEntity name ->
            let resName = resolveEntityRef name
            match layout.FindEntity resName with
            | None -> raise (ViewResolveException <| sprintf "Entity not found: %O" name)
            | Some entity ->
                let makeBoundField (name : FieldName) (field : ResolvedField) =
                    let ref = { entity = resName; name = name } : ResolvedFieldRef
                    QField <| Some { ref = ref; field = field }

                let realFields = mapAllFields makeBoundField entity
                let fields = Map.add funMain (QRename entity.mainField) realFields
                (Map.singleton (Some name.name) (Some resName.schema, fields), FEntity resName)
        | FJoin (jt, e1, e2, where) ->
            let (newMapping1, newE1) = resolveFromExpr e1
            let (newMapping2, newE2) = resolveFromExpr e2

            let newMapping =
                try
                    Map.unionUnique newMapping1 newMapping2
                with
                | Failure msg -> raise (ViewResolveException <| sprintf "Clashing entity names in a join: %s" msg)

            let (isLocal, newFieldExpr) = resolveFieldExpr newMapping where
            if not isLocal then
                raise (ViewResolveException <| sprintf "Cannot use dereferences in join expressions: %O" where)
            (newMapping, FJoin (jt, newE1, newE2, newFieldExpr))
        | FSubExpr (name, q) ->
            checkName (name.ToString())
            let (fields, newQ) = resolveSubSelectExpr q
            (Map.singleton (Some name) (None, fields), FSubExpr (name, newQ))

    let resolveMainEntity (fields : QSubqueryFields) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity =
        let ref = resolveEntityRef main.entity
        let entity =
            match layout.FindEntity ref with
            | None -> raise (ViewResolveException <| sprintf "Entity not found: %O" main.entity)
            | Some e -> e
        let (mappedResults, mainEntity) =
            match findMainEntity fields query with
            | Some (fields, mainEntity) when mainEntity.entity = ref -> (fields, mainEntity)
            | someMain -> raise (ViewResolveException <| sprintf "Cannot map main entity to the expression: %O, possible value: %O" ref someMain)

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.defaultValue && not field.isNullable then
                if not (Map.containsKey fieldName mappedResults) then
                    raise (ViewResolveException <| sprintf "Required inserted entity field is not in the view expression: %O" fieldName)
        entity.columnFields |> Map.iter checkField

        { entity = ref
          name = mainEntity.name
          fieldsToColumns = Map.reverse mappedResults
          columnsToFields = mappedResults
        }

    member this.ResolveSelectExpr = resolveSelectExpr
    member this.ResolveMainEntity = resolveMainEntity
    member this.UsedArguments = usedArguments

let rec private getColumns : ResolvedSelectExpr -> FieldName[] = function
    | SSelect query ->
        query.results |> Array.map (fun r -> resultName r.result)
    | SSetOp (op, a, b, limits) ->
        // Columns should be the same
        getColumns a

let resolveViewExpr (layout : Layout) (globalArguments : SomeArgumentsMap) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let checkArgument name arg =
        checkName name
        match arg.argType with
        | FTReference (entityRef, where) ->
            if Option.isNone <| layout.FindEntity(resolveEntityRef entityRef) then
                raise (ViewResolveException <| sprintf "Unknown entity in reference: %O" entityRef)
        | FTEnum vals ->
            if Set.isEmpty vals then
                raise (ViewResolveException "Enums must not be empty")
        | _ -> ()
    Map.iter checkArgument viewExpr.arguments

    let localArguments = Map.mapKeys PLocal viewExpr.arguments
    let globalArguments = globalArguments |> Map.toSeq |> Seq.map (fun (name, argType) -> (PGlobal name, { argType = argType; optional = false })) |> Map.ofSeq
    let allArguments = Map.union localArguments globalArguments
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr viewExpr.select
    let mainEntity = Option.map (qualifier.ResolveMainEntity results.fields qQuery) viewExpr.mainEntity

    { arguments = Map.union localArguments (Map.filter (fun name _ -> Set.contains name qualifier.UsedArguments) globalArguments)
      select = qQuery
      mainEntity = mainEntity
      columns = getColumns qQuery
    }