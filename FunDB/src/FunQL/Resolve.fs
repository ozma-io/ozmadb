module FunWithFlags.FunDB.FunQL.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.
// ...that is, except checking references to other views.

type ViewResolveException (message : string) =
    inherit Exception(message)

[<NoComparison>]
type BoundField =
    { ref : ResolvedFieldRef
      immediate : bool // Set if field references value from a table directly, not via a subexpression.
    }

[<NoComparison>]
type BoundRef<'f> when 'f :> IFunQLString =
    { ref : 'f
      bound : BoundField option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = this.ref.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type LinkedBoundFieldRef = LinkedRef<BoundRef<FieldRef>>

[<NoComparison>]
type private BoundFieldInfo =
    { ref : ResolvedFieldRef
      entity : ResolvedEntity
      field : ResolvedField
      immediate : bool
    }

[<NoComparison>]
type private QSubqueryField =
    | QRename of FieldName
    | QField of BoundFieldInfo option

type private QSubqueryFields = Map<FieldName, QSubqueryField>

type private QMapping = Map<EntityName option, SchemaName option * QSubqueryFields>

type private SomeArgumentsMap = Map<ArgumentName, ParsedFieldType>

type ResolvedFieldExpr = FieldExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedSelectExpr = SelectExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedSingleSelectExpr = SingleSelectExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedQueryResult = QueryResult<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedQueryResultExpr = QueryResultExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedFromExpr = FromExpr<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedFromClause = FromClause<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedAttributeMap = AttributeMap<ResolvedEntityRef, LinkedBoundFieldRef>
type ResolvedOrderLimitClause = OrderLimitClause<ResolvedEntityRef, LinkedBoundFieldRef>

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

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>

[<NoComparison>]
type ResolvedViewExpr =
    { arguments : ResolvedArgumentsMap
      select : ResolvedSelectExpr
      mainEntity : ResolvedMainEntity option
      usedSchemas : UsedSchemas
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

type private MainEntities = Map<ResolvedEntityRef, EntityName>

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
    if not (goodName name) then
        raisef ViewResolveException "Invalid name: %s" name

let resultFieldRef : ResolvedQueryResultExpr -> LinkedBoundFieldRef option = function
    | QRField field -> Some field
    | QRExpr (name, FEColumn field) -> Some field
    | QRExpr (name, _) -> None

let resultName : ResolvedQueryResultExpr -> FieldName = function
    | QRField field when not (Array.isEmpty field.path) -> Array.last field.path
    | QRField field -> field.ref.ref.name
    | QRExpr (name, _) -> name

// Copy of that in Layout.Resolve but with a different exception.
let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match name.schema with
    | Some schema -> { schema = schema; name = name.name }
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

let rec private findMainsFromEntity : ResolvedFromExpr -> MainEntities = function
    | FEntity (pun, ent) -> Map.singleton ent <| Option.defaultValue ent.name pun
    | FJoin (ftype, a, b, where) ->
        match ftype with
        | Left -> findMainsFromEntity a
        | Right -> findMainsFromEntity b
        | Inner ->
            let mains1 = findMainsFromEntity a
            let mains2 = findMainsFromEntity b
            if Map.isEmpty <| Map.intersect mains1 mains2 then
                Map.union mains1 mains2
            else
                Map.empty
        | _ -> Map.empty
    | FSubExpr (name, (SSelect { clause = Some expr })) ->
        let mains = findMainsFromEntity expr.from
        if Map.count mains = 1 then
            let (ref, innerName) = mains |> Map.toSeq |> Seq.exactlyOne
            Map.singleton ref name
        else
            Map.empty
    | FSubExpr _ -> Map.empty
    | FValues _ -> Map.empty

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private findMainEntity (ref : ResolvedEntityRef) (fields : QSubqueryFields) : ResolvedSelectExpr -> (Map<FieldName, FieldName> * EntityName) option = function
    | SSelect { results = results; clause = Some clause } ->
        match Map.tryFind ref <| findMainsFromEntity clause.from with
        | None -> None
        | Some mainName ->
            let getField (result : ResolvedQueryResult) : (FieldName * FieldName) option =
                let name = resultName result.result
                match Map.find name fields with
                | QField (Some { ref = boundRef; field = RColumnField _ }) when boundRef.entity = ref -> Some (name, boundRef.name)
                | QRename newName -> failwith <| sprintf "Unexpected rename which should have been removed: %O -> %O" name newName
                | _ -> None
            let fields = results |> Seq.mapMaybe getField |> Map.ofSeqUnique
            Some (fields, mainName)
    | _ -> None

type private QueryResolver (layout : Layout, arguments : ResolvedArgumentsMap) =
    let mutable usedArguments : Set<Placeholder> = Set.empty
    let mutable usedSchemas : UsedSchemas = Map.empty

    let rec addUsedFields (ref : ResolvedFieldRef) (field : ResolvedField) : unit =
        match field with
        | RId ->
            usedSchemas <- addUsedEntityRef ref.entity usedSchemas
        | RColumnField col ->
            usedSchemas <- addUsedFieldRef ref usedSchemas
            match col.fieldType with
            | FTReference (entityRef, _) ->
                let (realName, newField) = layout.FindField entityRef funMain |> Option.get
                let newRef = { entity = entityRef; name = realName }
                addUsedFields newRef newField
            | _ -> ()
        | RComputedField comp ->
            usedSchemas <- mergeUsedSchemas comp.usedSchemas usedSchemas

    // Returns innermost bound field. It has sense only for result expressions in subexpressions,
    // where it will be bound to the result name.
    let rec resolvePath (boundField : BoundFieldInfo) : FieldName list -> BoundFieldInfo = function
        | [] ->
            addUsedFields boundField.ref boundField.field
            boundField
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
                    usedSchemas <- addUsedField entityRef.schema entityRef.name newRef usedSchemas
                    resolvePath nextBoundField refs
                | None -> raisef ViewResolveException "Column not found in dereference: %O" ref
            | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

    // Returns:
    // * Subquery field information in case this field will be used as a result from a subexpression.
    //   It will be added to `mapping` of the resulting SELECT, hence it bounds innermost field in path;
    // * Outer bound field, if exists;
    // * Resulting reference.
    let resolveField (mapping : QMapping) (f : LinkedFieldRef) : QSubqueryField * BoundFieldInfo option * LinkedBoundFieldRef =
        let (entityName, (schemaName, fields)) =
            match f.ref.entity with
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
            match findRootField f.ref.name fields with
            | Some r -> r
            | None -> raisef ViewResolveException "Unknown field %O in %O" f.ref.name f

        let (innerBoundField, outerRef) =
            match outerBoundField with
            | Some field ->
                let inner = resolvePath { field with immediate = false } (Array.toList f.path)
                (Some inner, Some { ref = field.ref; immediate = field.immediate })
            | None when Array.isEmpty f.path ->
                (None, None)
            | _ ->
                raisef ViewResolveException "Dereference on an unbound field in %O" f

        let newRef = { entity = Option.map (fun ename -> { schema = None; name = ename }) entityName; name = newName } : FieldRef
        (QField innerBoundField, outerBoundField, { ref = { ref = newRef; bound = outerRef }; path = f.path })

    let resolvePlaceholder (name : Placeholder) : Placeholder =
        if Map.containsKey name arguments then
            usedArguments <- Set.add name usedArguments
            name
        else
            raisef ViewResolveException "Undefined placeholder: %O" name

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let foundReference column = raisef ViewResolveException "Reference in LIMIT or OFFSET: %O" column
        let foundQuery query = raisef ViewResolveException "Subquery in LIMIT or OFFSET: %O" query
        mapFieldExpr id foundReference resolvePlaceholder foundQuery expr

    let rec resolveResult (mapping : QMapping) (result : ParsedQueryResult) : QSubqueryField * ResolvedQueryResult =
        let (boundField, expr) = resolveResultExpr mapping result.result
        let ret = {
            attributes = resolveAttributes mapping result.attributes
            result = expr
        }
        (boundField, ret)

    and resolveResultExpr (mapping : QMapping) : ParsedQueryResultExpr -> QSubqueryField * ResolvedQueryResultExpr = function
        | QRField f ->
            let (boundField, _, f') = resolveField mapping f
            (boundField, QRField f')
        | QRExpr (name, FEColumn f) ->
            let (boundField, _, f') = resolveField mapping f
            (boundField, QRExpr (name, FEColumn f'))
        | QRExpr (name, e) ->
            checkName name
            let (_, e') = resolveFieldExpr mapping e
            (unboundSubqueryField, QRExpr (name, e'))

    and resolveAttributes (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> snd <| resolveFieldExpr mapping expr) attributes

    and resolveFieldExpr (mapping : QMapping) (expr : ParsedFieldExpr) : bool * ResolvedFieldExpr =
        let mutable isLocal = true
        let resolveColumn col =
            let (_, outer, ret) = resolveField mapping col

            if not <| Array.isEmpty col.path then
                isLocal <- false
            match outer with
            | Some { field = RComputedField { isLocal = false } } ->
                isLocal <- false
            | _ -> ()

            ret
        let resolveQuery query =
            let (_, res) = resolveSubSelectExpr query
            res
        let ret = mapFieldExpr id resolveColumn resolvePlaceholder resolveQuery expr
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
                raisef ViewResolveException "Different number of columns in a set operation expression"
            for ((name1, attrs1), (name2, attrs2)) in Seq.zip results1.fieldAttributeNames results2.fieldAttributeNames do
                if name1 <> name2 then
                    raisef ViewResolveException "Different column names in a set operation: %O and %O" name1 name2
                if attrs1 <> attrs2 then
                    raisef ViewResolveException "Different attributes for column %O in a set operation expression" name1
            let newFields = Map.unionWith (fun name -> mergeSubqueryField) results1.fields results2.fields
            let orderLimitMapping = Map.singleton None (None, newFields)
            let (limitsAreLocal, resolvedLimits) = resolveOrderLimitClause orderLimitMapping limits
            if not limitsAreLocal then
                raisef ViewResolveException "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.orderBy
            ({ results1 with fields = newFields }, SSetOp (op, a', b', resolvedLimits))

    and resolveSubSelectExpr (query : ParsedSelectExpr) : QSubqueryFields * ResolvedSelectExpr =
        let (results, res) = resolveSelectExpr query
        if not <| Map.isEmpty results.attributes then
            raisef ViewResolveException "Subqueries cannot have attributes"
        for (name, attrs) in results.fieldAttributeNames do
            if not <| Set.isEmpty attrs then
                raisef ViewResolveException "Subquery field %O cannot have attributes" name
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
                | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

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
        | FEntity (pun, name) ->
            let resName = resolveEntityRef name
            match layout.FindEntity resName with
            | None -> raisef ViewResolveException "Entity not found: %O" name
            | Some entity ->
                let makeBoundField (name : FieldName) (field : ResolvedField) =
                    let ref = { entity = resName; name = name } : ResolvedFieldRef
                    QField <| Some { ref = ref; entity = entity; field = field; immediate = true }

                let realFields = mapAllFields makeBoundField entity
                let fields = Map.add funMain (QRename entity.mainField) realFields
                (Map.singleton (Some <| Option.defaultValue name.name pun) (Some resName.schema, fields), FEntity (pun, resName))
        | FJoin (jt, e1, e2, where) ->
            let (newMapping1, newE1) = resolveFromExpr e1
            let (newMapping2, newE2) = resolveFromExpr e2

            let newMapping =
                try
                    Map.unionUnique newMapping1 newMapping2
                with
                | Failure msg -> raisef ViewResolveException "Clashing entity names in a join: %s" msg

            let (isLocal, newFieldExpr) = resolveFieldExpr newMapping where
            if not isLocal then
                raisef ViewResolveException "Cannot use dereferences in join expressions: %O" where
            (newMapping, FJoin (jt, newE1, newE2, newFieldExpr))
        | FSubExpr (name, q) ->
            checkName name
            let (fields, newQ) = resolveSubSelectExpr q
            (Map.singleton (Some name) (None, fields), FSubExpr (name, newQ))
        | FValues (name, fieldNames, values) ->
            checkName name

            let mapEntry entry =
                if Array.length entry <> Array.length fieldNames then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map (resolveFieldExpr Map.empty >> snd)
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
            | Some e -> e
        let (mappedResults, mainName) =
            match findMainEntity ref fields query with
            | Some (fields, mainName) -> (fields, mainName)
            | someMain -> raisef ViewResolveException "Cannot map main entity to the expression: %O, possible value: %O" ref someMain

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.defaultValue && not field.isNullable then
                if not (Map.containsKey fieldName mappedResults) then
                    raisef ViewResolveException "Required inserted entity field is not in the view expression: %O" fieldName
        entity.columnFields |> Map.iter checkField

        { entity = ref
          name = mainName
          fieldsToColumns = Map.reverse mappedResults
          columnsToFields = mappedResults
        }

    member this.ResolveSelectExpr = resolveSelectExpr
    member this.ResolveMainEntity = resolveMainEntity
    member this.UsedArguments = usedArguments
    member this.UsedSchemas = usedSchemas

let resolveViewExpr (layout : Layout) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let arguments = viewExpr.arguments |> Map.map (fun name -> resolveArgument layout)
    let localArguments = Map.mapKeys PLocal arguments
    let allArguments = Map.union localArguments globalArgumentsMap
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr viewExpr.select
    let mainEntity = Option.map (qualifier.ResolveMainEntity results.fields qQuery) viewExpr.mainEntity

    { arguments = Map.union localArguments (Map.filter (fun name _ -> Set.contains name qualifier.UsedArguments) globalArgumentsMap)
      select = qQuery
      mainEntity = mainEntity
      usedSchemas = qualifier.UsedSchemas
    }
