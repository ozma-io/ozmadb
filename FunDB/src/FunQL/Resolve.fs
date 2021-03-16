module FunWithFlags.FunDB.FunQL.Resolve

open FunWithFlags.FunUtils
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
    { Ref : ResolvedFieldRef
      // Same as in `FieldInfo`
      ForceRename : bool
      Entity : IEntityFields
      Field : ResolvedField
      // Means that field is selected directly from an entity and not from a subexpression.
      Immediate : bool
    }

// BoundFieldInfo exists only for fields that are bound to one and only one field.
// Any set operation which merges fields with different bound fields discards that.
// It's used for:
// * checking dereferences;
// * finding bound columns for main entity.
// Should not be used for anything else - different cells can be bound to different
// fields.
type private QSubqueryFieldsMap = Map<FieldName, BoundFieldInfo option>
type private QSubqueryFields = (FieldName option * BoundFieldInfo option)[]

// None as EntityName is used only for offset/limit expressions in set operations.
type private QMapping = Map<EntityName option, SchemaName option * QSubqueryFieldsMap>
type private CTEMapping = Map<EntityName, QSubqueryFieldsMap>

type private SomeArgumentsMap = Map<ArgumentName, ParsedFieldType>

type private RecursiveValue<'a> =
    | RValue of 'a
    | RRecursive of EntityName

let private mapRecursiveValue (f : 'a -> 'b) = function
    | RValue a -> RValue (f a)
    | RRecursive r -> RRecursive r

let private renameFields (names : FieldName[]) (fields : QSubqueryFields) : QSubqueryFields =
    if Array.length names <> Array.length fields then
        raisef ViewResolveException "Inconsistent number of columns"
    Array.map2 (fun newName (oldName, info) -> (Some newName, info)) names fields

let private getFieldsMap (fields : QSubqueryFields) : QSubqueryFieldsMap =
    fields |> Seq.mapMaybe (fun (mname, info) -> Option.map (fun name -> (name, info)) mname) |> Map.ofSeq

// If a query has a main entity it satisfies two properties:
// 1. All rows can be bound to some entry of that entity;
// 2. Columns contain all required fields of that entity.
type ResolvedMainEntity =
    { Entity : ResolvedEntityRef
      ColumnsToFields : Map<FieldName, FieldName>
      FieldsToColumns : Map<FieldName, FieldName>
    }

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>

[<NoEquality; NoComparison>]
type ResolvedViewExpr =
    { Arguments : ResolvedArgumentsMap
      Select : ResolvedSelectExpr
      MainEntity : ResolvedMainEntity option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let selectStr = this.Select.ToFunQLString()
            if Map.isEmpty this.Arguments
            then selectStr
            else
                let printArgument (name : Placeholder, arg : ResolvedArgument) =
                    match name with
                    | PGlobal _ -> None
                    | PLocal _ -> Some <| sprintf "%s %s" (name.ToFunQLString()) (arg.ToFunQLString())
                let argStr = this.Arguments |> Map.toSeq |> Seq.mapMaybe printArgument |> String.concat ", "
                sprintf "(%s): %s" argStr selectStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

let private unionBoundFieldInfo (f1 : BoundFieldInfo option) (f2 : BoundFieldInfo option) : BoundFieldInfo option =
    match (f1, f2) with
    | (Some bf1, Some bf2) when bf1.Ref = bf2.Ref -> Some bf1
    | _ -> None

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ViewResolveException "Invalid name: %s" name

let refField : LinkedRef<ValueRef<'f>> -> LinkedRef<'f> option = function
    | { Ref = VRColumn field; Path = path } -> Some { Ref = field; Path = path }
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
    { ArgType = resolveArgumentFieldType layout arg.ArgType
      Optional = arg.Optional
    }

type private ResolvedExprInfo =
    { IsLocal : bool
      HasAggregates : bool
    }

type private ResolvedResultInfo =
    { InnerField : BoundFieldInfo option
      HasAggregates : bool
    }

type ResolvedSingleSelectInfo =
    { HasAggregates : bool
    }

type ResolvedSubEntityInfo =
    { AlwaysTrue : bool
    }

type ResolvedCommonTableExprsMap = Map<EntityName, ResolvedCommonTableExpr>

type private ResolvedCommonTableExprTempInfo =
    { mutable MainEntity : bool
    }

type private ResolvedCommonTableExprsTempInfo =
    { Bindings : ResolvedCommonTableExprsMap
    }

type ResolvedCommonTableExprInfo =
    { MainEntity : bool
    }

let private cteBindings (select : ResolvedSelectExpr) : ResolvedCommonTableExprsMap =
    match select.CTEs with
    | Some ctes ->
        (ctes.Extra :?> ResolvedCommonTableExprsTempInfo).Bindings
    | None ->
        Map.empty

let rec private findMainEntityFromExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedFromExpr -> RecursiveValue<ResolvedEntityRef> = function
    | FEntity (pun, { schema = Some schemaName; name = name }) -> RValue { schema = schemaName; name = name }
    | FEntity (pun, { schema = None; name = name }) ->
        let cte = Map.find name ctes
        let extra = cte.Extra :?> ResolvedCommonTableExprTempInfo
        if extra.MainEntity then
            RRecursive <| Option.get currentCte
        else
            extra.MainEntity <- true
            mapRecursiveValue fst <| findMainEntityExpr ctes (Some name) cte.Expr
    | FJoin join ->
        match join.Type with
        | Left -> findMainEntityFromExpr ctes currentCte join.A
        | Right -> findMainEntityFromExpr ctes currentCte join.B
        | _ -> raisef ViewResolveException "Unsupported JOIN type when validating main entity"
    | FSubExpr (name, subExpr) ->
        mapRecursiveValue fst <| findMainEntityExpr ctes currentCte subExpr

and private findMainEntityTreeExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedSelectTreeExpr -> RecursiveValue<ResolvedEntityRef * ResolvedQueryResult[] > = function
    | SSelect sel when not (Array.isEmpty sel.GroupBy) -> raisef ViewResolveException "Queries with GROUP BY cannot use FOR INSERT INTO"
    | SSelect { Results = results; From = Some from } ->
        mapRecursiveValue (fun ref -> (ref, results)) <| findMainEntityFromExpr ctes currentCte from
    | SSelect _ -> raisef ViewResolveException "FROM clause is not found when validating main entity"
    | SValues _ -> raisef ViewResolveException "VALUES clause found when validating main entity"
    | SSetOp setOp ->
        let ret1 = findMainEntityExpr ctes currentCte setOp.A
        let ret2 = findMainEntityExpr ctes currentCte setOp.B
        match (ret1, ret2) with
        | (RValue ((refA, resultsA) as ret), RValue (refB, resultsB)) ->
            if refA <> refB then
                raisef ViewResolveException "Conflicting main entities found in set operation"
            RValue ret
        | (RRecursive cte, RValue ret)
        | (RValue ret, RRecursive cte) ->
            assert (cte = Option.get currentCte)
            RValue ret
        | (RRecursive r1, RRecursive r2) ->
            assert (r1 = r2)
            RRecursive r1

and private findMainEntityExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) (select : ResolvedSelectExpr) : RecursiveValue<ResolvedEntityRef * ResolvedQueryResult[]> =
    let ctes = Map.union ctes (cteBindings select)
    findMainEntityTreeExpr ctes currentCte select.Tree

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private findMainEntity (select : ResolvedSelectExpr) : (ResolvedEntityRef * ResolvedQueryResult[]) =
    let getValue = function
    | RValue (ref, results) -> (ref, results)
    | RRecursive r -> failwithf "Impossible recursion detected in %O" r
    getValue <| findMainEntityExpr Map.empty None select

type private ResolveCTE = EntityName -> QSubqueryFieldsMap

let private failCte (name : EntityName) =
    raisef ViewResolveException "Table %O not found" name

let rec followPath (layout : ILayoutFields) (fieldRef : ResolvedFieldRef) : FieldName list -> ResolvedFieldRef = function
    | [] -> fieldRef
    | (ref :: refs) ->
        let entity = layout.FindEntity fieldRef.entity |> Option.get
        let fieldInfo = entity.FindField fieldRef.name |> Option.get
        match fieldInfo.Field with
        | RColumnField { fieldType = FTReference (entityRef, _) } ->
            let newFieldRef = { entity = entityRef; name = ref }
            followPath layout newFieldRef refs
        | _ -> failwith <| sprintf "Invalid dereference in path: %O" ref

let resolveSubEntity (layout : ILayoutFields) (ctx : SubEntityContext) (field : LinkedBoundFieldRef) (subEntityInfo : SubEntityRef) : SubEntityRef =
    let fieldRef =
        let bound =
            match field.Ref with
            | VRColumn { Bound = Some bound } -> bound
            | _ -> raisef ViewResolveException "Unbound field in a type assertion"
        followPath layout bound.Ref (Array.toList field.Path)
    let fields = layout.FindEntity fieldRef.entity |> Option.get
    match fields.FindField fieldRef.name with
    | Some { Field = RSubEntity } -> ()
    | _ -> raisef ViewResolveException "Bound field in a type assertion is not a SubEntity field"
    let subEntityRef = { schema = Option.defaultValue fieldRef.entity.schema subEntityInfo.Ref.schema; name = subEntityInfo.Ref.name }
    let subEntity =
        match layout.FindEntity subEntityRef with
        | None -> raisef ViewResolveException "Couldn't find subentity %O" subEntityInfo.Ref
        | Some r -> r
    let info =
        match ctx with
        | SECInheritedFrom ->
            if checkInheritance layout subEntityRef fieldRef.entity then
                { AlwaysTrue = true }
            else if checkInheritance layout fieldRef.entity subEntityRef then
                { AlwaysTrue = false }
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"
        | SECOfType ->
            if subEntity.IsAbstract then
                raisef ViewResolveException "Instances of abstract entity %O do not exist" subEntityRef
            if subEntityRef = fieldRef.entity then
                { AlwaysTrue = true }
            else if checkInheritance layout fieldRef.entity subEntityRef then
                { AlwaysTrue = false }
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"

    { Ref = relaxEntityRef subEntityRef; Extra = info }

type private SelectFlags =
    { OneColumn : bool
      RequireNames : bool
      NoAttributes : bool
    }

type private ReferenceInfo =
    { // `InnerField` may be there and `OuterField` absent in case of placeholders with paths.
      InnerField : BoundFieldInfo option
      OuterField : BoundFieldInfo option
      Ref : LinkedBoundFieldRef
    }

let private viewExprSelectFlags =
    { NoAttributes = false
      OneColumn = false
      RequireNames = true
    }

let private subExprSelectFlags =
    { NoAttributes = true
      OneColumn = true
      RequireNames = false
    }

type private QueryResolver (layout : ILayoutFields, arguments : ResolvedArgumentsMap) =
    let mutable usedArguments : Set<Placeholder> = Set.empty

    // Returns innermost bound field. It has sense only for result expressions in subexpressions,
    // where it will be bound to the result name.
    let rec resolvePath (useMain : bool) (boundField : BoundFieldInfo) : FieldName list -> BoundFieldInfo = function
        | [] -> boundField
        | (ref :: refs) ->
            match boundField.Field with
            | RColumnField { fieldType = FTReference (entityRef, _) } ->
                let newEntity = layout.FindEntity entityRef |> Option.get
                match newEntity.FindField ref with
                | Some fieldInfo ->
                    let nextBoundField =
                        { Ref =
                              { entity = entityRef
                                name = fieldInfo.Name
                              }
                          Field = fieldInfo.Field
                          ForceRename = fieldInfo.ForceRename
                          Entity = newEntity
                          Immediate = false
                        }
                    resolvePath useMain nextBoundField refs
                | None -> raisef ViewResolveException "Column not found in dereference: %O" ref
            | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

    let resolveReference (useMain : bool) (mapping : QMapping) (f : LinkedFieldRef) : ReferenceInfo =
        match f.Ref with
        | VRColumn ref ->
            let (entityName, (schemaName, fields)) =
                match ref.entity with
                | None ->
                    if mapping.Count = 1 then
                        Map.toSeq mapping |> Seq.head
                    else
                        raisef ViewResolveException "None or more than one possible interpretation of %O in %O" f.Ref f
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
            let outerBoundField =
                match Map.tryFind ref.name fields with
                | Some r -> r
                | None -> raisef ViewResolveException "Unknown field %O in %O" ref.name f
            let newName =
                match outerBoundField with
                | Some f -> f.Ref.name
                | None -> ref.name

            let (innerBoundField, outerRef) =
                match outerBoundField with
                | Some field ->
                    // FIXME: why immediate = false here?
                    let inner = resolvePath useMain { field with Immediate = false } (Array.toList f.Path)
                    (Some inner, Some { Ref = field.Ref; Immediate = field.Immediate })
                | None when Array.isEmpty f.Path ->
                    (None, None)
                | _ ->
                    raisef ViewResolveException "Dereference on an unbound field in %O" f

            let newRef = { entity = Option.map (fun ename -> { schema = None; name = ename }) entityName; name = newName } : FieldRef
            { InnerField = innerBoundField
              OuterField = outerBoundField
              Ref = { Ref = VRColumn { Ref = newRef; Bound = outerRef }; Path = f.Path }
            }
        | VRPlaceholder arg ->
            let argInfo =
                match Map.tryFind arg arguments with
                | None -> raisef ViewResolveException "Unknown argument: %O" arg
                | Some argInfo -> argInfo
            let innerBoundField =
                if Array.isEmpty f.Path then
                    None
                else
                    match argInfo.ArgType with
                    | FTReference (entityRef, where) ->
                        let (name, remainingPath) =
                            match Array.toList f.Path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "imposssible"
                        let argEntity = layout.FindEntity entityRef |> Option.get
                        let argField =
                            match argEntity.FindField name with
                            | None -> raisef ViewResolveException "Field doesn't exist in %O: %O" entityRef name
                            | Some argField -> argField
                        let fieldInfo =
                            { Ref = { entity = entityRef; name = name }
                              Entity = argEntity
                              ForceRename = false
                              Field = argField.Field
                              Immediate = false
                            }
                        let inner = resolvePath useMain fieldInfo remainingPath
                        Some inner
                    | _ -> raisef ViewResolveException "Argument is not a reference: %O" ref
            usedArguments <- Set.add arg usedArguments
            { InnerField = innerBoundField
              OuterField = None
              Ref = { Ref = VRPlaceholder arg; Path = f.Path }
            }

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { Ref = VRPlaceholder name; Path = [||] } ->
                if Map.containsKey name arguments then
                    usedArguments <- Set.add name usedArguments
                    { Ref = VRPlaceholder name; Path = [||] }
                else
                    raisef ViewResolveException "Undefined placeholder: %O" name
            | ref -> raisef ViewResolveException "Invalid reference in LIMIT or OFFSET: %O" ref
        let voidQuery query = raisef ViewResolveException "Forbidden subquery in LIMIT or OFFSET: %O" query
        let voidAggr aggr = raisef ViewResolveException "Forbidden aggregate function in LIMIT or OFFSET"
        let voidSubEntity field subEntity = raisef ViewResolveException "Forbidden type assertion in LIMIT or OFFSET"
        let mapper =
            { idFieldExprMapper resolveReference voidQuery with
                  Aggregate = voidAggr
                  SubEntity = voidSubEntity
            }
        mapFieldExpr mapper expr

    let applyAlias (alias : EntityAlias) (results : QSubqueryFields) : QSubqueryFields =
        checkName alias.Name
        match alias.Fields with
        | None -> results
        | Some fieldNames ->
            fieldNames |> Seq.iter checkName
            try
                fieldNames |> Set.ofSeqUnique |> ignore
            with
                | Failure msg -> raisef ViewResolveException "Clashing names: %s" msg
            renameFields fieldNames results

    let rec resolveResult (flags : SelectFlags) (resolveCte : ResolveCTE) (mapping : QMapping) (result : ParsedQueryResult) : ResolvedResultInfo * ResolvedQueryResult =
        if not (Map.isEmpty result.Attributes) && flags.NoAttributes then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        let (exprInfo, expr) = resolveResultExpr flags resolveCte mapping result.Result
        let ret = {
            Attributes = resolveAttributes resolveCte mapping result.Attributes
            Result = expr
        }
        (exprInfo, ret)

    // Should be in sync with resultField
    and resolveResultExpr (flags : SelectFlags) (resolveCte : ResolveCTE) (mapping : QMapping) : ParsedQueryResultExpr -> ResolvedResultInfo * ResolvedQueryResultExpr = function
        | QRExpr (name, FERef f) ->
            Option.iter checkName name
            let ref = resolveReference true mapping f
            match ref.InnerField with
            | Some field when field.ForceRename && Option.isNone name -> raisef ViewResolveException "Field should be explicitly named in result expression: %s" (f.ToFunQLString())
            | _ -> ()
            let info =
                { InnerField = ref.InnerField
                  HasAggregates = false
                }
            (info, QRExpr (name, FERef ref.Ref))
        | QRExpr (name, e) ->
            match name with
            | Some n -> checkName n
            | None when flags.RequireNames -> raisef ViewResolveException "Unnamed results are allowed only inside expression queries"
            | None -> ()
            let (exprInfo, expr) = resolveFieldExpr true resolveCte mapping e
            let info =
                { InnerField = None
                  HasAggregates = exprInfo.HasAggregates
                }
            (info, QRExpr (name, expr))

    and resolveAttributes (resolveCte : ResolveCTE) (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveNonaggrFieldExpr resolveCte mapping expr) attributes

    and resolveNonaggrFieldExpr (resolveCte : ResolveCTE) (mapping : QMapping) (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let (info, res) = resolveFieldExpr false resolveCte mapping expr
        if info.HasAggregates then
            raisef ViewResolveException "Aggregate functions are not allowed here"
        res

    and resolveFieldExpr (useMain : bool) (resolveCte : ResolveCTE) (mapping : QMapping) (expr : ParsedFieldExpr) : ResolvedExprInfo * ResolvedFieldExpr =
        let mutable isLocal = true
        let mutable hasAggregates = false
        let resolveExprReference col =
            let ref = resolveReference useMain mapping col

            if not <| Array.isEmpty col.Path then
                isLocal <- false
            match ref.OuterField with
            | Some { Field = RComputedField { isLocal = false } } ->
                isLocal <- false
            | _ -> ()

            ref.Ref
        let resolveQuery query =
            let (_, res) = resolveSelectExpr resolveCte subExprSelectFlags query
            res
        let resolveAggr aggr =
            hasAggregates <- true
            aggr
        let mapper =
            { idFieldExprMapper resolveExprReference resolveQuery with
                  Aggregate = resolveAggr
                  SubEntity = resolveSubEntity layout
            }
        let ret = mapFieldExpr mapper expr
        let info =
            { IsLocal = isLocal
              HasAggregates = hasAggregates
            }
        (info, ret)

    and resolveOrderLimitClause (resolveCte : ResolveCTE) (mapping : QMapping) (limits : ParsedOrderLimitClause) : bool * ResolvedOrderLimitClause =
        let mutable isLocal = true
        let resolveOrderBy (ord, expr) =
            let (info, ret) = resolveFieldExpr false resolveCte mapping expr
            if info.HasAggregates then
                raisef ViewResolveException "Aggregates are not allowed here"
            if not info.IsLocal then
                isLocal <- false
            (ord, ret)
        let ret =
            { OrderBy = Array.map resolveOrderBy limits.OrderBy
              Limit = Option.map resolveLimitFieldExpr limits.Limit
              Offset = Option.map resolveLimitFieldExpr limits.Offset
            }
        (isLocal, ret)

    and resolveCommonTableExpr (resolveCte : ResolveCTE) (allowRecursive : bool) (name : EntityName) (cte : ParsedCommonTableExpr) : QSubqueryFieldsMap * ResolvedCommonTableExpr =
        let (resolveCte, newCtes) =
            match cte.Expr.CTEs with
            | None -> (resolveCte, None)
            | Some ctes ->
                let (resolveCte, newCtes) = resolveCommonTableExprs resolveCte ctes
                (resolveCte, Some newCtes)

        let cteFlags =
            { OneColumn = false
              RequireNames = Option.isNone cte.Fields
              NoAttributes = false
            }

        let (results, tree) =
            match cte.Expr.Tree with
            | SSetOp setOp when allowRecursive ->
                // We trait left side of outer UNION as a non-recursive side.
                let (results1, a') = resolveSelectExpr resolveCte cteFlags setOp.A
                let recResults =
                    match cte.Fields with
                    | None -> results1
                    | Some names -> renameFields names results1
                let fieldsMap = getFieldsMap recResults
                let newResolveCte (currName : EntityName) =
                    if currName = name then
                        fieldsMap
                    else
                        resolveCte currName
                let (results2, b') = resolveSelectExpr newResolveCte { cteFlags with RequireNames = false } setOp.B
                finishResolveSetOpExpr resolveCte setOp results1 a' results2 b' cteFlags
            | _ -> resolveSelectTreeExpr resolveCte cteFlags cte.Expr.Tree

        let results =
            match cte.Fields with
            | None -> results
            | Some names -> renameFields names results

        let newSelect =
            { CTEs = newCtes
              Tree = tree
              Extra = null
            }
        let info =
            { MainEntity = false
            } : ResolvedCommonTableExprTempInfo
        let newCte =
            { Fields = cte.Fields
              Expr = newSelect
              Extra = info
            }
        (getFieldsMap results, newCte)

    and resolveCommonTableExprs (resolveCte : ResolveCTE) (ctes : ParsedCommonTableExprs) : ResolveCTE * ResolvedCommonTableExprs =
        let exprsMap =
            try
                Map.ofSeqUnique ctes.Exprs
            with
            | Failure msg -> raisef ViewResolveException "Clashing names in WITH binding: %s" msg
        let mutable resultsMap : Map<EntityName, QSubqueryFieldsMap * ResolvedCommonTableExpr> = Map.empty
        let mutable resolved : (EntityName * ResolvedCommonTableExpr) list = []
        let resolveOne (name : EntityName, cte : ParsedCommonTableExpr) =
            if not (Map.containsKey name resultsMap) then
                let mutable processing = Set.singleton name

                let rec tmpResolveCte (currName : EntityName) =
                    match Map.tryFind currName resultsMap with
                    | None when ctes.Recursive ->
                        match Map.tryFind currName exprsMap with
                        | None -> resolveCte currName
                        | Some expr when name = currName ->
                            raisef ViewResolveException "Invalid recursive statement"
                        | Some expr when Set.contains currName processing ->
                            raisef ViewResolveException "Mutual recursion in WITH statement"
                        | Some cte ->
                            processing <- Set.add currName processing
                            let (results, newCte) = resolveCommonTableExpr tmpResolveCte true currName cte
                            resultsMap <- Map.add currName (results, newCte) resultsMap
                            resolved <- (currName, newCte) :: resolved
                            results
                    | None -> resolveCte currName
                    | Some (results, cte) -> results

                let (results, newCte) = resolveCommonTableExpr tmpResolveCte ctes.Recursive name cte
                resultsMap <- Map.add name (results, newCte) resultsMap
                resolved <- (name, newCte) :: resolved

        Array.iter resolveOne ctes.Exprs

        let newResolveCte (name : EntityName) =
            match Map.tryFind name resultsMap with
            | None -> resolveCte name
            | Some (results, cte) -> results
        let info =
            { Bindings = Map.map (fun name (results, cte) -> cte) resultsMap
            } : ResolvedCommonTableExprsTempInfo
        let ret =
            { Recursive = ctes.Recursive
              Exprs = resolved |> Seq.rev |> Array.ofSeq
              Extra = info
            }
        (newResolveCte, ret)

    and resolveSelectExpr (resolveCte : ResolveCTE) (flags : SelectFlags) (select : ParsedSelectExpr) : QSubqueryFields * ResolvedSelectExpr =
        let (resolveCte, newCtes) =
            match select.CTEs with
            | None -> (resolveCte, None)
            | Some ctes ->
                let (resolveCte, newCtes) = resolveCommonTableExprs resolveCte ctes
                (resolveCte, Some newCtes)
        let (results, tree) = resolveSelectTreeExpr resolveCte flags select.Tree
        let newSelect =
            { CTEs = newCtes
              Tree = tree
              Extra = null
            }
        (results, newSelect)

    and finishResolveSetOpExpr (resolveCte : ResolveCTE) (setOp : ParsedSetOperationExpr) (results1 : QSubqueryFields) (a : ResolvedSelectExpr) (results2 : QSubqueryFields) (b : ResolvedSelectExpr) (flags : SelectFlags) =
        if Array.length results1 <> Array.length results2 then
            raisef ViewResolveException "Different number of columns in a set operation expression"
        if flags.RequireNames then
            for (name, info) in results1 do
                if Option.isNone name then
                    raisef ViewResolveException "Name is required for column"
        let newResults = Array.map2 (fun (name1, info1) (name2, info2) -> (name1, unionBoundFieldInfo info1 info2)) results1 results2
        let orderLimitMapping = Map.singleton None (None, getFieldsMap newResults)
        let (limitsAreLocal, resolvedLimits) = resolveOrderLimitClause resolveCte orderLimitMapping setOp.OrderLimit
        if not limitsAreLocal then
            raisef ViewResolveException "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.OrderBy
        let ret =
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = a
              B = b
              OrderLimit = resolvedLimits
            }
        (newResults, SSetOp ret)

    and resolveSelectTreeExpr (resolveCte : ResolveCTE) (flags : SelectFlags) : ParsedSelectTreeExpr -> QSubqueryFields * ResolvedSelectTreeExpr = function
        | SSelect query ->
            let (results, res) = resolveSingleSelectExpr resolveCte flags query
            (results, SSelect res)
        | SValues values ->
            if flags.RequireNames then
                raisef ViewResolveException "Column aliases are required for this VALUES entry"
            let valuesLength = Array.length values.[0]
            let mapEntry entry =
                if Array.length entry <> valuesLength then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map (resolveFieldExpr true resolveCte Map.empty >> snd)
            let newValues = values |> Array.map mapEntry
            let fields = Seq.init valuesLength (fun i -> (None, None)) |> Array.ofSeq
            (fields, SValues newValues)
        | SSetOp setOp ->
            let (results1, a') = resolveSelectExpr resolveCte flags setOp.A
            let (results2, b') = resolveSelectExpr resolveCte { flags with RequireNames = false } setOp.B
            finishResolveSetOpExpr resolveCte setOp results1 a' results2 b' flags

    and resolveSingleSelectExpr (resolveCte : ResolveCTE) (flags : SelectFlags) (query : ParsedSingleSelectExpr) : QSubqueryFields * ResolvedSingleSelectExpr =
        if flags.NoAttributes && not (Map.isEmpty query.Attributes) then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        if flags.OneColumn && Array.length query.Results <> 1 then
            raisef ViewResolveException "Expression queries must have only one resulting column"
        let (fromMapping, qFrom) =
            match query.From with
            | None -> (Map.empty, None)
            | Some from ->
                let (mapping, res) = resolveFromExpr resolveCte flags from
                (mapping, Some res)
        let qWhere = Option.map (resolveNonaggrFieldExpr resolveCte fromMapping) query.Where
        let qGroupBy = Array.map (resolveNonaggrFieldExpr resolveCte fromMapping) query.GroupBy
        let rawResults = Array.map (resolveResult flags resolveCte fromMapping) query.Results
        let hasAggregates = Array.exists (fun (info : ResolvedResultInfo, expr) -> info.HasAggregates) rawResults || not (Array.isEmpty qGroupBy)
        let results = Array.map snd rawResults
        let newFields = rawResults |> Array.map (fun (info, res) -> (res.Result.TryToName (), if hasAggregates then None else info.InnerField))
        try
            newFields |> Seq.mapMaybe fst |> Set.ofSeqUnique |> ignore
        with
            | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

        let extra =
            { HasAggregates = hasAggregates
            } : ResolvedSingleSelectInfo

        let newQuery = {
            Attributes = resolveAttributes resolveCte fromMapping query.Attributes
            From = qFrom
            Where = qWhere
            GroupBy = qGroupBy
            Results = results
            OrderLimit = snd <| resolveOrderLimitClause resolveCte fromMapping query.OrderLimit
            Extra = extra
        }
        (newFields, newQuery)

    and resolveFromExpr (resolveCte : ResolveCTE) (flags : SelectFlags) : ParsedFromExpr -> QMapping * ResolvedFromExpr = function
        | FEntity (pun, ({ schema = Some schemaName; name = name } as ref)) ->
            let resRef = { schema = schemaName; name = name }
            let entity =
                match layout.FindEntity resRef with
                | None -> raisef ViewResolveException "Entity not found: %O" name
                | Some entity -> entity

            let makeBoundField (name : FieldName) (field : ResolvedField) =
                let ref = { entity = resRef; name = name } : ResolvedFieldRef
                Some { Ref = ref; Entity = entity; Field = field; ForceRename = false; Immediate = true }

            let realFields = mapAllFields makeBoundField entity
            let mainFieldInfo = Option.get <| entity.FindField funMain
            let mainBoundField = { Ref = { entity = resRef; name = entity.MainField }; Entity = entity; Field = mainFieldInfo.Field; ForceRename = true; Immediate = true }
            let fields = Map.add funMain (Some mainBoundField) realFields

            let newName = Option.defaultValue name pun
            (Map.singleton (Some newName) (Some resRef.schema, fields), FEntity (pun, ref))
        | FEntity (pun, ({ schema = None; name = name } as ref)) ->
            let fields = resolveCte name
            let newName = Option.defaultValue name pun
            (Map.singleton (Some newName) (None, fields), FEntity (pun, ref))
        | FJoin join ->
            let (newMapping1, newA) = resolveFromExpr resolveCte flags join.A
            let (newMapping2, newB) = resolveFromExpr resolveCte flags join.B

            let newMapping =
                try
                    Map.unionUnique newMapping1 newMapping2
                with
                | Failure msg -> raisef ViewResolveException "Clashing entity names in a join: %s" msg

            let (info, newFieldExpr) = resolveFieldExpr false resolveCte newMapping join.Condition
            if not info.IsLocal then
                raisef ViewResolveException "Cannot use dereferences in join expressions: %O" join.Condition
            if info.HasAggregates then
                raisef ViewResolveException "Cannot use aggregate functions in join expression"
            let ret =
                { Type = join.Type
                  A = newA
                  B = newB
                  Condition = newFieldExpr
                }
            (newMapping, FJoin ret)
        | FSubExpr (alias, q) ->
            let (info, newQ) = resolveSelectExpr resolveCte { flags with RequireNames = Option.isNone alias.Fields; OneColumn = false } q
            let info = applyAlias alias info
            let mapping = Map.singleton (Some alias.Name) (None, getFieldsMap info)
            (mapping, FSubExpr (alias, newQ))

    let resolveMainEntity (fields : QSubqueryFieldsMap) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity =
        let ref = resolveEntityRef main.Entity
        let entity =
            match layout.FindEntity ref with
            | None -> raisef ViewResolveException "Entity not found: %O" main.Entity
            | Some e -> e :?> ResolvedEntity
        if entity.isAbstract then
            raisef ViewResolveException "Entity is abstract: %O" main.Entity
        let (foundRef, columns) = findMainEntity query
        if foundRef <> ref then
            raisef ViewResolveException "Cannot map main entity to the expression: %O, possible value: %O" ref foundRef

        let getField (result : ResolvedQueryResult) : (FieldName * FieldName) option =
            let name = result.Result.TryToName () |> Option.get
            match Map.tryFind name fields with
            | Some (Some { Ref = boundRef; Field = RColumnField _ }) when boundRef.entity = ref -> Some (name, boundRef.name)
            | _ -> None
        let mappedResults = columns |> Seq.mapMaybe getField |> Map.ofSeqUnique

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.defaultValue && not field.isNullable then
                if not (Map.containsKey fieldName mappedResults) then
                    raisef ViewResolveException "Required inserted entity field is not in the view expression: %O" fieldName
        entity.columnFields |> Map.iter checkField

        { Entity = ref
          FieldsToColumns = Map.reverse mappedResults
          ColumnsToFields = mappedResults
        }

    member this.ResolveSelectExpr flags select = resolveSelectExpr failCte flags select
    member this.ResolveMainEntity fields select main = resolveMainEntity fields select main
    member this.UsedArguments = usedArguments

// Remove and replace unneeded Extra fields.
let rec private relabelSelectExpr (select : ResolvedSelectExpr) : ResolvedSelectExpr =
    { CTEs = Option.map relabelCommonTableExprs select.CTEs
      Tree = relabelSelectTreeExpr select.Tree
      Extra = select.Extra
    }

and private relabelCommonTableExprs (ctes : ResolvedCommonTableExprs) : ResolvedCommonTableExprs =
    { Recursive = ctes.Recursive
      Exprs = Array.map (fun (name, cte) -> (name, relabelCommonTableExpr cte)) ctes.Exprs
      Extra = null
    }

and private relabelCommonTableExpr (cte : ResolvedCommonTableExpr) : ResolvedCommonTableExpr =
    let extra = cte.Extra :?> ResolvedCommonTableExprTempInfo
    { Fields = cte.Fields
      Expr = relabelSelectExpr cte.Expr
      Extra = ({ MainEntity = extra.MainEntity } : ResolvedCommonTableExprInfo)
    }

and private relabelSelectTreeExpr : ResolvedSelectTreeExpr -> ResolvedSelectTreeExpr = function
    | SSelect sel -> SSelect (relabelSingleSelectExpr sel)
    | SValues values -> SValues (Array.map (Array.map relabelFieldExpr) values)
    | SSetOp setOp ->
        SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = relabelSelectExpr setOp.A
              B = relabelSelectExpr setOp.B
              OrderLimit = relabelOrderLimitClause setOp.OrderLimit
            }

and private relabelSingleSelectExpr (select : ResolvedSingleSelectExpr) : ResolvedSingleSelectExpr =
    { Attributes = Map.map (fun name -> relabelFieldExpr) select.Attributes
      Results = Array.map relabelQueryResult select.Results
      From  = Option.map relabelFromExpr select.From
      Where = Option.map relabelFieldExpr select.Where
      GroupBy = Array.map relabelFieldExpr select.GroupBy
      OrderLimit = relabelOrderLimitClause select.OrderLimit
      Extra = select.Extra
    }

and private relabelFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
    | FEntity (pun, ref) -> FEntity (pun, ref)
    | FJoin join ->
        FJoin
            { Type = join.Type
              A = relabelFromExpr join.A
              B = relabelFromExpr join.B
              Condition = relabelFieldExpr join.Condition
            }
    | FSubExpr (name, subExpr) -> FSubExpr (name, relabelSelectExpr subExpr)

and private relabelFieldExpr (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
    let mapper = idFieldExprMapper id relabelSelectExpr
    mapFieldExpr mapper expr

and private relabelOrderLimitClause (clause : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
    { Offset = Option.map relabelFieldExpr clause.Offset
      Limit = Option.map relabelFieldExpr clause.Limit
      OrderBy = Array.map (fun (order, expr) -> (order, relabelFieldExpr expr)) clause.OrderBy
    }

and private relabelQueryResult (result : ResolvedQueryResult) : ResolvedQueryResult =
    { Attributes = Map.map (fun name -> relabelFieldExpr) result.Attributes
      Result = relabelQueryResultExpr result.Result
    }

and private relabelQueryResultExpr : ResolvedQueryResultExpr -> ResolvedQueryResultExpr = function
    | QRExpr (name, e) -> QRExpr (name, relabelFieldExpr e)

let resolveSelectExpr (layout : ILayoutFields) (select : ParsedSelectExpr) : Set<Placeholder> * ResolvedSelectExpr =
    let qualifier = QueryResolver (layout, globalArgumentsMap)
    let (results, qQuery) = qualifier.ResolveSelectExpr subExprSelectFlags select
    (qualifier.UsedArguments, relabelSelectExpr qQuery)

let resolveViewExpr (layout : Layout) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let arguments = viewExpr.Arguments |> Map.map (fun name -> resolveArgument layout)
    let localArguments = Map.mapKeys PLocal arguments
    let allArguments = Map.union localArguments globalArgumentsMap
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr viewExprSelectFlags viewExpr.Select
    let qQuery = relabelSelectExpr qQuery
    let mainEntity = Option.map (qualifier.ResolveMainEntity (getFieldsMap results) qQuery) viewExpr.MainEntity

    { Arguments = Map.union localArguments (Map.filter (fun name _ -> Set.contains name qualifier.UsedArguments) globalArgumentsMap)
      Select = qQuery
      MainEntity = mainEntity
    }
