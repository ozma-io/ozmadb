module FunWithFlags.FunDB.FunQL.View

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.Layout.Types

// Validates all fields and expressions. Further processing can skip all the checks.

exception ViewResolveException of info : string with
    override this.Message = this.info

type ResolvedFieldRef =
    { entity : ResolvedEntityRef
      name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s.%s" (this.entity.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ResolvedFieldName =
    | RFField of ResolvedFieldRef
    | RFSubquery of EntityName * FieldName * (ResolvedFieldRef option)
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | RFField fieldRef -> fieldRef.ToFunQLString()
                | RFSubquery (tableName, columnName, boundField) -> sprintf "%s.%s" (tableName.ToFunQLString()) (columnName.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type private QSubqueryFields = Map<FieldName, ResolvedFieldRef option>

type private QMappedEntity =
    | QMEntity of ResolvedEntityRef * ResolvedEntity
    | QMSubquery of EntityName * QSubqueryFields

type private QMapping = Map<EntityName, QMappedEntity>
type private SomeArgumentsMap = Map<string, ParsedFieldType>
type private ArgumentsMap = Map<Placeholder, ParsedFieldType>

type ResolvedFieldExpr = FieldExpr<ResolvedFieldName>
type ResolvedQueryExpr = QueryExpr<ResolvedEntityRef, ResolvedFieldName>
type ResolvedQueryResult = QueryResult<ResolvedFieldName>
type ResolvedFromExpr = FromExpr<ResolvedEntityRef, ResolvedFieldName>
type ResolvedConditionClause = ConditionClause<ResolvedEntityRef, ResolvedFieldName>
type ResolvedFromClause = FromClause<ResolvedEntityRef, ResolvedFieldName>
type ResolvedAttributeMap = AttributeMap<ResolvedFieldName>

type ResolvedUpdateExpr =
    { entity : ResolvedEntityRef
      namesToFields : Map<FieldName, FieldName>
      fieldsToNames : Map<FieldName, FieldName>
    }

type ResolvedViewExpr =
    { arguments : ArgumentsMap
      attributes : ResolvedAttributeMap
      results : ViewResults<ResolvedFieldName>
      clause : ResolvedFromClause
      update : ResolvedUpdateExpr option
      fields : QSubqueryFields
    }

type private MainEntity =
    { entity : ResolvedEntityRef
    }

let rec private findMainEntity : ResolvedFromExpr -> MainEntity option = function
    | FEntity ent -> Some { entity = ent }
    | FJoin (ftype, a, b, where) ->
        match ftype with
            | Left -> findMainEntity a
            | Right -> findMainEntity b
            | _ -> None
    | FSubExpr (name, query) -> findMainEntity query.clause.from

let private checkName (name : string) : unit =
    if not (goodName name) then
        raise (ViewResolveException <| sprintf "Invalid name: %s" name)

let resultBoundField (result : ResolvedQueryResult) : ResolvedFieldRef option =
    match result.expression with
        | FEColumn (RFField fieldRef) -> Some fieldRef
        | FEColumn (RFSubquery (queryName, fieldName, Some boundField)) -> Some boundField
        | _ -> None

let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match name.schema with
        | Some schema -> { schema = schema; name = name.name }
        | None -> raise (ViewResolveException <| sprintf "Unspecified schema in name: %O" name)

let private lookupField (mapping : QMapping) (f : FieldRef) : ResolvedFieldName =
    let mappedEntity =
        match f.entity with
            | None ->
                if mapping.Count = 1 then
                    Map.toSeq mapping |> Seq.map snd |> Seq.head
                else
                    raise (ViewResolveException <| sprintf "None or more than one possible interpretation: %O" f.name)
            | Some ename ->
                match Map.tryFind ename.name mapping with
                    | Some ref ->
                        match ename.schema with
                            | None -> ref
                            | Some sch ->
                                match ref with
                                    | QMEntity ({ schema = entitySchema; name = name }, _) when entitySchema = sch -> ref
                                    | _ -> raise (ViewResolveException <| sprintf "Invalid entity schema: %O" sch)
                    | None ->
                        raise (ViewResolveException <| sprintf "Field entity not found: %O" ename.name)
    match mappedEntity with
        // FIXME: improve error reporting
        | QMEntity (entityRef, entity) ->
            if f.name = funId then
                RFField { entity = entityRef; name = funId }
            else
                match entity.FindField(f.name) with
                    | None ->
                        raise (ViewResolveException <| sprintf "Field not found: %O" f.name)
                    | Some field -> RFField { entity = entityRef; name = f.name }
        | QMSubquery (queryName, fields) ->
            match Map.tryFind f.name fields with
                | Some boundField -> RFSubquery (queryName, f.name, boundField)
                | None -> raise (ViewResolveException <| sprintf "Field not found: %O" f.name)

let private resolveFieldExprGeneric (arguments: ArgumentsMap) (mapping : QMapping) : ParsedFieldExpr -> ResolvedFieldExpr =
    let resolvePlaceholder name =
        if Map.containsKey name arguments
        then name
        else raise (ViewResolveException <| sprintf "Undefined placeholder: %O" name)
    mapFieldExpr (lookupField mapping) resolvePlaceholder

let private resolveConditionClauseGeneric (arguments: ArgumentsMap) (mapping : QMapping) (clause : ParsedConditionClause) : ResolvedConditionClause =
    { where = Option.map (resolveFieldExprGeneric arguments mapping) clause.where
      orderBy = Array.map (fun (ord, expr) -> (ord, resolveFieldExprGeneric arguments mapping expr)) clause.orderBy
    }

type private QueryResolver (layout : Layout, arguments : ArgumentsMap) =
    let resolveFieldExpr = resolveFieldExprGeneric arguments
 
    let resolveResult (mapping : QMapping) (result : ParsedQueryResult) : ResolvedQueryResult =
        checkName (result.name.ToString())
        { name = result.name; expression = resolveFieldExpr mapping result.expression }

    let rec resolveQueryExpr (query : ParsedQueryExpr) : QMapping * ResolvedQueryExpr =
        let (newMapping, qClause) = resolveFromClause query.clause
        try
            query.results |> Seq.map (fun res -> res.name) |> Set.ofSeqUnique |> ignore
        with
            | Failure msg -> raise (ViewResolveException <| sprintf "Clashing result names: %s" msg)
        let newQuery = {
            results = Array.map (resolveResult newMapping) query.results
            clause = qClause
        }
        (newMapping, newQuery)

    and resolveFromClause (clause : ParsedFromClause) : QMapping * ResolvedFromClause =
        let (newMapping, qFrom) = resolveFromExpr clause.from
        let res = { from = qFrom
                    condition = resolveConditionClause newMapping clause.condition
                  }
        (newMapping, res)

    and resolveConditionClause = resolveConditionClauseGeneric arguments

    and resolveFromExpr : ParsedFromExpr -> (QMapping * ResolvedFromExpr) = function
        | FEntity name ->
            let resName = resolveEntityRef name
            match layout.FindEntity(resName) with
            | None -> raise (ViewResolveException <| sprintf "Entity not found: %O" name)
            | Some entity -> (Map.singleton name.name (QMEntity (resName, entity)), FEntity resName)
        | FJoin (jt, e1, e2, where) ->
            let (newMapping1, newE1) = resolveFromExpr e1
            let (newMapping2, newE2) = resolveFromExpr e2

            let newMapping =
                try
                    Map.unionUnique newMapping1 newMapping2
                with
                | Failure msg -> raise (ViewResolveException <| sprintf "Clashing entity names in a join: %s" msg)

            let newFieldExpr = resolveFieldExpr newMapping where
            (newMapping, FJoin (jt, newE1, newE2, newFieldExpr))
        | FSubExpr (name, q) ->
            checkName (name.ToString())
            let (newMapping, newQ) = resolveQueryExpr q
            let fields = newQ.results |> Seq.map (fun x -> (x.name, resultBoundField x)) |> Map.ofSeq
            (Map.singleton name (QMSubquery (name, fields)), FSubExpr (name, newQ))

    let resolveAttributes (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveFieldExpr mapping expr) attributes
    
    let resolveUpdate (query : ResolvedQueryExpr) (updateEntity : ResolvedEntityRef) : ResolvedUpdateExpr =
        let entity =
            match layout.FindEntity(updateEntity) with
            | None -> raise (ViewResolveException <| sprintf "Entity not found: %O" updateEntity)
            | Some e -> e
        match findMainEntity query.clause.from with
        | Some mainEntity when mainEntity.entity = updateEntity -> ()
        | someMain -> raise (ViewResolveException <| sprintf "Cannot map updated entity to the expression: %O, possible value: %O" updateEntity someMain)
        let getField (result : ResolvedQueryResult) : (FieldName * FieldName) option =
            match resultBoundField result with
            | Some fieldRef when fieldRef.entity = updateEntity && fieldRef.name <> funId -> Some (fieldRef.name, result.name)
            | _ -> None
        let mappedResults =
            try
                query.results |> Seq.mapMaybe getField |> Map.ofSeqUnique
            with
            | Failure msg -> raise (ViewResolveException <| sprintf "Repeating updated entity fields in results: %s" msg)

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.defaultValue then
                if not <| Map.containsKey fieldName mappedResults then
                    raise (ViewResolveException <| sprintf "Required updated entity field is not in the view expression: %O" fieldName)
        entity.columnFields |> Map.iter checkField

        { entity = updateEntity
          fieldsToNames = mappedResults
          namesToFields = mappedResults |> Map.toSeq |> Seq.map (fun (a, b) -> (b, a)) |> Map.ofSeq
        }

    member this.ResolveQueryExpr = resolveQueryExpr
    member this.ResolveFieldExpr = resolveFieldExpr
    member this.ResolveAttributes = resolveAttributes
    member this.ResolveUpdate = resolveUpdate

let resolveViewExpr (layout : Layout) (globalArguments : SomeArgumentsMap) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let checkArgument name typ =
        checkName name
        match typ with
        | FTReference (entityRef, where) ->
            if Option.isNone <| layout.FindEntity(resolveEntityRef entityRef) then
                raise (ViewResolveException <| sprintf "Unknown entity in reference: %O" entityRef)
        | FTEnum vals ->
            if Set.isEmpty vals then
                raise (ViewResolveException "Enums must not be empty")
        | _ -> ()
    Map.iter checkArgument viewExpr.arguments

    let allArguments = Map.union (Map.mapKeys PLocal viewExpr.arguments) (Map.mapKeys PGlobal globalArguments)
    let qualifier = QueryResolver (layout, allArguments)
    let topQuery =
        { results = Array.map snd viewExpr.results
          clause = viewExpr.clause
        }
    let (newMapping, qQuery) = qualifier.ResolveQueryExpr topQuery
    let attributes = qualifier.ResolveAttributes newMapping viewExpr.attributes
    let results = Array.map2 (fun (attrs, oldResult) result -> (qualifier.ResolveAttributes newMapping attrs, result)) viewExpr.results qQuery.results
    let update = Option.map (qualifier.ResolveUpdate qQuery << resolveEntityRef) viewExpr.updateName
    let fields = results |> Seq.map (fun (attrs, res) -> (res.name, resultBoundField res)) |> Map.ofSeq

    { arguments = allArguments
      attributes = attributes
      // FIXME: filter to only include those global arguments mentioned in the view
      results = results
      clause = qQuery.clause
      update = update
      fields = fields
    }

let resolveAddedCondition (viewExpr : ResolvedViewExpr) (clause : ParsedConditionClause) : ResolvedConditionClause =
    let mapping = Map.singleton funView (QMSubquery (funView, viewExpr.fields))
    resolveConditionClauseGeneric viewExpr.arguments mapping clause