module FunWithFlags.FunDB.FunQL.View

open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.Layout.Types

// Validates all fields and expressions. Further processing can skip all the checks.

exception ViewError of info : string with
    override this.Message = this.info

type ResolvedFieldRef =
    { entity : EntityRef
      name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = sprintf "%s.%s" (this.entity.ToFunQLString()) (this.name.ToFunQLString())

type ResolvedFieldName =
    | RFField of ResolvedFieldRef
    | RFEntityId of EntityRef
    | RFSubquery of EntityName * FieldName
    with
        override this.ToString () = this.ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () =
                match this with
                    | RFField fieldRef -> fieldRef.ToFunQLString()
                    | RFEntityId e -> sprintf "%s.%s" (e.ToFunQLString()) (funId.ToFunQLString())
                    | RFSubquery (tableName, columnName) -> sprintf "%s.%s" (renderSqlName tableName) (renderSqlName columnName)

type private QMappedEntity =
    | QMEntity of ResolvedEntity
    | QMSubquery of EntityName * Set<FieldName>

type private QMapping = Map<EntityName, QMappedEntity>

type private QPlaceholders = Set<FunQLName>

type ResolvedViewExpr = ViewExpr<ResolvedFieldRef>
type ResolvedFieldExpr = FieldExpr<ResolvedFieldRef>
type ResolvedQueryExpr = QueryExpr<EntityRef, ResolvedFieldRef>
type ResolvedFromExpr = FromExpr<EntityRef, ResolvedFieldRef>
type ResolvedFromClause = FromClause<EntityRef, ResolvedFieldRef>

let private checkName (name : string) : () =
    if name = "" then
        raise <| ViewError "Empty names are not allowed"
    if name.IndexOf("__") <> -1 then
        raise <| ViewError <| sprintf "Names should not contain '__': %s" name

let private lookupField (mapping : QMapping) (f : FieldName) : ResolvedFieldRef =
    let entity =
        match f.entity with
            | None ->
                if mapping.Count = 1 then
                    Map.toSeq mapping |> Seq.map snd |> Seq.head
                else
                    raise <| ViewError <| sprintf "None or more than one possible interpretation: %s" f.name
            | Some ename ->
                match Map.tryFind ename mapping with
                    | None ->
                        raise <| ViewError <| sprintf "Field entity not found: %s" ename.name
                    | Some e -> e
    match entity with
        // FIXME: improve error reporting
        | QMEntity entity ->
            if f.name = funId then
                RFEntityId entity
            else
                match entity.FindField(f.name) with
                    | None ->
                        raise <| ViewError <| sprintf "Field not found: %s" f.name
                    | Some field -> RFField { entity = entity; name = f.name }
        | QMSubquery (queryName, fields) ->
            if Set.contains f.name fields then
                RFSubquery (queryName, f.name)
            else
                raise <| ViewError <| sprintf "Field not found: %s" f.name

type private QueryResolver (layout : Layout) (placeholders : QPlaceholders) =
    let resolveFieldExpr (mapping : QMapping) : ParsedFieldExpr -> ResolvedFieldExpr =
        let resolvePlaceholder name =
            if Set.contains name placeholders
            then name
            else raise <| ViewError <| sprintf "Undefined placeholder: %s" name
        mapFieldExpr (lookupField mapping) resolvePlaceholder

    let rec resolveQueryExpr (query : ParsedQueryExpr) : QMapping * ResolvedQueryExpr =
        let (newMapping, qClause) = resolveFromClause query.clause
        try
            query.results |> Seq.map (fun res -> res.name) |> setOfSeqUnique
        with
            | Failure msg -> raise <| ViewError <| sprintf "Clashing result names: %s" msg
        let newQuery = {
            results = Array.map (resolveResult newMapping) query.results
            clause = qClause
        }
        (newMapping, newQuery)

    and resolveFromClause (clause : ParsedFromClause) : QMapping * ResolvedFromClause =
        let (qFrom, newMapping) = resolveFromExpr clause.from
        let res = { from = qFrom
                    where = Option.map (resolveFieldExpr newMapping) clause.where
                    orderBy = Array.map (fun (ord, expr) -> (ord, resolveFieldExpr newMapping expr)) clause.orderBy
                  }
        (newMapping, res)

    and resolveFromExpr : ParsedFromExpr -> (QMapping * ResolvedFromExpr) = function
        | FEntity name ->
            match layout.FindEntity(name) with
                | None -> raise <| ViewError <| sprintf "Entity not found: %O" name
                | Some entity ->
                    if Set.isEmpty entity.possibleSubEntities then
                        raise <| ViewError <| sprintf "Cannot select from abstract entity with no non-abstract descendants: %O" name
                    (mapSingleton entity (QMEntity entity), FEntity name)
        | FJoin (jt, e1, e2, where) ->
            let (newMapping1, newE1) = resolveFromExpr e1
            let (newMapping2, newE2) = resolveFromExpr e2

            let newMapping =
                try
                    mapUnionUnique newMapping1 newMapping2
                with
                    | Failure msg -> raise <| ViewError <| sprintf "Clashing entity names in a join: %s" msg

            let newFieldExpr = resolveFieldExpr newMapping where
            (newMapping, FJoin (jt, newE1, newE2, newFieldExpr))
        | FSubExpr (name, q) ->
            checkName name
            let newQ = resolveQueryExpr entities q
            let fields = newQ.results |> Seq.map resultName |> Set.ofSeq
            (FSubExpr (name, newQ), mapSingleton { schema = None; name = name } (QMSubquery (name, fields)))

    let resolveResult (mapping : QMapping) : ParsedQueryResult -> ResolvedQueryResult = function
        | RField f -> RField (lookupField mapping f)
        | RExpr (name, e) ->
            checkName name
            RExpr (name, resolveFieldExpr mapping e)

    let resolveAttributes (mapping : QMapping) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveFieldExpr mapping expr) attributes

    member this.ResolveQueryExpr = resolveQueryExpr
    member this.ResolveFieldExpr = resolveFieldExpr
    member this.ResolveAttributes = resolveAttributes

let resolveViewExpr (layout : Layout) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let qualifier = QueryResolver layout (setOfMap viewExpr.arguments)
    let topQuery =
        { results = Array.map snd viewExpr.results
          clause = viewExpr.clause
        }
    let (newMapping, qQuery) = qualifier.ResolveQueryExpr topQuery
    let attributes = qualifier.ResolveAttributes newMapping viewExpr.attributes
    let results = Array.map2 (fun (attrs, oldResult) result -> (qualifier.ResolveAttributes newMapping attrs, result)) viewExpr.results qQuery.results
    
    { arguments = viewExpr.arguments
      attributes = attributes
      results = results
      clause = qQuery.clause
    }
