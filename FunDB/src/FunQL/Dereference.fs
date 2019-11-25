module FunWithFlags.FunDB.FunQL.Dereference

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve

type ViewDereferenceException (message : string) =
    inherit Exception(message)

type private ReferenceResolver (checkViewExists : ResolvedUserViewRef -> unit, homeSchema : SchemaName option) =
    let resolveRef (ref : UserViewRef) : UserViewRef =
        let schemaName =
            match ref.schema with
            | None ->
                match homeSchema with
                | None -> raisef ViewDereferenceException "No default schema for user view reference %O" ref
                | Some r -> r
            | Some schemaName -> schemaName
        checkViewExists { schema = schemaName; name = ref.name }
        { schema = Some schemaName; name = ref.name }

    let resolveValue (value : FieldValue) : FieldValue =
        match value with
        | FUserViewRef ref ->
            let r = resolveRef ref
            FUserViewRef r
        | FUserViewRefArray vals ->
            let arr = vals |> Seq.map resolveRef |> Seq.toArray
            FUserViewRefArray arr
        | v -> v

    let rec resolveResult (result : ResolvedQueryResult) : ResolvedQueryResult =
        let attributes = resolveAttributes result.attributes
        let result = resolveResultExpr result.result
        { attributes = attributes
          result = result
        }

    and resolveResultExpr : ResolvedQueryResultExpr -> ResolvedQueryResultExpr = function
        | QRExpr (name, e) -> QRExpr (name, resolveFieldExpr e)

    and resolveAttributes (attributes : ResolvedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveFieldExpr expr) attributes

    and resolveFieldExpr : ResolvedFieldExpr -> ResolvedFieldExpr =
        mapFieldExpr resolveValue id id id

    and resolveOrderLimitClause (limits : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
        let resolveOrderBy (ord, expr) = (ord, resolveFieldExpr expr)
        let orderBy = Array.map resolveOrderBy limits.orderBy
        let limit = Option.map resolveFieldExpr limits.limit
        let offset = Option.map resolveFieldExpr limits.offset
        { orderBy = orderBy
          limit = limit
          offset = offset
        }

    and resolveSelectExpr : ResolvedSelectExpr -> ResolvedSelectExpr = function
        | SSelect query -> SSelect <| resolveSingleSelectExpr query
        | SSetOp (op, a, b, limits) ->
            let a' = resolveSelectExpr a
            let b' = resolveSelectExpr b
            let limits' = resolveOrderLimitClause limits
            SSetOp (op, a', b', limits')

    and resolveSingleSelectExpr (query : ResolvedSingleSelectExpr) : ResolvedSingleSelectExpr =
            let attributes = resolveAttributes query.attributes
            let from = Option.map resolveFromExpr query.from
            let where = Option.map resolveFieldExpr query.where
            let groupBy = Array.map resolveFieldExpr query.groupBy
            let results = Array.map resolveResult query.results
            let orderLimit = resolveOrderLimitClause query.orderLimit
            { attributes = attributes
              from = from
              where = where
              groupBy = groupBy
              results = results
              orderLimit = orderLimit
              extra = query.extra
            }

    and resolveFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
        | FEntity (pun, name) -> FEntity (pun, name)
        | FJoin (jt, e1, e2, where) ->
            let e1' = resolveFromExpr e1
            let e2' = resolveFromExpr e2
            let where' = resolveFieldExpr where
            FJoin (jt, e1', e2', where')
        | FSubExpr (name, q) -> FSubExpr (name, resolveSelectExpr q)
        | FValues (name, fieldNames, values) ->
            let resolveOne = Array.map resolveFieldExpr
            FValues (name, fieldNames, Array.map resolveOne values)

    member this.ResolveSelectExpr expr = resolveSelectExpr expr

// checkViewExists is supposed to throw an exception if the view doesn't exist -- this way we can distinguish between views that aren't found
// and views that are broken while resolving user views.
let dereferenceViewExpr (checkViewExists : ResolvedUserViewRef -> unit) (homeSchema : SchemaName option) (expr : ResolvedViewExpr) : ResolvedViewExpr =
    let resolver = ReferenceResolver (checkViewExists, homeSchema)
    let select' = resolver.ResolveSelectExpr expr.select
    { expr with
          select = select'
    }