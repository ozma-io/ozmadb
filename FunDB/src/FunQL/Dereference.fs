module FunWithFlags.FunDB.FunQL.Dereference

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve

type ViewDereferenceException (message : string) =
    inherit Exception(message)

type private ReferenceResolver (checkViewExists : ResolvedUserViewRef -> Task<unit>, homeSchema : SchemaName option) =
    let resolveRef (ref : UserViewRef) : Task<UserViewRef> = task {
        let schemaName =
            match ref.schema with
            | None ->
                match homeSchema with
                | None -> raisef ViewDereferenceException "No default schema for user view reference %O" ref
                | Some r -> r
            | Some schemaName -> schemaName
        do! checkViewExists { schema = schemaName; name = ref.name }
        return { schema = Some schemaName; name = ref.name }
    }

    let resolveValue (value : FieldValue) : Task<FieldValue> = task {
        match value with
        | FUserViewRef ref ->
            let! r = resolveRef ref
            return FUserViewRef r
        | FUserViewRefArray vals ->
            let! arr = Seq.mapTaskSync resolveRef vals
            return FUserViewRefArray (Seq.toArray arr)
        | v -> return v
    }

    let resolveLimitFieldExpr (expr : ResolvedFieldExpr) : Task<ResolvedFieldExpr> =
        mapTaskSyncFieldExpr resolveValue Task.result Task.result expr

    let rec resolveResult (result : ResolvedQueryResult) : Task<ResolvedQueryResult> =
        task {
            let! attributes = resolveAttributes result.attributes
            let! result = resolveResultExpr result.result
            return
                { attributes = attributes
                  result = result
                }
        }

    and resolveResultExpr : ResolvedQueryResultExpr -> Task<ResolvedQueryResultExpr> = function
        | QRField f -> Task.result <| QRField f
        | QRExpr (name, e) -> Task.map (fun e' -> QRExpr (name, e')) <| resolveFieldExpr e

    and resolveAttributes (attributes : ResolvedAttributeMap) : Task<ResolvedAttributeMap> =
        Map.mapTaskSync (fun name expr -> resolveFieldExpr expr) attributes

    and resolveFieldExpr : ResolvedFieldExpr -> Task<ResolvedFieldExpr> =
        mapTaskSyncFieldExpr resolveValue Task.result Task.result

    and resolveOrderLimitClause (limits : ResolvedOrderLimitClause) : Task<ResolvedOrderLimitClause> =
        let resolveOrderBy (ord, expr) = Task.map (fun expr' -> (ord, expr')) (resolveFieldExpr expr)
        task {
            let! orderBy = Array.mapTaskSync resolveOrderBy limits.orderBy
            let! limit = Option.mapTask resolveLimitFieldExpr limits.limit
            let! offset = Option.mapTask resolveLimitFieldExpr limits.offset
            return
                { orderBy = orderBy
                  limit = limit
                  offset = offset
                }
        }

    and resolveSelectExpr : ResolvedSelectExpr -> Task<ResolvedSelectExpr> = function
        | SSelect query -> Task.map SSelect <| resolveSingleSelectExpr query
        | SSetOp (op, a, b, limits) -> task {
            let! a' = resolveSelectExpr a
            let! b' = resolveSelectExpr b
            let! limits' = resolveOrderLimitClause limits
            return SSetOp (op, a', b', limits')
        }

    and resolveSingleSelectExpr (query : ResolvedSingleSelectExpr) : Task<ResolvedSingleSelectExpr> =
        task {
            let! attributes = resolveAttributes query.attributes
            let! clause = Option.mapTask resolveFromClause query.clause
            let! results = Array.mapTaskSync resolveResult query.results
            let! orderLimit = resolveOrderLimitClause query.orderLimit
            return
                { attributes = attributes
                  clause = clause
                  results = results
                  orderLimit = orderLimit
                }
        }

    and resolveFromClause (clause : ResolvedFromClause) : Task<ResolvedFromClause> =
        task {
            let! from = resolveFromExpr clause.from
            let! where = Option.mapTask resolveFieldExpr clause.where
            return
                { from = from
                  where = where
                }
        }

    and resolveFromExpr : ResolvedFromExpr -> Task<ResolvedFromExpr> = function
        | FEntity (pun, name) -> Task.result <| FEntity (pun, name)
        | FJoin (jt, e1, e2, where) ->
            task {
                let! e1' = resolveFromExpr e1
                let! e2' = resolveFromExpr e2
                let! where' = resolveFieldExpr where
                return FJoin (jt, e1', e2', where')
            }
        | FSubExpr (name, q) -> Task.map (fun q' -> FSubExpr (name, q')) (resolveSelectExpr q)
        | FValues (name, fieldNames, values) ->
            let resolveOne = Array.mapTaskSync resolveFieldExpr
            Task.map (fun values' -> FValues (name, fieldNames, values')) (Array.mapTaskSync resolveOne values)

    member this.ResolveSelectExpr = resolveSelectExpr

// checkViewExists is supposed to throw an exception if the view doesn't exist -- this way we can distinguish between views that aren't found
// and views that are broken while resolving user views.
let dereferenceViewExpr (checkViewExists : ResolvedUserViewRef -> Task<unit>) (homeSchema : SchemaName option) (expr : ResolvedViewExpr) : Task<ResolvedViewExpr> =
    task {
        let resolver = ReferenceResolver (checkViewExists, homeSchema)
        let! select' = resolver.ResolveSelectExpr expr.select
        return
            { expr with
                  select = select'
            }
    }