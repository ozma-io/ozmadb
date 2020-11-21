module FunWithFlags.FunDB.FunQL.Dereference

open FunWithFlags.FunUtils
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
        let attributes = resolveAttributes result.Attributes
        let result = resolveResultExpr result.Result
        { Attributes = attributes
          Result = result
        }

    and resolveResultExpr : ResolvedQueryResultExpr -> ResolvedQueryResultExpr = function
        | QRExpr (name, e) -> QRExpr (name, resolveFieldExpr e)

    and resolveAttributes (attributes : ResolvedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveFieldExpr expr) attributes

    and resolveFieldExpr : ResolvedFieldExpr -> ResolvedFieldExpr =
        let mapper =
            { idFieldExprMapper id id with
                Value = resolveValue
            }
        mapFieldExpr mapper

    and resolveOrderLimitClause (limits : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
        let resolveOrderBy (ord, expr) = (ord, resolveFieldExpr expr)
        let orderBy = Array.map resolveOrderBy limits.OrderBy
        let limit = Option.map resolveFieldExpr limits.Limit
        let offset = Option.map resolveFieldExpr limits.Offset
        { OrderBy = orderBy
          Limit = limit
          Offset = offset
        }

    and resolveSelectTreeExpr : ResolvedSelectTreeExpr -> ResolvedSelectTreeExpr = function
        | SSelect query -> SSelect <| resolveSingleSelectExpr query
        | SSetOp setOp ->
            SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = resolveSelectExpr setOp.A
                  B = resolveSelectExpr setOp.B
                  OrderLimit = resolveOrderLimitClause setOp.OrderLimit
                }
        | SValues values ->
            let resolveOne = Array.map resolveFieldExpr
            SValues (Array.map resolveOne values)

    and resolveCommonTableExpr (cte : ResolvedCommonTableExpr) : ResolvedCommonTableExpr =
        let expr = resolveSelectExpr cte.Expr
        { Expr = expr
          Fields = cte.Fields
          Extra = cte.Extra
        }

    and resolveCommonTableExprs (ctes : ResolvedCommonTableExprs) : ResolvedCommonTableExprs =
        let exprs = Array.map (fun (name, expr) -> (name, resolveCommonTableExpr expr)) ctes.Exprs
        { Exprs = exprs
          Recursive = ctes.Recursive
          Extra = ctes.Extra
        }

    and resolveSelectExpr (select : ResolvedSelectExpr) : ResolvedSelectExpr =
        let ctes = Option.map resolveCommonTableExprs select.CTEs
        let tree = resolveSelectTreeExpr select.Tree
        { CTEs = ctes
          Tree = tree
          Extra = select.Extra
        }

    and resolveSingleSelectExpr (query : ResolvedSingleSelectExpr) : ResolvedSingleSelectExpr =
            let attributes = resolveAttributes query.Attributes
            let from = Option.map resolveFromExpr query.From
            let where = Option.map resolveFieldExpr query.Where
            let groupBy = Array.map resolveFieldExpr query.GroupBy
            let results = Array.map resolveResult query.Results
            let orderLimit = resolveOrderLimitClause query.OrderLimit
            { Attributes = attributes
              From = from
              Where = where
              GroupBy = groupBy
              Results = results
              OrderLimit = orderLimit
              Extra = query.Extra
            }

    and resolveFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
        | FEntity (pun, name) -> FEntity (pun, name)
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = resolveFromExpr join.A
                  B = resolveFromExpr join.B
                  Condition = resolveFieldExpr join.Condition
                }
        | FSubExpr (name, q) -> FSubExpr (name, resolveSelectExpr q)

    member this.ResolveSelectExpr expr = resolveSelectExpr expr

// checkViewExists is supposed to throw an exception if the view doesn't exist -- this way we can distinguish between views that aren't found
// and views that are broken while resolving user views.
let dereferenceViewExpr (checkViewExists : ResolvedUserViewRef -> unit) (homeSchema : SchemaName option) (expr : ResolvedViewExpr) : ResolvedViewExpr =
    let resolver = ReferenceResolver (checkViewExists, homeSchema)
    let select' = resolver.ResolveSelectExpr expr.Select
    { expr with
          Select = select'
    }