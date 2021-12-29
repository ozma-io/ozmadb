module FunWithFlags.FunDB.FunQL.Dereference

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve

type ViewDereferenceException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ViewDereferenceException (message, innerException, isUserException innerException)

    new (message : string) = ViewDereferenceException (message, null, true)

type private ReferenceResolver (checkViewExists : ResolvedUserViewRef -> unit, homeSchema : SchemaName option) =
    let resolveRef (ref : UserViewRef) : UserViewRef =
        let schemaName =
            match ref.Schema with
            | None ->
                match homeSchema with
                | None -> raisef ViewDereferenceException "No default schema for user view reference %O" ref
                | Some r -> r
            | Some schemaName -> schemaName
        checkViewExists { Schema = schemaName; Name = ref.Name }
        { Schema = Some schemaName; Name = ref.Name }

    let resolveValue (value : FieldValue) : FieldValue =
        match value with
        | FUserViewRef ref ->
            let r = resolveRef ref
            FUserViewRef r
        | FUserViewRefArray vals ->
            let arr = vals |> Seq.map resolveRef |> Seq.toArray
            FUserViewRefArray arr
        | v -> v

    let rec resolveResult : ResolvedQueryResult -> ResolvedQueryResult = function
        | QRAll alias -> QRAll alias
        | QRExpr result -> QRExpr <| resolveColumnResult result

    and resolveColumnResult (result : ResolvedQueryColumnResult) : ResolvedQueryColumnResult =
        let attributes = resolveAttributes result.Attributes
        let expr = resolveFieldExpr result.Result
        { Alias = result.Alias
          Attributes = attributes
          Result = expr
        }

    and resolveAttributes (attributes : ResolvedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveFieldExpr expr) attributes

    and resolveFieldExpr (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
        let mapper =
            { queryFieldExprMapper id resolveSelectExpr with
                Value = resolveValue
            }
        mapFieldExpr mapper expr

    and resolveOrderColumn (ord : ResolvedOrderColumn) : ResolvedOrderColumn =
        let expr = resolveFieldExpr ord.Expr
        { Expr = expr
          Order = ord.Order
          Nulls = ord.Nulls
        }

    and resolveOrderLimitClause (limits : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
        let orderBy = Array.map resolveOrderColumn limits.OrderBy
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
          Materialized = cte.Materialized
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
        | FEntity ent -> FEntity ent
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = resolveFromExpr join.A
                  B = resolveFromExpr join.B
                  Condition = resolveFieldExpr join.Condition
                }
        | FSubExpr subsel -> FSubExpr { subsel with Select = resolveSelectExpr subsel.Select }

    and resolveInsertValue : ResolvedInsertValue -> ResolvedInsertValue = function
        | IVDefault -> IVDefault
        | IVValue expr -> IVValue <| resolveFieldExpr expr

    and resolveInsertSource : ResolvedInsertSource -> ResolvedInsertSource = function
        | ISDefaultValues -> ISDefaultValues
        | ISSelect select -> ISSelect <| resolveSelectExpr select
        | ISValues vals -> ISValues <| Array.map (Array.map resolveInsertValue) vals

    and resolveInsertExpr (insert : ResolvedInsertExpr) : ResolvedInsertExpr =
        let ctes = Option.map resolveCommonTableExprs insert.CTEs
        let source = resolveInsertSource insert.Source
        { CTEs = ctes
          Entity = insert.Entity
          Fields = insert.Fields
          Source = source
          Extra = insert.Extra
        }

    and resolveUpdateAssignExpr = function
        | UAESet (name, expr) -> UAESet (name, resolveInsertValue expr)
        | UAESelect (cols, select) -> UAESelect (cols, resolveSelectExpr select)

    and resolveUpdateExpr (update : ResolvedUpdateExpr) : ResolvedUpdateExpr =
        let ctes = Option.map resolveCommonTableExprs update.CTEs
        let assigns = Array.map resolveUpdateAssignExpr update.Assignments
        { CTEs = ctes
          Entity = update.Entity
          Assignments = assigns
          From = Option.map resolveFromExpr update.From
          Where = Option.map resolveFieldExpr update.Where
          Extra = update.Extra
        }

    and resolveDeleteExpr (delete : ResolvedDeleteExpr) : ResolvedDeleteExpr =
        let ctes = Option.map resolveCommonTableExprs delete.CTEs
        { CTEs = ctes
          Entity = delete.Entity
          Using = Option.map resolveFromExpr delete.Using
          Where = Option.map resolveFieldExpr delete.Where
          Extra = delete.Extra
        }

    and resolveDataExpr : ResolvedDataExpr -> ResolvedDataExpr = function
        | DESelect select -> DESelect <| resolveSelectExpr select
        | DEInsert insert -> DEInsert <| resolveInsertExpr insert
        | DEUpdate update -> DEUpdate <| resolveUpdateExpr update
        | DEDelete delete -> DEDelete <| resolveDeleteExpr delete

    let resolveArgument (arg : ResolvedArgument) : ResolvedArgument =
        { arg with Attributes = resolveAttributes arg.Attributes }

    let resolveArgumentsMap (argsMap : ResolvedArgumentsMap) : ResolvedArgumentsMap =
        Map.map (fun name -> resolveArgument) argsMap

    member this.ResolveSelectExpr expr = resolveSelectExpr expr
    member this.ResolveArgumentsMap argsMap = resolveArgumentsMap argsMap

// checkViewExists is supposed to throw an exception if the view doesn't exist -- this way we can distinguish between views that aren't found
// and views that are broken while resolving user views.
let dereferenceViewExpr (checkViewExists : ResolvedUserViewRef -> unit) (homeSchema : SchemaName option) (expr : ResolvedViewExpr) : ResolvedViewExpr =
    let resolver = ReferenceResolver (checkViewExists, homeSchema)
    let args = resolver.ResolveArgumentsMap expr.Arguments
    let select = resolver.ResolveSelectExpr expr.Select
    { expr with
          Arguments = args
          Select = select
    }