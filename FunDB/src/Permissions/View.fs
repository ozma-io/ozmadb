module FunWithFlags.FunDB.Permissions.View

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Compile
module FunQL = FunWithFlags.FunDB.FunQL.AST

type private PermissionsApplier (layout : Layout, allowedDatabase : AppliedAllowedDatabase, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let compileSingleRestriction (ref : FunQL.ResolvedEntityRef) : ResolvedOptimizedFieldExpr -> CompiledRestriction option = function
        | OFETrue -> None
        | expr ->
            let (newArguments, restr) = compileRestriction layout ref arguments expr
            arguments <- newArguments
            Some restr

    let rec buildSelectUpdateRestriction entityRef =
        let filter = Map.find entityRef allowedDatabase
        compileSingleRestriction entityRef (Option.get filter.SelectUpdate) 
    and getSelectUpdateRestriction = memoizeN buildSelectUpdateRestriction

    let rec buildSelectRestriction entityRef =
        buildSelectUpdateRestriction entityRef |> Option.map (restrictionToSelect entityRef)
    and getSelectRestriction = memoizeN buildSelectRestriction

    let rec buildUpdateRestriction entityRef =
        buildSelectUpdateRestriction entityRef |> Option.map (restrictionToValueExpr layout entityRef)
    and getUpdateRestriction = memoizeN buildUpdateRestriction

    let rec buildDeleteValueRestriction entityRef =
        let filter = Map.find entityRef allowedDatabase
        compileSingleRestriction entityRef (Option.get filter.Delete)
            |> Option.map (restrictionToValueExpr layout entityRef)
    
    and getDeleteRestriction = memoizeN buildDeleteValueRestriction

    let renameValueRestriction (entityRef : FunQL.ResolvedEntityRef) (from : OperationTable) (restr : ValueExpr) =
        let entity = layout.FindEntity entityRef |> Option.get
        let oldTableName = renameResolvedEntityRef entity.Root
        let newTableName =
            match from.Alias with
            | None -> from.Table.Name
            | Some alias -> alias
        let renamesMap = Map.singleton oldTableName newTableName
        renameAllValueExprTables renamesMap restr

    let rec applyToSelectTreeExpr : SelectTreeExpr -> SelectTreeExpr = function
        | SSelect query -> SSelect <| applyToSingleSelectExpr query
        | SSetOp setOp ->
            SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = applyToSelectExpr setOp.A
                  B = applyToSelectExpr setOp.B
                  OrderLimit = applyToOrderLimitClause setOp.OrderLimit
                }
        | SValues values -> SValues values

    and applyToDataExpr : DataExpr -> DataExpr = function
        | DESelect expr -> DESelect (applyToSelectExpr expr)
        | DEInsert expr -> failwith "Not implemented"
        | DEUpdate expr -> failwith "Not implemented"
        | DEDelete expr -> failwith "Not implemented"

    and applyToCommonTableExpr (cte : CommonTableExpr) =
        { Fields = cte.Fields
          Materialized = cte.Materialized
          Expr = applyToDataExpr cte.Expr
        }

    and applyToCommonTableExprs (ctes : CommonTableExprs) =
        { Recursive = ctes.Recursive
          Exprs = Array.map (fun (name, expr) -> (name, applyToCommonTableExpr expr)) ctes.Exprs
        }

    and applyToSelectExpr (select : SelectExpr) : SelectExpr =
        match select.Extra with
        // Special case -- subentity select which gets generated when someone uses subentity in FROM.
        | :? RealEntityAnnotation as ann when not ann.AsRoot ->
            match getSelectRestriction ann.RealEntity with
            | None -> select
            | Some newSelect -> select
        | _ ->
            { CTEs = Option.map applyToCommonTableExprs select.CTEs
              Tree = applyToSelectTreeExpr select.Tree
              Extra = select.Extra
            }

    and applyToSingleSelectExpr (query : SingleSelectExpr) : SingleSelectExpr =
        let from = Option.map applyToFromExpr query.From
        let where = Option.map applyToValueExpr query.Where

        let (from, where) =
            match query.Extra with
            | :? SelectFromInfo as info ->
                let fromVal = Option.get from

                let restrictOne (from, where, joins) (tableName : TableName, entityInfo : FromEntityInfo) =
                    if not entityInfo.IsInner || entityInfo.AsRoot then
                        // We restrict them in FROM expression.
                        (from, where, joins)
                    else
                        match buildSelectUpdateRestriction entityInfo.Ref with
                        | None -> (from, where, joins)
                        | Some restr ->
                            // Rename old table reference in restriction joins and expression.
                            let entity = layout.FindEntity entityInfo.Ref |> Option.get
                            let oldTableName = renameResolvedEntityRef entity.Root
                            let renameJoinKey (key : JoinKey) =
                                if key.Table = oldTableName then
                                    { key with Table = tableName }
                                else
                                    key
                            let restrJoinsMap = Map.mapKeys renameJoinKey restr.Joins.Map
                            let (renamesMap, addedJoins, joins) = augmentJoinPaths joins { restr.Joins with Map = restrJoinsMap }
                            let (entitiesMap, from) = buildJoins layout info.Entities from addedJoins
                            let renamesMap = Map.add oldTableName tableName renamesMap
                            let check = renameAllValueExprTables renamesMap restr.Where

                            let newWhere =
                                match where with
                                | None -> check
                                | Some oldWhere -> VEAnd (oldWhere, check)
                            (from, Some newWhere, joins)

                let (fromVal, where, joins) = info.Entities |> Map.toSeq |> Seq.fold restrictOne (fromVal, where, info.Joins)
                (Some fromVal, where)
            | _ -> (from, where)

        { Columns = Array.map applyToSelectedColumn query.Columns
          From = from
          Where = where
          GroupBy = Array.map applyToValueExpr query.GroupBy
          OrderLimit = applyToOrderLimitClause query.OrderLimit
          Locking = query.Locking
          Extra = query.Extra
        }

    and applyToOrderColumn (col : OrderColumn) : OrderColumn =
        { Expr = applyToValueExpr col.Expr
          Order = col.Order
          Nulls = col.Nulls
        }

    and applyToOrderLimitClause (clause : OrderLimitClause) : OrderLimitClause =
        { Limit = Option.map applyToValueExpr clause.Limit
          Offset = Option.map applyToValueExpr clause.Offset
          OrderBy = Array.map applyToOrderColumn clause.OrderBy
        }

    and applyToSelectedColumn : SelectedColumn -> SelectedColumn = function
        | SCAll _ -> failwith "Unexpected SELECT *"
        | SCExpr (name, expr) -> SCExpr (name, applyToValueExpr expr)

    and applyToValueExpr =
        let mapper = { idValueExprMapper with Query = applyToSelectExpr }
        mapValueExpr mapper

    and applyToFromExpr : FromExpr -> FromExpr = function
        | FTable ({ Extra = :? RealEntityAnnotation as ann } as fTable) when not ann.IsInner && not ann.AsRoot ->
            match getSelectRestriction ann.RealEntity with
            | None -> FTable fTable
            | Some newSelect ->
                // `Alias` is guaranteed to be there for all table queries.
                let subsel = subSelectExpr (Option.get fTable.Alias) newSelect
                FSubExpr subsel
        | FTable fTable -> FTable fTable
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = applyToFromExpr join.A
                  B = applyToFromExpr join.B
                  Condition = applyToValueExpr join.Condition
                }
        | FSubExpr subsel ->
            FSubExpr { subsel with Select = applyToSelectExpr subsel.Select }

    and applyToInsertValue = function
        | IVDefault -> IVDefault
        | IVValue expr -> IVValue <| applyToValueExpr expr

    and applyToInsertExpr (query : InsertExpr) : InsertExpr =
        let source =
            match query.Source with
            | ISDefaultValues -> ISDefaultValues
            | ISSelect expr -> ISSelect <| applyToSelectExpr expr
            | ISValues vals -> ISValues <| Array.map (Array.map applyToInsertValue) vals
        // INSERT permissions are checked when permissions are applied; no need to add any checks here.
        { CTEs = Option.map applyToCommonTableExprs query.CTEs
          Table = query.Table
          Columns = query.Columns
          Source = source
          Returning = query.Returning
          Extra = query.Extra
        }

    and applyToUpdateExpr (query : UpdateExpr) : UpdateExpr =
        let newWhere =
            match query.Extra with
            | :? RealEntityAnnotation as tableInfo ->
                let newWhere = getUpdateRestriction tableInfo.RealEntity
                Option.map (renameValueRestriction tableInfo.RealEntity query.Table) newWhere
            | _ -> None
        let oldWhere = Option.map applyToValueExpr query.Where
        let where = Option.unionWith (curry VEAnd) oldWhere newWhere
        { CTEs = Option.map applyToCommonTableExprs query.CTEs
          Table = query.Table
          Columns = query.Columns
          From = Option.map applyToFromExpr query.From
          Where = where
          Returning = query.Returning
          Extra = query.Extra
        }

    and applyToDeleteExpr (query : DeleteExpr) : DeleteExpr =
        let newWhere =
            match query.Extra with
            | :? RealEntityAnnotation as tableInfo ->
                let newWhere = getDeleteRestriction tableInfo.RealEntity
                Option.map (renameValueRestriction tableInfo.RealEntity query.Table) newWhere
            | _ -> None
        let oldWhere = Option.map applyToValueExpr query.Where
        let where = Option.unionWith (curry VEAnd) oldWhere newWhere
        { CTEs = Option.map applyToCommonTableExprs query.CTEs
          Table = query.Table
          Using = Option.map applyToFromExpr query.Using
          Where = where
          Returning = query.Returning
          Extra = query.Extra
        }

    and applyToDataExpr = function
        | DESelect expr -> DESelect <| applyToSelectExpr expr
        | DEInsert expr -> DEInsert <| applyToInsertExpr expr
        | DEUpdate expr -> DEUpdate <| applyToUpdateExpr expr
        | DEDelete expr -> DEDelete <| applyToDeleteExpr expr

    member this.ApplyToSelectExpr expr = applyToSelectExpr expr
    member this.ApplyToValueExpr expr = applyToValueExpr expr
    member this.ApplyToInsertExpr expr = applyToInsertExpr expr
    member this.ApplyToUpdateExpr expr = applyToUpdateExpr expr
    member this.ApplyToDeleteExpr expr = applyToDeleteExpr expr
    member this.ApplyToDataExpr expr = applyToDataExpr expr

    member this.Arguments = arguments

let private applyRoleQueryExpr (apply : PermissionsApplier -> 'expr -> 'expr) (layout : Layout) (allowedDatabase : AppliedAllowedDatabase) (query : Query<'expr>) : Query<'expr> =
    let applier = PermissionsApplier (layout, allowedDatabase, query.Arguments)
    let expression = apply applier query.Expression
    { Expression = expression
      Arguments = applier.Arguments
    }

let applyRoleSelectExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToSelectExpr)
// let applyRoleInsertExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToInsertExpr)
let applyRoleUpdateExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToUpdateExpr)
let applyRoleDeleteExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToDeleteExpr)
let applyRoleDataExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToDataExpr)

let applyRoleViewExpr (layout : Layout) (allowedDatabase : AppliedAllowedDatabase) (view : CompiledViewExpr) : CompiledViewExpr =
    let applier = PermissionsApplier (layout, allowedDatabase, view.Query.Arguments)
    let queryExpression = applier.ApplyToSelectExpr view.Query.Expression
    let newQuery =
        { Expression = queryExpression
          Arguments = applier.Arguments
        }
    let mapAttributeColumn (typ, name, expr) =
        let expr = applier.ApplyToValueExpr expr
        (typ, name, expr)
    { view with
          AttributesQuery = { view.AttributesQuery with AttributeColumns = Array.map mapAttributeColumn view.AttributesQuery.AttributeColumns }
          Query = newQuery
    }
