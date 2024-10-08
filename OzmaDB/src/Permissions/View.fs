module OzmaDB.Permissions.View

open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.SQL.AST
open OzmaDB.SQL.Rename
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Layout.Types
open OzmaDB.Permissions.Apply
open OzmaDB.Permissions.Resolve
open OzmaDB.Permissions.Compile

module OzmaQL = OzmaDB.OzmaQL.AST

type private PermissionsApplier
    (layout: Layout, allowedDatabase: AppliedAllowedDatabase, initialArguments: QueryArguments) =
    let mutable arguments = initialArguments

    let compileSingleRestriction
        (ref: OzmaQL.ResolvedEntityRef)
        (expr: OzmaQL.ResolvedFieldExpr)
        : CompiledRestriction =
        let (newArguments, restr) = compileRestriction layout ref arguments expr
        arguments <- newArguments
        restr

    let rec buildSelectUpdateRestriction entityRef =
        let filter = Map.find entityRef allowedDatabase

        filter.SelectUpdate
        |> Option.get
        |> filterToOption
        |> Option.map (compileSingleRestriction entityRef)

    and getSelectUpdateRestriction = memoizeN buildSelectUpdateRestriction

    let rec buildSelectRestriction entityRef =
        buildSelectUpdateRestriction entityRef
        |> Option.map (restrictionToSelect entityRef)

    and getSelectRestriction = memoizeN buildSelectRestriction

    let getUpdateValueRestriction entityRef newTableName =
        buildSelectUpdateRestriction entityRef
        |> Option.map (restrictionToValueExpr entityRef newTableName)

    let rec buildDeleteRestriction entityRef =
        let filter = Map.find entityRef allowedDatabase

        filter.Delete
        |> Option.get
        |> filterToOption
        |> Option.map (compileSingleRestriction entityRef)

    and getDeleteRestriction = memoizeN buildDeleteRestriction

    let getDeleteValueRestriction entityRef newTableName =
        getDeleteRestriction entityRef
        |> Option.map (restrictionToValueExpr entityRef newTableName)

    let rec applyToSelectTreeExpr: SelectTreeExpr -> SelectTreeExpr =
        function
        | SSelect query -> SSelect <| applyToSingleSelectExpr query
        | SSetOp setOp ->
            SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = applyToSelectExpr setOp.A
                  B = applyToSelectExpr setOp.B
                  OrderLimit = applyToOrderLimitClause setOp.OrderLimit }
        | SValues values -> SValues values

    and applyToCommonTableExpr (cte: CommonTableExpr) =
        { Fields = cte.Fields
          Materialized = cte.Materialized
          Expr = applyToDataExpr cte.Expr }

    and applyToCommonTableExprs (ctes: CommonTableExprs) =
        { Recursive = ctes.Recursive
          Exprs = Array.map (fun (name, expr) -> (name, applyToCommonTableExpr expr)) ctes.Exprs }

    and applyToSelectExpr (select: SelectExpr) : SelectExpr =
        match select.Extra with
        // Special case -- subentity select which gets generated when someone uses subentity in FROM.
        | :? RealEntityAnnotation as ann when not ann.AsRoot ->
            match getSelectRestriction ann.RealEntity with
            | None -> select
            | Some newSelect -> newSelect
        | _ ->
            { CTEs = Option.map applyToCommonTableExprs select.CTEs
              Tree = applyToSelectTreeExpr select.Tree
              Extra = select.Extra }

    and applyToSingleSelectExpr (query: SingleSelectExpr) : SingleSelectExpr =
        let from = Option.map applyToFromExpr query.From

        let (from, where) =
            match query.Extra with
            | :? SelectFromInfo as info ->
                let fromVal = Option.get from

                let restrictOne
                    (from, where: ValueExpr option, joins)
                    (tableName: TableName, entityInfo: FromEntityInfo)
                    =
                    if entityInfo.AsRoot then
                        let newWhere = Option.unionWith (curry VEAnd) where entityInfo.Check
                        (from, newWhere, joins)
                    else if not entityInfo.IsInner then
                        // We restrict them in FROM expression.
                        let newWhere = Option.unionWith (curry VEAnd) where entityInfo.Check
                        (from, newWhere, joins)
                    else
                        match buildSelectUpdateRestriction entityInfo.Ref with
                        | None ->
                            let newWhere = Option.unionWith (curry VEAnd) where entityInfo.Check
                            (from, newWhere, joins)
                        | Some restr ->
                            // Rename old table reference in restriction joins and expression.
                            let renameJoinKey (key: JoinKey) =
                                if key.Table = restrictedTableRef.Name then
                                    { key with Table = tableName }
                                else
                                    key

                            let restrJoinsMap = Map.mapKeys renameJoinKey restr.Joins.Map

                            let (renamesMap, addedJoins, joins) =
                                augmentJoinPaths joins { restr.Joins with Map = restrJoinsMap }

                            let (entitiesMap, from) = buildJoins layout info.Entities from addedJoins
                            let renamesMap = Map.add restrictedTableRef.Name tableName renamesMap
                            let check = naiveRenameTablesExpr renamesMap restr.Where
                            // We flip here so that check is added to the right. It is important to put user conditions first,
                            // so that we won't slow down the query when PostgreSQL can't find out priority by itself.
                            // For example:
                            // > foo IN (SELECT small_select UNION SELECT 1) AND (slow permissions check)
                            // Works better than when they are flipped, because PostgreSQL can't always determine which one of
                            // those SELECTs are faster.
                            // Case:
                            // http://localhost:8080/views/pm/actions_for_contact_table_conn?id=3072692
                            // user.Manager
                            let newWhere = Option.addWith (flip (curry VEAnd)) check where
                            (from, Some newWhere, joins)

                let where = Option.map applyToValueExpr info.WhereWithoutSubentities

                let (fromVal, where, joins) =
                    info.Entities |> Map.toSeq |> Seq.fold restrictOne (fromVal, where, info.Joins)

                (Some fromVal, where)
            | :? NoSelectFromInfo ->
                let where = Option.map applyToValueExpr query.Where
                (from, where)
            | _ -> failwith "SELECT annotation is required"

        { Columns = Array.map applyToSelectedColumn query.Columns
          From = from
          Where = where
          GroupBy = Array.map applyToValueExpr query.GroupBy
          OrderLimit = applyToOrderLimitClause query.OrderLimit
          Locking = query.Locking
          Extra = query.Extra }

    and applyToOrderColumn (col: OrderColumn) : OrderColumn =
        { Expr = applyToValueExpr col.Expr
          Order = col.Order
          Nulls = col.Nulls }

    and applyToOrderLimitClause (clause: OrderLimitClause) : OrderLimitClause =
        { Limit = Option.map applyToValueExpr clause.Limit
          Offset = Option.map applyToValueExpr clause.Offset
          OrderBy = Array.map applyToOrderColumn clause.OrderBy }

    and applyToSelectedColumn: SelectedColumn -> SelectedColumn =
        function
        | SCAll _ -> failwith "Unexpected SELECT *"
        | SCExpr(name, expr) -> SCExpr(name, applyToValueExpr expr)

    and applyToValueExpr =
        let mapper =
            { idValueExprMapper with
                Query = applyToSelectExpr }

        mapValueExpr mapper

    and applyToTableExpr: TableExpr -> TableExpr =
        function
        | TESelect subsel -> TESelect <| applyToSelectExpr subsel
        | TEFunc(name, args) -> TEFunc(name, Array.map applyToValueExpr args)

    and applyToFromTableExpr (expr: FromTableExpr) : FromTableExpr =
        { Expression = applyToTableExpr expr.Expression
          Alias = expr.Alias
          Lateral = expr.Lateral }

    and applyToFromExpr: FromExpr -> FromExpr =
        function
        | FTable({ Extra = :? RealEntityAnnotation as ann } as fTable) ->
            if ann.IsInner || ann.AsRoot then
                FTable fTable
            else
                match getSelectRestriction ann.RealEntity with
                | None -> FTable fTable
                | Some newSelect ->
                    // `Alias` is guaranteed to be there for all table queries.
                    let newExpr = subSelectExpr (Option.get fTable.Alias) newSelect
                    FTableExpr newExpr
        | FTable({ Extra = :? NoEntityAnnotation } as fTable) -> FTable fTable
        | FTable fTable -> failwith "Entity annotation is required"
        | FTableExpr expr -> FTableExpr <| applyToFromTableExpr expr
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = applyToFromExpr join.A
                  B = applyToFromExpr join.B
                  Condition = applyToValueExpr join.Condition }

    and applyToInsertValue =
        function
        | IVDefault -> IVDefault
        | IVExpr expr -> IVExpr <| applyToValueExpr expr

    and applyToUpdateAssignExpr =
        function
        | UAESet(name, expr) -> UAESet(name, applyToInsertValue expr)
        | UAESelect(cols, select) -> UAESelect(cols, applyToSelectExpr select)

    and applyToUpdateConflictAction (update: UpdateConflictAction) : UpdateConflictAction =
        { Assignments = Array.map applyToUpdateAssignExpr update.Assignments
          Where = Option.map applyToValueExpr update.Where }

    and applyToConflictExpr (conflict: OnConflictExpr) : OnConflictExpr =
        let action =
            match conflict.Action with
            | CANothing -> CANothing
            | CAUpdate update -> CAUpdate <| applyToUpdateConflictAction update

        { Target = conflict.Target
          Action = action }

    and applyToInsertExpr (query: InsertExpr) : InsertExpr =
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
          OnConflict = Option.map applyToConflictExpr query.OnConflict
          Extra = query.Extra }

    and applyToUpdateExpr (query: UpdateExpr) : UpdateExpr =
        let newWhere =
            match query.Table.Extra with
            | :? RealEntityAnnotation as tableInfo ->
                let newTableName = (operationTableRef query.Table).Name
                getUpdateValueRestriction tableInfo.RealEntity newTableName
            | :? NoEntityAnnotation -> None
            | _ -> failwith "Entity annotation is required"

        let oldWhere =
            match query.Extra with
            | :? UpdateFromInfo as tableInfo -> tableInfo.WhereWithoutSubentities
            | _ -> query.Where

        let oldWhere = Option.map applyToValueExpr oldWhere
        let where = Option.unionWith (curry VEAnd) oldWhere newWhere

        { CTEs = Option.map applyToCommonTableExprs query.CTEs
          Table = query.Table
          Assignments = Array.map applyToUpdateAssignExpr query.Assignments
          From = Option.map applyToFromExpr query.From
          Where = where
          Returning = query.Returning
          Extra = query.Extra }

    and applyToDeleteExpr (query: DeleteExpr) : DeleteExpr =
        let newWhere =
            match query.Table.Extra with
            | :? RealEntityAnnotation as tableInfo ->
                let newTableName = (operationTableRef query.Table).Name
                getDeleteValueRestriction tableInfo.RealEntity newTableName
            | :? NoEntityAnnotation -> None
            | _ -> failwith "Entity annotation is required"

        let oldWhere =
            match query.Extra with
            | :? UpdateFromInfo as tableInfo -> tableInfo.WhereWithoutSubentities
            | _ -> query.Where

        let oldWhere = Option.map applyToValueExpr oldWhere
        let where = Option.unionWith (curry VEAnd) oldWhere newWhere

        { CTEs = Option.map applyToCommonTableExprs query.CTEs
          Table = query.Table
          Using = Option.map applyToFromExpr query.Using
          Where = where
          Returning = query.Returning
          Extra = query.Extra }

    and applyToDataExpr =
        function
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

let private applyRoleQueryExpr
    (apply: PermissionsApplier -> 'expr -> 'expr)
    (layout: Layout)
    (allowedDatabase: AppliedAllowedDatabase)
    (query: Query<'expr>)
    : Query<'expr> =
    let applier = PermissionsApplier(layout, allowedDatabase, query.Arguments)
    let expression = apply applier query.Expression

    { Expression = expression
      Arguments = applier.Arguments }

let applyRoleSelectExpr =
    applyRoleQueryExpr (fun applier -> applier.ApplyToSelectExpr)
// let applyRoleInsertExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToInsertExpr)
let applyRoleUpdateExpr =
    applyRoleQueryExpr (fun applier -> applier.ApplyToUpdateExpr)

let applyRoleDeleteExpr =
    applyRoleQueryExpr (fun applier -> applier.ApplyToDeleteExpr)

let applyRoleDataExpr = applyRoleQueryExpr (fun applier -> applier.ApplyToDataExpr)

let applyRoleViewExpr
    (layout: Layout)
    (allowedDatabase: AppliedAllowedDatabase)
    (view: CompiledViewExpr)
    : CompiledViewExpr =
    let applier = PermissionsApplier(layout, allowedDatabase, view.Query.Arguments)
    let queryExpression = applier.ApplyToSelectExpr view.Query.Expression

    let mapSingleRowColumn (info, expr) =
        let expr = applier.ApplyToValueExpr expr
        (info, expr)

    let singleRowQuery =
        { view.SingleRowQuery with
            SingleRowColumns = Array.map mapSingleRowColumn view.SingleRowQuery.SingleRowColumns }

    let newQuery =
        { Expression = queryExpression
          Arguments = applier.Arguments }

    { view with
        SingleRowQuery = singleRowQuery
        Query = newQuery }
