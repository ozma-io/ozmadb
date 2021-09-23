module FunWithFlags.FunDB.Permissions.View

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Compile
module FunQL = FunWithFlags.FunDB.FunQL.AST

type private FieldAccess = CompiledRestriction option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

type private AccessCompiler (layout : Layout, role : ResolvedRole, fieldAccesses : FilteredAllowedDatabase, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let compileRestriction (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) : FieldAccess =
        let childrenFieldAccesses = Map.find entity.Root fieldAccesses
        let entityAccesses = Map.find entity.Root role.Flattened.Entities
        let applied = applyRestrictionExpression (fun access -> access.Select) layout entityAccesses childrenFieldAccesses ref
        match applied with
        | OFETrue -> None
        | _ ->
            let (newArguments, restriction) = compileRestriction layout ref arguments applied
            arguments <- newArguments
            Some restriction

    let compileEntityAccess (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (usedEntities : FunQL.UsedEntities) : EntityAccess =
        let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
            let entity = Map.find name schema.Entities
            let ref = { Schema = schemaName; Name = name } : FunQL.ResolvedEntityRef
            compileRestriction ref entity

        Map.map mapEntity usedEntities

    let compileSchemaAccess (usedSchemas : FunQL.UsedSchemas) : SchemaAccess =
        let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
            let schema = Map.find name layout.Schemas
            compileEntityAccess name schema usedEntities

        Map.map mapSchema usedSchemas

    member this.Arguments = arguments
    member this.CompileSchemaAccess usedSchemas = compileSchemaAccess usedSchemas

// Can throw PermissionsApplyException.
let private compileRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (arguments : QueryArguments) : QueryArguments * SchemaAccess =
    let filteredAccess = filterAccessForUsedSchemas (fun field -> field.Select) layout role usedSchemas
    let accessCompiler = AccessCompiler (layout, role, filteredAccess, arguments)
    let access = accessCompiler.CompileSchemaAccess usedSchemas
    (accessCompiler.Arguments, access)

type private PermissionsApplier (layout : Layout, access : SchemaAccess) =
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
            let accessSchema = Map.find ann.RealEntity.Schema access
            let accessEntity = Map.find ann.RealEntity.Name accessSchema
            match accessEntity with
            | None -> select
            | Some restr -> restrictionToSelect ann.RealEntity restr
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
                        let accessSchema = Map.find entityInfo.Ref.Schema access
                        let accessEntity = Map.find entityInfo.Ref.Name accessSchema
                        match accessEntity with
                        | None -> (from, where, joins)
                        | Some restr ->
                            // Rename old table reference in restriction joins and expression.
                            let oldTableName = renameResolvedEntityRef entityInfo.Ref
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
        | FTable (:? RealEntityAnnotation as ann, pun, entity) when not ann.IsInner && not ann.AsRoot ->
            let accessSchema = Map.find ann.RealEntity.Schema access
            let accessEntity = Map.find ann.RealEntity.Name accessSchema
            match accessEntity with
            | None -> FTable (ann, pun, entity)
            | Some restr ->
                // `pun` is guaranteed to be there for all table queries.
                let subsel =
                    { Alias = Option.get pun
                      Select = restrictionToSelect ann.RealEntity restr
                      Lateral = false
                    }
                FSubExpr subsel
        | FTable (extra, pun, entity) -> FTable (extra, pun, entity)
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = applyToFromExpr join.A
                  B = applyToFromExpr join.B
                  Condition = applyToValueExpr join.Condition
                }
        | FSubExpr subsel ->
            FSubExpr { subsel with Select = applyToSelectExpr subsel.Select }

    member this.ApplyToSelectExpr = applyToSelectExpr
    member this.ApplyToValueExpr = applyToValueExpr

let applyRoleQueryExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (query : Query<SelectExpr>) : Query<SelectExpr> =
    let (arguments, access) = compileRoleViewExpr layout role usedSchemas query.Arguments
    let applier = PermissionsApplier (layout, access)
    let expression = applier.ApplyToSelectExpr query.Expression
    { Expression = expression
      Arguments = arguments
    }

let checkRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) : unit =
    let filteredAccess = filterAccessForUsedSchemas (fun field -> field.Select) layout role usedSchemas
    ()

let applyRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (view : CompiledViewExpr) : CompiledViewExpr =
    let (arguments, access) = compileRoleViewExpr layout role usedSchemas view.Query.Arguments
    let applier = PermissionsApplier (layout, access)
    let queryExpression = applier.ApplyToSelectExpr view.Query.Expression
    let newQuery =
        { Expression = queryExpression
          Arguments = arguments
        }
    let mapAttributeColumn (typ, name, expr) =
        let expr = applier.ApplyToValueExpr expr
        (typ, name, expr)
    { view with
          AttributesQuery = { view.AttributesQuery with AttributeColumns = Array.map mapAttributeColumn view.AttributesQuery.AttributeColumns }
          Query = newQuery
    }
