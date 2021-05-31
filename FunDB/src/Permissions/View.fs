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

type PermissionsViewException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsViewException (message, null)

type private FieldAccess = CompiledRestriction option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

type private FieldTempAccess = Map<FunQL.ResolvedEntityRef, ResolvedOptimizedFieldExpr>

type private EntityTempAccess = Map<FunQL.EntityName, ResolvedOptimizedFieldExpr>
type private SchemaTempAccess = Map<FunQL.SchemaName, EntityTempAccess>

type private AccessAggregator (layout : Layout, role : ResolvedRole) =
    let mutable fieldAccesses = Map.empty : FieldTempAccess

    let filterUsedFields (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) (usedFields : FunQL.UsedFields) : ResolvedOptimizedFieldExpr =
        let flattened =
            match Map.tryFind entity.Root role.Flattened with
            | Some f -> f
            | None -> raisef PermissionsViewException "Access denied to entity %O" ref
        let accessor (derived : FlatAllowedDerivedEntity) = derived.Select
        let selectRestr = applyRestrictionExpression accessor layout flattened ref

        let fieldsRestriction = Map.tryFind entity.Root fieldAccesses |> Option.defaultValue OFETrue
        let addRestriction restriction name =
            if name = FunQL.funId || name = FunQL.funSubEntity then
                restriction
            else
                let field = Map.find name entity.ColumnFields
                let parentEntity = Option.defaultValue ref field.InheritedFrom
                match Map.tryFind ({ Entity = parentEntity; Name = name } : FunQL.ResolvedFieldRef) flattened.Fields with
                | Some r -> andFieldExpr restriction r.Select
                | _ -> raisef PermissionsViewException "Access denied to select field %O" name
        let fieldsRestriction = usedFields |> Set.toSeq |> Seq.fold addRestriction fieldsRestriction

        match fieldsRestriction with
        | OFEFalse -> raisef PermissionsViewException "Access denied to select"
        | _ ->
            fieldAccesses <- Map.add entity.Root fieldsRestriction fieldAccesses
            selectRestr

    let filterUsedEntities (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (usedEntities : FunQL.UsedEntities) : EntityTempAccess =
        let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
            let entity = Map.find name schema.Entities
            let ref = { Schema = schemaName; Name = name } : FunQL.ResolvedEntityRef
            try
                filterUsedFields ref entity usedFields
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for entity %O: %s" name e.Message

        Map.map mapEntity usedEntities

    let filterUsedSchemas (usedSchemas : FunQL.UsedSchemas) : SchemaTempAccess =
        let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
            let schema = Map.find name layout.Schemas
            try
                filterUsedEntities name schema usedEntities
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for schema %O: %s" name e.Message

        Map.map mapSchema usedSchemas
    
    member this.FieldAccesses = fieldAccesses
    member this.FilterUsedSchemas usedSchemas = filterUsedSchemas usedSchemas

type private AccessCompiler (layout : Layout, fieldAccesses : FieldTempAccess, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let compileRestriction (entity : ResolvedEntity) (ref : FunQL.ResolvedEntityRef) (restr : ResolvedOptimizedFieldExpr) : FieldAccess =
        let fieldAccess = Map.find entity.Root fieldAccesses
        match andFieldExpr restr fieldAccess with
        | OFETrue -> None
        | finalRestr ->
            let (newArguments, restriction) = compileRestriction layout ref arguments finalRestr
            arguments <- newArguments
            Some restriction

    let compileEntityAccess (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (entityAccess : EntityTempAccess) : EntityAccess =
        let mapEntity (name : FunQL.EntityName) (restr : ResolvedOptimizedFieldExpr) =
            let entity = Map.find name schema.Entities
            let ref = { Schema = schemaName; Name = name } : FunQL.ResolvedEntityRef
            compileRestriction entity ref restr

        Map.map mapEntity entityAccess

    let compileSchemaAccess (schemaAccess : SchemaTempAccess) : SchemaAccess =
        let mapSchema (name : FunQL.SchemaName) (entityAccess : EntityTempAccess) =
            let schema = Map.find name layout.Schemas
            compileEntityAccess name schema entityAccess

        Map.map mapSchema schemaAccess
    
    member this.Arguments = arguments
    member this.CompileSchemaAccess schemaAccess = compileSchemaAccess schemaAccess

let private compileRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (arguments : QueryArguments) : QueryArguments * SchemaAccess =
    let accessAgg = AccessAggregator (layout, role)
    let halfAccess = accessAgg.FilterUsedSchemas usedSchemas

    let accessCompiler = AccessCompiler (layout, accessAgg.FieldAccesses, arguments)
    let access = accessCompiler.CompileSchemaAccess halfAccess
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

    and applyToCommonTableExpr (cte : CommonTableExpr) =
        { Fields = cte.Fields
          Materialized = cte.Materialized
          Expr = applyToSelectExpr cte.Expr
        }

    and applyToCommonTableExprs (ctes : CommonTableExprs) =
        { Recursive = ctes.Recursive
          Exprs = Array.map (fun (name, expr) -> (name, applyToCommonTableExpr expr)) ctes.Exprs
        }

    and applyToSelectExpr (select : SelectExpr) : SelectExpr =
        match select.Extra with
        // Special case -- subentity select which gets generated when someone uses subentity in FROM.
        | :? RealEntityAnnotation as ann ->
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
                    if not entityInfo.IsInner then
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
          Extra = query.Extra
        }

    and applyToOrderLimitClause (clause : OrderLimitClause) : OrderLimitClause =
        { Limit = Option.map applyToValueExpr clause.Limit
          Offset = Option.map applyToValueExpr clause.Offset
          OrderBy = Array.map (fun (ord, expr) -> (ord, applyToValueExpr expr)) clause.OrderBy
        }

    and applyToSelectedColumn : SelectedColumn -> SelectedColumn = function
        | SCAll _ -> failwith "Unexpected SELECT *"
        | SCExpr (name, expr) -> SCExpr (name, applyToValueExpr expr)

    and applyToValueExpr =
        let mapper = { idValueExprMapper with Query = applyToSelectExpr }
        mapValueExpr mapper

    and applyToFromExpr : FromExpr -> FromExpr = function
        | FTable (:? RealEntityAnnotation as ann, pun, entity) when not ann.IsInner ->
            let accessSchema = Map.find ann.RealEntity.Schema access
            let accessEntity = Map.find ann.RealEntity.Name accessSchema
            match accessEntity with
            | None -> FTable (ann, pun, entity)
            | Some restr ->
                // `pun` is guaranteed to be there for all table queries.
                FSubExpr (Option.get pun, restrictionToSelect ann.RealEntity restr)
        // From CTE.
        | FTable (extra, pun, entity) -> FTable (extra, pun, entity)
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = applyToFromExpr join.A
                  B = applyToFromExpr join.B
                  Condition = applyToValueExpr join.Condition
                }
        | FSubExpr (alias, q) ->
            FSubExpr (alias, applyToSelectExpr q)

    member this.ApplyToSelectExpr = applyToSelectExpr
    member this.ApplyToValueExpr = applyToValueExpr

let applyRoleQueryExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (query : Query<SelectExpr>) : Query<SelectExpr> =
    let (arguments, access) = compileRoleViewExpr layout role usedSchemas query.Arguments
    let applier = PermissionsApplier (layout, access)
    let expression = applier.ApplyToSelectExpr query.Expression
    { Expression = expression
      Arguments = arguments
    }

let checkRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (expr : CompiledViewExpr) : unit =
    let accessAgg = AccessAggregator (layout, role)
    let halfAccess = accessAgg.FilterUsedSchemas usedSchemas
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
          AttributesQuery = Option.map (fun info -> { info with AttributeColumns = Array.map mapAttributeColumn info.AttributeColumns }) view.AttributesQuery
          Query = newQuery
    }
