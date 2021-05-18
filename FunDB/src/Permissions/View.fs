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

type private FieldAccess = SelectExpr option
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
            let select =
                { CTEs = None
                  Tree = SSelect restriction
                  Extra = null
                }
            arguments <- newArguments
            Some select

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

type private PermissionsApplier (access : SchemaAccess) =
    let mutable subEntityCTEs = Map.empty : Map<TableName, CommonTableExpr>
    let newSubEntityFrom (entityRef : FunQL.ResolvedEntityRef) (restr : SelectExpr) : TableRef =
        let newName = renameResolvedEntityRef entityRef
        if not <| Map.containsKey newName subEntityCTEs then
            let cte =
                { Fields = None
                  Materialized = Some false
                  Expr = restr
                } : CommonTableExpr
            eprintfn "New CTE: %O" cte
            subEntityCTEs <- Map.add newName cte subEntityCTEs

        { Schema = None; Name = newName }

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
            | Some restr -> restr
        | _ ->
            { CTEs = Option.map applyToCommonTableExprs select.CTEs
              Tree = applyToSelectTreeExpr select.Tree
              Extra = select.Extra
            }

    and applyToSingleSelectExpr (query : SingleSelectExpr) : SingleSelectExpr =
        { Columns = Array.map applyToSelectedColumn query.Columns
          From = Option.map applyToFromExpr query.From
          Where = Option.map applyToValueExpr query.Where
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
        | FTable (extra, pun, entity) ->
            match extra with
            | :? RealEntityAnnotation as ann ->
                let accessSchema = Map.find ann.RealEntity.Schema access
                let accessEntity = Map.find ann.RealEntity.Name accessSchema
                match accessEntity with
                | None -> FTable (null, pun, entity)
                | Some restr ->
                    let subRef = newSubEntityFrom ann.RealEntity restr
                    eprintfn "Restricting %O" ann.RealEntity
                    FTable (null, pun, subRef)
            // From CTE.
            | _ -> FTable (extra, pun, entity)
        | FJoin join ->
            FJoin
                { Type = join.Type
                  A = applyToFromExpr join.A
                  B = applyToFromExpr join.B
                  Condition = applyToValueExpr join.Condition
                }
        | FSubExpr (alias, q) ->
            FSubExpr (alias, applyToSelectExpr q)

    member this.ApplyToSelectExpr expr = applyToSelectExpr expr
    member this.ApplyToValueExpr expr = applyToValueExpr expr
    member this.SubEntityCTEs = subEntityCTEs

let applyRoleQueryExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (query : Query<SelectExpr>) : Query<SelectExpr> =
    let (arguments, access) = compileRoleViewExpr layout role usedSchemas query.Arguments
    let applier = PermissionsApplier access
    let expression = applier.ApplyToSelectExpr query.Expression
    { Expression = addTopLevelCTEs (Map.toSeq applier.SubEntityCTEs) expression
      Arguments = arguments
    }

let checkRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (expr : CompiledViewExpr) : unit =
    let accessAgg = AccessAggregator (layout, role)
    let halfAccess = accessAgg.FilterUsedSchemas usedSchemas
    ()

let applyRoleViewExpr (layout : Layout) (role : ResolvedRole) (usedSchemas : FunQL.UsedSchemas) (view : CompiledViewExpr) : CompiledViewExpr =
    let (arguments, access) = compileRoleViewExpr layout role usedSchemas view.Query.Arguments
    let applier = PermissionsApplier access
    let queryExpression = applier.ApplyToSelectExpr view.Query.Expression
    let mapAttributeColumn (typ, name, expr) =
        let expr = applier.ApplyToValueExpr expr
        (typ, name, expr)
    let mapAttributesQuery query =
        { query with
              AttributeColumns = Array.map mapAttributeColumn query.AttributeColumns
              CTEs = addCTEs (Map.toSeq applier.SubEntityCTEs) query.CTEs
        }
    let attributesQuery = Option.map mapAttributesQuery view.AttributesQuery
    let newQuery =
        { Expression = addTopLevelCTEs (Map.toSeq applier.SubEntityCTEs) queryExpression
          Arguments = arguments
        }
    { view with
          AttributesQuery = attributesQuery
          Query = newQuery
    }
