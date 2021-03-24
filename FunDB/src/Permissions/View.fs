module FunWithFlags.FunDB.Permissions.View

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.Permissions.Compile
module FunQL = FunWithFlags.FunDB.FunQL.AST

type PermissionsViewException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsViewException (message, null)

type private UsedArguments = Set<FunQL.ArgumentName>
type private FieldAccess = SelectExpr option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

type private AccessCompiler (layout : Layout, role : ResolvedRole, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let filterUsedFields (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) (usedFields : FunQL.UsedFields) : FieldAccess =
        let flattened =
            match Map.tryFind entity.Root role.Flattened with
            | Some f -> f
            | None -> raisef PermissionsViewException "Access denied to entity %O" ref
        let accessor (derived : FlatAllowedDerivedEntity) = derived.Select
        let selectRestr = applyRestrictionExpression accessor layout flattened ref

        let addRestriction restriction name =
            let field = Map.find name entity.ColumnFields
            let parentEntity = Option.defaultValue ref field.InheritedFrom
            match Map.tryFind ({ entity = parentEntity; name = name } : FunQL.ResolvedFieldRef) flattened.Fields with
            | Some r -> andRestriction restriction r.Select
            | _ -> raisef PermissionsViewException "Access denied to select field %O" name
        let fieldsRestriction = usedFields |> Set.toSeq |> Seq.fold addRestriction selectRestr

        match fieldsRestriction.Expression with
        | OFEFalse -> raisef PermissionsViewException "Access denied to select"
        | OFETrue -> None
        | _ ->
            for arg in fieldsRestriction.GlobalArguments do
                let (argPlaceholder, newArguments) = addArgument (FunQL.PGlobal arg) FunQL.globalArgumentTypes.[arg] arguments
                arguments <- newArguments
            let singleSelect = compileRestriction layout ref arguments.Types fieldsRestriction
            let select =
                { CTEs = None
                  Tree = SSelect singleSelect
                  Extra = null
                }
            Some select

    let filterUsedEntities (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (usedEntities : FunQL.UsedEntities) : EntityAccess =
        let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
            let entity = Map.find name schema.Entities
            let ref = { schema = schemaName; name = name } : FunQL.ResolvedEntityRef
            try
                filterUsedFields ref entity usedFields
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for entity %O: %s" name e.Message

        Map.map mapEntity usedEntities

    let filterUsedSchemas (layout : Layout) (usedSchemas : FunQL.UsedSchemas) : SchemaAccess =
        let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
            let schema = Map.find name layout.Schemas
            try
                filterUsedEntities name schema usedEntities
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for schema %O: %s" name e.Message

        Map.map mapSchema usedSchemas

    member this.Arguments = arguments
    member this.FilterUsedSchemas = filterUsedSchemas

type private PermissionsApplier (access : SchemaAccess) =
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
            let accessSchema = Map.find ann.RealEntity.schema access
            let accessEntity = Map.find ann.RealEntity.name accessSchema
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
            let entityRef = (extra :?> RealEntityAnnotation).RealEntity
            let accessSchema = Map.find entityRef.schema access
            let accessEntity = Map.find entityRef.name accessSchema
            match accessEntity with
            | None -> FTable (null, pun, entity)
            | Some restr ->
                // `pun` is guaranteed to be there for all table queries.
                FSubExpr (Option.get pun, restr)
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

let checkRoleViewExpr (layout : Layout) (role : ResolvedRole) (expr : CompiledViewExpr) : unit =
    let accessCompiler = AccessCompiler (layout, role, expr.Query.Arguments)
    let access = accessCompiler.FilterUsedSchemas layout expr.UsedSchemas
    ()

let applyRoleViewExpr (layout : Layout) (role : ResolvedRole) (view : CompiledViewExpr) : CompiledViewExpr =
    let accessCompiler = AccessCompiler (layout, role, view.Query.Arguments)
    let access = accessCompiler.FilterUsedSchemas layout view.UsedSchemas
    let applier = PermissionsApplier access
    let expression = applier.ApplyToSelectExpr view.Query.Expression
    { view with
          Query =
              { view.Query with
                    Expression = expression
                    Arguments = accessCompiler.Arguments
              }
    }
