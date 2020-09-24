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
type private FieldAccess = SingleSelectExpr option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

type private AccessCompiler (layout : Layout, role : ResolvedRole, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let filterUsedFields (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) (usedFields : FunQL.UsedFields) : FieldAccess =
        let flattened =
            match Map.tryFind entity.root role.Flattened with
            | Some f -> f
            | None -> raisef PermissionsViewException "Access denied to entity %O" ref
        let accessor (derived : FlatAllowedDerivedEntity) = derived.Select
        let selectRestr = applyRestrictionExpression accessor layout flattened ref

        let addRestriction restriction name =
            let field = Map.find name entity.columnFields
            let parentEntity = Option.defaultValue ref field.inheritedFrom
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
            compileRestriction layout ref arguments.Types fieldsRestriction |> Some

    let filterUsedEntities (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (usedEntities : FunQL.UsedEntities) : EntityAccess =
        let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
            let entity = Map.find name schema.entities
            let ref = { schema = schemaName; name = name } : FunQL.ResolvedEntityRef
            try
                filterUsedFields ref entity usedFields
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for entity %O: %s" name e.Message

        Map.map mapEntity usedEntities

    let filterUsedSchemas (layout : Layout) (usedSchemas : FunQL.UsedSchemas) : SchemaAccess =
        let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
            let schema = Map.find name layout.schemas
            try
                filterUsedEntities name schema usedEntities
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for schema %O: %s" name e.Message

        Map.map mapSchema usedSchemas

    member this.Arguments = arguments
    member this.FilterUsedSchemas = filterUsedSchemas

let private (|SubEntitySelect|_|) (expr : SingleSelectExpr) : FunQL.ResolvedEntityRef option =
    match expr.Extra with
    | :? RealEntityAnnotation as ent -> Some ent.RealEntity
    | _ -> None

type private PermissionsApplier (access : SchemaAccess) =
    let rec applyToSelectTreeExpr : SelectTreeExpr -> SelectTreeExpr = function
        | SSelect query -> SSelect <| applyToSingleSelectExpr query
        | SSetOp (op, a, b, limits) ->
            let a' = applyToSelectTreeExpr a
            let b' = applyToSelectTreeExpr b
            let limits' = applyToOrderLimitClause limits
            SSetOp (op, a', b', limits')
        | SValues values -> SValues values

    and applyToCommonTableExprs (cte : CommonTableExprs) =
        { Recursive = cte.Recursive
          Exprs = Map.map (fun name -> applyToSelectExpr) cte.Exprs
        }

    and applyToSelectExpr (select : SelectExpr) : SelectExpr =
        { CTEs = Option.map applyToCommonTableExprs select.CTEs
          Tree = applyToSelectTreeExpr select.Tree
        }

    and applyToSingleSelectExpr (query : SingleSelectExpr) : SingleSelectExpr =
        match query with
        // Special case -- subentity select which gets generated when someone uses subentity in FROM.
        | SubEntitySelect realRef as select ->
            let accessSchema = Map.find realRef.schema access
            let accessEntity = Map.find realRef.name accessSchema
            match accessEntity with
            | None -> select
            | Some restr -> restr
        | _ ->
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
            | None -> FTable (extra, pun, entity)
            | Some restr ->
                let name = Option.defaultValue entity.name pun
                let select = { CTEs = None; Tree = SSelect restr }
                FSubExpr (name, None, select)
        | FJoin (jt, e1, e2, where) ->
            let e1' = applyToFromExpr e1
            let e2' = applyToFromExpr e2
            let where' = applyToValueExpr where
            FJoin (jt, e1', e2', where')
        | FSubExpr (name, cols, q) ->
            FSubExpr (name, cols, applyToSelectExpr q)

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
