module FunWithFlags.FunDB.Permissions.View

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Flatten
module FunQL = FunWithFlags.FunDB.FunQL.AST

type PermissionsViewException (message : string) =
    inherit Exception(message)

type private FieldAccess = ValueExpr option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

let private filterUsedFields (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) (entityPerms : FlatAllowedEntity) (usedFields : FunQL.UsedFields) : FieldAccess =
    let select =
        match entityPerms.select with
        | None -> raisef PermissionsViewException "No read access"
        | Some select -> select    
    
    let findOne name =
        match Map.tryFind name select with
        | None -> raisef PermissionsViewException "No read access for field %O" name
        | Some restrictions -> restrictions

    let compileRestriction expr =
        let compiledRef = compileResolvedEntityRef ref
        compileLocalFieldExpr compiledRef entity expr

    usedFields
        |> Set.toSeq
        |> Seq.map findOne
        |> Seq.fold1 mergeAllowedOperation
        |> allowedOperationExpression
        |> Option.map compileRestriction

let private filterUsedEntities (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (schemaPerms : FlatAllowedSchema) (usedEntities : FunQL.UsedEntities) : EntityAccess =
    let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
        try
            match Map.tryFind name schemaPerms.entities with
            | Some entityPerms ->
                let entity = Map.find name schema.entities
                let ref = { schema = schemaName; name = name } : FunQL.ResolvedEntityRef
                filterUsedFields ref entity entityPerms usedFields
            | None -> raisef PermissionsViewException "No access to fields %O" usedFields
        with
        | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied in entity %O: %s" name e.Message

    Map.map mapEntity usedEntities

let private filterUsedSchemas (layout : Layout) (role : FlatRole) (usedSchemas : FunQL.UsedSchemas) : SchemaAccess =
    let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
        try
            match Map.tryFind name role.permissions.schemas with
            | Some schemaPerms ->
                let schema = Map.find name layout.schemas
                filterUsedEntities name schema schemaPerms usedEntities
            | None -> raisef PermissionsViewException "No access to entities %O" usedEntities
        with
        | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied in schema %O: %s" name e.Message

    Map.map mapSchema usedSchemas

type private PermissionsApplier (access : SchemaAccess) =
    let rec applyToSelectExpr : SelectExpr -> SelectExpr = function
        | SSelect query -> SSelect <| applyToSingleSelectExpr query
        | SSetOp (op, a, b, limits) ->
            let a' = applyToSelectExpr a
            let b' = applyToSelectExpr b
            let limits' = applyToOrderLimitClause limits
            SSetOp (op, a', b', limits')
        | SValues values -> SValues values

    and applyToSingleSelectExpr (query : SingleSelectExpr) : SingleSelectExpr =
        match query.columns with
        | [| SCAll None |] ->
            // Leaf-level select
            match query.clause with
            | Some { from = FTable { schema = Some schemaName; name = entityName } as from; where = None } ->
                let accessSchema = Map.find (decompileName schemaName) access
                let accessEntity = Map.find (decompileName entityName) accessSchema
                { clause = Some { from = from; where = accessEntity }
                  columns = query.columns
                  orderLimit = query.orderLimit
                }
            | _ -> failwith "Unexpected clause in leaf-level SELECT"
        | _ ->
            // Some other select
            { columns = Array.map applyToSelectedColumn query.columns
              clause = Option.map applyToFromClause query.clause
              orderLimit = applyToOrderLimitClause query.orderLimit
            }

    and applyToOrderLimitClause (clause : OrderLimitClause) : OrderLimitClause =
        { limit = Option.map applyToValueExpr clause.limit
          offset = Option.map applyToValueExpr clause.offset
          orderBy = Array.map (fun (ord, expr) -> (ord, applyToValueExpr expr)) clause.orderBy
        }

    and applyToSelectedColumn : SelectedColumn -> SelectedColumn = function
        | SCAll _ -> failwith "Unexpected SELECT *"
        | SCColumn col -> SCColumn col
        | SCExpr (name, expr) -> SCExpr (name, applyToValueExpr expr)

    and applyToValueExpr =
        mapValueExpr id id applyToSelectExpr

    and applyToFromClause (clause : FromClause) : FromClause =
        { from = applyToFromExpr clause.from
          where = Option.map applyToValueExpr clause.where
        }

    and applyToFromExpr : FromExpr -> FromExpr = function
        | FTable _ -> failwith "Unexpected SELECT from table"
        | FJoin (jt, e1, e2, where) ->
            let e1' = applyToFromExpr e1
            let e2' = applyToFromExpr e2
            let where' = applyToValueExpr where
            FJoin (jt, e1', e2', where')
        | FSubExpr (name, cols, q) ->
            FSubExpr (name, cols, applyToSelectExpr q)

    member this.ApplyToSelectExpr = applyToSelectExpr

let checkRoleViewExpr (layout : Layout) (role : FlatRole) (expr : CompiledViewExpr) : unit =
    let access = filterUsedSchemas layout role expr.usedSchemas
    ()

let applyRoleViewExpr (layout : Layout) (role : FlatRole) (expr : CompiledViewExpr) : CompiledViewExpr =
    let access = filterUsedSchemas layout role expr.usedSchemas
    let applier = PermissionsApplier access
    let query = applier.ApplyToSelectExpr expr.query
    { expr with
          query = query
    }
