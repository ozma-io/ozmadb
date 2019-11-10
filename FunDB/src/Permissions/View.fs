module FunWithFlags.FunDB.Permissions.View

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Compile
open FunWithFlags.FunDB.Permissions.Flatten
module FunQL = FunWithFlags.FunDB.FunQL.AST

type PermissionsViewException (message : string) =
    inherit Exception(message)

type private UsedArguments = Set<FunQL.ArgumentName>
type private FieldAccess = SingleSelectExpr option
type private EntityAccess = Map<FunQL.EntityName, FieldAccess>
type private SchemaAccess = Map<FunQL.SchemaName, EntityAccess>

type private AccessCompiler (layout : Layout, initialArguments : QueryArguments) =
    let mutable arguments = initialArguments

    let filterUsedFields (ref : FunQL.ResolvedEntityRef) (entity : ResolvedEntity) (entityPerms : FlatAllowedEntity) (usedFields : FunQL.UsedFields) : FieldAccess =
        let select =
            match entityPerms.select with
            | None -> raisef PermissionsViewException "No read access"
            | Some select -> select

        let findOne name =
            match Map.tryFind name select with
            | None -> raisef PermissionsViewException "No read access for field %O" name
            | Some restrictions -> restrictions

        let prepareRestriction restr =
            for arg in restr.globalArguments do
                arguments <- addArgument (FunQL.PGlobal arg) FunQL.globalArgumentTypes.[arg] arguments
            compileRestriction layout ref arguments.types restr

        usedFields
            |> Set.toSeq
            |> Seq.map findOne
            |> Seq.fold1 mergeAllowedOperation
            |> allowedOperationRestriction
            |> Option.map prepareRestriction

    let filterUsedEntities (schemaName : FunQL.SchemaName) (schema : ResolvedSchema) (schemaPerms : FlatAllowedSchema) (usedEntities : FunQL.UsedEntities) : EntityAccess =
        let mapEntity (name : FunQL.EntityName) (usedFields : FunQL.UsedFields) =
            try
                match Map.tryFind name schemaPerms.entities with
                | Some entityPerms ->
                    let entity = Map.find name schema.entities
                    let ref = { schema = schemaName; name = name } : FunQL.ResolvedEntityRef
                    filterUsedFields ref entity entityPerms usedFields
                | None -> raisef PermissionsViewException "No read access"
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for entity %O: %s" name e.Message

        Map.map mapEntity usedEntities

    let filterUsedSchemas (layout : Layout) (role : FlatRole) (usedSchemas : FunQL.UsedSchemas) : SchemaAccess =
        let mapSchema (name : FunQL.SchemaName) (usedEntities : FunQL.UsedEntities) =
            try
                match Map.tryFind name role.permissions.schemas with
                | Some schemaPerms ->
                    let schema = Map.find name layout.schemas
                    filterUsedEntities name schema schemaPerms usedEntities
                | None -> raisef PermissionsViewException "No read access"
            with
            | :? PermissionsViewException as e -> raisef PermissionsViewException "Access denied for schema %O: %s" name e.Message

        Map.map mapSchema usedSchemas

    member this.Arguments = arguments
    member this.FilterUsedSchemas = filterUsedSchemas

let private (|SubEntitySelect|_|) : SingleSelectExpr -> FunQL.ResolvedEntityRef option = function
    | { columns = columns; from = Some (FTable (None, tableRef)) } when not (Array.isEmpty columns) ->
        match columns.[0] with
        | SCExpr (name, VEValue (VString rootName)) when name = sqlFunRootEntity ->
            let rootRef = { schema = decompileName <| Option.get tableRef.schema; name = decompileName tableRef.name } : FunQL.ResolvedEntityRef
            let realRef = parseTypeName rootRef rootName
            Some realRef
        | _ -> None
    | _ -> None

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
        match query with
        // Special case -- subentity select which gets generated when someone uses subentity in FROM.
        | SubEntitySelect realRef as select ->
            let accessSchema = Map.find realRef.schema access
            let accessEntity = Map.find realRef.name accessSchema
            match accessEntity with
            | None -> select
            | Some restr -> restr
        | _ ->
            { columns = Array.map applyToSelectedColumn query.columns
              from = Option.map applyToFromExpr query.from
              where = Option.map applyToValueExpr query.where
              groupBy = Array.map applyToValueExpr query.groupBy
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

    and applyToFromExpr : FromExpr -> FromExpr = function
        | FTable (pun, entity) ->
            let accessSchema = Map.find (decompileName <| Option.get entity.schema) access
            let accessEntity = Map.find (decompileName entity.name) accessSchema
            match accessEntity with
            | None -> FTable (pun, entity)
            | Some restr ->
                let name = Option.defaultValue entity.name pun
                FSubExpr (name, None, SSelect restr)
        | FJoin (jt, e1, e2, where) ->
            let e1' = applyToFromExpr e1
            let e2' = applyToFromExpr e2
            let where' = applyToValueExpr where
            FJoin (jt, e1', e2', where')
        | FSubExpr (name, cols, q) ->
            FSubExpr (name, cols, applyToSelectExpr q)

    member this.ApplyToSelectExpr = applyToSelectExpr

let checkRoleViewExpr (layout : Layout) (role : FlatRole) (expr : CompiledViewExpr) : unit =
    let accessCompiler = AccessCompiler (layout, expr.query.arguments)
    let access = accessCompiler.FilterUsedSchemas layout role expr.usedSchemas
    ()

let applyRoleViewExpr (layout : Layout) (role : FlatRole) (view : CompiledViewExpr) : CompiledViewExpr =
    let accessCompiler = AccessCompiler (layout, view.query.arguments)
    let access = accessCompiler.FilterUsedSchemas layout role view.usedSchemas
    let applier = PermissionsApplier access
    let expression = applier.ApplyToSelectExpr view.query.expression
    { view with
          query =
              { view.query with
                    expression = expression
                    arguments = accessCompiler.Arguments
              }
    }
