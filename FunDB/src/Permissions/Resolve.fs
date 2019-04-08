module FunWithFlags.FunDB.Permissions.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

type ResolvePermissionsException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolvePermissionsException (message, null)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolvePermissionsException "Invalid role name"

let private mergeWhere (a : LocalFieldExpr option) (b : LocalFieldExpr option) =
    match (a, b) with
    | (None, _) -> None
    | (_, None) -> None
    | (Some e1, Some e2) -> Some <| FEOr (e1, e2)

let private mergeRestrictions (a : Restrictions option) (b : Restrictions option) =
    match (a, b) with
    | (Some r1, Some r2) -> Some <| Map.union r1 r2
    | _ -> None

let private mergeAllowedEntity (a : FlatAllowedEntity) (b : FlatAllowedEntity) : FlatAllowedEntity =
    { fields = Map.unionWith (fun _ -> mergeRestrictions) a.fields b.fields
    }

let private mergeAllowedSchema (a : FlatAllowedSchema) (b : FlatAllowedSchema) : FlatAllowedSchema =
    { entities = Map.unionWith (fun name -> mergeAllowedEntity) a.entities b.entities
    }

let private mergeAllowedDatabase (a : FlatAllowedDatabase) (b : FlatAllowedDatabase) : FlatAllowedDatabase =
    { schemas = Map.unionWith (fun name -> mergeAllowedSchema) a.schemas b.schemas
    }

let private flatAllowedEntity (ent : AllowedEntity) : FlatAllowedEntity =
    let restrictions = Option.map (fun (where : LocalFieldExpr) -> Map.singleton (where.ToFunQLString()) where) ent.where
    { fields = ent.fields |> Set.toSeq |> Seq.map (fun field -> (field, restrictions)) |> Map.ofSeq
    }

let private flatAllowedSchema (schema : AllowedSchema) : FlatAllowedSchema =
    { entities = Map.map (fun name -> flatAllowedEntity) schema.entities
    }

let private flatAllowedDatabase (db : AllowedDatabase) : FlatAllowedDatabase =
    { schemas = Map.map (fun name -> flatAllowedSchema) db.schemas
    }

type private Phase1Resolver (layout : Layout) =
    let resolveWhere (entity : ResolvedEntity) (where : string) =
        let whereExpr =
            match parse tokenizeFunQL fieldExpr where with
            | Ok r -> r
            | Error msg -> raisef ResolvePermissionsException "Error parsing restriction expression: %s" msg

        let resolveColumn : LinkedFieldRef -> FieldName = function
            | { ref = { entity = None; name = name }; path = [||] } ->
                match entity.FindField name with
                | None -> raisef ResolvePermissionsException "Column not found in restriction expression: %O" name
                | Some (_, RId)
                | Some (_, RColumnField _) -> name
                | Some (_, RComputedField comp) ->
                    if comp.isLocal then
                        name
                    else
                        raisef ResolvePermissionsException "Non-local computed field reference in restriction expression: %O" name
            | ref ->
                raisef ResolvePermissionsException"Invalid reference in restriction expression: %O" ref
        let voidPlaceholder name =
            raisef ResolvePermissionsException "Placeholders are not allowed in restriction expressions: %O" name
        let voidQuery query =
            raisef ResolvePermissionsException "Queries are not allowed in restriction expressions: %O" query

        mapFieldExpr id resolveColumn voidPlaceholder voidQuery whereExpr

    let resolveAllowedEntity (entity : ResolvedEntity) (allowedEntity : SourceAllowedEntity) : AllowedEntity =
        let checkField fieldName =
            if not <| Map.containsKey fieldName entity.columnFields then
                raisef ResolvePermissionsException "Undefined column field: %O" fieldName
        allowedEntity.fields |> Seq.iter checkField

        { fields = allowedEntity.fields
          where = Option.map (resolveWhere entity) allowedEntity.where
        }

    let resolveAllowedSchema (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : AllowedSchema =
        let mapEntity name allowedEntity =
            try
                let entity =
                    match Map.tryFind name schema.entities with
                    | None -> raisef ResolvePermissionsException "Undefined entity"
                    | Some s -> s
                resolveAllowedEntity entity allowedEntity
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed entity %O: %s" name e.Message
        { entities = allowedSchema.entities |> Map.map mapEntity
        }

    let resolveAllowedDatabase (db : SourceAllowedDatabase) : AllowedDatabase =
        let mapSchema name allowedSchema =
            try
                let schema =
                    match Map.tryFind name layout.schemas with
                    | None -> raisef ResolvePermissionsException "Undefined schema"
                    | Some s -> s
                resolveAllowedSchema schema allowedSchema
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed schema %O: %s" name e.Message
        { schemas = db.schemas |> Map.map mapSchema
        }

    let resolveRole (role : SourceRole) : ResolvedRole =
        let resolvedDb = resolveAllowedDatabase role.permissions
        { parents = role.parents
          permissions = resolvedDb
          flatPermissions = flatAllowedDatabase resolvedDb
        }

    let resolvePermissionsSchema (schema : SourcePermissionsSchema) : PermissionsSchema =
        let mapRole name role =
            try
                checkName name
                resolveRole role
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in role %O: %s" name e.Message
        { roles = schema.roles |> Map.map mapRole
        }

    let resolvePermissions (perms : SourcePermissions) : Permissions =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef ResolvePermissionsException "Unknown schema name"
                resolvePermissionsSchema schema
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in schema %O: %s" name e.Message
        { schemas = perms.schemas |> Map.map mapSchema
        }

    member this.ResolvePermissions = resolvePermissions

type private RolesFlattener (layout : Layout, perms : Permissions) =
    let mutable flattened : Map<RoleRef, FlatAllowedDatabase> = Map.empty

    let rec flattenRoleByKey (stack : Set<RoleRef>) (parentRole : RoleRef) (name : RoleRef) : FlatAllowedDatabase =
        match Map.tryFind name flattened with
        | Some res -> res
        | None ->
            if Set.contains name stack then
                raisef ResolvePermissionsException "Cycle detected: %O" stack
            let role =
                match Map.tryFind name.schema perms.schemas with
                | None -> raisef ResolvePermissionsException "Unknown parent role of %O: %O" parentRole name
                | Some schema ->
                    match Map.tryFind name.name schema.roles with
                    | None -> raisef ResolvePermissionsException "Unknown parent role of %O: %O" parentRole name
                    | Some s -> s
            let newStack = Set.add name stack

            flattenRole newStack name role

    and flattenRole (stack : Set<RoleRef>) (name : RoleRef) (role : ResolvedRole) : FlatAllowedDatabase =
            let flat = role.parents |> Set.toSeq |> Seq.map (flattenRoleByKey stack name) |> Seq.fold mergeAllowedDatabase role.flatPermissions
            flattened <- Map.add name flat flattened
            flat

    let flattenPermissionsSchema schemaName (schema : PermissionsSchema) =
        let resolveOne entityName role =
            let ref = { schema = schemaName; name = entityName }
            { role with
                  flatPermissions = flattenRole (Set.singleton ref) ref role
            }
        { roles = Map.map resolveOne schema.roles
        }

    let flattenPermissions () =
        { schemas = Map.map flattenPermissionsSchema perms.schemas
        }

    member this.FlattenRoles = flattenPermissions

let resolvePermissions (layout : Layout) (source : SourcePermissions) : Permissions =
    let phase1 = Phase1Resolver layout
    let perms1 = phase1.ResolvePermissions source
    let flattener = RolesFlattener (layout, perms1)
    flattener.FlattenRoles ()