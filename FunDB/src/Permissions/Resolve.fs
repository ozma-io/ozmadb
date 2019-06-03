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

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, permissions : SourcePermissions) =
    let mutable goodParents = Set.empty

    let rec checkPath (allowIds : bool) (entity : ResolvedEntity) (name : FieldName) (fields : FieldName list) : unit =
        match fields with
        | [] ->
            match entity.FindField name with
            | None -> raisef ResolvePermissionsException "Column not found in restriction expression: %O" name
            | Some (_, RId) ->
                if allowIds then
                    ()
                else
                    raisef ResolvePermissionsException "Ids aren't allowed in this restriction expression: %O" name
            | Some (_, RColumnField _) -> ()
            | Some (_, RComputedField comp) ->
                if allowIds || not comp.hasId then
                    ()
                else
                    raisef ResolvePermissionsException "Ids aren't allowed in this restriction expression: %O" name
        | (ref :: refs) ->
            match Map.tryFind name entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity.name (Map.find refEntity.schema layout.schemas).entities
                checkPath allowIds newEntity ref refs
            | _ -> raisef ResolvePermissionsException "Invalid dereference in path: %O" ref

    let resolveRestriction (entity : ResolvedEntity) (allowIds : bool) (where : string) : Restriction =
        let whereExpr =
            match parse tokenizeFunQL fieldExpr where with
            | Ok r -> r
            | Error msg -> raisef ResolvePermissionsException "Error parsing restriction expression: %s" msg

        let mutable globalArguments = Set.empty

        let resolveColumn : LinkedFieldRef -> LinkedFieldName = function
            | { ref = { entity = None; name = name }; path = path } ->
                checkPath allowIds entity name (Array.toList path)
                { ref = name; path = path }
            | ref ->
                raisef ResolvePermissionsException "Invalid reference in restriction expression: %O" ref
        let resolvePlaceholder = function
            | PLocal name -> raisef ResolvePermissionsException "Local argument %O is not allowed in restriction expression" name
            | PGlobal name when Map.containsKey name globalArgumentTypes ->
                globalArguments <- Set.add name globalArguments
                PGlobal name
            | PGlobal name -> raisef ResolvePermissionsException "Unknown global argument %O" name
        let voidQuery query =
            raisef ResolvePermissionsException "Query is not allowed in restriction expression: %O" query

        let expr = mapFieldExpr id resolveColumn resolvePlaceholder voidQuery whereExpr
        { expression = expr
          globalArguments = globalArguments
        }

    let resolveAllowedField (entity : ResolvedEntity) (allowedField : SourceAllowedField) : AllowedField =
        let resolveOne = Option.map (resolveRestriction entity true)
        { change = allowedField.change
          select = resolveOne allowedField.select
        }

    let resolveAllowedEntity (entity : ResolvedEntity) (allowedEntity : SourceAllowedEntity) : (exn option * AllowedEntity) =
        let mutable error = None

        let mapField name (allowedField : SourceAllowedField) =
            try
                checkName name
                if not <| Map.containsKey name entity.columnFields then
                    raisef ResolvePermissionsException "Unknown field"
                if allowedField.change && Option.isNone allowedEntity.check then
                    raisef ResolvePermissionsException "Cannot allow to change without providing check expression"
                resolveAllowedField entity allowedField
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed field %O: %s" name e.Message
        let checkField fieldName =
            if not <| Map.containsKey fieldName entity.columnFields then
                raisef ResolvePermissionsException "Undefined column field: %O" fieldName

        let resolveOne allowIds = Option.map (resolveRestriction entity allowIds)
        let fields = allowedEntity.fields |> Map.map mapField

        let insert =
            if not allowedEntity.insert then
                Ok false
            else
                try
                    // Check than we can change all required fields.
                    if Option.isNone allowedEntity.check then
                        raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                    for KeyValue(fieldName, field) in entity.columnFields do
                        if Option.isNone field.defaultValue && not field.isNullable then
                            match Map.tryFind fieldName fields with
                            | Some { change = true } -> ()
                            | _ -> raisef ResolvePermissionsException "Required field %O is not allowed for inserting" fieldName
                    Ok true
                with
                | :? ResolvePermissionsException as e when allowedEntity.allowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    Error (e :> exn)

        let resolveDelete delete =
            try
                // Check that we can read all fields.
                if Option.isNone allowedEntity.select then
                    raisef ResolvePermissionsException "Read access needs to be granted to delete records"
                for KeyValue(fieldName, field) in entity.columnFields do
                    match Map.tryFind fieldName fields with
                    | Some { select = Some _ } -> ()
                    | _ -> raisef ResolvePermissionsException "Read access to field %O needs to be granted to delete records" fieldName
                Ok <| resolveRestriction entity true delete
            with
            | :? ResolvePermissionsException as e when allowedEntity.allowBroken || forceAllowBroken ->
                error <- Some (e :> exn)
                Error ({ source = delete; error = e } : AllowedOperationError)

        if Option.isSome allowedEntity.update && Option.isNone allowedEntity.check then
            raisef ResolvePermissionsException "Cannot allow to update without providing check expression"

        let ret =
            { allowBroken = allowedEntity.allowBroken
              fields = fields
              check = resolveOne false allowedEntity.check
              insert = insert
              select = resolveOne true allowedEntity.select
              update = resolveOne true allowedEntity.update
              delete = Option.map resolveDelete allowedEntity.delete
            }
        (error, ret)

    let resolveAllowedSchema (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : ErroredAllowedSchema * AllowedSchema =
        let mutable errors = Map.empty

        let mapEntity name allowedEntity =
            try
                let entity =
                    match Map.tryFind name schema.entities with
                    | None -> raisef ResolvePermissionsException "Undefined entity"
                    | Some s -> s
                try
                    let (error, ret) = resolveAllowedEntity entity allowedEntity
                    match error with
                    | Some e ->
                        errors <- Map.add name e errors
                    | None -> ()
                    Ok ret
                with
                | :? ResolvePermissionsException as e when allowedEntity.allowBroken || forceAllowBroken ->
                    errors <- Map.add name (e :> exn) errors
                    Error { source = allowedEntity; error = e }
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed entity %O: %s" name e.Message

        let ret =
            { entities = allowedSchema.entities |> Map.map mapEntity
            }
        (errors, ret)

    let resolveAllowedDatabase (db : SourceAllowedDatabase) : ErroredAllowedDatabase * AllowedDatabase =
        let mutable errors = Map.empty

        let mapSchema name allowedSchema =
            try
                let schema =
                    match Map.tryFind name layout.schemas with
                    | None -> raisef ResolvePermissionsException "Undefined schema"
                    | Some s -> s
                let (schemaErrors, newAllowed) = resolveAllowedSchema schema allowedSchema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newAllowed
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed schema %O: %s" name e.Message

        let ret =
            { schemas = db.schemas |> Map.map mapSchema
            } : AllowedDatabase
        (errors, ret)

    let rec checkParents (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : unit =
        if Set.contains ref goodParents then
            ()
        else
            if Set.contains ref stack then
                raisef ResolvePermissionsException "Cycle detected: %O" stack
            let newStack = Set.add ref stack
            let checkParent (parent : ResolvedRoleRef) =
                let schema =
                    match Map.tryFind parent.schema permissions.schemas with
                    | None -> raisef ResolvePermissionsException "Undefined parent schema for %O" parent
                    | Some s -> s
                let parentRole =
                    match Map.tryFind parent.name schema.roles with
                    | None -> raisef ResolvePermissionsException "Undefined parent role for %O" parent
                    | Some s -> s
                checkParents newStack parent parentRole
            role.parents |> Set.iter checkParent
            goodParents <- Set.add ref goodParents

    let resolveRole (ref : ResolvedRoleRef) (role : SourceRole) : ErroredAllowedDatabase * ResolvedRole =
        let (errors, resolvedDb) = resolveAllowedDatabase role.permissions
        checkParents Set.empty ref role
        let ret =
            { parents = role.parents
              permissions = resolvedDb
            }
        (errors, ret)

    let resolvePermissionsSchema (schemaName : SchemaName) (schema : SourcePermissionsSchema) : ErroredRoles * PermissionsSchema =
        let mutable errors = Map.empty

        let mapRole name role =
            try
                checkName name
                let ref = { schema = schemaName; name = name }
                let (roleErrors, newRole) = resolveRole ref role
                if not <| Map.isEmpty roleErrors then
                    errors <- Map.add name roleErrors errors
                newRole
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in role %O: %s" name e.Message

        let ret =
            { roles = schema.roles |> Map.map mapRole
            }
        (errors, ret)

    let resolvePermissions () : ErroredPermissions * Permissions =
        let mutable errors = Map.empty

        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef ResolvePermissionsException "Unknown schema name"
                let (schemaErrors, newSchema) = resolvePermissionsSchema name schema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { schemas = permissions.schemas |> Map.map mapSchema
            }
        (errors, ret)

    member this.ResolvePermissions = resolvePermissions

let resolvePermissions (layout : Layout) (forceAllowBroken : bool) (source : SourcePermissions) : ErroredPermissions * Permissions =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, source)
    phase1.ResolvePermissions ()