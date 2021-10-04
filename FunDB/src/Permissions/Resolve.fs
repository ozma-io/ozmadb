module FunWithFlags.FunDB.Permissions.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.UsedReferences
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

type ResolvePermissionsException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ResolvePermissionsException (message, innerException, isUserException innerException)

    new (message : string) = ResolvePermissionsException (message, null, true)

type ResolvePermissionsParentException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ResolvePermissionsParentException (message, innerException, isUserException innerException)

    new (message : string) = ResolvePermissionsParentException (message, null, true)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolvePermissionsException "Invalid role name"

type private HalfAllowedEntity =
    { Allowed : AllowedEntity
      Flattened : FlatAllowedDerivedEntity
      Error : exn option
    }

let private flattenAllowedEntity (entityRef : ResolvedEntityRef) (entity : AllowedEntity) : Map<ResolvedFieldRef,AllowedField> * FlatAllowedDerivedEntity =
    let fields = entity.Fields |> Map.toSeq |> Seq.map (fun (name, field) -> ({ Entity = entityRef; Name = name }, field)) |> Map.ofSeq

    let ret =
        { Check = entity.Check
          Insert = entity.Insert
          Select = entity.Select
          Update = entity.Update
          Delete = entity.Delete
        }

    (fields, ret)

let private unionFlatEntities (a : FlatAllowedEntity) (b : FlatAllowedEntity) : FlatAllowedEntity =
    { // Allowed entities for all roles are expected to be fully built when merging flat entities.
      Roles = Map.union a.Roles b.Roles
    }

let private unionFlatAllowedDatabases : FlatAllowedDatabase -> FlatAllowedDatabase -> FlatAllowedDatabase = Map.unionWith (fun name -> unionFlatEntities)

let private unionFlatRoles (a : FlatRole) (b: FlatRole) =
    { Entities = unionFlatAllowedDatabases a.Entities b.Entities
    }

type private RoleResolver (layout : Layout, forceAllowBroken : bool, allowedDb : SourceAllowedDatabase) =
    let mutable resolved : Map<ResolvedEntityRef, Result<HalfAllowedEntity, AllowedEntityError>> = Map.empty
    let mutable flattened : Map<ResolvedEntityRef, FlatAllowedRoleEntity> = Map.empty

    let resolveRestriction (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowIds : bool) (where : string) : ResolvedOptimizedFieldExpr =
        let whereExpr =
            match parse tokenizeFunQL fieldExpr where with
            | Ok r -> r
            | Error msg -> raisef ResolvePermissionsException "Error parsing: %s" msg

        let entityInfo = SFEntity entityRef
        let (localArguments, expr) =
            try
                resolveSingleFieldExpr layout Map.empty localExprFromEntityId emptyExprResolutionFlags entityInfo whereExpr
            with
            | :? ViewResolveException as e -> raisefWithInner ResolvePermissionsException e "Failed to resolve restriction expression"
        let (exprInfo, usedReferences) = fieldExprUsedReferences layout expr
        if exprInfo.HasAggregates then
            raisef ResolvePermissionsException "Forbidden aggregate function in a restriction"
        if not allowIds && Option.isSome (usedReferences.UsedDatabase.FindField entityRef funId) then
            raisef ResolvePermissionsException "Cannot use id in this restriction"
        optimizeFieldExpr expr

    let resolveAllowedField (fieldRef : ResolvedFieldRef) (entity : ResolvedEntity) (allowedField : SourceAllowedField) : AllowedField =
        let field =
            match Map.tryFind fieldRef.Name entity.ColumnFields with
            | None -> raisef ResolvePermissionsException "Unknown field"
            | Some f -> f
        if Option.isSome field.InheritedFrom then
            raisef ResolvePermissionsException "Cannot define restrictions on parent entity fields in children"
        let resolveOne = Option.map (resolveRestriction fieldRef.Entity entity true) >> Option.defaultValue OFEFalse
        { Change = allowedField.Change
          Select = resolveOne allowedField.Select
        }

    let resolveSelfAllowedEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (parentEntity : HalfAllowedEntity option) (allowedEntity : SourceAllowedEntity) : (exn option * AllowedEntity) =
        let mutable error = None

        let mapField name (allowedField : SourceAllowedField) =
            try
                resolveAllowedField { Entity = entityRef; Name = name } entity allowedField
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed field %O" name

        let propagateFromParent accessor allowIds mexpr =
            match mexpr with
            | Some expr -> resolveRestriction entityRef entity allowIds expr
            | None ->
                match parentEntity with
                | None -> OFEFalse
                | Some parent ->
                    match accessor parent.Allowed with
                    | OFEFalse -> OFEFalse
                    // Parent permissions will be multiplied by these, so just leave `true` here.
                    | _ -> OFETrue

        let check = propagateFromParent (fun pent -> pent.Check) false allowedEntity.Check
        let select = propagateFromParent (fun pent -> pent.Select) true allowedEntity.Select
        let update = propagateFromParent (fun pent -> pent.Update) true allowedEntity.Update
        let delete = propagateFromParent (fun pent -> pent.Delete) true allowedEntity.Delete

        let fields = allowedEntity.Fields |> Map.map mapField

        let ret =
            { AllowBroken = allowedEntity.AllowBroken
              Fields = fields
              Check = check
              Insert = allowedEntity.Insert
              Select = select
              Update = update
              Delete = delete
            }
        (error, ret)

    let rec checkParentRowAccess (entity : ResolvedEntity) (check : AllowedEntity option -> unit) =
        match entity.Parent with
        | None -> ()
        | Some parent ->
            let parentEntity = layout.FindEntity parent |> Option.get
            if not parentEntity.IsAbstract then
                // It's impossible to have broken entity parent access.
                let parentAllowed = Map.tryFind parent resolved |> Option.map (fun ent -> (Result.get ent).Allowed)
                check parentAllowed
            else
                checkParentRowAccess parentEntity check

    let checkEntity (entity : ResolvedEntity) (allowedEntity : AllowedEntity) : exn option * AllowedEntity =
        let mutable error = None

        let iterField (name : FieldName) (allowedField : AllowedField) =
            try
                if allowedField.Change && optimizedIsFalse allowedEntity.Check then
                    raisef ResolvePermissionsException "Cannot allow to change without providing check expression"
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed field %O" name

        Map.iter iterField allowedEntity.Fields

        let allowedEntity =
            if allowedEntity.Insert then
                try
                    if entity.IsAbstract then
                        raisef ResolvePermissionsException "Cannot allow insertion of abstract entities"
                    checkParentRowAccess entity <| function
                        | Some { Insert = true } -> ()
                        | _ -> raisef ResolvePermissionsException "Cannot insert to child entity when parent entity insert is forbidden"
                    // Check that we can change all required fields.
                    if optimizedIsFalse allowedEntity.Check then
                        raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                    for KeyValue(fieldName, field) in entity.ColumnFields do
                        if Option.isNone field.InheritedFrom && Option.isNone field.DefaultValue && not field.IsNullable then
                            match Map.tryFind fieldName allowedEntity.Fields with
                            | Some { Change = true } -> ()
                            | _ -> raisef ResolvePermissionsException "Required field %O is not allowed for inserting" fieldName
                    allowedEntity
                with
                | :? ResolvePermissionsException as e when allowedEntity.AllowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { allowedEntity with Insert = false }
            else
                allowedEntity

        if not (optimizedIsFalse allowedEntity.Update) then
            // Check that we can change all required fields.
            if optimizedIsFalse allowedEntity.Check then
                raisef ResolvePermissionsException "Cannot allow to update without providing check expression"

        let allowedEntity =
            if not (optimizedIsFalse allowedEntity.Delete) then
                try
                    checkParentRowAccess entity <| function
                        | Some access when not (optimizedIsFalse access.Delete) -> ()
                        | _ -> raisef ResolvePermissionsException "Cannot delete from child entity when parent entity delete is forbidden"
                    // Check that we can can view all column fields.
                    if entity.IsAbstract then
                        raisef ResolvePermissionsException "Cannot allow deletion of abstract entities"
                    for KeyValue(fieldName, field) in entity.ColumnFields do
                        if Option.isNone field.InheritedFrom then
                            match Map.tryFind fieldName allowedEntity.Fields with
                            | Some f when not (optimizedIsFalse f.Select) -> ()
                            | _ -> raisef ResolvePermissionsException "Field %O is not allowed for selection, which is required for deletion" fieldName
                    allowedEntity
                with
                | :? ResolvePermissionsException as e when allowedEntity.AllowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { allowedEntity with Delete = OFEFalse }
            else
                allowedEntity

        (error, allowedEntity)

    // Recursively resolve access rights for entity parents.
    let rec resolveParentEntity (entity : ResolvedEntity) : HalfAllowedEntity option =
        match entity.Parent with
        | None -> None
        | Some parentRef ->
            let parentRights =
                match Map.tryFind parentRef.Schema allowedDb.Schemas with
                | None -> None
                | Some parentSchema -> Map.tryFind parentRef.Name parentSchema.Entities
            match parentRights with
            | Some right ->
                match resolveAllowedEntity parentRef right with
                | Ok ret -> Some ret
                | Error err -> raisefWithInner ResolvePermissionsParentException err.Error "In parent %O" parentRef
            | None ->
                let parentEntity = layout.FindEntity parentRef |> Option.get
                resolveParentEntity parentEntity

    and resolveOneAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : HalfAllowedEntity =
        let entity =
            match layout.FindEntity entityRef with
            | Some s when not s.IsHidden -> s
            | _ -> raisef ResolvePermissionsException "Undefined entity"
        let parentEntity = resolveParentEntity entity

        let (error1, resolved) = resolveSelfAllowedEntity entityRef entity parentEntity source
        let (error2, resolved) = checkEntity entity resolved
        let (fields, myFlat) = flattenAllowedEntity entityRef resolved

        let (myFlat, flatEntity) =
            match Map.tryFind entity.Root flattened with
            | None ->
                let flatEntity =
                    { Children = Map.singleton entityRef myFlat
                      Fields = fields
                    }
                (myFlat, flatEntity)
            | Some oldFlat ->
                let flatEntity =
                    { Children = Map.add entityRef myFlat oldFlat.Children
                      Fields = Map.unionUnique fields oldFlat.Fields
                    }
                (myFlat, flatEntity)
        let error = Option.orElseWith (fun _ -> error2) error1
        let ret =
            { Allowed = resolved
              Flattened = myFlat
              Error = error
            } : HalfAllowedEntity
        flattened <- Map.add entity.Root flatEntity flattened
        ret

    and resolveAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : Result<HalfAllowedEntity, AllowedEntityError> =
        match Map.tryFind entityRef resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = Ok <| resolveOneAllowedEntity entityRef source
                resolved <- Map.add entityRef ret resolved
                ret
            with
            | :? ResolvePermissionsException as e when source.AllowBroken || forceAllowBroken ->
                let ret = Error ({ Source = source; Error = e } : AllowedEntityError)
                resolved <- Map.add entityRef ret resolved
                ret

    let resolveAllowedSchema (schemaName : SchemaName) (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : ErroredAllowedSchema * AllowedSchema =
        let mutable errors = Map.empty

        let mapEntity name (allowedEntity : SourceAllowedEntity) : Result<AllowedEntity, AllowedEntityError> =
            try
                let entityRef = { Schema = schemaName; Name = name }
                match resolveAllowedEntity entityRef allowedEntity with
                | Ok ret ->
                    match ret.Error with
                    | Some e when not allowedEntity.AllowBroken ->
                        errors <- Map.add name e errors
                    | _ -> ()
                    Ok ret.Allowed
                | Error e ->
                    match e.Error with
                    | :? ResolvePermissionsParentException ->
                        Error e
                    | _ ->
                        if not allowedEntity.AllowBroken then
                            errors <- Map.add name e.Error errors
                        Error e
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed entity %O" name

        let ret =
            { Entities = allowedSchema.Entities |> Map.map mapEntity
            } : AllowedSchema
        (errors, ret)

    let resolveAllowedDatabase (): ErroredAllowedDatabase * AllowedDatabase =
        let mutable errors = Map.empty

        let mapSchema name allowedSchema =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolvePermissionsException "Undefined schema"
                    | Some s -> s
                let (schemaErrors, newAllowed) = resolveAllowedSchema name schema allowedSchema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newAllowed
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed schema %O" name

        let ret =
            { Schemas = allowedDb.Schemas |> Map.map mapSchema
            } : AllowedDatabase
        (errors, ret)

    member this.ResolveAllowedDatabase () = resolveAllowedDatabase ()
    member this.Flattened = flattened

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, permissions : SourcePermissions) =
    let mutable resolved : Map<ResolvedRoleRef, Result<ErroredAllowedDatabase * ResolvedRole, exn>> = Map.empty

    let rec resolveOneRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : ErroredAllowedDatabase * ResolvedRole =
        if Set.contains ref stack then
            raisef ResolvePermissionsException "Cycle detected: %O" stack
        let newStack = Set.add ref stack
        let resolveParent parentRef =
            let schema =
                    match Map.tryFind parentRef.Schema permissions.Schemas with
                    | None -> raisef ResolvePermissionsException "Undefined parent schema for %O" parentRef
                    | Some s -> s
            let parentRole =
                match Map.tryFind parentRef.Name schema.Roles with
                | None -> raisef ResolvePermissionsException "Undefined parent role for %O" parentRef
                | Some s -> s
            match resolveRole newStack parentRef parentRole with
            | Ok (errors, role) -> role.Flattened
            | Error e -> raisefWithInner ResolvePermissionsParentException e "Error in parent %O" parentRef
        let resolver = RoleResolver (layout, forceAllowBroken, role.Permissions)
        let flattenedParents = role.Parents |> Set.toSeq |> Seq.map resolveParent |> Seq.fold unionFlatRoles emptyFlatRole
        let (errors, resolved) = resolver.ResolveAllowedDatabase ()
        let flattened =
            { Entities = Map.map (fun name roleEntity -> { Roles = Map.singleton ref roleEntity }) resolver.Flattened
            }

        let ret =
            { Parents = role.Parents
              Permissions = resolved
              Flattened = unionFlatRoles flattenedParents flattened
              AllowBroken = role.AllowBroken
            }
        (errors, ret)

    and resolveRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : Result<ErroredAllowedDatabase * ResolvedRole, exn> =
        match Map.tryFind ref resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = resolveOneRole stack ref role
                resolved <- Map.add ref (Ok ret) resolved
                Ok ret
            with
            | :? ResolvePermissionsException as e when role.AllowBroken || forceAllowBroken ->
                let ret = Error (e :> exn)
                resolved <- Map.add ref ret resolved
                ret

    let resolvePermissionsSchema (schemaName : SchemaName) (schema : SourcePermissionsSchema) : ErroredRoles * PermissionsSchema =
        let mutable errors = Map.empty

        let mapRole name role =
            try
                checkName name
                let ref = { Schema = schemaName; Name = name }
                match resolveRole Set.empty ref role with
                | Ok (error, ret) ->
                    if not <| Map.isEmpty error then
                        errors <- Map.add name (ERDatabase error) errors
                    Ok ret
                | Error (:? ResolvePermissionsParentException as e) ->
                    Error (e :> exn)
                | Error e ->
                    if not role.AllowBroken then
                        errors <- Map.add name (ERFatal e) errors
                    Error e
            with
            | e -> raisefWithInner ResolvePermissionsException e "In role %O" name

        let ret =
            { Roles = schema.Roles |> Map.map mapRole
            }
        (errors, ret)

    let resolvePermissions () : ErroredPermissions * Permissions =
        let mutable errors = Map.empty

        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolvePermissionsException "Unknown schema name"
                let (schemaErrors, newSchema) = resolvePermissionsSchema name schema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | e -> raisefWithInner ResolvePermissionsException e "In schema %O" name

        let ret =
            { Schemas = permissions.Schemas |> Map.map mapSchema
            }
        (errors, ret)

    member this.ResolvePermissions = resolvePermissions

let resolvePermissions (layout : Layout) (forceAllowBroken : bool) (source : SourcePermissions) : ErroredPermissions * Permissions =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, source)
    phase1.ResolvePermissions ()