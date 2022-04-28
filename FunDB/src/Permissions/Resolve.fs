module FunWithFlags.FunDB.Permissions.Resolve

open FSharpPlus

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
open FunWithFlags.FunDB.Objects.Types

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
    }

let private flattenAllowedEntity (entityRef : ResolvedEntityRef) (parentEntity : HalfAllowedEntity option) (entity : AllowedEntity) : Map<ResolvedFieldRef,AllowedField> * FlatAllowedDerivedEntity =
    let parentAllowed =
        match parentEntity with
        | None ->
            // Current entity is root.
            fullFlatAllowedDerivedEntity
        | Some ent -> ent.Flattened

    let fields = entity.Fields |> Map.toSeq |> Seq.map (fun (name, field) -> ({ Entity = entityRef; Name = name }, field)) |> Map.ofSeq

    let select = parentAllowed.CombinedSelect && not (optimizedIsFalse entity.Select)

    let insert = Result.defaultValue false entity.Insert
    let delete = Result.defaultValue OFEFalse entity.Delete

    let ret =
        { Check = entity.Check
          Insert = insert
          Select = entity.Select
          Update = entity.Update
          Delete = delete
          CombinedInsert = parentAllowed.Insert && insert
          CombinedDelete = select && not (optimizedIsFalse delete)
          CombinedSelect = select
        }

    (fields, ret)

let private unionFlatEntities (a : FlatAllowedEntity) (b : FlatAllowedEntity) : FlatAllowedEntity =
    { // Allowed entities for all roles are expected to be fully built when merging flat entities.
      Roles = Map.union a.Roles b.Roles
    }

let private unionFlatAllowedDatabases : FlatAllowedDatabase -> FlatAllowedDatabase -> FlatAllowedDatabase = Map.unionWith unionFlatEntities

let private unionFlatRoles (a : FlatRole) (b: FlatRole) =
    { Entities = unionFlatAllowedDatabases a.Entities b.Entities
    }

type private RoleResolver (layout : Layout, forceAllowBroken : bool, allowedDb : SourceAllowedDatabase) =
    let mutable resolved : Map<ResolvedEntityRef, PossiblyBroken<HalfAllowedEntity>> = Map.empty
    let mutable flattened : Map<ResolvedEntityRef, FlatAllowedRoleEntity> = Map.empty

    let resolveRestriction (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowIds : bool) (where : string) : ResolvedOptimizedFieldExpr =
        let whereExpr =
            match parse tokenizeFunQL fieldExpr where with
            | Ok r -> r
            | Error msg -> raisef ResolvePermissionsException "Error parsing: %s" msg

        let entityInfo = SFEntity entityRef
        let (localArguments, expr) =
            try
                resolveSingleFieldExpr layout OrderedMap.empty localExprFromEntityId emptyExprResolutionFlags entityInfo whereExpr
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
        let resolveOne allowIds = Option.map (resolveRestriction fieldRef.Entity entity allowIds) >> Option.defaultValue OFEFalse
        { Insert = allowedField.Insert
          Update = resolveOne true allowedField.Update
          Select = resolveOne true allowedField.Select
          Check = resolveOne false allowedField.Check
        }

    let resolveSelfAllowedEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (parentEntity : HalfAllowedEntity option) (allowedEntity : SourceAllowedEntity) : AllowedEntity =
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
                    | _ -> OFETrue

        let check = propagateFromParent (fun pent -> pent.Check) false allowedEntity.Check
        let select = propagateFromParent (fun pent -> pent.Select) true allowedEntity.Select
        let update = propagateFromParent (fun pent -> pent.Update) true allowedEntity.Update
        let delete = propagateFromParent (fun pent -> Result.defaultValue OFEFalse pent.Delete) true allowedEntity.Delete

        let fields = allowedEntity.Fields |> Map.map mapField

        match check with
        | OFETrue
        | OFEFalse -> ()
        | expr -> raisef ResolvePermissionsException "Non-trivial check expressions are not currently supported"

        { AllowBroken = allowedEntity.AllowBroken
          Fields = fields
          Check = check
          Insert = Ok allowedEntity.Insert
          Select = select
          Update = update
          Delete = Ok delete
        }

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

    let checkEntity (entity : ResolvedEntity) (allowedEntity : AllowedEntity) : AllowedEntity =
        let iterField (name : FieldName) (allowedField : AllowedField) =
            try
                let noCheck = optimizedIsFalse allowedEntity.Check || optimizedIsFalse allowedField.Check
                if allowedField.Insert && noCheck then
                    raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                match allowedField.Update with
                | OFEFalse -> ()
                | expr ->
                    if noCheck then
                        raisef ResolvePermissionsException "Cannot allow to update without providing check expression"
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed field %O" name

        Map.iter iterField allowedEntity.Fields

        let allowedEntity =
            if Result.defaultValue false allowedEntity.Insert then
                try
                    if entity.IsAbstract then
                        raisef ResolvePermissionsException "Cannot allow insertion of abstract entities"
                    checkParentRowAccess entity <| function
                        | Some { Insert = Ok true } -> ()
                        | Some { Insert = Error err } -> raisefWithInner ResolvePermissionsException err "Parent entity insert is broken"
                        | _ -> raisef ResolvePermissionsException "Parent entity insert is forbidden"
                    // Check that we can change all required fields.
                    if optimizedIsFalse allowedEntity.Check then
                        raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                    for KeyValue(fieldName, field) in entity.ColumnFields do
                        if Option.isNone field.InheritedFrom && not (fieldIsOptional field) then
                            match Map.tryFind fieldName allowedEntity.Fields with
                            | Some { Insert = true } -> ()
                            | _ -> raisef ResolvePermissionsException "Required field %O is not allowed for inserting" fieldName
                    allowedEntity
                with
                | :? ResolvePermissionsException as e when allowedEntity.AllowBroken || forceAllowBroken ->
                    { allowedEntity with Insert = Error e }
            else
                allowedEntity

        if not (optimizedIsFalse allowedEntity.Update) then
            // Check that we can change all required fields.
            if optimizedIsFalse allowedEntity.Check then
                raisef ResolvePermissionsException "Cannot allow to update without providing check expression"

        let allowedEntity =
            if allowedEntity.Delete |> Result.defaultValue OFEFalse |> optimizedIsFalse |> not then
                try
                    checkParentRowAccess entity <| function
                        | Some { Delete = Ok delete } when not <| optimizedIsFalse delete -> ()
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
                    { allowedEntity with Delete = Error e }
            else
                allowedEntity

        allowedEntity

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

        let resolved = resolveSelfAllowedEntity entityRef entity parentEntity source
        let resolved = checkEntity entity resolved
        let (fields, myFlat) = flattenAllowedEntity entityRef parentEntity resolved

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
        let ret =
            { Allowed = resolved
              Flattened = myFlat
            } : HalfAllowedEntity
        flattened <- Map.add entity.Root flatEntity flattened
        ret

    and resolveAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : PossiblyBroken<HalfAllowedEntity> =
        match Map.tryFind entityRef resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = Ok <| resolveOneAllowedEntity entityRef source
                resolved <- Map.add entityRef ret resolved
                ret
            with
            | :? ResolvePermissionsException as e when source.AllowBroken || forceAllowBroken ->
                let ret = Error { Error = e; AllowBroken = source.AllowBroken }
                resolved <- Map.add entityRef ret resolved
                ret

    let resolveAllowedSchema (schemaName : SchemaName) (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : AllowedSchema =
        let mapEntity name (allowedEntity : SourceAllowedEntity) : PossiblyBroken<AllowedEntity> =
            try
                let entityRef = { Schema = schemaName; Name = name }
                resolveAllowedEntity entityRef allowedEntity |> Result.map (fun ret -> ret.Allowed)
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed entity %O" name

        { Entities = allowedSchema.Entities |> Map.map mapEntity
        }

    let resolveAllowedDatabase (): AllowedDatabase =
        let mapSchema name allowedSchema =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolvePermissionsException "Undefined schema"
                    | Some s -> s
                resolveAllowedSchema name schema allowedSchema
            with
            | e -> raisefWithInner ResolvePermissionsException e "In allowed schema %O" name

        { Schemas = allowedDb.Schemas |> Map.map mapSchema
        }

    member this.ResolveAllowedDatabase () = resolveAllowedDatabase ()
    member this.Flattened = flattened

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, permissions : SourcePermissions) =
    let mutable resolved : Map<ResolvedRoleRef, PossiblyBroken<ResolvedRole>> = Map.empty

    let rec resolveOneRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : ResolvedRole =
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
            | Ok role -> role.Flattened
            | Error e -> raisefWithInner ResolvePermissionsParentException e.Error "Error in parent %O" parentRef
        let resolver = RoleResolver (layout, forceAllowBroken, role.Permissions)
        let flattenedParents = role.Parents |> Set.toSeq |> Seq.map resolveParent |> Seq.fold unionFlatRoles emptyFlatRole
        let resolved = resolver.ResolveAllowedDatabase ()
        let flattened =
            { Entities = Map.map (fun name roleEntity -> { Roles = Map.singleton ref roleEntity }) resolver.Flattened
            }

        { Parents = role.Parents
          Permissions = resolved
          Flattened = unionFlatRoles flattenedParents flattened
        }

    and resolveRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : PossiblyBroken<ResolvedRole> =
        match Map.tryFind ref resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = resolveOneRole stack ref role
                resolved <- Map.add ref (Ok ret) resolved
                Ok ret
            with
            | :? ResolvePermissionsException as e when role.AllowBroken || forceAllowBroken ->
                Error { Error = e; AllowBroken = role.AllowBroken }

    let resolvePermissionsSchema (schemaName : SchemaName) (schema : SourcePermissionsSchema) : PermissionsSchema =
        let mapRole name role =
            try
                checkName name
                let ref = { Schema = schemaName; Name = name }
                resolveRole Set.empty ref role
            with
            | e -> raisefWithInner ResolvePermissionsException e "In role %O" name

        { Roles = schema.Roles |> Map.map mapRole
        }

    let resolvePermissions () : Permissions =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolvePermissionsException "Unknown schema name"
                resolvePermissionsSchema name schema
            with
            | e -> raisefWithInner ResolvePermissionsException e "In schema %O" name

        { Schemas = permissions.Schemas |> Map.map mapSchema
        }

    member this.ResolvePermissions = resolvePermissions

let resolvePermissions (layout : Layout) (forceAllowBroken : bool) (source : SourcePermissions) : Permissions =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, source)
    phase1.ResolvePermissions ()
