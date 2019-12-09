module FunWithFlags.FunDB.Permissions.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Types

type ResolvePermissionsException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolvePermissionsException (message, null)

type ResolvePermissionsParentException (message : string, innerException : Exception) =
    inherit ResolvePermissionsException(message, innerException)

    new (message : string) = ResolvePermissionsParentException (message, null)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolvePermissionsException "Invalid role name"

type private HalfResolvedEntity =
    { allowed : AllowedEntity
      flat : FlatAllowedEntity
      error : exn option
    }

let emptyRestriction : Restriction =
    { expression = OFEFalse
      globalArguments = Set.empty
    }

let andRestriction (a : Restriction) (b : Restriction) =
    // FIXME: drop unused global arguments here after optimization
    { expression = andFieldExpr a.expression b.expression
      globalArguments = Set.union a.globalArguments b.globalArguments
    }

let orRestriction (a : Restriction) (b : Restriction) =
    // FIXME: drop unused global arguments here after optimization
    { expression = orFieldExpr a.expression b.expression
      globalArguments = Set.union a.globalArguments b.globalArguments
    }

let private flattenAllowedEntity (entityRef : ResolvedEntityRef) (ent : AllowedEntity) : FlatAllowedEntity =
    let fields = ent.fields |> Map.toSeq |> Seq.map (fun (name, field) -> ({ entity = entityRef; name = name }, field)) |> Map.ofSeq

    let derived =
        { check = ent.check
          insert = ent.insert
          select = ent.select
          update = ent.update
          delete = ent.delete
        }
    { children = Map.singleton entityRef derived
      fields = fields
    }
let private mergeField (a : AllowedField) (b : AllowedField) : AllowedField =
    { change = a.change || b.change
      select = orRestriction a.select b.select
    }

let private mergeFlatDerivedEntity (a : FlatAllowedDerivedEntity) (b : FlatAllowedDerivedEntity) : FlatAllowedDerivedEntity =
    { check = orRestriction a.check b.check
      insert = a.insert || b.insert
      select = orRestriction a.select b.select
      update = orRestriction a.update b.update
      delete = orRestriction a.delete b.delete
    }

let private mergeFlatEntity (a : FlatAllowedEntity) (b : FlatAllowedEntity) : FlatAllowedEntity =
    { children = Map.unionWith (fun name -> mergeFlatDerivedEntity) a.children b.children
      fields = Map.unionWith (fun name -> mergeField) a.fields b.fields
    }

type private RoleResolver (layout : Layout, forceAllowBroken : bool, allowedDb : SourceAllowedDatabase, startingFlattened : FlatAllowedDatabase) =
    let mutable resolved : Map<ResolvedEntityRef, Result<HalfResolvedEntity, AllowedEntityError>> = Map.empty
    let mutable flattened = startingFlattened

    let rec checkPath (allowIds : bool) (entity : ResolvedEntity) (name : FieldName) (fields : FieldName list) : unit =
        match fields with
        | [] ->
            match entity.FindField name with
            | None -> raisef ResolvePermissionsException "Column not found: %O" name
            | Some (_, RId) ->
                if allowIds then
                    ()
                else
                    raisef ResolvePermissionsException "Ids aren't allowed: %O" name
            | Some (_, RSubEntity)
            | Some (_, RColumnField _) -> ()
            | Some (_, RComputedField comp) ->
                if allowIds || not comp.hasId then
                    ()
                else
                    raisef ResolvePermissionsException "Ids aren't allowed: %O" name
        | (ref :: refs) ->
            match Map.tryFind name entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity.name (Map.find refEntity.schema layout.schemas).entities
                checkPath allowIds newEntity ref refs
            | _ -> raisef ResolvePermissionsException "Invalid dereference in path: %O" ref

    let resolveRestriction (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowIds : bool) (where : string) : Restriction =
        let whereExpr =
            match parse tokenizeFunQL fieldExpr where with
            | Ok r -> r
            | Error msg -> raisef ResolvePermissionsException "Error parsing: %s" msg

        let relaxedRef = relaxEntityRef entityRef
        let mutable globalArguments = Set.empty

        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { ref = VRColumn { entity = None; name = name }; path = path } ->
                checkPath allowIds entity name (Array.toList path)
                let fieldRef = { entity = entityRef; name = name }
                let bound = { ref = fieldRef; immediate = true }
                { ref = VRColumn { ref = { entity = Some relaxedRef; name = name }; bound = Some bound }; path = path }
            | { ref = VRPlaceholder (PLocal name) } ->
                raisef ResolvePermissionsException "Local argument %O is not allowed" name
            | { ref = VRPlaceholder (PGlobal name as arg); path = path } ->
                let argInfo =
                    match Map.tryFind name globalArgumentTypes with
                    | None -> raisef ResolvePermissionsException "Unknown global argument: %O" ref
                    | Some argInfo -> argInfo
                if not <| Array.isEmpty path then
                    match argInfo.argType with
                    | FTReference (entityRef, where) ->
                        let (name, remainingPath) =
                            match Array.toList path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "impossible"
                        let argEntity = layout.FindEntity entityRef |> Option.get
                        checkPath allowIds argEntity name remainingPath
                    | _ -> raisef ResolvePermissionsException "Argument is not a reference: %O" ref
                globalArguments <- Set.add name globalArguments
                { ref = VRPlaceholder arg; path = path }
            | ref ->
                raisef ResolvePermissionsException "Invalid reference: %O" ref
        let resolveQuery query =
            let (usedArgs, newQuery) = resolveSelectExpr layout query
            for arg in usedArgs do
                match arg with
                | PGlobal name ->
                    globalArguments <- Set.add name globalArguments
                | PLocal name ->
                    raisef ResolvePermissionsException "Local argument %O is not allowed" name
            newQuery
        let voidAggr aggr = raisef ViewResolveException "Forbidden aggregate function in a restriction"

        let mapper =
            { idFieldExprMapper resolveReference resolveQuery with
                  aggregate = voidAggr
            }
        let expr = mapFieldExpr mapper whereExpr |> optimizeFieldExpr
        { expression = expr
          globalArguments = globalArguments
        }

    let resolveAllowedField (fieldRef : ResolvedFieldRef) (entity : ResolvedEntity) (allowedField : SourceAllowedField) : AllowedField =

        let field =
            match Map.tryFind fieldRef.name entity.columnFields with
            | None -> raisef ResolvePermissionsException "Unknown field"
            | Some f -> f
        if Option.isSome field.inheritedFrom then
            raisef ResolvePermissionsException "Cannot define restrictions on parent entity fields in children"
        let resolveOne = Option.map (resolveRestriction fieldRef.entity entity true) >> Option.defaultValue emptyRestriction
        { change = allowedField.change
          select = resolveOne allowedField.select
        }

    let resolveSelfAllowedEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowedEntity : SourceAllowedEntity) : (exn option * AllowedEntity) =
        let mutable error = None

        let mapField name (allowedField : SourceAllowedField) =
            try
                resolveAllowedField { entity = entityRef; name = name } entity allowedField
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed field %O: %s" name e.Message

        let resolveOne allowIds = Option.map (resolveRestriction entityRef entity allowIds) >> Option.defaultValue emptyRestriction
        let fields = allowedEntity.fields |> Map.map mapField

        let check = resolveOne false allowedEntity.check

        let select = resolveOne true allowedEntity.select

        let resolveUpdate update = resolveRestriction entityRef entity true update

        let update = Option.map resolveUpdate allowedEntity.update |> Option.defaultValue emptyRestriction

        let resolveDelete delete = resolveRestriction entityRef entity true delete

        let delete = Option.map resolveDelete allowedEntity.delete |> Option.defaultValue emptyRestriction

        let ret =
            { allowBroken = allowedEntity.allowBroken
              fields = fields
              check = check
              insert = allowedEntity.insert
              select = select
              update = update
              delete = delete
            }
        (error, ret)

    let checkFlatEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowedEntity : AllowedEntity) (flat : FlatAllowedEntity) : exn option * FlatAllowedDerivedEntity =
        let mutable error = None

        let myPerms = Map.find entityRef flat.children

        let iterField (ref : ResolvedFieldRef) (allowedField : AllowedField) =
            if ref.entity = entityRef then
                try
                    if allowedField.change && optimizedIsFalse myPerms.check.expression then
                        raisef ResolvePermissionsException "Cannot allow to change without providing check expression"
                with
                | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed field %O: %s" ref e.Message

        Map.iter iterField flat.fields

        let myPerms =
            if myPerms.insert then
                try
                    if entity.isAbstract then
                        raisef ResolvePermissionsException "Cannot allow insertion of abstract entities"
                    // Check that we can change all required fields.
                    if optimizedIsFalse myPerms.check.expression then
                        raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                    for KeyValue(fieldName, field) in entity.columnFields do
                        if Option.isNone field.defaultValue && not field.isNullable then
                            let parentEntity = Option.defaultValue entityRef field.inheritedFrom
                            match Map.tryFind { entity = parentEntity; name = fieldName } flat.fields with
                            | Some { change = true } -> ()
                            | _ -> raisef ResolvePermissionsException "Required field %O is not allowed for inserting" fieldName
                    myPerms
                with
                | :? ResolvePermissionsException as e when allowedEntity.allowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { myPerms with insert = false }
            else
                myPerms

        if not (optimizedIsFalse myPerms.update.expression) then
            // Check that we can change all required fields.
            if optimizedIsFalse myPerms.check.expression then
                raisef ResolvePermissionsException "Cannot allow to update without providing check expression"
            if optimizedIsFalse myPerms.select.expression then
                raisef ResolvePermissionsException "Cannot allow to update without allowing to select"

        let myPerms =
            if not (optimizedIsFalse myPerms.delete.expression) then
                try
                    // Check that we can can view all column fields.
                    if entity.isAbstract then
                        raisef ResolvePermissionsException "Cannot allow deletion of abstract entities"
                    if optimizedIsFalse myPerms.select.expression then
                        raisef ResolvePermissionsException "Cannot allow to delete without allowing to select"
                    for KeyValue(fieldName, field) in entity.columnFields do
                        let parentEntity = Option.defaultValue entityRef field.inheritedFrom
                        match Map.tryFind { entity = parentEntity; name = fieldName } flat.fields with
                        | Some f when not (optimizedIsFalse f.select.expression) -> ()
                        | _ -> raisef ResolvePermissionsException "Field %O is not allowed for selection, which is required for deletion" fieldName
                    myPerms
                with
                | :? ResolvePermissionsException as e when allowedEntity.allowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { myPerms with delete = emptyRestriction }
            else
                myPerms

        (error, myPerms)

    let rec resolveOneAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : HalfResolvedEntity =
        // Recursively resolve access rights for entity parents.
        let rec resolveParent parentRef =
            let parentRights =
                match Map.tryFind parentRef.schema allowedDb.schemas with
                | None -> None
                | Some parentSchema -> Map.tryFind parentRef.name parentSchema.entities
            match parentRights with
            | Some right ->
                match resolveAllowedEntity parentRef right with
                | Ok ret -> Seq.singleton ret.flat
                | Error err -> raisefWithInner ResolvePermissionsParentException err.error "Error in parent %O" parentRef
            | None ->
                let parentEntity = layout.FindEntity parentRef |> Option.get
                match parentEntity.inheritance with
                | None -> Seq.empty
                | Some inheritance -> resolveParent inheritance.parent

        let entity =
            match layout.FindEntity entityRef with
            | None -> raisef ResolvePermissionsException "Undefined entity"
            | Some s -> s
        let parentFlat =
            match entity.inheritance with
            | None -> Seq.empty
            | Some inheritance -> resolveParent inheritance.parent

        let (error1, resolved) = resolveSelfAllowedEntity entityRef entity source
        let myFlat = flattenAllowedEntity entityRef resolved
        let inheritedFlat =
            match Map.tryFind entity.root flattened with
            | Some f -> Seq.singleton f
            | None -> Seq.empty
        let resultFlat = Seq.concat [Seq.singleton myFlat; parentFlat; inheritedFlat] |> Seq.fold1 mergeFlatEntity
        let (error2, newMyPerms) = checkFlatEntity entityRef entity resolved resultFlat
        let newResultFlat = { resultFlat with children = Map.add entityRef newMyPerms resultFlat.children }
        let error = Option.orElseWith (fun _ -> error2) error1
        let ret =
            { allowed = resolved
              flat = newResultFlat
              error = error
            } : HalfResolvedEntity
        flattened <- Map.add entity.root newResultFlat flattened
        ret

    and resolveAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : Result<HalfResolvedEntity, AllowedEntityError> =
        match Map.tryFind entityRef resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = Ok <| resolveOneAllowedEntity entityRef source
                resolved <- Map.add entityRef ret resolved
                ret
            with
            | :? ResolvePermissionsException as e when source.allowBroken || forceAllowBroken ->
                let ret = Error ({ source = source; error = e } : AllowedEntityError)
                resolved <- Map.add entityRef ret resolved
                ret

    let resolveAllowedSchema (schemaName : SchemaName) (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : ErroredAllowedSchema * AllowedSchema =
        let mutable errors = Map.empty

        let mapEntity name (allowedEntity : SourceAllowedEntity) : Result<AllowedEntity, AllowedEntityError> =
            try
                let entityRef = { schema = schemaName; name = name }
                match resolveAllowedEntity entityRef allowedEntity with
                | Ok ret ->
                    match ret.error with
                    | Some e ->
                        errors <- Map.add name e errors
                    | None -> ()
                    Ok ret.allowed
                | Error e ->
                    match e.error with
                    | :? ResolvePermissionsParentException ->
                        Error e
                    | _ ->
                        errors <- Map.add name e.error errors
                        Error e
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed entity %O: %s" name e.Message

        let ret =
            { entities = allowedSchema.entities |> Map.map mapEntity
            } : AllowedSchema
        (errors, ret)

    let resolveAllowedDatabase (): ErroredAllowedDatabase * AllowedDatabase =
        let mutable errors = Map.empty

        let mapSchema name allowedSchema =
            try
                let schema =
                    match Map.tryFind name layout.schemas with
                    | None -> raisef ResolvePermissionsException "Undefined schema"
                    | Some s -> s
                let (schemaErrors, newAllowed) = resolveAllowedSchema name schema allowedSchema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newAllowed
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "Error in allowed schema %O: %s" name e.Message

        let ret =
            { schemas = allowedDb.schemas |> Map.map mapSchema
            } : AllowedDatabase
        (errors, ret)

    member this.ResolveAllowedDatabase () = resolveAllowedDatabase ()
    member this.Flattened = flattened

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, permissions : SourcePermissions) =
    let mutable resolved : Map<ResolvedRoleRef, Result<ErroredAllowedDatabase * ResolvedRole, RoleError>> = Map.empty

    let rec resolveOneRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : ErroredAllowedDatabase * ResolvedRole =
        if Set.contains ref stack then
            raisef ResolvePermissionsException "Cycle detected: %O" stack
        let newStack = Set.add ref stack
        let resolveParent parentRef =
            let schema =
                    match Map.tryFind parentRef.schema permissions.schemas with
                    | None -> raisef ResolvePermissionsException "Undefined parent schema for %O" parentRef
                    | Some s -> s
            let parentRole =
                match Map.tryFind parentRef.name schema.roles with
                | None -> raisef ResolvePermissionsException "Undefined parent role for %O" parentRef
                | Some s -> s
            match resolveRole newStack parentRef parentRole with
            | Ok (errors, role) -> role.flattened
            | Error e -> raisefWithInner ResolvePermissionsParentException e.error "Error in parent %O" parentRef
        let flattenedParents = role.parents |> Set.toSeq |> Seq.map resolveParent |> Seq.fold (Map.unionWith (fun name -> mergeFlatEntity)) Map.empty
        let resolver = RoleResolver (layout, forceAllowBroken, role.permissions, flattenedParents)
        let (errors, resolved) = resolver.ResolveAllowedDatabase ()

        let ret =
            { parents = role.parents
              permissions = resolved
              flattened = resolver.Flattened
              allowBroken = role.allowBroken
            }
        (errors, ret)

    and resolveRole (stack : Set<ResolvedRoleRef>) (ref : ResolvedRoleRef) (role : SourceRole) : Result<ErroredAllowedDatabase * ResolvedRole, RoleError> =
        match Map.tryFind ref resolved with
        | Some ret -> ret
        | None ->
            try
                let ret = resolveOneRole stack ref role
                resolved <- Map.add ref (Ok ret) resolved
                Ok ret
            with
            | :? ResolvePermissionsException as e when role.allowBroken || forceAllowBroken ->
                let ret = Error ({ source = role; error = e } : RoleError)
                resolved <- Map.add ref ret resolved
                ret

    let resolvePermissionsSchema (schemaName : SchemaName) (schema : SourcePermissionsSchema) : ErroredRoles * PermissionsSchema =
        let mutable errors = Map.empty

        let mapRole name role =
            try
                checkName name
                let ref = { schema = schemaName; name = name }
                match resolveRole Set.empty ref role with
                | Ok (error, ret) ->
                    if not <| Map.isEmpty error then
                        errors <- Map.add name (EDatabase error) errors
                    Ok ret
                | Error e ->
                    match e.error with
                    | :? ResolvePermissionsParentException ->
                        Error e
                    | _ ->
                        errors <- Map.add name (EFatal e.error) errors
                        Error e
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