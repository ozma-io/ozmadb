module FunWithFlags.FunDB.Permissions.Resolve

open FunWithFlags.FunUtils
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
    { Allowed : AllowedEntity
      Flat : FlatAllowedEntity
      Error : exn option
    }

let emptyRestriction : Restriction =
    { Expression = OFEFalse
      GlobalArguments = Set.empty
    }

let andRestriction (a : Restriction) (b : Restriction) =
    // FIXME: drop unused global arguments here after optimization
    { Expression = andFieldExpr a.Expression b.Expression
      GlobalArguments = Set.union a.GlobalArguments b.GlobalArguments
    }

let orRestriction (a : Restriction) (b : Restriction) =
    // FIXME: drop unused global arguments here after optimization
    { Expression = orFieldExpr a.Expression b.Expression
      GlobalArguments = Set.union a.GlobalArguments b.GlobalArguments
    }

let private flattenAllowedEntity (entityRef : ResolvedEntityRef) (ent : AllowedEntity) : FlatAllowedEntity =
    let fields = ent.Fields |> Map.toSeq |> Seq.map (fun (name, field) -> ({ entity = entityRef; name = name }, field)) |> Map.ofSeq

    let derived =
        { Check = ent.Check
          Insert = ent.Insert
          Select = ent.Select
          Update = ent.Update
          Delete = ent.Delete
        }
    { Children = Map.singleton entityRef derived
      Fields = fields
    }
let private mergeField (a : AllowedField) (b : AllowedField) : AllowedField =
    { Change = a.Change || b.Change
      Select = orRestriction a.Select b.Select
    }

let private mergeFlatDerivedEntity (a : FlatAllowedDerivedEntity) (b : FlatAllowedDerivedEntity) : FlatAllowedDerivedEntity =
    { Check = orRestriction a.Check b.Check
      Insert = a.Insert || b.Insert
      Select = orRestriction a.Select b.Select
      Update = orRestriction a.Update b.Update
      Delete = orRestriction a.Delete b.Delete
    }

let private mergeFlatEntity (a : FlatAllowedEntity) (b : FlatAllowedEntity) : FlatAllowedEntity =
    { Children = Map.unionWith (fun name -> mergeFlatDerivedEntity) a.Children b.Children
      Fields = Map.unionWith (fun name -> mergeField) a.Fields b.Fields
    }

type private RoleResolver (layout : Layout, forceAllowBroken : bool, allowedDb : SourceAllowedDatabase, startingFlattened : FlatAllowedDatabase) =
    let mutable resolved : Map<ResolvedEntityRef, Result<HalfResolvedEntity, AllowedEntityError>> = Map.empty
    let mutable flattened = startingFlattened

    let rec checkPath (allowIds : bool) (entity : ResolvedEntity) (name : FieldName) (fields : FieldName list) : unit =
        match fields with
        | [] ->
            match entity.FindField name with
            | None -> raisef ResolvePermissionsException "Column not found: %O" name
            | Some { Field = RId } ->
                if allowIds then
                    ()
                else
                    raisef ResolvePermissionsException "Ids aren't allowed: %O" name
            | Some { Field = RSubEntity }
            | Some { Field = RColumnField _ } -> ()
            | Some { Field = RComputedField comp } ->
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
            | { Ref = VRColumn { entity = None; name = name }; Path = path } ->
                checkPath allowIds entity name (Array.toList path)
                let fieldRef = { entity = entityRef; name = name }
                let bound = { Ref = fieldRef; Immediate = true }
                { Ref = VRColumn { Ref = { entity = Some relaxedRef; name = name }; Bound = Some bound }; Path = path }
            | { Ref = VRPlaceholder (PLocal name) } ->
                raisef ResolvePermissionsException "Local argument %O is not allowed" name
            | { Ref = VRPlaceholder (PGlobal name as arg); Path = path } ->
                let argInfo =
                    match Map.tryFind name globalArgumentTypes with
                    | None -> raisef ResolvePermissionsException "Unknown global argument: %O" ref
                    | Some argInfo -> argInfo
                if not <| Array.isEmpty path then
                    match argInfo.ArgType with
                    | FTReference (entityRef, where) ->
                        let (name, remainingPath) =
                            match Array.toList path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "impossible"
                        let argEntity = layout.FindEntity entityRef |> Option.get
                        checkPath allowIds argEntity name remainingPath
                    | _ -> raisef ResolvePermissionsException "Argument is not a reference: %O" ref
                globalArguments <- Set.add name globalArguments
                { Ref = VRPlaceholder arg; Path = path }
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
                  Aggregate = voidAggr
            }
        let expr = mapFieldExpr mapper whereExpr |> optimizeFieldExpr
        { Expression = expr
          GlobalArguments = globalArguments
        }

    let resolveAllowedField (fieldRef : ResolvedFieldRef) (entity : ResolvedEntity) (allowedField : SourceAllowedField) : AllowedField =
        let field =
            match Map.tryFind fieldRef.name entity.columnFields with
            | None -> raisef ResolvePermissionsException "Unknown field"
            | Some f -> f
        if Option.isSome field.inheritedFrom then
            raisef ResolvePermissionsException "Cannot define restrictions on parent entity fields in children"
        let resolveOne = Option.map (resolveRestriction fieldRef.entity entity true) >> Option.defaultValue emptyRestriction
        { Change = allowedField.Change
          Select = resolveOne allowedField.Select
        }

    let resolveSelfAllowedEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowedEntity : SourceAllowedEntity) : (exn option * AllowedEntity) =
        let mutable error = None

        let mapField name (allowedField : SourceAllowedField) =
            try
                resolveAllowedField { entity = entityRef; name = name } entity allowedField
            with
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In allowed field %O: %s" name e.Message

        let resolveOne allowIds = Option.map (resolveRestriction entityRef entity allowIds) >> Option.defaultValue emptyRestriction
        let fields = allowedEntity.Fields |> Map.map mapField

        let check = resolveOne false allowedEntity.Check

        let select = resolveOne true allowedEntity.Select

        let resolveUpdate update = resolveRestriction entityRef entity true update

        let update = Option.map resolveUpdate allowedEntity.Update |> Option.defaultValue emptyRestriction

        let resolveDelete delete = resolveRestriction entityRef entity true delete

        let delete = Option.map resolveDelete allowedEntity.Delete |> Option.defaultValue emptyRestriction

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

    let checkFlatEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (allowedEntity : AllowedEntity) (flat : FlatAllowedEntity) : exn option * FlatAllowedDerivedEntity =
        let mutable error = None

        let myPerms = Map.find entityRef flat.Children

        let iterField (ref : ResolvedFieldRef) (allowedField : AllowedField) =
            if ref.entity = entityRef then
                try
                    if allowedField.Change && optimizedIsFalse myPerms.Check.Expression then
                        raisef ResolvePermissionsException "Cannot allow to change without providing check expression"
                with
                | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In allowed field %O: %s" ref e.Message

        Map.iter iterField flat.Fields

        let myPerms =
            if myPerms.Insert then
                try
                    if entity.isAbstract then
                        raisef ResolvePermissionsException "Cannot allow insertion of abstract entities"
                    // Check that we can change all required fields.
                    if optimizedIsFalse myPerms.Check.Expression then
                        raisef ResolvePermissionsException "Cannot allow to insert without providing check expression"
                    for KeyValue(fieldName, field) in entity.columnFields do
                        if Option.isNone field.defaultValue && not field.isNullable then
                            let parentEntity = Option.defaultValue entityRef field.inheritedFrom
                            match Map.tryFind { entity = parentEntity; name = fieldName } flat.Fields with
                            | Some { Change = true } -> ()
                            | _ -> raisef ResolvePermissionsException "Required field %O is not allowed for inserting" fieldName
                    myPerms
                with
                | :? ResolvePermissionsException as e when allowedEntity.AllowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { myPerms with Insert = false }
            else
                myPerms

        if not (optimizedIsFalse myPerms.Update.Expression) then
            // Check that we can change all required fields.
            if optimizedIsFalse myPerms.Check.Expression then
                raisef ResolvePermissionsException "Cannot allow to update without providing check expression"
            if optimizedIsFalse myPerms.Select.Expression then
                raisef ResolvePermissionsException "Cannot allow to update without allowing to select"

        let myPerms =
            if not (optimizedIsFalse myPerms.Delete.Expression) then
                try
                    // Check that we can can view all column fields.
                    if entity.isAbstract then
                        raisef ResolvePermissionsException "Cannot allow deletion of abstract entities"
                    if optimizedIsFalse myPerms.Select.Expression then
                        raisef ResolvePermissionsException "Cannot allow to delete without allowing to select"
                    for KeyValue(fieldName, field) in entity.columnFields do
                        let parentEntity = Option.defaultValue entityRef field.inheritedFrom
                        match Map.tryFind { entity = parentEntity; name = fieldName } flat.Fields with
                        | Some f when not (optimizedIsFalse f.Select.Expression) -> ()
                        | _ -> raisef ResolvePermissionsException "Field %O is not allowed for selection, which is required for deletion" fieldName
                    myPerms
                with
                | :? ResolvePermissionsException as e when allowedEntity.AllowBroken || forceAllowBroken ->
                    error <- Some (e :> exn)
                    { myPerms with Delete = emptyRestriction }
            else
                myPerms

        (error, myPerms)

    let rec resolveOneAllowedEntity (entityRef : ResolvedEntityRef) (source : SourceAllowedEntity) : HalfResolvedEntity =
        // Recursively resolve access rights for entity parents.
        let rec resolveParent parentRef =
            let parentRights =
                match Map.tryFind parentRef.schema allowedDb.Schemas with
                | None -> None
                | Some parentSchema -> Map.tryFind parentRef.name parentSchema.Entities
            match parentRights with
            | Some right ->
                match resolveAllowedEntity parentRef right with
                | Ok ret -> Seq.singleton ret.Flat
                | Error err -> raisefWithInner ResolvePermissionsParentException err.Error "In parent %O" parentRef
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
        let newResultFlat = { resultFlat with Children = Map.add entityRef newMyPerms resultFlat.Children }
        let error = Option.orElseWith (fun _ -> error2) error1
        let ret =
            { Allowed = resolved
              Flat = newResultFlat
              Error = error
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
            | :? ResolvePermissionsException as e when source.AllowBroken || forceAllowBroken ->
                let ret = Error ({ Source = source; Error = e } : AllowedEntityError)
                resolved <- Map.add entityRef ret resolved
                ret

    let resolveAllowedSchema (schemaName : SchemaName) (schema : ResolvedSchema) (allowedSchema : SourceAllowedSchema) : ErroredAllowedSchema * AllowedSchema =
        let mutable errors = Map.empty

        let mapEntity name (allowedEntity : SourceAllowedEntity) : Result<AllowedEntity, AllowedEntityError> =
            try
                let entityRef = { schema = schemaName; name = name }
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
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In allowed entity %O: %s" name e.Message

        let ret =
            { Entities = allowedSchema.Entities |> Map.map mapEntity
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
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In allowed schema %O: %s" name e.Message

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
                    match Map.tryFind parentRef.schema permissions.Schemas with
                    | None -> raisef ResolvePermissionsException "Undefined parent schema for %O" parentRef
                    | Some s -> s
            let parentRole =
                match Map.tryFind parentRef.name schema.Roles with
                | None -> raisef ResolvePermissionsException "Undefined parent role for %O" parentRef
                | Some s -> s
            match resolveRole newStack parentRef parentRole with
            | Ok (errors, role) -> role.Flattened
            | Error e -> raisefWithInner ResolvePermissionsParentException e "Error in parent %O" parentRef
        let flattenedParents = role.Parents |> Set.toSeq |> Seq.map resolveParent |> Seq.fold (Map.unionWith (fun name -> mergeFlatEntity)) Map.empty
        let resolver = RoleResolver (layout, forceAllowBroken, role.Permissions, flattenedParents)
        let (errors, resolved) = resolver.ResolveAllowedDatabase ()

        let ret =
            { Parents = role.Parents
              Permissions = resolved
              Flattened = resolver.Flattened
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
                let ref = { schema = schemaName; name = name }
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
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In role %O: %s" name e.Message

        let ret =
            { Roles = schema.Roles |> Map.map mapRole
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
            | :? ResolvePermissionsException as e -> raisefWithInner ResolvePermissionsException e.InnerException "In schema %O: %s" name e.Message

        let ret =
            { Schemas = permissions.Schemas |> Map.map mapSchema
            }
        (errors, ret)

    member this.ResolvePermissions = resolvePermissions

let resolvePermissions (layout : Layout) (forceAllowBroken : bool) (source : SourcePermissions) : ErroredPermissions * Permissions =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, source)
    phase1.ResolvePermissions ()