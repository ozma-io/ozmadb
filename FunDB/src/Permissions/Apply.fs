module FunWithFlags.FunDB.Permissions.Apply

// Allow delayed
#nowarn "40"

open System
open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.FunQL.UsedReferences
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsApplyException (message : string, innerExceptions : Exception seq, isUserException : bool) =
    inherit AggregateException(message, innerExceptions)

    new (message : string, innerException : Exception) =
        PermissionsApplyException (message, Seq.singleton innerException, isUserException innerException)

    new (message : string) = PermissionsApplyException (message, Seq.empty, true)

    member this.IsUserException = isUserException

    interface IUserException with
        member this.IsUserException = isUserException
        member this.UserData = None

type EntityFilter<'e> =
    | FUnfiltered
    | FFiltered of 'e

let filterToOption = function
    | FUnfiltered -> None
    | FFiltered e -> Some e

type EntityFilterExpr = EntityFilter<ResolvedFieldExpr>

// Out aim is to build a database view that is:
// * Restricted (according to current user role);
// * Consistent; that is, it it impossible to witness different views of the database from different sub-expressions,
//   even if this would expose more allowed entries to the client;
// * Full -- as small amount of restrictions should be applied as possible, given requirements above aren't violated.
type AppliedAllowedEntity =
    // option: "did we compile an expression for this access type".
    { SelectUpdate : EntityFilterExpr option // Apply to SELECTs and UPDATEs. Filters out this entity and all its children.
      Delete : EntityFilterExpr option // Apply to DELETEs. Filters out only this entity, not propagated to children.
      Check : EntityFilterExpr option
    }

type AppliedAllowedDatabase = Map<ResolvedEntityRef, AppliedAllowedEntity>

type private HalfAppliedAllowedEntity =
    { SelectUpdate : ResolvedOptimizedFieldExpr option
      Delete : ResolvedOptimizedFieldExpr option
      Check : ResolvedOptimizedFieldExpr option
    }

type private HalfAppliedAllowedDatabase = Map<ResolvedEntityRef, HalfAppliedAllowedEntity>

let private unionOptionalExprs (ma : ResolvedOptimizedFieldExpr option) (mb : ResolvedOptimizedFieldExpr option) : ResolvedOptimizedFieldExpr option =
    match (ma, mb) with
    | (Some a, Some b) -> Some (orFieldExpr a b)
    | (None, None) -> None
    | _ -> failwith "Impossible"

let private unionHalfAppliedAllowedEntities (a : HalfAppliedAllowedEntity) (b : HalfAppliedAllowedEntity) : HalfAppliedAllowedEntity =
    { SelectUpdate = unionOptionalExprs a.SelectUpdate b.SelectUpdate
      Delete = unionOptionalExprs a.Delete b.Delete
      Check = unionOptionalExprs a.Check b.Check
    }

type private EntityAccessFilter =
    { Filter : ResolvedOptimizedFieldExpr
      Check : ResolvedOptimizedFieldExpr
      UsedEntity : UsedEntity
      // Flags are used to optimize building filters.
      // If corresponding `Propagated` flag is set, we
      // expect that all restrictions from parents
      // have already been applied.
      // Otherwise, we traverse parents and build the
      // restriction from scratch.
      PropagatedInsert : bool
      PropagatedSelect : bool
      PropagatedUpdate : bool
      PropagatedDelete : bool
    }

let private emptyEntityAccessFilter : EntityAccessFilter =
    { Filter = OFETrue
      Check = OFETrue
      UsedEntity = emptyUsedEntity
      PropagatedInsert = false
      PropagatedSelect = false
      PropagatedUpdate = false
      PropagatedDelete = false
    }

type private FiltersMap = Map<ResolvedEntityRef, EntityAccessFilter>
type private CachedFilterExprsMap = Map<ResolvedEntityRef, ResolvedOptimizedFieldExpr>

let private inheritedFlatAllowedDerivedEntity : FlatAllowedDerivedEntity =
    { Check = OFETrue
      Select = OFETrue
      Update = OFETrue
      Delete = OFETrue
      Insert = false
      CombinedSelect = true
      CombinedInsert = false
      CombinedDelete = true
    }

let inline private getAccessFilter
        (getParent : ResolvedEntityRef -> ResolvedOptimizedFieldExpr)
        (accessor : ResolvedEntityRef -> FlatAllowedDerivedEntity -> ResolvedOptimizedFieldExpr)
        (layout : Layout) (flatAllowedEntity : FlatAllowedRoleEntity)
        (entityRef : ResolvedEntityRef)
        : ResolvedOptimizedFieldExpr =
    let entity = layout.FindEntity entityRef |> Option.get
    let parentFilter =
        match entity.Parent with
        | None -> OFETrue
        | Some parentRef -> getParent parentRef
    let defaultAllowedEntity =
        if Option.isNone entity.Parent then
            emptyFlatAllowedDerivedEntity
        else
            inheritedFlatAllowedDerivedEntity
    let allowedEntity = Map.findWithDefault entityRef defaultAllowedEntity flatAllowedEntity.Children
    match accessor entityRef allowedEntity with
    | OFEFalse -> raisef PermissionsApplyException "Access denied to entity %O" entityRef
    | thisFilter -> andFieldExpr parentFilter thisFilter

// This class builds _filter expressions_ for each allowed entity.
// Filter expression is a WHERE expression that restricts given entity.
// Filters do not include children and only include parent restrictions at nodes where first access happens.
// For example, in hierarchy A -> B -> C, where B and C are selected:
// * filter for A is `TRUE`;
// * filter for B is `A.Select && B.Select`;
// * filter for C is `C.Select`.
// Expressions take into account all types of access to it. This way we restrict the access uniformly: for example,
// if an entity is accessed for both UPDATE and DELETE, both operations will observe the same rows (restricted for both operations).
type private EntityAccessFilterBuilder (layout : Layout, flatAllowedEntity : FlatAllowedRoleEntity, flatUsedEntity : FlatUsedEntity) =
    let addSelectFilter (usedEntity : UsedEntity) (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let addFieldRestriction (name : FieldName, usedField : UsedField) : ResolvedOptimizedFieldExpr =
            // We never expect `id` or `sub_entity` to be here -- they should be filtered out during used fields flattening.
            let ref = { Entity = entityRef; Name = name }
            match Map.tryFind ref flatAllowedEntity.Fields with
            | Some flatField ->
                let filter =
                    if usedField.Select || usedField.Update then
                        match flatField.Select with
                        | OFEFalse -> raisef PermissionsApplyException "Access denied to select field %O" ref
                        | newSelectFilter -> newSelectFilter
                    else
                        OFETrue
                if usedField.Update then
                    match flatField.Update with
                    | OFEFalse -> raisef PermissionsApplyException "Access denied to update field %O" ref
                    | newUpdateFilter -> andFieldExpr filter newUpdateFilter
                else
                    filter
            | None -> raisef PermissionsApplyException "Access denied to field %O" ref

        usedEntity.Fields |> Map.toSeq |> Seq.map addFieldRestriction |> Seq.fold andFieldExpr allowedEntity.Select

    let addCheckFilter (usedEntity : UsedEntity) (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let addFieldRestriction (name : FieldName, usedField : UsedField) : ResolvedOptimizedFieldExpr =
            let ref = { Entity = entityRef; Name = name }
            match Map.tryFind ref flatAllowedEntity.Fields with
            | Some flatField ->
                if usedField.Insert || usedField.Update then
                    flatField.Check
                else
                    OFETrue
            | None -> raisef PermissionsApplyException "Access denied to field %O" ref

        usedEntity.Fields |> Map.toSeq |> Seq.map addFieldRestriction |> Seq.fold andFieldExpr allowedEntity.Check

    let addStandaloneSelectFilter (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let usedEntity = Map.findWithDefault entityRef emptyUsedEntity flatUsedEntity.Children
        addSelectFilter usedEntity entityRef allowedEntity

    let addStandaloneCheckFilter (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let usedEntity = Map.findWithDefault entityRef emptyUsedEntity flatUsedEntity.Children
        addCheckFilter usedEntity entityRef allowedEntity

    // Build combined parents filter for a given access type.
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getSelectAccessFilter = memoizeN (getAccessFilter (fun ent -> getSelectAccessFilter ent) addStandaloneSelectFilter layout flatAllowedEntity)
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getUpdateAccessFilter = memoizeN (getAccessFilter (fun ent -> getUpdateAccessFilter ent) (fun entityRef allowedEntity -> allowedEntity.Update) layout flatAllowedEntity)
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getDeleteAccessFilter = memoizeN (getAccessFilter (fun ent -> getDeleteAccessFilter ent) (fun entityRef allowedEntity -> allowedEntity.Delete) layout flatAllowedEntity)
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getCheckAccessFilter = memoizeN (getAccessFilter (fun ent -> getCheckAccessFilter ent) addStandaloneCheckFilter layout flatAllowedEntity)

    let rec buildFilter (entityRef : ResolvedEntityRef) : EntityAccessFilter =
        let entity = layout.FindEntity entityRef |> Option.get
        let parentFilter =
            match entity.Parent with
            | None -> emptyEntityAccessFilter
            | Some parentRef -> getFilter parentRef

        let usedEntity = Map.findWithDefault entityRef emptyUsedEntity flatUsedEntity.Children
        let defaultParentAllowedEntity =
            if Option.isNone entity.Parent then
                emptyFlatAllowedDerivedEntity
            else
                inheritedFlatAllowedDerivedEntity
        let allowedEntity = Map.findWithDefault entityRef defaultParentAllowedEntity flatAllowedEntity.Children

        if usedEntity.Insert then
            if not allowedEntity.Insert then
                raisef PermissionsApplyException "Access denied to insert entity %O" entityRef

        for KeyValue(name, usedField) in usedEntity.Fields do
            if usedField.Insert then
                let ref = { Entity = entityRef; Name = name }
                match Map.tryFind ref flatAllowedEntity.Fields with
                | Some flatField when flatField.Insert -> ()
                | _ -> raisef PermissionsApplyException "Access denied to insert field %O" ref

        let isSelect = usedEntity.Select || usedEntity.Update || usedEntity.Delete
        let filterExpr =
            if isSelect then
                let selectFilter =
                    // Pull parent filters to the first entity (this) that actually needs them.
                    // For example, consider hierarchy A -> B -> C, where B needs SELECT, and C needs UPDATE.
                    // All three entities have their respective restrictions on both SELECT and UPDATE, so we need to combine them in B and C respectively.
                    // This does _not_ make the tree intergrated; instead, it pushes filters as far as possible into hierarchy.
                    // For example, when DELETE and UPDATE are used in different subnodes of the hierarchy, they won't be mistakenly all filtered for both.
                    if parentFilter.PropagatedSelect then
                        addSelectFilter usedEntity entityRef allowedEntity
                    else
                        getSelectAccessFilter entityRef
                match selectFilter with
                | OFEFalse -> raisef PermissionsApplyException "Access denied to select for entity %O" entityRef
                | selectFilter -> selectFilter
            else
                OFETrue
        let filterExpr =
            if usedEntity.Update then
                let updateFilter =
                    if parentFilter.PropagatedUpdate then
                        allowedEntity.Update
                    else
                        getUpdateAccessFilter entityRef
                match updateFilter with
                | OFEFalse -> raisef PermissionsApplyException "Access denied to update for entity %O" entityRef
                | updateFilter -> andFieldExpr filterExpr updateFilter
            else
                filterExpr
        let filterExpr =
            if usedEntity.Delete then
                let deleteFilter =
                    if parentFilter.PropagatedDelete then
                        allowedEntity.Delete
                    else
                        getDeleteAccessFilter entityRef
                match deleteFilter with
                | OFEFalse -> raisef PermissionsApplyException "Access denied to delete for entity %O" entityRef
                | deleteFilter -> andFieldExpr filterExpr deleteFilter
            else
                filterExpr

        let checkExpr =
            if usedEntity.Insert || usedEntity.Update then
                let checkExpr =
                    if parentFilter.PropagatedInsert || parentFilter.PropagatedUpdate then
                        addCheckFilter usedEntity entityRef allowedEntity
                    else
                        getCheckAccessFilter entityRef
                match checkExpr with
                | OFEFalse -> raisef PermissionsApplyException "Access denied for insert/update check for entity %O" entityRef
                | checkExpr -> checkExpr
            else
                OFETrue

        { Filter = filterExpr
          Check = checkExpr
          UsedEntity = usedEntity
          PropagatedSelect = parentFilter.PropagatedSelect || isSelect
          PropagatedInsert = parentFilter.PropagatedInsert || usedEntity.Insert
          PropagatedUpdate = parentFilter.PropagatedUpdate || usedEntity.Update
          PropagatedDelete = parentFilter.PropagatedDelete || usedEntity.Delete
        }

    and getFilter = memoizeN buildFilter

    member this.GetFilter entityRef = getFilter entityRef

type private EntityChecksCache (layout : Layout, rootRef : ResolvedEntityRef) =
    let subEntityFieldRef =
        lazy (
            let fieldRef = { Entity = Some <| relaxEntityRef rootRef; Name = funSubEntity } : FieldRef
            let meta =
                { simpleColumnMeta rootRef with
                    ForceSQLTable = Some restrictedTableRef
                }
            makeColumnReference layout meta fieldRef
        )

    let rec buildOfTypeCheck (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        if not (hasSubType entity) then
            OFETrue
        else if entity.IsAbstract then
            OFEFalse
        else
            let subEntityRef : SubEntityRef =
                { Ref = relaxEntityRef entityRef
                  Extra = ObjectMap.empty
                }
            optimizeFieldExpr <| FEOfType (subEntityFieldRef.Value, subEntityRef)
    and getOfTypeCheck = memoizeN buildOfTypeCheck

    let rec buildInheritedFromCheck (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        if not (hasSubType entity) then
            OFETrue
        else if Option.isNone entity.Parent then
            OFETrue
        else
            let subEntityRef : SubEntityRef =
                { Ref = relaxEntityRef entityRef
                  Extra = ObjectMap.empty
                }
            optimizeFieldExpr <| FEInheritedFrom (subEntityFieldRef.Value, subEntityRef)
    and getInheritedFromCheck = memoizeN buildInheritedFromCheck

    member this.GetOfTypeCheck entityRef = getOfTypeCheck entityRef
    member this.GetInheritedFromCheck entityRef = getInheritedFromCheck entityRef

type private SubEntityCheck =
    { AllowChildren : bool
      Check : ResolvedOptimizedFieldExpr
    }

let private addTypecheck (checksCache : EntityChecksCache) (entityRef : ResolvedEntityRef) (check : SubEntityCheck) =
    let typeCheckExpr =
        if check.AllowChildren then
            checksCache.GetInheritedFromCheck entityRef
        else
            checksCache.GetOfTypeCheck entityRef
    andFieldExpr typeCheckExpr check.Check

type private EntityFiltersCombiner (layout : Layout, checksCache : EntityChecksCache, getFilter : ResolvedEntityRef -> ResolvedOptimizedFieldExpr) =
    let rec buildParentFilter (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let parentCheck =
            match entity.Parent with
            | None -> OFETrue
            | Some parentRef -> getParentFilter parentRef
        let currentCheck = getFilter entityRef
        andFieldExpr parentCheck currentCheck
    and getParentFilter = memoizeN buildParentFilter

    let rec buildChildrenFilter (entityRef : ResolvedEntityRef) : SubEntityCheck =
        let entity = layout.FindEntity entityRef |> Option.get

        let getChild (childRef, child) =
            if not child.Direct then
                None
            else
                let filter = getFilter childRef
                let subchildrenCheck = getChildrenFilter childRef
                let childrenCheck =
                    { subchildrenCheck with Check = andFieldExpr filter subchildrenCheck.Check } : SubEntityCheck
                Some (childRef, childrenCheck)

        let children =
            entity.Children
            |> Map.toSeq
            |> Seq.mapMaybe getChild
            |> Seq.cache

        let singleExpr =
            match Seq.trySnoc children with
            | None -> Some OFETrue
            | Some ((firstChildRef, { AllowChildren = true; Check = firstExpr }), tailChildren)
              when Seq.forall (fun (childRef, check) -> check.AllowChildren) tailChildren
                   && Seq.forall (fun (childRef, check) -> check.Check = firstExpr) tailChildren ->
                Some firstExpr
            | _ -> None

        match singleExpr with
        | Some expr -> { Check = expr; AllowChildren = true }
        | None ->
            let childExpr = children |> Seq.map (uncurry (addTypecheck checksCache)) |> Seq.fold orFieldExpr OFEFalse
            { Check = childExpr; AllowChildren = false }

    and getChildrenFilter = memoizeN buildChildrenFilter

    let getFullEntityCheck (entityRef : ResolvedEntityRef) =
        let childrenCheck = getChildrenFilter entityRef
        andFieldExpr (getParentFilter entityRef) (addTypecheck checksCache entityRef childrenCheck)

    let getSelfEntityCheck (entityRef : ResolvedEntityRef) =
        getParentFilter entityRef

    member this.GetFilter entityRef = getFilter entityRef
    member this.ChecksCache = checksCache

    member this.GetFullEntityCheck entityRef = getFullEntityCheck entityRef
    member this.GetSelfEntityCheck entityRef = getSelfEntityCheck entityRef

let private renameAllFieldExprEntities (toEntityRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
    let mapReference : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
        | { Ref = { Ref = VRColumn { Entity = Some entityRef; Name = fieldName } } } as ref ->
            { ref with Ref = { ref.Ref with Ref = VRColumn { Entity = Some toEntityRef; Name = fieldName } } }
        | { Ref = { Ref = VRColumn ({ Entity = None; Name = name } as fieldRef) } } ->
            failwithf "Unexpected column ref during rename: %O" fieldRef
        | { Ref = { Ref = VRArgument _ } } as ref -> ref
    mapFieldExpr { idFieldExprMapper with FieldReference = mapReference }

let private buildFinalRestriction (name: string) (entityRef : ResolvedEntityRef) : ResolvedOptimizedFieldExpr -> EntityFilterExpr = function
    | OFEFalse -> raisef PermissionsApplyException "Access denied to %s for entity %O" name entityRef
    | OFETrue -> FUnfiltered
    | expr ->
        expr.ToFieldExpr() |> renameAllFieldExprEntities (relaxEntityRef entityRef) |> FFiltered

let private getAppliedEntity
        (treeBuilder : EntityAccessFilterBuilder)
        (filterBuilder : EntityFiltersCombiner)
        (checkBuilder : EntityFiltersCombiner)
        (entityRef : ResolvedEntityRef) : HalfAppliedAllowedEntity =
    let filter = treeBuilder.GetFilter entityRef

    let selectUpdateFilter =
        if not (filter.UsedEntity.Select || filter.UsedEntity.Update) then
            None
        else
            Some <| filterBuilder.GetFullEntityCheck entityRef
    let deleteFilter =
        if not filter.UsedEntity.Delete then
            None
        else
            let expr =
                andFieldExpr
                    (filterBuilder.GetSelfEntityCheck entityRef)
                    (filterBuilder.ChecksCache.GetOfTypeCheck entityRef)
            Some expr
    let checkExpr =
        if not (filter.UsedEntity.Insert || filter.UsedEntity.Update) then
            None
        else
            Some <| checkBuilder.GetFullEntityCheck entityRef

    { SelectUpdate = selectUpdateFilter
      Delete = deleteFilter
      Check = checkExpr
    }

let private buildFinalAllowedEntity (entityRef : ResolvedEntityRef) (allowedEntity : HalfAppliedAllowedEntity) : AppliedAllowedEntity =
    { SelectUpdate = Option.map (buildFinalRestriction "select/update" entityRef) allowedEntity.SelectUpdate
      Delete = Option.map (buildFinalRestriction "delete" entityRef) allowedEntity.Delete
      Check = Option.map (buildFinalRestriction "insert/update" entityRef) allowedEntity.Check
    }

let private applyPermissionsForEntity (layout : Layout) (checksCache : EntityChecksCache) (allowedEntity : FlatAllowedRoleEntity) (usedEntity : FlatUsedEntity) : HalfAppliedAllowedDatabase =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    let filterBuilder = EntityFiltersCombiner (layout, checksCache, fun ref -> treeBuilder.GetFilter(ref).Filter)
    let checkBuilder = EntityFiltersCombiner (layout, checksCache, fun ref -> treeBuilder.GetFilter(ref).Check)

    usedEntity.EntryPoints |> Seq.map (fun ref -> (ref, getAppliedEntity treeBuilder filterBuilder checkBuilder ref)) |> Map.ofSeq

let inline private throwRoleExceptions (rootRef : ResolvedRoleRef) (exceptions : (ResolvedRoleRef * PermissionsApplyException) list) =
    let postfix = exceptions |> Seq.map (fun (roleRef, e) -> sprintf "for role %O: %O" roleRef e) |> String.concat ", "
    let msg = sprintf "Access denied to the root entity %O: %s" rootRef postfix
    raise <| PermissionsApplyException(msg, List.map (fun (roleRef, e) -> upcast e) exceptions, true)

let applyPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : AppliedAllowedDatabase =
    let applyToOne (rootRef, usedEntity) =
        // FIXME: better implement it in a different way, exceptions are slow!
        let mutable exceptions = []
        let checksCache = EntityChecksCache (layout, rootRef)
        let tryRole maybeAppliedDb (roleRef, allowedRoleEntity) =
            try
                let newAppliedDb = applyPermissionsForEntity layout checksCache allowedRoleEntity usedEntity
                match maybeAppliedDb with
                | None -> Some newAppliedDb
                | Some oldAppliedDb -> Some <| Map.unionWith unionHalfAppliedAllowedEntities oldAppliedDb newAppliedDb
            with
            | :? PermissionsApplyException as e ->
                exceptions <- (roleRef, e) :: exceptions
                maybeAppliedDb

        let allowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to the root entity %O" rootRef
            | Some allowed -> allowed
        match allowedEntity.Roles |> Map.toSeq |> Seq.fold tryRole None with
        | None -> throwRoleExceptions rootRef exceptions
        | Some appliedDb -> Map.map buildFinalAllowedEntity appliedDb

    usedDatabase |> Map.toSeq |> Seq.map applyToOne |> Seq.fold Map.unionUnique Map.empty

let private checkPermissionsForEntity (layout : Layout) (allowedEntity : FlatAllowedRoleEntity) (usedEntity : FlatUsedEntity) : unit =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    for entryPoint in usedEntity.EntryPoints do
        ignore <| treeBuilder.GetFilter entryPoint

let checkPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : unit =
    for KeyValue(rootRef, usedEntity) in usedDatabase do
        let mutable exceptions = []
        let tryRole _ (roleRef, allowedRoleEntity) =
            try
                checkPermissionsForEntity layout allowedRoleEntity usedEntity
                None
            with
            | :? PermissionsApplyException as e ->
                exceptions <- (roleRef, e) :: exceptions
                Some ()

        let allowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to the root entity %O" rootRef
            | Some allowed -> allowed
        // Counter-intuitively we use `Some ()` here to signal failed check, and `None` to early drop when found at least one matching role.
        match allowedEntity.Roles |> Map.toSeq |> Seq.foldOption tryRole () with
        | None -> ()
        | Some () -> throwRoleExceptions rootRef exceptions

let rec private getParentCheckExpression (layout : Layout) (allowedRoleEntity : FlatAllowedRoleEntity) =
    getAccessFilter
        (getParentCheckExpression layout allowedRoleEntity)
        (fun entityRef allowedEntity -> allowedEntity.Check)
        layout
        allowedRoleEntity

let getSingleCheckExpression (layout : Layout) (role : ResolvedRole) (entityRef : ResolvedEntityRef) (fields : FieldName seq) : EntityFilterExpr =
    let entity = layout.FindEntity entityRef |> Option.get

    let checksCache = EntityChecksCache (layout, entity.Root)

    let getRoleCheck (allowedRoleEntity : FlatAllowedRoleEntity) =
        let getEntityCheck currRef =
            match Map.tryFind currRef allowedRoleEntity.Children with
            | None ->
                let currEntity = layout.FindEntity currRef |> Option.get
                if Option.isNone currEntity.Parent then
                    OFEFalse
                else
                    OFETrue
            | Some allowedEntity -> allowedEntity.Check

        let filterBuilder = EntityFiltersCombiner (layout, checksCache, getEntityCheck)
        let check = filterBuilder.GetSelfEntityCheck entityRef

        let getFieldCheck name =
            let fieldInfo = entity.ColumnFields.[name]
            let fieldEntityRef = Option.defaultValue entityRef fieldInfo.InheritedFrom
            let fieldRef = { Entity = fieldEntityRef; Name = name }
            match Map.tryFind fieldRef allowedRoleEntity.Fields with
            | None -> raisef PermissionsApplyException "Access denied to field %O" fieldRef
            | Some allowedField -> allowedField.Check
        fields |> Seq.map getFieldCheck |> Seq.fold andFieldExpr check

    let tryGetRoleCheck (roleRef, allowedRoleEntity : FlatAllowedRoleEntity) =
        try
            getRoleCheck allowedRoleEntity
        with
        | :? PermissionsApplyException ->
            OFEFalse

    let allowedEntity =
        match Map.tryFind entity.Root role.Flattened.Entities with
        | None -> raisef PermissionsApplyException "Access denied to the root entity %O for entity %O" entity.Root entityRef
        | Some allowed -> allowed
    allowedEntity.Roles
        |> Map.toSeq
        |> Seq.map tryGetRoleCheck
        |> Seq.fold orFieldExpr OFEFalse
        |> buildFinalRestriction "check" entityRef
