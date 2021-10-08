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

// Out aim is to build a database view that is:
// * Restricted (according to current user role);
// * Consistent; that is, it it impossible to witness different views of the database from different sub-expressions,
//   even if this would expose more allowed entries to the client;
// * Full -- as small amount of restrictions should be applied as possible, given requirements above aren't violated.
type AppliedAllowedEntity =
    // FIXME: don't be lazy, make a custom type.
    // External `option` = "did we compile an expression for this access type".
    // Internal `option` = "is there a need to restrict".
    { SelectUpdate : (ResolvedFieldExpr option) option // Filters out this entity and all its children.
      Delete : (ResolvedFieldExpr option) option // Filters out only this entity, not propagated to children.
    }

type AppliedAllowedDatabase = Map<ResolvedEntityRef, AppliedAllowedEntity>

type private HalfAppliedAllowedEntity =
    { SelectUpdate : ResolvedOptimizedFieldExpr option
      Delete : ResolvedOptimizedFieldExpr option
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
    }

type private EntityAccessFilter =
    { Filter : ResolvedOptimizedFieldExpr
      UsedEntity : UsedEntity
      PropagatedSelect : bool
      PropagatedUpdate : bool
      PropagatedDelete : bool
    }

let private emptyEntityAccessFilter : EntityAccessFilter =
    { Filter = OFETrue
      UsedEntity = emptyUsedEntity
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
    let defaultParentAllowedEntity =
        if Option.isNone entity.Parent then
            emptyFlatAllowedDerivedEntity
        else
            inheritedFlatAllowedDerivedEntity
    let allowedEntity = Map.findWithDefault entityRef defaultParentAllowedEntity flatAllowedEntity.Children
    match accessor entityRef allowedEntity with
    | OFEFalse -> raisef PermissionsApplyException "Access denied to entity %O" entityRef
    | thisFilter -> andFieldExpr parentFilter thisFilter

type private EntityAccessFilterBuilder (layout : Layout, flatAllowedEntity : FlatAllowedRoleEntity, flatUsedEntity : FlatUsedEntity) =
    let addSelectFilter (usedEntity : UsedEntity) (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let addFieldRestriction (name : FieldName, usedField : UsedField) : ResolvedOptimizedFieldExpr =
            let ref = { Entity = entityRef; Name = name }
            match Map.tryFind ref flatAllowedEntity.Fields with
            | Some flatField ->
                if usedField.Select then
                    match flatField.Select with
                    | OFEFalse -> raisef PermissionsApplyException "Access denied to select field %O" ref
                    | newSelectFilter -> newSelectFilter
                else
                    OFETrue
            | None -> raisef PermissionsApplyException "Access denied to field %O" ref

        usedEntity.Fields |> Map.toSeq |> Seq.map addFieldRestriction |> Seq.fold andFieldExpr allowedEntity.Select

    let addStandaloneSelectFilter (entityRef : ResolvedEntityRef) (allowedEntity : FlatAllowedDerivedEntity) =
        let usedEntity = Map.findWithDefault entityRef emptyUsedEntity flatUsedEntity.Children
        addSelectFilter usedEntity entityRef allowedEntity

    // Build combined parents filter for a given access type.
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getSelectAccessFilter = memoizeN (getAccessFilter (fun ent -> getSelectAccessFilter ent) addStandaloneSelectFilter layout flatAllowedEntity)
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getUpdateAccessFilter = memoizeN (getAccessFilter (fun ent -> getUpdateAccessFilter ent) (fun entityRef allowedEntity -> allowedEntity.Update) layout flatAllowedEntity)
    // fsharplint:disable-next-line ReimplementsFunction
    let rec getDeleteAccessFilter = memoizeN (getAccessFilter (fun ent -> getDeleteAccessFilter ent) (fun entityRef allowedEntity -> allowedEntity.Delete) layout flatAllowedEntity)

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
            if usedField.Insert || usedField.Update then
                let ref = { Entity = entityRef; Name = name }
                match Map.tryFind ref flatAllowedEntity.Fields with
                | Some flatField when flatField.Change -> ()
                | _ -> raisef PermissionsApplyException "Access denied to change field %O" ref

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
                | OFEFalse -> raisef PermissionsApplyException "Access denied to select entity %O" entityRef
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
                | OFEFalse -> raisef PermissionsApplyException "Access denied to update entity %O" entityRef
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
                | OFEFalse -> raisef PermissionsApplyException "Access denied to delete entity %O" entityRef
                | deleteFilter -> andFieldExpr filterExpr deleteFilter
            else
                filterExpr
        
        { Filter = filterExpr
          UsedEntity = usedEntity
          PropagatedSelect = parentFilter.PropagatedSelect || isSelect
          PropagatedUpdate = parentFilter.PropagatedUpdate || usedEntity.Update
          PropagatedDelete = parentFilter.PropagatedDelete || usedEntity.Delete
        }

    and getFilter = memoizeN buildFilter

    member this.GetFilter entityRef = getFilter entityRef

type private ChildrenCheck =
    { IncludingChildren : bool
      Check : ResolvedOptimizedFieldExpr
    }

type private EntityFiltersCombiner (layout : Layout, rootRef : ResolvedEntityRef, getFilter : ResolvedEntityRef -> EntityAccessFilter) =
    let boundFieldRef : LinkedBoundFieldRef =
        let fieldRef = { Entity = rootRef; Name = funSubEntity }
        let boundInfo =
            { Ref = fieldRef
              Immediate = true
              Path = [||]
            } : BoundFieldMeta
        let fieldInfo =
            { Bound = Some boundInfo
              FromEntityId = localExprFromEntityId
              ForceSQLName = None
            } : FieldMeta
        let linkedFieldRef =
            { Ref = VRColumn { Entity = Some <| relaxEntityRef rootRef; Name = funSubEntity }
              Path = [||]
              AsRoot = false
            } : LinkedFieldRef
        { Ref = linkedFieldRef
          Extra = ObjectMap.singleton fieldInfo
        }
            
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
            optimizeFieldExpr <| FEOfType (boundFieldRef, subEntityRef)
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
            optimizeFieldExpr <| FEInheritedFrom (boundFieldRef, subEntityRef)
    and getInheritedFromCheck = memoizeN buildInheritedFromCheck

    let rec buildParentCheck (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let parentCheck =
            match entity.Parent with
            | None -> OFETrue
            | Some parentRef -> getParentCheck parentRef
        let currentCheck = getFilter entityRef
        andFieldExpr parentCheck currentCheck.Filter
    and getParentCheck = memoizeN buildParentCheck

    let addTypecheck (entityRef : ResolvedEntityRef) (check : ChildrenCheck) =
        let typeCheckExpr =
            if check.IncludingChildren then
                getInheritedFromCheck entityRef
            else
                getOfTypeCheck entityRef
        andFieldExpr typeCheckExpr check.Check

    let rec buildChildrenCheck (entityRef : ResolvedEntityRef) : ChildrenCheck =
        let entity = layout.FindEntity entityRef |> Option.get
        
        let getChild (childRef, child) =
            if not child.Direct then
                None
            else
                let filter = getFilter childRef
                let subchildrenCheck = getChildrenCheck childRef
                let childrenCheck =
                    { subchildrenCheck with Check = andFieldExpr filter.Filter subchildrenCheck.Check } : ChildrenCheck
                Some (childRef, childrenCheck)

        let children =
            entity.Children
            |> Map.toSeq
            |> Seq.mapMaybe getChild
            |> Seq.cache
        
        let singleExpr =
            match Seq.trySnoc children with
            | None -> Some OFETrue
            | Some ((firstChildRef, { IncludingChildren = true; Check = firstExpr }), tailChildren)
              when Seq.forall (fun (childRef, check) -> check.IncludingChildren) tailChildren
                   && Seq.forall (fun (childRef, check) -> check.Check = firstExpr) tailChildren ->
                Some firstExpr
            | _ -> None
    
        match singleExpr with
        | Some expr -> { Check = expr; IncludingChildren = true }
        | None ->
            let childExpr = children |> Seq.map (uncurry addTypecheck) |> Seq.fold orFieldExpr OFEFalse
            { Check = childExpr; IncludingChildren = false }

    and getChildrenCheck = memoizeN buildChildrenCheck

    and getAppliedEntity (entityRef : ResolvedEntityRef) : HalfAppliedAllowedEntity =
        let filter = getFilter entityRef

        let selectUpdateCheck =
            if not (filter.UsedEntity.Select || filter.UsedEntity.Update) then
                None
            else
                let childrenCheck = getChildrenCheck entityRef
                match andFieldExpr (getParentCheck entityRef) (addTypecheck entityRef childrenCheck) with
                | OFEFalse -> raisef PermissionsApplyException "Access denied to entity %O" entityRef
                | entityCheck -> Some entityCheck
        let deleteCheck =
            if not filter.UsedEntity.Delete then
                None
            else
                match andFieldExpr (getParentCheck entityRef) (getOfTypeCheck entityRef) with
                | OFEFalse -> raisef PermissionsApplyException "Access denied to entity %O" entityRef
                | entityCheck -> Some entityCheck
        
        { SelectUpdate = selectUpdateCheck
          Delete = deleteCheck
        }
    
    member this.GetAppliedEntity entityRef = getAppliedEntity entityRef

let private renameAllFieldExprEntities (toEntityRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
    let mapReference : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
        | { Ref = { Ref = VRColumn { Entity = Some entityRef; Name = fieldName } } } as ref ->
            { ref with Ref = { ref.Ref with Ref = VRColumn { Entity = Some toEntityRef; Name = fieldName } } }
        | { Ref = { Ref = VRColumn ({ Entity = None; Name = name } as fieldRef) } } ->
            failwithf "Unexpected column ref during rename: %O" fieldRef
        | { Ref = { Ref = VRPlaceholder _ } } as ref -> ref
    mapFieldExpr { idFieldExprMapper with FieldReference = mapReference }

let private buildFinalRestriction (entityRef : ResolvedEntityRef) : ResolvedOptimizedFieldExpr -> ResolvedFieldExpr option = function
    | OFETrue -> None
    | expr ->
        expr.ToFieldExpr() |> renameAllFieldExprEntities (relaxEntityRef entityRef) |> Some

let private buildFinalAllowedEntity (entityRef : ResolvedEntityRef) (allowedEntity : HalfAppliedAllowedEntity) : AppliedAllowedEntity =
    { SelectUpdate = Option.map (buildFinalRestriction entityRef) allowedEntity.SelectUpdate
      Delete = Option.map (buildFinalRestriction entityRef) allowedEntity.Delete
    }

let private applyPermissionsForEntity (layout : Layout) (rootRef : ResolvedEntityRef) (allowedEntity : FlatAllowedRoleEntity) (usedEntity : FlatUsedEntity) : HalfAppliedAllowedDatabase =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    let filterBuilder = EntityFiltersCombiner (layout, rootRef, treeBuilder.GetFilter)

    usedEntity.EntryPoints |> Seq.map (fun ref -> (ref, filterBuilder.GetAppliedEntity ref)) |> Map.ofSeq

let inline private throwRoleExceptions (rootRef : ResolvedRoleRef) (exceptions : (ResolvedRoleRef * PermissionsApplyException) list) =
    let postfix = exceptions |> Seq.map (fun (roleRef, e) -> sprintf "for role %O: %O" roleRef e) |> String.concat ", "
    let msg = sprintf "Access denied to root entity %O: %s" rootRef postfix
    raise <| PermissionsApplyException(msg, List.map (fun (roleRef, e) -> upcast e) exceptions, true)

let applyPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : AppliedAllowedDatabase =
    let applyToOne (rootRef, usedEntity) =
        let rolesAllowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to entity %O" rootRef
            | Some allowed -> allowed

        // FIXME: better implement it in a different way, exceptions are slow!
        let mutable exceptions = []
        let tryRole maybeAppliedDb (roleRef, allowedEntity) =
            try
                let newAppliedDb = applyPermissionsForEntity layout rootRef allowedEntity usedEntity
                match maybeAppliedDb with
                | None -> Some newAppliedDb
                | Some oldAppliedDb -> Some <| Map.unionWith (fun name -> unionHalfAppliedAllowedEntities) oldAppliedDb newAppliedDb
            with
            | :? PermissionsApplyException as e ->
                exceptions <- (roleRef, e) :: exceptions
                maybeAppliedDb
        
        match rolesAllowedEntity.Roles |> Map.toSeq |> Seq.fold tryRole None with
        | None -> throwRoleExceptions rootRef exceptions
        | Some appliedDb -> Map.map buildFinalAllowedEntity appliedDb

    usedDatabase |> Map.toSeq |> Seq.map applyToOne |> Seq.fold Map.unionUnique Map.empty

let private checkPermissionsForEntity (layout : Layout) (allowedEntity : FlatAllowedRoleEntity) (usedEntity : FlatUsedEntity) : unit =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    for entryPoint in usedEntity.EntryPoints do
        ignore <| treeBuilder.GetFilter entryPoint

let checkPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : unit =
    for KeyValue(rootRef, usedEntity) in usedDatabase do
        let rolesAllowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to entity %O" rootRef
            | Some allowed -> allowed

        let mutable exceptions = []
        let tryRole _ (roleRef, allowedEntity) =
            try
                checkPermissionsForEntity layout allowedEntity usedEntity
                None
            with
            | :? PermissionsApplyException as e -> 
                exceptions <- (roleRef, e) :: exceptions
                Some ()
        
        // Counter-intuitively we use `Some ()` here to signal failed check, and `None` to early drop when found at least one matching role.
        match rolesAllowedEntity.Roles |> Map.toSeq |> Seq.foldOption tryRole () with
        | None -> ()
        | Some () -> throwRoleExceptions rootRef exceptions