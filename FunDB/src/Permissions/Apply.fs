module FunWithFlags.FunDB.Permissions.Apply

// Allow delayed 
#nowarn "40"

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

type PermissionsApplyException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        PermissionsApplyException (message, innerException, isUserException innerException)

    new (message : string) = PermissionsApplyException (message, null, true)

// Out aim is to build a database view that is:
// * Restricted (according to current user role);
// * Consistent; that is, it it impossible to witness different views of the database from different sub-expressions,
//   even if this would expose more allowed entries to the client;
// * Full -- as small amount of restrictions should be applied as possible, given requirements above aren't violated.
type AppliedAllowedEntity =
    { SelectUpdate : ResolvedOptimizedFieldExpr option // Filters out this entity and all its children.
      Delete : ResolvedOptimizedFieldExpr option // Filters out only this entity, not propagated to children.
    }

type AppliedAllowedDatabase = Map<ResolvedEntityRef, AppliedAllowedEntity>

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

let inline getAccessFilter
        (getParent : ResolvedEntityRef -> ResolvedOptimizedFieldExpr)
        (accessor : ResolvedEntityRef -> FlatAllowedDerivedEntity -> ResolvedOptimizedFieldExpr)
        (layout : Layout) (flatAllowedEntity : FlatAllowedEntity)
        (entityRef : ResolvedEntityRef)
        : ResolvedOptimizedFieldExpr =
    let entity = layout.FindEntity entityRef |> Option.get
    let parentFilter =
        match entity.Parent with
        | None -> OFETrue
        | Some parentRef -> getParent parentRef
    let allowedEntity = Map.findWithDefault entityRef emptyFlatAllowedDerivedEntity flatAllowedEntity.Children
    match accessor entityRef allowedEntity with
    | OFEFalse -> raisef PermissionsApplyException "Access denied to entity %O" entityRef
    | thisFilter -> andFieldExpr parentFilter thisFilter

type private EntityAccessFilterBuilder (layout : Layout, flatAllowedEntity : FlatAllowedEntity, flatUsedEntity : FlatUsedEntity) =
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
        let allowedEntity = Map.findWithDefault entityRef emptyFlatAllowedDerivedEntity flatAllowedEntity.Children

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

    and getAppliedEntity (entityRef : ResolvedEntityRef) : AppliedAllowedEntity =
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

let private applyPermissionsForEntity (layout : Layout) (rootRef : ResolvedEntityRef) (allowedEntity : FlatAllowedEntity) (usedEntity : FlatUsedEntity) : AppliedAllowedDatabase =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    let filterBuilder = EntityFiltersCombiner (layout, rootRef, treeBuilder.GetFilter)
    usedEntity.EntryPoints |> Seq.map (fun ref -> (ref, filterBuilder.GetAppliedEntity ref)) |> Map.ofSeq

let applyPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : AppliedAllowedDatabase =
    let applyToOne (rootRef, usedEntity) =
        let allowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to entity %O" rootRef
            | Some allowed -> allowed
        applyPermissionsForEntity layout rootRef allowedEntity usedEntity
    usedDatabase |> Map.toSeq |> Seq.map applyToOne |> Seq.fold Map.unionUnique Map.empty

let private checkPermissionsForEntity (layout : Layout) (allowedEntity : FlatAllowedEntity) (usedEntity : FlatUsedEntity) : unit =
    let treeBuilder = EntityAccessFilterBuilder (layout, allowedEntity, usedEntity)
    for entryPoint in usedEntity.EntryPoints do
        ignore <| treeBuilder.GetFilter entryPoint

let checkPermissions (layout : Layout) (role : ResolvedRole) (usedDatabase : FlatUsedDatabase) : unit =
    for KeyValue(rootRef, usedEntity) in usedDatabase do
        let allowedEntity =
            match Map.tryFind rootRef role.Flattened.Entities with
            | None -> raisef PermissionsApplyException "Access denied to entity %O" rootRef
            | Some allowed -> allowed
        checkPermissionsForEntity layout allowedEntity usedEntity
