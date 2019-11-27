module FunWithFlags.FunDB.Permissions.Apply

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

// Rename top-level entities in a restriction expression
let private renameRestriction (entityRef : ResolvedEntityRef) (restr : ResolvedOptimizedFieldExpr) : ResolvedOptimizedFieldExpr =
    let renameBound (bound : BoundField) : BoundField =
        { bound with ref = { entity = entityRef; name = bound.ref.name } }
    let resetReference (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
        let link =
            match ref.ref with
            | VRColumn c -> VRColumn { bound = Option.map renameBound c.bound; ref = ({ entity = Some (relaxEntityRef entityRef); name = c.ref.name } : FieldRef) }
            | VRPlaceholder p -> VRPlaceholder p
        { ref = link; path = ref.path }
    let mapper = idFieldExprMapper resetReference id
    mapOptimizedFieldExpr mapper restr

let applyRestrictionExpression (accessor : FlatAllowedDerivedEntity -> Restriction) (layout : Layout) (allowedEntity : FlatAllowedEntity) (entityRef : ResolvedEntityRef) : Restriction =
    // We add restrictions from all parents, including the entity itself.
    let rec buildParentRestrictions (oldRestrs : Restriction) (currRef : ResolvedEntityRef) =
        let newRestrs =
            match Map.tryFind currRef allowedEntity.children with
            | None -> oldRestrs
            | Some child ->
                let currRestr = accessor child
                let currExpr = renameRestriction entityRef currRestr.expression
                orRestriction oldRestrs { currRestr with expression = currExpr }
        let entity = layout.FindEntity currRef |> Option.get
        match entity.inheritance with
        | None -> newRestrs
        | Some inheritance -> buildParentRestrictions newRestrs inheritance.parent

    // We allow any child too.
    let buildChildRestrictions (currRestrs : Restriction) (currRef : ResolvedEntityRef) =
        let entity = layout.FindEntity currRef |> Option.get
        match Map.tryFind currRef allowedEntity.children with
        | None -> currRestrs
        | Some child ->
            let restrs = accessor child
            if restrs.expression = OFEFalse then
                currRestrs
            else
                let expr = renameRestriction currRef restrs.expression
                let fieldRef = { entity = entityRef; name = funSubEntity }
                let bound = { ref = fieldRef; immediate = true }
                let boundFieldRef = { ref = VRColumn { ref = relaxFieldRef fieldRef; bound = Some bound }; path = [||] }
                let subEntityRef =
                    { ref = relaxEntityRef currRef
                      extra =
                        { alwaysTrue = false
                        }
                    }

                let typeCheck = FEInheritedFrom (boundFieldRef, subEntityRef) |> optimizeFieldExpr
                let currCheck = andFieldExpr typeCheck expr
                orRestriction currRestrs { restrs with expression = currCheck }

    let parentRestrs = buildParentRestrictions emptyRestriction entityRef
    let selfEntity = layout.FindEntity entityRef |> Option.get
    selfEntity.children |> Map.keys |> Seq.fold buildChildRestrictions parentRestrs