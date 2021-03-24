module FunWithFlags.FunDB.Permissions.Apply

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Resolve
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

// Rename top-level entities in a restriction expression
let private renameRestriction (boundRef : ResolvedEntityRef) (entityRef : ResolvedEntityRef) (restr : ResolvedOptimizedFieldExpr) : ResolvedOptimizedFieldExpr =
    let renameBound (bound : BoundField) : BoundField =
        { bound with Ref = { entity = boundRef; name = bound.Ref.name } }
    let resetReference (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
        let link =
            match ref.Ref with
            | VRColumn c -> VRColumn { Bound = Option.map renameBound c.Bound; Ref = ({ entity = Some { schema = None; name = entityRef.name }; name = c.Ref.name } : FieldRef) }
            | VRPlaceholder p -> VRPlaceholder p
        { Ref = link; Path = ref.Path }
    let mapper = idFieldExprMapper resetReference id
    mapOptimizedFieldExpr mapper restr

let applyRestrictionExpression (accessor : FlatAllowedDerivedEntity -> Restriction) (layout : Layout) (allowedEntity : FlatAllowedEntity) (entityRef : ResolvedEntityRef) : Restriction =
    // We add restrictions from all parents, including the entity itself.
    let rec buildParentRestrictions (oldRestrs : Restriction) (currRef : ResolvedEntityRef) =
        let newRestrs =
            match Map.tryFind currRef allowedEntity.Children with
            | None -> oldRestrs
            | Some child ->
                let currRestr = accessor child
                let currExpr = renameRestriction entityRef entityRef currRestr.Expression
                orRestriction oldRestrs { currRestr with Expression = currExpr }
        let entity = layout.FindEntity currRef |> Option.get
        match entity.Inheritance with
        | None -> newRestrs
        | Some inheritance -> buildParentRestrictions newRestrs inheritance.Parent

    // We allow any child too.
    let buildChildRestrictions (currRestrs : Restriction) (currRef : ResolvedEntityRef) =
        let entity = layout.FindEntity currRef |> Option.get
        match Map.tryFind currRef allowedEntity.Children with
        | None -> currRestrs
        | Some child ->
            let restrs = accessor child
            if optimizedIsFalse restrs.Expression then
                currRestrs
            else
                let expr = renameRestriction currRef entityRef restrs.Expression
                let fieldRef = { entity = entityRef; name = funSubEntity }
                let bound = { Ref = fieldRef; Immediate = true }
                let boundFieldRef = { Ref = VRColumn { Ref = relaxFieldRef fieldRef; Bound = Some bound }; Path = [||] }
                let subEntityRef =
                    { Ref = relaxEntityRef currRef
                      Extra =
                        { AlwaysTrue = false
                        }
                    }

                let typeCheck = FEInheritedFrom (boundFieldRef, subEntityRef) |> optimizeFieldExpr
                let currCheck = andFieldExpr typeCheck expr
                orRestriction currRestrs { restrs with Expression = currCheck }

    let parentRestrs = buildParentRestrictions emptyRestriction entityRef
    let selfEntity = layout.FindEntity entityRef |> Option.get
    selfEntity.Children |> Map.keys |> Seq.fold buildChildRestrictions parentRestrs