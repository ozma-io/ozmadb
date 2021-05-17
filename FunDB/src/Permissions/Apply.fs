module FunWithFlags.FunDB.Permissions.Apply

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

// Rename top-level entities in a restriction expression
let private renameRestriction (entityRef : EntityRef) (restr : ResolvedOptimizedFieldExpr) : ResolvedOptimizedFieldExpr =
    mapOptimizedFieldExpr (replaceEntityRefInExpr (Some entityRef)) restr

// Filter entities, allowing only those that satisfy access conditions, taking into account parent and children permissions.
let applyRestrictionExpression (accessor : FlatAllowedDerivedEntity -> ResolvedOptimizedFieldExpr) (layout : Layout) (allowedEntity : FlatAllowedEntity) (entityRef : ResolvedEntityRef) : ResolvedOptimizedFieldExpr =
    let relaxedRef = relaxEntityRef entityRef
    // We add restrictions from all parents, including the entity itself.
    let rec buildParentRestrictions (oldRestrs : ResolvedOptimizedFieldExpr) (currRef : ResolvedEntityRef) =
        let newRestrs =
            match Map.tryFind currRef allowedEntity.Children with
            | None -> oldRestrs
            | Some child ->
                let currRestr = accessor child
                let currExpr = renameRestriction relaxedRef currRestr
                orFieldExpr oldRestrs currExpr
        let entity = layout.FindEntity currRef |> Option.get
        match entity.Parent with
        | None -> newRestrs
        | Some parent -> buildParentRestrictions newRestrs parent

    // We allow any child too.
    let buildChildRestrictions (currRestrs : ResolvedOptimizedFieldExpr) (currRef : ResolvedEntityRef) =
        let entity = layout.FindEntity currRef |> Option.get
        match Map.tryFind currRef allowedEntity.Children with
        | None -> currRestrs
        | Some child ->
            let restrs = accessor child
            if optimizedIsFalse restrs then
                currRestrs
            else
                let expr = renameRestriction relaxedRef restrs
                let fieldRef = { Entity = entityRef; Name = funSubEntity }
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
                let boundFieldRef = { Ref = { Ref = VRColumn { Entity = Some relaxedRef; Name = funSubEntity }; Path = [||] }; Extra = ObjectMap.singleton fieldInfo } : LinkedBoundFieldRef
                let subEntityRef =
                    { Ref = relaxEntityRef currRef
                      Extra = ObjectMap.empty
                    } : SubEntityRef

                let typeCheck = FEInheritedFrom (boundFieldRef, subEntityRef) |> optimizeFieldExpr
                let currCheck = andFieldExpr typeCheck expr
                orFieldExpr currRestrs currCheck

    let parentRestrs = buildParentRestrictions OFEFalse entityRef
    let selfEntity = layout.FindEntity entityRef |> Option.get
    selfEntity.Children |> Map.keys |> Seq.fold buildChildRestrictions parentRestrs