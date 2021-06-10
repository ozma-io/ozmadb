module FunWithFlags.FunDB.Permissions.Apply

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Optimize
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type PermissionsApplyException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = PermissionsApplyException (message, null)

type FilteredAllowedEntity = Map<ResolvedEntityRef, ResolvedOptimizedFieldExpr>

type FilteredAllowedDatabase = Map<ResolvedEntityRef, FilteredAllowedEntity>

let unionFilteredAllowedDatabase = Map.unionWith (fun name -> Map.unionUnique)

type private FieldsAccessAggregator (accessor : AllowedField -> ResolvedOptimizedFieldExpr, layout : Layout, role : ResolvedRole) =
    let filterDerivedEntity (ref : ResolvedEntityRef) (entity : ResolvedEntity) (usedFields : UsedFields) : ResolvedOptimizedFieldExpr =
        let flattened =
            match Map.tryFind entity.Root role.Flattened with
            | Some f -> f
            | None -> raisef PermissionsApplyException "Access denied to entity %O" ref
        let addRestriction restriction name =
            if name = funId || name = funSubEntity then
                restriction
            else
                let field = Map.find name entity.ColumnFields
                let parentEntity = Option.defaultValue ref field.InheritedFrom
                match Map.tryFind ({ Entity = parentEntity; Name = name } : ResolvedFieldRef) flattened.Fields with
                | Some r -> andFieldExpr restriction (accessor r)
                | _ -> raisef PermissionsApplyException "Access denied to select field %O" name
        let fieldsRestriction = usedFields |> Set.toSeq |> Seq.fold addRestriction OFETrue

        match fieldsRestriction with
        | OFEFalse -> raisef PermissionsApplyException "Access denied to select"
        | _ -> fieldsRestriction

    let filterUsedEntities (schemaName : SchemaName) (schema : ResolvedSchema) (usedEntities : UsedEntities) =
        let mapEntity (name : EntityName, usedFields : UsedFields) : FilteredAllowedDatabase =
            let entity = Map.find name schema.Entities
            let ref = { Schema = schemaName; Name = name } : ResolvedEntityRef
            
            let child =
                try
                    filterDerivedEntity ref entity usedFields
                with
                | :? PermissionsApplyException as e -> raisefWithInner PermissionsApplyException e "Access denied for entity %O" name

            Map.singleton entity.Root (Map.singleton ref child)

        usedEntities |> Map.toSeq |> Seq.map mapEntity |> Seq.fold unionFilteredAllowedDatabase Map.empty

    let filterUsedSchemas (usedSchemas : UsedSchemas) : FilteredAllowedDatabase =
        let mapSchema (name : SchemaName, usedEntities : UsedEntities) =
            let schema = Map.find name layout.Schemas
            try
                filterUsedEntities name schema usedEntities
            with
            | :? PermissionsApplyException as e -> raisefWithInner PermissionsApplyException e "Access denied for schema %O" name

        usedSchemas |> Map.toSeq |> Seq.map mapSchema |> Seq.fold unionFilteredAllowedDatabase Map.empty
    
    member this.FilterUsedSchemas usedSchemas = filterUsedSchemas usedSchemas

let filterAccessForUsedSchemas (accessor : AllowedField -> ResolvedOptimizedFieldExpr) (layout : Layout) (role : ResolvedRole) (usedSchemas : UsedSchemas) : FilteredAllowedDatabase =
    let aggregator = FieldsAccessAggregator (accessor, layout, role)
    aggregator.FilterUsedSchemas usedSchemas

// Rename top-level entities in a restriction expression
let private renameRestriction (entityRef : EntityRef) (restr : ResolvedOptimizedFieldExpr) : ResolvedOptimizedFieldExpr =
    mapOptimizedFieldExpr (replaceEntityRefInExpr (Some entityRef)) restr

type private TypeCheckedExprs = Map<string, ResolvedOptimizedFieldExpr * ResolvedOptimizedFieldExpr>

let private optimizeTypeCheckedExprs merge vals =
    vals
    |> Map.values
    |> Seq.map (fun (check, expr) -> (string check, (check, expr)))
    |> Map.ofSeqWith (fun name (check1, expr1) (check2, expr2) -> (check1, merge expr1 expr2))
    |> Map.values
    |> Seq.map (fun (check, expr) -> (string expr, (check, expr)))
    |> Map.ofSeq

let private unionTypeCheckedOrExprs = Map.unionWith (fun name (check1, expr1) (check2, expr2) -> (orFieldExpr check1 check2, expr2))

let private buildTypeCheckedOrExprs = Map.values >> Seq.map (fun (check, expr) -> andFieldExpr check expr) >> Seq.fold orFieldExpr OFEFalse

let private optimizeTypeCheckedOrExprs vals = optimizeTypeCheckedExprs orFieldExpr vals

let private unionTypeCheckedAndExprs = Map.unionWith (fun name (check1, expr1) (check2, expr2) -> (andFieldExpr check1 check2, expr2))

let private buildTypeCheckedAndExprs = Map.values >> Seq.map (fun (check, expr) -> orFieldExpr check expr) >> Seq.fold andFieldExpr OFETrue

let private optimizeTypeCheckedAndExprs vals = optimizeTypeCheckedExprs andFieldExpr vals

// Filter entities, allowing only those that satisfy access conditions, taking into account parent and children permissions.
let applyRestrictionExpression (accessor : FlatAllowedDerivedEntity -> ResolvedOptimizedFieldExpr) (layout : Layout) (allowedEntity : FlatAllowedEntity) (allowedFields : FilteredAllowedEntity) (entityRef : ResolvedEntityRef) : ResolvedOptimizedFieldExpr =
    let relaxedRef = relaxEntityRef entityRef
    let selfEntity = layout.FindEntity entityRef |> Option.get

    let checkEntityForExpr (currRef : ResolvedEntityRef) =
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

        FEInheritedFrom (boundFieldRef, subEntityRef)

    let parentCheck =
        match selfEntity.Parent with
        | None -> OFETrue
        | Some parent -> optimizeFieldExpr (checkEntityForExpr entityRef)

    // We add restrictions from all parents, including the entity itself.
    let rec buildParentOrRestrictions (oldRestrs : TypeCheckedExprs) (currRef : ResolvedEntityRef) =
        let newRestrs =
            match Map.tryFind currRef allowedEntity.Children with
            | None -> oldRestrs
            | Some child ->
                let currRestr = accessor child
                let currExpr = renameRestriction relaxedRef currRestr
                Map.add (string currExpr) (parentCheck, currExpr) oldRestrs
        let entity = layout.FindEntity currRef |> Option.get
        match entity.Parent with
        | None -> newRestrs
        | Some parent -> buildParentOrRestrictions newRestrs parent

    // We allow any child too.
    let buildChildOrRestrictions (currRef : ResolvedEntityRef) =
        match Map.tryFind currRef allowedEntity.Children with
        | None -> Map.empty
        | Some child ->
            let restrs = accessor child
            let expr = renameRestriction relaxedRef restrs
            let typeCheck = checkEntityForExpr currRef |> optimizeFieldExpr
            Map.singleton (string expr) (typeCheck, expr)

    let parentOrRestrs = buildParentOrRestrictions Map.empty entityRef
    let orRestrs =
        selfEntity.Children
        |> Map.keys
        |> Seq.map buildChildOrRestrictions
        |> Seq.fold unionTypeCheckedOrExprs parentOrRestrs
        |> optimizeTypeCheckedOrExprs
        |> buildTypeCheckedOrExprs

    // We restrict access based on parent fields that are accessed.
    let rec buildParentAndRestrictions (oldRestrs : TypeCheckedExprs) (currRef : ResolvedEntityRef) =
        let newRestrs =
            match Map.tryFind currRef allowedFields with
            | None -> oldRestrs
            | Some fieldRestr ->
                let currExpr = renameRestriction relaxedRef fieldRestr
                Map.add (string currExpr) (notFieldExpr parentCheck, currExpr) oldRestrs
        let entity = layout.FindEntity currRef |> Option.get
        match entity.Parent with
        | None -> newRestrs
        | Some parent -> buildParentAndRestrictions newRestrs parent

    // We also restrict access to children rows based on accessed fields.
    let buildChildAndRestrictions (currRef : ResolvedEntityRef) =
        match Map.tryFind currRef allowedFields with
        | None -> Map.empty
        | Some fieldRestr ->
            let expr = renameRestriction relaxedRef fieldRestr
            let typeCheck = checkEntityForExpr currRef |> optimizeFieldExpr |> notFieldExpr
            Map.singleton (string expr) (typeCheck, expr)

    let parentAndRestrs = buildParentAndRestrictions Map.empty entityRef
    let andRestrs =
        selfEntity.Children
        |> Map.keys
        |> Seq.map buildChildAndRestrictions
        |> Seq.fold unionTypeCheckedAndExprs parentAndRestrs
        |> optimizeTypeCheckedAndExprs
        |> buildTypeCheckedAndExprs
    andFieldExpr orRestrs andRestrs
