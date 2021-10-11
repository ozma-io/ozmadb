module FunWithFlags.FunDB.FunQL.UsedReferences

// Count used references without accounting for meta columns.

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type UsedReferences =
    { UsedDatabase : UsedDatabase
      UsedArguments : UsedArguments
      HasRestrictedEntities : bool
    }

type ExprInfo =
    { IsLocal : bool
      HasQuery : bool
      HasAggregates : bool
    }

let emptyExprInfo =
    { IsLocal = true
      HasQuery = false
      HasAggregates = false
    }

let unionExprInfo (a : ExprInfo) (b : ExprInfo) =
    { IsLocal = a.IsLocal && b.IsLocal
      HasQuery = a.HasQuery || b.HasQuery
      HasAggregates = a.HasAggregates || b.HasAggregates
    }

type private UsedReferencesBuilder (layout : ILayoutBits) =
    let mutable usedArguments : UsedArguments = Set.empty
    let mutable usedDatabase : UsedDatabase = emptyUsedDatabase
    let mutable hasRestrictedEntities = false

    let rec addField (extra : ObjectMap) (ref : ResolvedFieldRef) (field : ResolvedFieldInfo) : ExprInfo =
        match field.Field with
        | RId
        | RSubEntity
        | RColumnField _ ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
            emptyExprInfo
        | RComputedField comp ->
            let info =
                computedFieldCases layout extra { ref with Name = field.Name } comp
                |> Seq.map (fun (case, comp) -> buildForFieldExpr comp.Expression)
                |> Seq.fold unionExprInfo emptyExprInfo
            if comp.IsMaterialized then
                emptyExprInfo
            else
                info

    and buildForPath (extra : ObjectMap) (ref : ResolvedFieldRef) (asRoot : bool) : (ResolvedEntityRef * PathArrow) list -> ExprInfo = function
        | [] ->
            let entity = layout.FindEntity ref.Entity |> Option.get
            let field = entity.FindField ref.Name |> Option.get
            addField extra ref field
        | (entityRef, arrow) :: paths ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
            if not asRoot then
                hasRestrictedEntities <- true
            let info = buildForPath extra { Entity = entityRef; Name = arrow.Name } arrow.AsRoot paths
            { info with IsLocal = false }

    and buildForReference (ref : LinkedBoundFieldRef) : ExprInfo =
        match ref.Ref.Ref with
        | VRColumn _ ->
            match ObjectMap.tryFindType<FieldMeta> ref.Extra with
            | Some { Bound = Some boundInfo } ->
                let pathWithEntities = Seq.zip boundInfo.Path ref.Ref.Path |> Seq.toList
                buildForPath ref.Extra boundInfo.Ref ref.Ref.AsRoot pathWithEntities
            | _ -> emptyExprInfo
        | VRPlaceholder name ->
            usedArguments <- Set.add name usedArguments
            match ObjectMap.tryFindType<ReferencePlaceholderMeta> ref.Extra with
            | Some argInfo when not (Array.isEmpty ref.Ref.Path) ->
                let argRef = { Entity = argInfo.Path.[0]; Name = ref.Ref.Path.[0].Name }
                let pathWithEntities = Seq.zip argInfo.Path ref.Ref.Path |> Seq.skip 1 |> Seq.toList
                buildForPath ref.Extra argRef ref.Ref.AsRoot pathWithEntities
            | _ -> emptyExprInfo

    and buildForResult : ResolvedQueryResult -> unit = function
        | QRAll alias -> ()
        | QRExpr result -> buildForColumnResult result

    and buildForColumnResult (result : ResolvedQueryColumnResult) =
        buildForAttributes result.Attributes
        ignore <| buildForFieldExpr result.Result

    and buildForAttributes (attributes : ResolvedAttributeMap) =
        Map.iter (fun name expr -> ignore <| buildForFieldExpr expr) attributes

    and buildForFieldExpr (expr : ResolvedFieldExpr) : ExprInfo =
        let mutable exprInfo = emptyExprInfo

        let iterReference ref =
            let newExprInfo = buildForReference ref
            exprInfo <- unionExprInfo exprInfo newExprInfo

        let iterQuery query =
            buildForSelectExpr query
            exprInfo <-
                { exprInfo with
                    IsLocal = false
                    HasQuery = true
                }

        let iterAggregate aggr =
            exprInfo <- { exprInfo with HasAggregates = true }

        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference
                Query = iterQuery
                Aggregate = iterAggregate
            }

        iterFieldExpr mapper expr
        exprInfo

    and buildForOrderColumn (ord : ResolvedOrderColumn) =
        buildForFieldExpr ord.Expr |> ignore

    and buildForOrderLimitClause (limits : ResolvedOrderLimitClause) =
        Array.iter buildForOrderColumn limits.OrderBy
        Option.iter (ignore << buildForFieldExpr) limits.Limit
        Option.iter (ignore << buildForFieldExpr) limits.Offset

    and buildForSelectTreeExpr : ResolvedSelectTreeExpr -> unit = function
        | SSelect query -> buildForSingleSelectExpr query
        | SSetOp setOp ->
            buildForSelectExpr setOp.A
            buildForSelectExpr setOp.B
            buildForOrderLimitClause setOp.OrderLimit
        | SValues values ->
            let buildForOne = Array.iter (ignore << buildForFieldExpr)
            Array.iter buildForOne values

    and buildForCommonTableExpr (cte : ResolvedCommonTableExpr) =
        buildForSelectExpr cte.Expr

    and buildForCommonTableExprs (ctes : ResolvedCommonTableExprs) =
        Array.iter (fun (name, expr) -> buildForCommonTableExpr expr) ctes.Exprs

    and buildForSelectExpr (select : ResolvedSelectExpr) =
        Option.iter buildForCommonTableExprs select.CTEs
        buildForSelectTreeExpr select.Tree

    and buildForSingleSelectExpr (query : ResolvedSingleSelectExpr) =
        buildForAttributes query.Attributes
        Option.iter buildForFromExpr query.From
        Option.iter (ignore << buildForFieldExpr) query.Where
        Array.iter (ignore << buildForFieldExpr) query.GroupBy
        Array.iter buildForResult query.Results
        buildForOrderLimitClause query.OrderLimit

    and buildForFromExpr : ResolvedFromExpr -> unit = function
        | FEntity fromEnt ->
            if not fromEnt.AsRoot then
                hasRestrictedEntities <- true
        | FJoin join ->
            buildForFromExpr join.A
            buildForFromExpr join.B
            ignore <| buildForFieldExpr join.Condition
        | FSubExpr subsel -> buildForSelectExpr subsel.Select

    and buildForInsertValue : ResolvedInsertValue -> unit = function
        | IVDefault -> ()
        | IVValue expr -> ignore <| buildForFieldExpr expr

    and buildForInsertExpr (insert : ResolvedInsertExpr) =
        Option.iter buildForCommonTableExprs insert.CTEs
        let entityRef = tryResolveEntityRef insert.Entity.Ref |> Option.get
        let usedFields = insert.Fields |> Seq.map (fun fieldName -> (fieldName, usedFieldInsert)) |> Map.ofSeq
        let usedEntity = { usedEntityInsert with Fields = usedFields }
        usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        match insert.Source with
        | ISDefaultValues -> ()
        | ISValues vals -> Array.iter (Array.iter buildForInsertValue) vals
        | ISSelect select -> buildForSelectExpr select

    and buildForUpdateExpr (update : ResolvedUpdateExpr) =
        Option.iter buildForCommonTableExprs update.CTEs
        let entityRef = tryResolveEntityRef update.Entity.Ref |> Option.get
        let usedFields = update.Fields |> Map.map (fun fieldName expr -> usedFieldUpdate)
        let usedEntity = { emptyUsedEntity with Fields = usedFields }
        usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        Option.iter buildForFromExpr update.From
        Option.iter (ignore << buildForFieldExpr) update.Where

    and buildForDeleteExpr (delete : ResolvedDeleteExpr) =
        Option.iter buildForCommonTableExprs delete.CTEs
        let entityRef = tryResolveEntityRef delete.Entity.Ref |> Option.get
        usedDatabase <- addUsedEntityRef entityRef usedEntityDelete usedDatabase
        Option.iter buildForFromExpr delete.Using
        Option.iter (ignore << buildForFieldExpr) delete.Where
    
    and buildForDataExpr = function
        | DESelect select -> buildForSelectExpr select
        | DEInsert insert -> buildForInsertExpr insert
        | DEUpdate update -> buildForUpdateExpr update
        | DEDelete delete -> buildForDeleteExpr delete

    member this.BuildForSelectExpr expr = buildForSelectExpr expr
    member this.BuildForFieldExpr expr = buildForFieldExpr expr

    member this.UsedDatabase = usedDatabase
    member this.UsedArguments = usedArguments
    member this.HasRestrictedEntities = hasRestrictedEntities

let selectExprUsedReferences (layout : ILayoutBits) (expr : ResolvedSelectExpr) : UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    builder.BuildForSelectExpr expr
    { UsedDatabase = builder.UsedDatabase
      UsedArguments = builder.UsedArguments
      HasRestrictedEntities = builder.HasRestrictedEntities
    }

let fieldExprUsedReferences (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ExprInfo * UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    let info = builder.BuildForFieldExpr expr
    let ret =
        { UsedDatabase = builder.UsedDatabase
          UsedArguments = builder.UsedArguments
          HasRestrictedEntities = builder.HasRestrictedEntities
        }
    (info, ret)

// Used entities grouped by root entity.
// All fields in entities belong to that entities.
type FlatUsedEntity =
    { Children : Map<ResolvedEntityRef, UsedEntity>
      EntryPoints : Set<ResolvedEntityRef>
    }

let emptyFlatUsedEntity : FlatUsedEntity =
    { Children = Map.empty
      EntryPoints = Set.empty
    }

let unionFlatUsedChildren = Map.unionWith (fun name -> unionUsedEntities)

let unionFlatUsedEntities (a : FlatUsedEntity) (b : FlatUsedEntity) : FlatUsedEntity =
    { Children = unionFlatUsedChildren a.Children b.Children
      EntryPoints = Set.union a.EntryPoints b.EntryPoints
    }

type FlatUsedDatabase = Map<ResolvedEntityRef, FlatUsedEntity>

// After flattening, field and entity access flags change meaning.
// Before, if a field is SELECTed the entity is also SELECTed, and so it is with UPDATEs and INSERTs.
// After, flags set in entity signify that it is encountered in a FROM clause.
// A flattened used entity with all access flags reset and used fields mean it is a parent entity,
// which is not used directly in the query.
// We also set all propagated flags for fields: for example, we use implicitly SELECT all fields when we delete a row.
// Finally we drop system fields like "id" and "sub_entity".
// We do not propagate flags for entities.
type private UsedDatabaseFlattener (layout : Layout) =
    let mutable flattenedRoots : FlatUsedDatabase = Map.empty

    let flattenUsedEntity (entityRef : ResolvedEntityRef) (usedEntity : UsedEntity) =
        let entity = layout.FindEntity entityRef |> Option.get

        let makeFlatField (fieldName, usedField : UsedField) =
            let field = entity.FindField fieldName |> Option.get
            match field.Field with
            | RId
            | RSubEntity -> None
            | RColumnField col ->
                let newUsedField =
                    { usedField with
                          Select = usedField.Select || usedField.Update
                    }
                let field = Map.find fieldName entity.ColumnFields
                let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
                Some (fieldEntity, { emptyUsedEntity with Fields = Map.singleton fieldName newUsedField })
            | _ -> failwith "Impossible"

        let makeFlatDeleteField (fieldName, field : ResolvedColumnField) =
            let fieldEntity = Option.defaultValue entityRef field.InheritedFrom
            (fieldEntity, { emptyUsedEntity with Fields = Map.singleton fieldName usedFieldSelect })

        let fieldChildren = usedEntity.Fields |> Map.toSeq |> Seq.mapMaybe makeFlatField |> Map.ofSeqWith (fun name -> unionUsedEntities)
        let deleteChildren =
            if not usedEntity.Delete then
                Map.empty
            else
                entity.ColumnFields |> Map.toSeq |> Seq.map makeFlatDeleteField |> Map.ofSeqWith (fun name -> unionUsedEntities)
        let thisEntity = { usedEntity with Fields = Map.empty }
        let entryPoints =
            if usedEntity.Select || usedEntity.Insert || usedEntity.Update || usedEntity.Delete then
                Set.singleton entityRef
            else
                Set.empty
        let thisChildren = Map.singleton entityRef thisEntity
        let children = unionFlatUsedChildren (unionFlatUsedChildren thisChildren fieldChildren) deleteChildren
        let flatEntity =
            { Children = children
              EntryPoints = entryPoints
            }
        flattenedRoots <- Map.addWith unionFlatUsedEntities entity.Root flatEntity flattenedRoots

    let flattenUsedSchema (schemaName : SchemaName) (usedSchema : UsedSchema) =
        let iterOne entityName usedEntity =
            let ref = { Schema = schemaName; Name = entityName }
            flattenUsedEntity ref usedEntity
        Map.iter iterOne usedSchema.Entities

    let flattenUsedDatabase (usedDatabase : UsedDatabase) =
        Map.iter flattenUsedSchema usedDatabase.Schemas
    
    member this.FlattenUsedDatabase usedDatabase = flattenUsedDatabase usedDatabase
    member this.FlattenedRoots = flattenedRoots

let flattenUsedDatabase (layout : Layout) (usedDatabase : UsedDatabase) : FlatUsedDatabase =
    let flattener = UsedDatabaseFlattener layout
    flattener.FlattenUsedDatabase usedDatabase
    flattener.FlattenedRoots

let singleKnownFlatEntity (rootRef : ResolvedEntityRef) (entityRef : ResolvedEntityRef) (usedEntity : UsedEntity) (fields : (ResolvedFieldRef * UsedField) seq) : FlatUsedDatabase =
    let flatFieldEntities =
        fields
        |> Seq.map (fun (ref, usedField) -> (ref.Entity, { emptyUsedEntity with Fields = Map.singleton ref.Name usedField }))
        |> Map.ofSeqWith (fun name -> unionUsedEntities)
    let usedEntity =
        { Children = Map.addWith unionUsedEntities entityRef usedEntity flatFieldEntities
          EntryPoints = Set.singleton entityRef
        } : FlatUsedEntity
    Map.singleton rootRef usedEntity
