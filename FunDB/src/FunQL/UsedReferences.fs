module FunWithFlags.FunDB.FunQL.UsedReferences

// Count used references without accounting for meta columns.

open FSharpPlus

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

type private UsedReferencesBuilder (layout : ILayoutBits) =
    let mutable usedArguments : UsedArguments = Set.empty
    let mutable usedDatabase : UsedDatabase = emptyUsedDatabase
    let mutable hasRestrictedEntities = false

    let rec addField (extra : ObjectMap) (ref : ResolvedFieldRef) (field : ResolvedFieldInfo)=
        match field.Field with
        | RId
        | RSubEntity
        | RColumnField _ ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
        | RComputedField comp ->
            for (case, comp) in computedFieldCases layout extra { ref with Name = field.Name } comp do
                buildForFieldExpr comp.Expression

    and buildForPath (extra : ObjectMap) (ref : ResolvedFieldRef) (asRoot : bool) : (ResolvedEntityRef * PathArrow) list -> unit = function
        | [] ->
            let entity = layout.FindEntity ref.Entity |> Option.get
            let field = entity.FindField ref.Name |> Option.get
            addField extra ref field
        | (entityRef, arrow) :: paths ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
            if not asRoot then
                hasRestrictedEntities <- true
            buildForPath extra { Entity = entityRef; Name = arrow.Name } arrow.AsRoot paths

    and buildForReference (ref : LinkedBoundFieldRef) =
        let info = ObjectMap.tryFindType<FieldRefMeta> ref.Extra |> Option.defaultValue emptyFieldRefMeta
        match info.Bound with
        | Some (BMColumn boundInfo) ->
            let pathWithEntities = Seq.zip info.Path ref.Ref.Path |> Seq.toList
            buildForPath ref.Extra boundInfo.Ref ref.Ref.AsRoot pathWithEntities
        | Some (BMArgument arg) ->
            if not <| Array.isEmpty ref.Ref.Path then
                let argRef = { Entity = info.Path.[0]; Name = ref.Ref.Path.[0].Name }
                let pathWithEntities = Seq.zip info.Path ref.Ref.Path |> Seq.skip 1 |> Seq.toList
                buildForPath ref.Extra argRef ref.Ref.AsRoot pathWithEntities
        | None ->
            assert (Array.isEmpty ref.Ref.Path)

    and buildForResult : ResolvedQueryResult -> unit = function
        | QRAll alias -> ()
        | QRExpr result -> buildForColumnResult result

    and buildForColumnResult (result : ResolvedQueryColumnResult) =
        buildForBoundAttributesMap result.Attributes
        buildForFieldExpr result.Result

    and buildForBoundAttributeExpr : ResolvedBoundAttributeExpr -> unit = function
        | BAExpr expr -> buildForFieldExpr expr
        | BAMapping mapping -> ()

    and buildForBoundAttribute (attr : ResolvedBoundAttribute) =
        buildForBoundAttributeExpr attr.Expression

    and buildForAttribute (attr : ResolvedAttribute) =
        buildForFieldExpr attr.Expression

    and buildForBoundAttributesMap (attributes : ResolvedBoundAttributesMap) =
        Map.iter (fun name attr -> buildForBoundAttribute attr) attributes

    and buildForAttributesMap (attributes : ResolvedAttributesMap) =
        Map.iter (fun name attr -> buildForAttribute attr) attributes

    and buildForFieldExpr (expr : ResolvedFieldExpr) =
        let mapper =
            { idFieldExprIter with
                FieldReference = buildForReference
                Query = buildForSelectExpr
            }

        iterFieldExpr mapper expr

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
            let buildForOne = Array.iter buildForValuesValue
            Array.iter buildForOne values

    and buildForCommonTableExpr (cte : ResolvedCommonTableExpr) =
        buildForSelectExpr cte.Expr

    and buildForCommonTableExprs (ctes : ResolvedCommonTableExprs) =
        Array.iter (fun (name, expr) -> buildForCommonTableExpr expr) ctes.Exprs

    and buildForSelectExpr (select : ResolvedSelectExpr) =
        Option.iter buildForCommonTableExprs select.CTEs
        buildForSelectTreeExpr select.Tree

    and buildForSingleSelectExpr (query : ResolvedSingleSelectExpr) =
        buildForAttributesMap query.Attributes
        Option.iter buildForFromExpr query.From
        Option.iter (ignore << buildForFieldExpr) query.Where
        Array.iter (ignore << buildForFieldExpr) query.GroupBy
        Array.iter buildForResult query.Results
        buildForOrderLimitClause query.OrderLimit

    and buildForTableExpr : ResolvedTableExpr -> unit = function
        | TESelect subsel -> buildForSelectExpr subsel
        | TEDomain (dom, flags) ->
            if flags.AsRoot then
                hasRestrictedEntities <- true
        | TEFieldDomain (entity, field, flags) ->
            if flags.AsRoot then
                hasRestrictedEntities <- true
        | TETypeDomain (typ, flags) ->
            if flags.AsRoot then
                hasRestrictedEntities <- true

    and buildForFromTableExpr (expr : ResolvedFromTableExpr) =
        buildForTableExpr expr.Expression

    and buildForFromExpr : ResolvedFromExpr -> unit = function
        | FEntity fromEnt ->
            if fromEnt.AsRoot then
                hasRestrictedEntities <- true
        | FTableExpr expr -> buildForFromTableExpr expr
        | FJoin join ->
            buildForFromExpr join.A
            buildForFromExpr join.B
            ignore <| buildForFieldExpr join.Condition

    and buildForValuesValue : ResolvedValuesValue -> unit = function
        | VVDefault -> ()
        | VVExpr expr -> ignore <| buildForFieldExpr expr

    and buildForInsertExpr (insert : ResolvedInsertExpr) =
        Option.iter buildForCommonTableExprs insert.CTEs
        let entityRef = getResolvedEntityRef insert.Entity.Ref
        let usedFields = insert.Fields |> Seq.map (fun fieldName -> (fieldName, usedFieldInsert)) |> Map.ofSeq
        let usedEntity = { usedEntityInsert with Fields = usedFields }
        usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        match insert.Source with
        | ISDefaultValues -> ()
        | ISSelect select -> buildForSelectExpr select

    and buildForUpdateAssignExpr (entityRef : ResolvedEntityRef) = function
        | UAESet (name, expr) ->
            buildForValuesValue expr
            let usedEntity = { usedEntityUpdate with Fields = Map.singleton name usedFieldUpdate }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase
        | UAESelect (cols, expr) ->
            buildForSelectExpr expr
            let usedEntity = { usedEntityUpdate with Fields = cols |> Seq.map (fun name -> (name, usedFieldUpdate)) |> Map.ofSeq }
            usedDatabase <- addUsedEntityRef entityRef usedEntity usedDatabase

    and buildForUpdateExpr (update : ResolvedUpdateExpr) =
        Option.iter buildForCommonTableExprs update.CTEs
        let entityRef = getResolvedEntityRef update.Entity.Ref
        for assign in update.Assignments do
            buildForUpdateAssignExpr entityRef assign
        usedDatabase <- addUsedEntityRef entityRef usedEntityUpdate usedDatabase
        Option.iter buildForFromExpr update.From
        Option.iter (ignore << buildForFieldExpr) update.Where

    and buildForDeleteExpr (delete : ResolvedDeleteExpr) =
        Option.iter buildForCommonTableExprs delete.CTEs
        let entityRef = getResolvedEntityRef delete.Entity.Ref
        usedDatabase <- addUsedEntityRef entityRef usedEntityDelete usedDatabase
        Option.iter buildForFromExpr delete.Using
        Option.iter (ignore << buildForFieldExpr) delete.Where

    and buildForDataExpr = function
        | DESelect select -> buildForSelectExpr select
        | DEInsert insert -> buildForInsertExpr insert
        | DEUpdate update -> buildForUpdateExpr update
        | DEDelete delete -> buildForDeleteExpr delete

    member this.BuildForSelectExpr expr = buildForSelectExpr expr
    member this.BuildForInsertExpr expr = buildForInsertExpr expr
    member this.BuildForUpdateExpr expr = buildForUpdateExpr expr
    member this.BuildForDeleteExpr expr = buildForDeleteExpr expr
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

let fieldExprUsedReferences (layout : ILayoutBits) (expr : ResolvedFieldExpr) : UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    let info = builder.BuildForFieldExpr expr
    { UsedDatabase = builder.UsedDatabase
      UsedArguments = builder.UsedArguments
      HasRestrictedEntities = builder.HasRestrictedEntities
    }

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

let unionFlatUsedChildren = Map.unionWith unionUsedEntities

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
    let getField (ref : ResolvedFieldRef, usedField) =
        if ref.Name = funId || ref.Name = funSubEntity then
            None
        else
            Some (ref.Entity, { emptyUsedEntity with Fields = Map.singleton ref.Name usedField })
    let flatFieldEntities =
        fields
        |> Seq.mapMaybe getField
        |> Map.ofSeqWith (fun name -> unionUsedEntities)
    let usedEntity =
        { Children = Map.addWith unionUsedEntities entityRef usedEntity flatFieldEntities
          EntryPoints = Set.singleton entityRef
        } : FlatUsedEntity
    Map.singleton rootRef usedEntity
