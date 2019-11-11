module FunWithFlags.FunDB.Layout.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type ResolveLayoutException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveLayoutException (message, null)

let private checkName (FunQLName name) : unit =
    if not (goodName name) then
        raisef ResolveLayoutException "Invalid name"

let private checkSchemaName (FunQLName name) : unit =
    if name.StartsWith("pg_") || name = "information_schema" then
        raisef ResolveLayoutException "Invalid schema name"

let private checkFieldName (name : FunQLName) : unit =
    if name = funId || name = funSubEntity
    then raisef ResolveLayoutException "Name is forbidden"
    else checkName name

let private reduceDefaultExpr : ParsedFieldExpr -> FieldValue option = function
    | FEValue value -> Some value
    | FECast (FEValue value, typ) ->
        match (value, typ) with
        | (FString s, FETScalar SFTDate) -> Option.map FDate <| tryDateInvariant s
        | (FString s, FETScalar SFTDateTime) -> Option.map FDateTime <| tryDateTimeOffsetInvariant s
        | (FStringArray vals, FETArray SFTDate) -> Option.map (FDateArray << Array.ofSeq) (Seq.traverseOption tryDateInvariant vals)
        | (FStringArray vals, FETArray SFTDateTime) -> Option.map (FDateTimeArray << Array.ofSeq) (Seq.traverseOption tryDateTimeOffsetInvariant vals)
        | _ -> None
    | _ -> None

let private resolveReferenceExpr (thisEntity : SourceEntity) (refEntity : SourceEntity) : ParsedFieldExpr -> ResolvedReferenceFieldExpr =
    let resolveReference : LinkedFieldRef -> ReferenceRef = function
        | { ref = VRColumn { entity = Some { schema = None; name = FunQLName "this" }; name = thisName }; path = [||] } ->
            match thisEntity.FindField thisName with
            | Some _ -> RThis thisName
            | None when thisName = funId -> RThis thisName
            | None -> raisef ResolveLayoutException "Local column not found in reference condition: %s" (thisName.ToFunQLString())
        | { ref = VRColumn { entity = Some { schema = None; name = FunQLName "ref" }; name = refName }; path = [||] } ->
            match refEntity.FindField refName with
            | Some _ -> RRef refName
            | None when refName = funId -> RRef refName
            | None -> raisef ResolveLayoutException "Referenced column not found in reference condition: %s" (refName.ToFunQLString())
        | ref -> raisef ResolveLayoutException "Invalid reference in reference condition: %O" ref
    let voidQuery query =
        raisef ResolveLayoutException "Queries are not allowed in reference condition: %O" query
    let voidAggr aggr =
        raisef ResolveLayoutException "Aggregate functions are not allowed in reference conditions"

    mapFieldExpr id resolveReference voidQuery voidAggr

let private resolveUniqueConstraint (entity : SourceEntity) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    let checkColumn name =
        match entity.FindField(name) with
        | Some _ -> name
        | None -> raisef ResolveLayoutException "Unknown column %O in unique constraint" name

    { columns = Array.map checkColumn constr.columns }

let private resolveEntityRef : EntityRef -> ResolvedEntityRef = function
    | { schema = Some schema; name = name } -> { schema = schema; name = name }
    | ref -> raisef ResolveLayoutException "Unspecified schema for entity %O" ref.name

[<NoComparison>]
type private HalfResolvedComputedField =
    | HRInherited of ResolvedEntityRef
    | HRSource of SourceComputedField

[<NoComparison>]
type private HalfResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, HalfResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      children : Set<ResolvedEntityRef>
      typeName : string
      root : ResolvedEntityRef
      source : SourceEntity
    }
        member this.FindField (name : FieldName) =
            genericFindField this.columnFields this.computedFields this.source.mainField name

type private HalfResolvedEntities = Map<ResolvedEntityRef, HalfResolvedEntity>

let private makeCheckExprGeneric (getTypeName : 'a -> string) (getIsAbstract : 'a -> bool) (getChildren : 'a -> Set<ResolvedEntityRef>) (getEntity : ResolvedEntityRef -> 'a) (entity : 'a) =
    let column = SQL.VEColumn { table = None; name = sqlFunSubEntity }
    let rec recur entity =
        let typeName = getTypeName entity
        let isAbstract = getIsAbstract entity
        let children = getChildren entity
        let myName =
            if isAbstract then Seq.empty else Seq.singleton typeName
        let getEntityName childRef =
            let childEntity = getEntity childRef
            recur childEntity
        Seq.append myName (Seq.collect getEntityName children)
    let options = recur entity |> Seq.toArray
    if Array.isEmpty options then
        SQL.VEValue (SQL.VBool false)
    else if Array.length options = 1 then
        SQL.VEEq (column, SQL.VEValue (SQL.VString options.[0]))
    else
        let makeExpr name = SQL.VEValue (SQL.VString name)
        SQL.VEIn (column, Array.map makeExpr options)

let private makeCheckHalfExpr = makeCheckExprGeneric (fun (x : HalfResolvedEntity) -> x.typeName) (fun x -> x.source.isAbstract) (fun x -> x.children)

let makeCheckExpr = makeCheckExprGeneric (fun (x : ResolvedEntity) -> x.typeName) (fun x -> x.isAbstract) (fun x -> x.children)

type private Phase1Resolver (layout : SourceLayout) =
    let mutable cachedEntities : HalfResolvedEntities = Map.empty

    let resolveFieldType (ref : ResolvedEntityRef) (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference (entityRef, where) ->
            let resolvedRef = resolveEntityRef entityRef
            let refEntity =
                match layout.FindEntity(resolvedRef) with
                | None -> raisef ResolveLayoutException "Cannot find entity %O from reference type" resolvedRef
                | Some refEntity -> refEntity
            if refEntity.forbidExternalReferences && ref.schema <> resolvedRef.schema then
                raisef ResolveLayoutException "References from other schemas to entity %O are forbidden" entityRef
            let resolvedWhere = Option.map (resolveReferenceExpr entity refEntity) where
            FTReference (resolvedRef, resolvedWhere)
        | FTEnum vals ->
            if Set.isEmpty vals then
                raisef ResolveLayoutException "Enums must not be empty"
            FTEnum vals

    let resolveColumnField (ref : ResolvedEntityRef) (entity : SourceEntity) (fieldName : FieldName) (col : SourceColumnField) : ResolvedColumnField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.fieldType with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing column field type %s: %s" col.fieldType msg
        let defaultValue =
            match col.defaultValue with
            | None -> None
            | Some def ->
                match parse tokenizeFunQL fieldExpr def with
                | Ok r ->
                    match reduceDefaultExpr r with
                    | Some v -> Some v
                    | None -> raisef ResolveLayoutException "Default expression is not trivial: %s" def
                | Error msg -> raisef ResolveLayoutException "Error parsing column field default expression: %s" msg
        let fieldType = resolveFieldType ref entity fieldType

        let columnName =
            match entity.parent with
            | None -> fieldName.ToString()
            | Some p when p.schema = ref.schema -> sprintf "%O__%O" ref.name fieldName
            | Some p -> sprintf "%O__%O__%O" ref.schema ref.name fieldName

        { fieldType = fieldType
          valueType = compileFieldType fieldType
          defaultValue = defaultValue
          isNullable = col.isNullable
          isImmutable = col.isImmutable
          inheritedFrom = None
          columnName = SQL.SQLName columnName
        }

    let resolveEntity (parent : HalfResolvedEntity option) (entityRef : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        let mapColumnField name field =
            try
                checkFieldName name
                resolveColumnField entityRef entity name field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in column field %O: %s" name e.Message
        let mapComputedField name field =
            try
                checkFieldName name
                HRSource field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in computed field %O: %s" name e.Message

        let mapUniqueConstraint name constr =
            try
                checkName name
                resolveUniqueConstraint entity constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in unique constraint %O: %s" name e.Message
        let uniqueConstraints = Map.map mapUniqueConstraint entity.uniqueConstraints

        let columnFields =
            match parent with
            | None -> Map.map mapColumnField entity.columnFields
            | Some p ->
                let parentRef = Option.get entity.parent
                let selfFields = Map.map mapColumnField entity.columnFields
                let addParent (field : ResolvedColumnField) =
                    if Option.isNone field.inheritedFrom then
                        { field with inheritedFrom = Some parentRef }
                    else
                        field
                let inheritedFields = Map.map (fun name field -> addParent field) p.columnFields
                Map.unionUnique inheritedFields selfFields
        let computedFields =
            match parent with
            | None -> Map.map mapComputedField entity.computedFields
            | Some p ->
                let parentRef = Option.get entity.parent
                let selfFields = Map.map mapComputedField entity.computedFields
                let addParent = function
                    | HRSource source -> HRInherited parentRef
                    | HRInherited pref -> HRInherited pref
                let inheritedFields = Map.map (fun name field -> addParent field) p.computedFields
                Map.unionUnique inheritedFields selfFields

        let fields =
            try
                Set.ofSeqUnique <| Seq.append (Map.toSeq columnFields |> Seq.map fst) (Map.toSeq computedFields |> Seq.map fst)
            with
            | Failure msg -> raisef ResolveLayoutException "Clashing field names: %s" msg
        if entity.mainField <> funId then
            if not <| Set.contains entity.mainField fields then
                raisef ResolveLayoutException "Nonexistent main field: %O" entity.mainField

        let root =
            match parent with
            | None -> entityRef
            | Some p -> p.root

        let typeName =
            if root.schema = entityRef.schema then
                entityRef.name.ToString()
            else
                sprintf "%O__%O" entityRef.schema entityRef.name

        { columnFields = columnFields
          computedFields = computedFields
          uniqueConstraints = uniqueConstraints
          children = Set.empty
          root = root
          typeName = typeName
          source = entity
        }

    let rec resolveOneEntity (stack : Set<ResolvedEntityRef>) (child : ResolvedEntityRef option) (ref : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        try
            let resolveParent parentRef =
                let newStack = Set.add ref stack
                if Set.contains parentRef newStack then
                    raisef ResolveLayoutException "Inheritance cycle detected"
                else
                    if parentRef.schema <> ref.schema then
                        raisef ResolveLayoutException "Cross-schema inheritance is forbidden" parentRef
                    let parentEntity =
                        match layout.FindEntity parentRef with
                        | None -> raisef ResolveLayoutException "Parent entity %O not found" parentRef
                        | Some parent -> parent
                    resolveOneEntity newStack (Some ref) parentRef parentEntity

            match Map.tryFind ref cachedEntities with
            | Some cached ->
                match child with
                | None -> ()
                | Some childRef ->
                    cachedEntities <- Map.add ref { cached with children = Set.add childRef cached.children } cachedEntities
                cached
            | None ->
                checkName ref.name
                let parent = Option.map resolveParent entity.parent
                let entity = resolveEntity parent ref entity
                let entityWithChild =
                    match child with
                    | None -> entity
                    | Some childRef -> { entity with children = Set.singleton childRef }
                cachedEntities <- Map.add ref entityWithChild cachedEntities
                entity
        with
        | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in entity %O: %s" ref e.Message

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) =
        let rec iterEntity name entity =
            let ref = { schema = schemaName; name = name }
            let entity = resolveOneEntity Set.empty None ref entity
            ()
        schema.entities |> Map.iter iterEntity

    let resolveLayout () =
        let iterSchema name schema =
            try
                resolveSchema name schema
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in schema %O: %s" name e.Message
        Map.iter iterSchema layout.schemas

    member this.ResolveLayout () =
        resolveLayout ()
        cachedEntities

type private ComputedFieldProperties =
    { isLocal : bool
      hasId : bool
      usedSchemas : UsedSchemas
    }

type private Phase2Resolver (layout : SourceLayout, entities : HalfResolvedEntities) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, ResolvedComputedField> = Map.empty

    let rec checkPath (stack : Set<ResolvedFieldRef>) (usedSchemas : UsedSchemas) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (fields : FieldName list) : ComputedFieldProperties =
        match fields with
        | [] ->
            match entity.FindField fieldRef.name with
            | Some (_, RId) ->
                { isLocal = true; hasId = true; usedSchemas = usedSchemas }
            | Some (_, RComputedField comp) ->
                let field = resolveComputedField stack entity fieldRef comp
                { isLocal = field.isLocal; hasId = field.hasId; usedSchemas = mergeUsedSchemas usedSchemas field.usedSchemas }
            | Some (newName, RColumnField _) ->
                let newUsed = addUsedField fieldRef.entity.schema fieldRef.entity.name newName usedSchemas
                { isLocal = true; hasId = false; usedSchemas = newUsed }
            | None -> raisef ResolveLayoutException "Column field not found in path: %O" fieldRef.name
        | (ref :: refs) ->
            match Map.tryFind fieldRef.name entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity entities
                let newUsed = addUsedFieldRef fieldRef usedSchemas
                let ret = checkPath stack newUsed newEntity { entity = refEntity; name = ref } refs
                { ret with isLocal = false }
            | _ -> raisef ResolveLayoutException "Invalid dereference in path: %O" ref

    and resolveComputedExpr (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (expr : ParsedFieldExpr) : ResolvedComputedField =
        let mutable isLocal = true
        let mutable hasId = false
        let mutable usedSchemas = Map.empty

        let resolveReference : LinkedFieldRef -> LinkedFieldName = function
            | { ref = VRColumn { entity = None; name = name }; path = path } ->
                let res = checkPath stack usedSchemas entity { entity = entityRef; name = name } (Array.toList path)
                usedSchemas <- res.usedSchemas
                if not res.isLocal then
                    isLocal <- false
                if res.hasId then
                    hasId <- true
                { ref = VRColumn name; path = path }
            // Placeholders are forbidden because computed fields might be used in check expressions.
            | ref ->
                raisef ResolveLayoutException "Invalid reference in computed column: %O" ref
        let voidQuery query =
            raisef ResolveLayoutException "Queries are not allowed in computed columns: %O" query
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in computed columns"

        let exprRes = mapFieldExpr id resolveReference voidQuery voidAggr expr
        { expression = exprRes
          isLocal = isLocal
          hasId = hasId
          usedSchemas = usedSchemas
          inheritedFrom = None
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) : HalfResolvedComputedField -> ResolvedComputedField = function
        | HRInherited parentRef ->
            let origFieldRef = { fieldRef with entity = parentRef }
            match Map.tryFind origFieldRef cachedComputedFields with
            | Some f -> { f with inheritedFrom = Some parentRef }
            | None ->
                let origEntity = Map.find parentRef entities
                let f = resolveComputedField stack origEntity origFieldRef (Map.find fieldRef.name origEntity.computedFields)
                { f with inheritedFrom = Some parentRef }
        | HRSource comp ->
            match Map.tryFind fieldRef cachedComputedFields with
            | Some f -> f
            | None ->
                if Set.contains fieldRef stack then
                    raisef ResolveLayoutException "Cycle detected in computed fields: %O" stack
                let computedExpr =
                    match parse tokenizeFunQL fieldExpr comp.expression with
                    | Ok r -> r
                    | Error msg -> raisef ResolveLayoutException "Error parsing computed field expression: %s" msg
                let newStack = Set.add fieldRef stack
                let field = resolveComputedExpr newStack fieldRef.entity entity computedExpr
                cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                field

    let resolveCheckExpr (entity : ResolvedEntity) : ParsedFieldExpr -> LocalFieldExpr =
        let resolveReference : LinkedFieldRef -> ValueRef<FieldName> = function
            | { ref = VRColumn { entity = None; name = name }; path = [||] } ->
                let res =
                    match Map.tryFind name entity.columnFields with
                    | Some col -> name
                    | None ->
                        match Map.tryFind name entity.computedFields with
                        | Some comp ->
                            if comp.isLocal then
                                name
                            else
                                raise (ResolveLayoutException <| sprintf "Non-local computed field reference in check expression: %O" name)
                        | None when name = funId -> funId
                        | None -> raise (ResolveLayoutException <| sprintf "Column not found in check expression: %O" name)
                VRColumn res
            | ref ->
                raise (ResolveLayoutException <| sprintf "Invalid reference in check expression: %O" ref)
        let voidQuery query =
            raise (ResolveLayoutException <| sprintf "Queries are not allowed in check expressions: %O" query)
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in check expressions"

        mapFieldExpr id resolveReference voidQuery voidAggr

    let resolveCheckConstraint (entity : ResolvedEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let checkExpr =
            match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing check constraint expression: %s" msg)
        { expression = resolveCheckExpr entity checkExpr }

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ResolvedEntity =
        let mapComputedField name field =
            try
                resolveComputedField Set.empty entity { entity = entityRef; name = name } field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in computed field %O: %s" name e.Message

        let computedFields = Map.map mapComputedField entity.computedFields

        let makeInheritance parent =
            let checkExpr = makeCheckHalfExpr (fun ref -> Map.find ref entities) entity
            { parent = parent
              checkExpr = checkExpr
            }

        let tempEntity =
            { columnFields = entity.columnFields
              computedFields = computedFields
              uniqueConstraints = entity.uniqueConstraints
              checkConstraints = Map.empty
              mainField = entity.source.mainField
              forbidExternalReferences = entity.source.forbidExternalReferences
              hidden = entity.source.hidden
              inheritance = Option.map makeInheritance entity.source.parent
              children = entity.children
              root = entity.root
              typeName = entity.typeName
              isAbstract = entity.source.isAbstract
            } : ResolvedEntity
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint tempEntity constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in check constraint %O: %s" name e.Message
        let checkConstraints = Map.map mapCheckConstraint entity.source.checkConstraints

        { tempEntity with
              checkConstraints = checkConstraints
        }

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : ResolvedSchema =
        let mutable roots = Set.empty
        let mapEntity name entity =
            let ref = { schema = schemaName; name = name }
            let halfEntity = Map.find ref entities
            let entity = resolveEntity ref halfEntity
            if Option.isNone entity.inheritance then
                roots <- Set.add name roots
            entity

        let entities = schema.entities |> Map.map mapEntity
        { entities = entities
          roots = roots
        }

    let resolveLayout () =
        { schemas = Map.map resolveSchema layout.schemas
        } : Layout

    member this.ResolveLayout = resolveLayout

let resolveLayout (layout : SourceLayout) : Layout =
    let phase1 = Phase1Resolver layout
    let entities = phase1.ResolveLayout ()
    let phase2 = Phase2Resolver (layout, entities)
    let layout2 = phase2.ResolveLayout ()
    layout2