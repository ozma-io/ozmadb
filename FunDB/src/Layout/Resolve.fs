module FunWithFlags.FunDB.Layout.Resolve

open Newtonsoft.Json
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
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
    let voidSubEntity ent q =
        raisef ResolveLayoutException "Aggregate functions are not allowed in reference conditions"

    mapFieldExpr
        { idFieldExprMapper resolveReference voidQuery with
              aggregate = voidAggr
              subEntity = voidSubEntity
        }

let private resolveUniqueConstraint (entity : SourceEntity) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    let checkColumn name =
        match entity.FindField(name) with
        | Some _ -> name
        | None -> raisef ResolveLayoutException "Unknown column %O in unique constraint" name

    { columns = Array.map checkColumn constr.columns }

let private resolveEntityRef (name : EntityRef) =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ResolveLayoutException "Unspecified schema for entity %O" name

[<NoEquality; NoComparison>]
type private HalfResolvedComputedField =
    | HRInherited of ResolvedEntityRef
    | HRSource of SourceComputedField

[<NoEquality; NoComparison>]
type private HalfResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, HalfResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      children : Map<ResolvedEntityRef, ChildEntity>
      typeName : string
      root : ResolvedEntityRef
      source : SourceEntity
    } with
        member this.FindField name =
            genericFindField (fun name -> Map.tryFind name this.columnFields) (fun name -> Map.tryFind name this.computedFields) this.source.mainField name

        member this.Fields =
            let id = Seq.singleton (funId, RId)
            let subentity =
                if this.HasSubType then
                    Seq.singleton (funSubEntity, RSubEntity)
                else
                    Seq.empty
            let columns = this.columnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
            Seq.concat [id; subentity; columns]

        member this.HasSubType =
            Option.isSome this.source.parent || this.source.isAbstract || not (Map.isEmpty this.children)

        member this.MainField = this.source.mainField

        interface IEntityFields with
            member this.FindField name =
                genericFindField (fun name -> Map.tryFind name this.columnFields) (fun _ -> None) this.source.mainField name
            member this.Fields = this.Fields
            member this.MainField = this.source.mainField
            member this.IsAbstract = this.source.isAbstract
            member this.Parent = this.source.parent
            member this.Children = Map.toSeq this.children

type private HalfResolvedEntities = Map<ResolvedEntityRef, HalfResolvedEntity>

let private makeCheckExprGeneric (getTypeName : 'a -> string) (getIsAbstract : 'a -> bool) (getChildren : 'a -> Map<ResolvedEntityRef, ChildEntity>) (getEntity : ResolvedEntityRef -> 'a) (entity : 'a) =
    let getName entity =
        let isAbstract = getIsAbstract entity
        if isAbstract then
            None
        else
            entity |> getTypeName |> SQL.VString |> SQL.VEValue |> Some

    let column = SQL.VEColumn { table = None; name = sqlFunSubEntity }
    let childrenEntities = getChildren entity |> Map.keys |> Seq.map getEntity
    let allEntities = Seq.append (Seq.singleton entity) childrenEntities
    let options = allEntities |> Seq.mapMaybe getName |> Seq.toArray

    if Array.isEmpty options then
        SQL.VEValue (SQL.VBool false)
    else if Array.length options = 1 then
        SQL.VEEq (column, options.[0])
    else
        SQL.VEIn (column, options)

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
            if Option.isSome where then
                raisef ResolveLayoutException "Reference conditions are not yet implemented"
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
                    | Some FNull -> raisef ResolveLayoutException "Default expression cannot be NULL"
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
          children = Map.empty
          root = root
          typeName = typeName
          source = entity
        }

    let rec addIndirectChildren (childRef : ResolvedEntityRef) (entity : HalfResolvedEntity) =
        match entity.source.parent with
        | None -> ()
        | Some parentRef ->
            let oldParent = Map.find parentRef cachedEntities
            cachedEntities <- Map.add parentRef { oldParent with children = Map.add childRef { direct = false } oldParent.children } cachedEntities
            addIndirectChildren childRef oldParent

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
                    cachedEntities <- Map.add ref { cached with children = Map.add childRef { direct = true } cached.children } cachedEntities
                    addIndirectChildren childRef cached
                cached
            | None ->
                checkName ref.name
                let parent = Option.map resolveParent entity.parent
                let entity = resolveEntity parent ref entity
                let entityWithChild =
                    match child with
                    | None -> entity
                    | Some childRef ->
                        addIndirectChildren childRef entity
                        { entity with children = Map.singleton childRef { direct = true } }
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
                checkName name
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

type private Phase2Resolver (layout : SourceLayout, entities : HalfResolvedEntities, forceAllowBroken : bool) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, Result<ResolvedComputedField, ComputedFieldError>> = Map.empty

    let layoutFields =
        { new ILayoutFields with
              member this.FindEntity ref = Map.tryFind ref entities |> Option.map (fun e -> e :> IEntityFields)
        }

    let rec checkPath (stack : Set<ResolvedFieldRef>) (usedSchemas : UsedSchemas) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (fields : FieldName list) : ComputedFieldProperties =
        match fields with
        | [] ->
            match entity.FindField fieldRef.name with
            | Some (_, RId) ->
                { isLocal = true; hasId = true; usedSchemas = usedSchemas }
            | Some (_, RSubEntity) ->
                { isLocal = true; hasId = false; usedSchemas = usedSchemas }
            | Some (_, RComputedField comp) ->
                match resolveComputedField stack entity fieldRef comp with
                | Ok field -> { isLocal = field.isLocal; hasId = field.hasId; usedSchemas = mergeUsedSchemas usedSchemas field.usedSchemas }
                | Error e -> raisefWithInner ResolveLayoutException e.error "Computed field %O is broken" fieldRef
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

    and resolveComputedExpr (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (allowBroken : bool) (expr : ParsedFieldExpr) : ResolvedComputedField =
        let mutable isLocal = true
        let mutable hasId = false
        let mutable usedSchemas = Map.empty

        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { ref = VRColumn { entity = None; name = name }; path = path } ->
                let res = checkPath stack usedSchemas entity { entity = entityRef; name = name } (Array.toList path)
                usedSchemas <- res.usedSchemas
                if not res.isLocal then
                    isLocal <- false
                if res.hasId then
                    hasId <- true
                let bound =
                    { ref = { entity = entityRef; name = name }
                      immediate = true
                    }
                { ref = VRColumn { ref = ({ entity = None; name = name } : FieldRef); bound = Some bound }; path = path }
            // Placeholders are forbidden because computed fields might be used in check expressions.
            | ref ->
                raisef ResolveLayoutException "Invalid reference in computed column: %O" ref
        let resolveQuery query =
            isLocal <- false
            try
                let (_, res) = resolveSelectExpr layoutFields query
                res
            with
            | :? ViewResolveException as e -> raisefWithInner ResolveLayoutException e.InnerException "Invalid expression"
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in computed columns"

        let exprRes =
            mapFieldExpr
                { idFieldExprMapper resolveReference resolveQuery with
                      aggregate = voidAggr
                      subEntity = resolveSubEntity layoutFields
                } expr
        { expression = exprRes
          isLocal = isLocal
          hasId = hasId
          usedSchemas = usedSchemas
          inheritedFrom = None
          allowBroken = allowBroken
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) : HalfResolvedComputedField -> Result<ResolvedComputedField, ComputedFieldError> = function
        | HRInherited parentRef ->
            let origFieldRef = { fieldRef with entity = parentRef }
            match Map.tryFind origFieldRef cachedComputedFields with
            | Some (Ok f) -> Ok { f with inheritedFrom = Some parentRef }
            | Some (Error e) -> Error { e with inheritedFrom = Some parentRef }
            | None ->
                let origEntity = Map.find parentRef entities
                match resolveComputedField stack origEntity origFieldRef (Map.find fieldRef.name origEntity.computedFields) with
                | Ok f -> Ok { f with inheritedFrom = Some parentRef }
                | Error e -> Error { e with inheritedFrom = Some parentRef }
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
                try
                    let field = Ok <| resolveComputedExpr newStack fieldRef.entity entity comp.allowBroken computedExpr
                    cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                    field
                with
                | :? ResolveLayoutException as e when comp.allowBroken || forceAllowBroken ->
                    Error { source = comp; error = e; inheritedFrom = None }

    let resolveCheckExpr (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : ParsedFieldExpr -> LocalFieldExpr =
        let resolveReference : LinkedFieldRef -> FieldName = function
            | { ref = VRColumn { entity = None; name = name }; path = [||] } ->
                let res =
                    match Map.tryFind name entity.columnFields with
                    | Some col -> name
                    | None ->
                        match Map.tryFind name entity.computedFields with
                        | Some (Ok comp) ->
                            if comp.isLocal then
                                name
                            else
                                raise (ResolveLayoutException <| sprintf "Non-local computed field reference in check expression: %O" name)
                        | _ when name = funId -> funId
                        | _ when name = funSubEntity -> funSubEntity
                        | _ -> raise (ResolveLayoutException <| sprintf "Column not found in check expression: %O" name)
                res
            | ref ->
                raise (ResolveLayoutException <| sprintf "Invalid reference in check expression: %O" ref)
        let voidQuery query =
            raise (ResolveLayoutException <| sprintf "Queries are not allowed in check expressions: %O" query)
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in check expressions"
        let resolveLocalSubEntity ctx (field : FieldName) subEntity =
            let fieldRef = { entity = entityRef; name = field }
            let boundField = { ref = fieldRef; immediate = true }
            let linkedField = { ref = VRColumn { ref = { entity = None; name = field }; bound = Some boundField }; path = [||] } : LinkedBoundFieldRef
            resolveSubEntity layoutFields ctx linkedField subEntity

        mapFieldExpr
            { idFieldExprMapper resolveReference voidQuery with
                  aggregate = voidAggr
                  subEntity = resolveLocalSubEntity
            }

    let resolveCheckConstraint (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let checkExpr =
            match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing check constraint expression: %s" msg)
        { expression = resolveCheckExpr entityRef entity checkExpr }

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ErroredEntity * ResolvedEntity =
        let mutable computedErrors = Map.empty

        let mapComputedField name field =
            try
                let ret = resolveComputedField Set.empty entity { entity = entityRef; name = name } field
                match ret with
                | Ok _ -> ()
                | Error e ->
                    computedErrors <- Map.add name e.error computedErrors
                ret
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in computed field %O: %s" name e.Message

        let computedFields = Map.map mapComputedField entity.computedFields

        let makeInheritance parent =
            let checkExpr = makeCheckHalfExpr (fun ref -> Map.find ref entities) entity
            { parent = parent
              checkExpr = checkExpr
            }

        let subEntityParseExpr =
            let getName (ref : ResolvedEntityRef, entity : HalfResolvedEntity) =
                if entity.source.isAbstract then
                    None
                else
                    let json = JToken.FromObject ref |> SQL.VJson |> SQL.VEValue
                    Some (entity.typeName, json)

            let column = SQL.VEColumn { table = None; name = sqlFunSubEntity }
            let childrenEntities = entity.children |> Map.keys |> Seq.map (fun ref -> (ref, Map.tryFind ref entities |> Option.get))
            let allEntities = Seq.append (Seq.singleton (entityRef, entity)) childrenEntities
            let options = allEntities |> Seq.mapMaybe getName |> Seq.toArray

            if Array.isEmpty options then
                SQL.VEValue SQL.VNull
            else if Array.length options = 1 then
                let (name, json) = options.[0]
                json
            else
                let makeCase (name, json) =
                    let check = SQL.VEEq (column, SQL.VEValue (SQL.VString name))
                    (check, json)
                SQL.VECase (Array.map makeCase options, None)

        let tempEntity =
            { columnFields = entity.columnFields
              computedFields = computedFields
              uniqueConstraints = entity.uniqueConstraints
              checkConstraints = Map.empty
              mainField = entity.source.mainField
              forbidExternalReferences = entity.source.forbidExternalReferences
              hidden = entity.source.hidden
              inheritance = Option.map makeInheritance entity.source.parent
              subEntityParseExpr = subEntityParseExpr
              children = entity.children
              root = entity.root
              typeName = entity.typeName
              isAbstract = entity.source.isAbstract
            } : ResolvedEntity
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint entityRef tempEntity constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in check constraint %O: %s" name e.Message
        let checkConstraints = Map.map mapCheckConstraint entity.source.checkConstraints

        let ret =
            { tempEntity with
                  checkConstraints = checkConstraints
            }
        let errors =
            { computedFields = computedErrors
            }
        (errors, ret)

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : ErroredSchema * ResolvedSchema =
        let mutable roots = Set.empty
        let mutable errors = Map.empty

        let mapEntity name entity =
            let ref = { schema = schemaName; name = name }
            let halfEntity = Map.find ref entities
            try
                let (entityErrors, entity) = resolveEntity ref halfEntity
                if Option.isNone entity.inheritance then
                    roots <- Set.add name roots
                if not (Map.isEmpty entityErrors.computedFields) then
                    errors <- Map.add name entityErrors errors
                entity
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in entity %O: %s" name e.Message

        let entities = schema.entities |> Map.map mapEntity
        let ret =
            { entities = entities
              roots = roots
            }
        (errors, ret)

    let resolveLayout () : ErroredLayout * Layout =
        let mutable errors = Map.empty

        let mapSchema name schema =
            try
                let (schemaErrors, ret) = resolveSchema name schema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                ret
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { schemas = Map.map mapSchema layout.schemas
            } : Layout
        (errors, ret)

    member this.ResolveLayout () = resolveLayout ()

let resolveLayout (layout : SourceLayout) (forceAllowBroken : bool) : ErroredLayout * Layout =
    let phase1 = Phase1Resolver layout
    let entities = phase1.ResolveLayout ()
    let phase2 = Phase2Resolver (layout, entities, forceAllowBroken)
    let (errors, layout2) = phase2.ResolveLayout ()
    (errors, layout2)