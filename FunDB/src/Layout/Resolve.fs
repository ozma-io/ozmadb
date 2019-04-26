module FunWithFlags.FunDB.Layout.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source

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
    if name = funId
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
    let resolveColumn : LinkedFieldRef -> ReferenceRef = function
        | { ref = { entity = Some { schema = None; name = FunQLName "this" }; name = thisName }; path = [||] } ->
            match thisEntity.FindField thisName with
            | Some _ -> RThis thisName
            | None when thisName = funId -> RThis thisName
            | None -> raisef ResolveLayoutException "Local column not found in reference condition: %s" (thisName.ToFunQLString())
        | { ref = { entity = Some { schema = None; name = FunQLName "ref" }; name = refName }; path = [||] } ->
            match refEntity.FindField refName with
            | Some _ -> RRef refName
            | None when refName = funId -> RRef refName
            | None -> raisef ResolveLayoutException "Referenced column not found in reference condition: %s" (refName.ToFunQLString())
        | ref -> raisef ResolveLayoutException "Invalid column reference in reference condition: %O" ref
    let voidPlaceholder name =
        raisef ResolveLayoutException "Placeholders are not allowed in reference conditions: %O" name
    let voidQuery query =
        raisef ResolveLayoutException "Queries are not allowed in reference conditions: %O" query
    mapFieldExpr id resolveColumn voidPlaceholder voidQuery

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
type private HalfResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, SourceComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
    }
        member this.FindField (name : FieldName) =
            genericFindField this.columnFields this.computedFields this.mainField name

[<NoComparison>]
type private HalfResolvedSchema =
    { entities : Map<EntityName, HalfResolvedEntity>
    }

[<NoComparison>]
type private HalfResolvedLayout =
    { schemas : Map<SchemaName, HalfResolvedSchema>
    }

type private Phase1Resolver (layout : SourceLayout) =
    let resolveFieldType (schemaName : SchemaName) (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference (entityRef, where) ->
            let resolvedRef = resolveEntityRef entityRef
            let refEntity =
                match layout.FindEntity(resolvedRef) with
                | None -> raisef ResolveLayoutException "Cannot find entity %O from reference type" resolvedRef
                | Some refEntity -> refEntity
            if refEntity.forbidExternalReferences && schemaName <> resolvedRef.schema then
                raisef ResolveLayoutException "References from other schemas to entity %O are forbidden" entityRef
            let resolvedWhere = Option.map (resolveReferenceExpr entity refEntity) where
            FTReference (resolvedRef, resolvedWhere)
        | FTEnum vals ->
            if Set.isEmpty vals then
                raise (ResolveLayoutException "Enums must not be empty")
            FTEnum vals

    let resolveColumnField (schemaName : SchemaName) (entity : SourceEntity) (col : SourceColumnField) : ResolvedColumnField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.fieldType with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing column field type: %s" msg
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
        let fieldType = resolveFieldType schemaName entity fieldType

        { fieldType = fieldType
          valueType = compileFieldType fieldType
          defaultValue = defaultValue
          isNullable = col.isNullable
        }

    let resolveEntity (schemaName : SchemaName) (entity : SourceEntity) : HalfResolvedEntity =
        let mapColumnField name field =
            try
                checkFieldName name
                resolveColumnField schemaName entity field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in column field %O: %s" name e.Message
        let columnFields = Map.map mapColumnField entity.columnFields
        let mapUniqueConstraint name constr =
            try
                checkName name
                resolveUniqueConstraint entity constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in unique constraint %O: %s" name e.Message
        let uniqueConstraints = Map.map mapUniqueConstraint entity.uniqueConstraints

        let fields =
            try
                Set.ofSeqUnique <| Seq.append (Map.toSeq columnFields |> Seq.map fst) (Map.toSeq entity.computedFields |> Seq.map fst)
            with
            | Failure msg -> raisef ResolveLayoutException "Clashing field names: %s" msg
        if entity.mainField <> funId then
            if not <| Set.contains entity.mainField fields then
                raisef ResolveLayoutException "Nonexistent main field: %O" entity.mainField

        { columnFields = columnFields
          computedFields = entity.computedFields
          uniqueConstraints = uniqueConstraints
          checkConstraints = entity.checkConstraints
          mainField = entity.mainField
          forbidExternalReferences = entity.forbidExternalReferences
        }

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : HalfResolvedSchema =
        let mapEntity name entity =
            try
                checkName name
                resolveEntity schemaName entity
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in entity %O: %s" name e.Message
        { entities = schema.entities |> Map.map mapEntity
        }

    let resolveLayout () : HalfResolvedLayout =
        let mapSchema name schema =
            try
                checkSchemaName name
                resolveSchema name schema
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in schema %O: %s" name e.Message
        { schemas = Map.map mapSchema layout.schemas
        }

    member this.ResolveLayout = resolveLayout

type private ComputedFieldProperties =
    { isLocal : bool
      hasId : bool
      usedSchemas : UsedSchemas
    }

type private Phase2Resolver (layout : HalfResolvedLayout) =
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
            | Some (_, RColumnField _) ->
                let newUsed = addUsedField fieldRef.entity.schema fieldRef.entity.name fieldRef.name usedSchemas
                { isLocal = true; hasId = false; usedSchemas = newUsed }
            | None -> raisef ResolveLayoutException "Column field not found in path: %O" fieldRef.name
        | (ref :: refs) ->
            match Map.tryFind fieldRef.name entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity.name (Map.find refEntity.schema layout.schemas).entities
                let newUsed = addUsedFieldRef fieldRef usedSchemas
                let ret = checkPath stack newUsed newEntity { entity = refEntity; name = ref } refs
                { ret with isLocal = false }
            | _ -> raisef ResolveLayoutException "Invalid dereference in path: %O" ref

    and resolveComputedExpr (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (expr : ParsedFieldExpr) : ResolvedComputedField =
        let mutable isLocal = true
        let mutable hasId = false
        let mutable usedSchemas = Map.empty

        let resolveColumn : LinkedFieldRef -> LinkedFieldName = function
            | { ref = { entity = None; name = name }; path = path } ->
                let res = checkPath stack usedSchemas entity { entity = entityRef; name = name } (Array.toList path)
                usedSchemas <- res.usedSchemas
                if not res.isLocal then
                    isLocal <- false
                if res.hasId then
                    hasId <- true
                { ref = name; path = path }
            | ref ->
                raisef ResolveLayoutException "Invalid reference in computed column: %O" ref
        let voidPlaceholder name =
            raisef ResolveLayoutException "Placeholders are not allowed in computed columns: %O" name
        let voidQuery query =
            raisef ResolveLayoutException "Queries are not allowed in computed columns: %O" query

        let exprRes = mapFieldExpr id resolveColumn voidPlaceholder voidQuery expr
        { expression = exprRes
          isLocal = isLocal
          hasId = hasId
          usedSchemas = usedSchemas
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : SourceComputedField) : ResolvedComputedField =
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
        let resolveColumn : LinkedFieldRef -> FieldName = function
            | { ref = { entity = None; name = name }; path = [||] } ->
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
            | ref ->
                raise (ResolveLayoutException <| sprintf "Invalid reference in check expression: %O" ref)
        let voidPlaceholder name =
            raise (ResolveLayoutException <| sprintf "Placeholders are not allowed in check expressions: %O" name)
        let voidQuery query =
            raise (ResolveLayoutException <| sprintf "Queries are not allowed in check expressions: %O" query)

        mapFieldExpr id resolveColumn voidPlaceholder voidQuery

    let resolveCheckConstraint (entity : ResolvedEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let checkExpr =
            match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing check constraint expression: %s" msg)
        { expression = resolveCheckExpr entity checkExpr }

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ResolvedEntity =
        let mapComputedField name field =
            try
                checkFieldName name
                resolveComputedField Set.empty entity { entity = entityRef; name = name } field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in computed field %O: %s" name e.Message
        let computedFields = Map.map mapComputedField entity.computedFields
        let tempEntity =
            { columnFields = entity.columnFields
              computedFields = computedFields
              uniqueConstraints = entity.uniqueConstraints
              checkConstraints = Map.empty
              mainField = entity.mainField
              forbidExternalReferences = entity.forbidExternalReferences
            } : ResolvedEntity
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint tempEntity constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in check constraint %O: %s" name e.Message
        let checkConstraints = Map.map mapCheckConstraint entity.checkConstraints

        { tempEntity with
              checkConstraints = checkConstraints
        }

    let resolveSchema (schemaName : SchemaName) (schema : HalfResolvedSchema) : ResolvedSchema =
        let mapEntity name entity =
            try
                let ref = { schema = schemaName; name = name }
                resolveEntity ref entity
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in entity %O: %s" name e.Message
        { entities = schema.entities |> Map.map mapEntity
        }

    let resolveLayout () =
        let mapSchema name schema =
            try
                resolveSchema name schema
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "Error in schema %O: %s" name e.Message
        { schemas = Map.map mapSchema layout.schemas
        } : Layout

    member this.ResolveLayout = resolveLayout

let resolveLayout (layout : SourceLayout) : Layout =
    let phase1 = Phase1Resolver layout
    let layout1 = phase1.ResolveLayout ()
    let phase2 = Phase2Resolver layout1
    let layout2 = phase2.ResolveLayout ()
    layout2