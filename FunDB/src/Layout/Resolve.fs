module FunWithFlags.FunDB.Layout.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source

exception ResolveLayoutException of info : string with
    override this.Message = this.info

let private checkName : FunQLName -> unit = function
    | FunQLName name when not (goodName name) -> raise (ResolveLayoutException <| sprintf "Invalid name: %s" name)
    | _ -> ()

let private checkSchemaName : FunQLName -> unit = function
    | FunQLName name when name.StartsWith("pg_") || name = "information_schema" -> raise (ResolveLayoutException <| sprintf "Invalid schema name: %s" name)
    | fname -> checkName fname

let private checkFieldName (name : FunQLName) : unit =
    if name = funId
    then raise (ResolveLayoutException <| sprintf "Name is forbidden: %O" name)
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

let private resolveLocalExpr (entity : SourceEntity) : ParsedFieldExpr -> LocalFieldExpr =
    let resolveColumn = function
        | { entity = None; name = fieldName } ->
            // Ids are allowed in computed fields and in check constraints, but not in unique constraints.
            match entity.FindField(fieldName) with
            | Some _ -> fieldName
            | None when fieldName = funId -> fieldName
            | None -> raise (ResolveLayoutException <| sprintf "Column not found in local expression: %s" (fieldName.ToFunQLString()))
        | ref -> raise (ResolveLayoutException <| sprintf "Local expression cannot contain qualified field references: %O" ref)
    let voidPlaceholder name =
        raise (ResolveLayoutException <| sprintf "Placeholders are not allowed in local expressions: %O" name)
    let voidQuery query =
        raise (ResolveLayoutException <| sprintf "Queries are not allowed in local expressions: %O" query)
    mapFieldExpr resolveColumn voidPlaceholder voidQuery

let private resolveReferenceExpr (thisEntity : SourceEntity) (refEntity : SourceEntity) : ParsedFieldExpr -> ResolvedReferenceFieldExpr =
    let resolveColumn = function
        | { entity = Some { schema = None; name = FunQLName "this" }; name = thisName } ->
            match thisEntity.FindField thisName with
            | Some _ -> RThis thisName
            | None when thisName = funId -> RThis thisName
            | None -> raise (ResolveLayoutException <| sprintf "Local column not found in reference condition: %s" (thisName.ToFunQLString()))
        | { entity = Some { schema = None; name = FunQLName "ref" }; name = refName } ->
            match refEntity.FindField refName with
            | Some _ -> RRef refName
            | None when refName = funId -> RRef refName
            | None -> raise (ResolveLayoutException <| sprintf "Referenced column not found in reference condition: %s" (refName.ToFunQLString()))
        | ref -> raise (ResolveLayoutException <| sprintf "Invalid column reference in reference condition: %O" ref)
    let voidPlaceholder name =
        raise (ResolveLayoutException <| sprintf "Placeholders are not allowed in reference conditions: %O" name)
    let voidQuery query =
        raise (ResolveLayoutException <| sprintf "Queries are not allowed in reference conditions: %O" query)
    mapFieldExpr resolveColumn voidPlaceholder voidQuery

let private resolveUniqueConstraint (entity : SourceEntity) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    let checkColumn name =
        match entity.FindField(name) with
        | Some _ -> name
        | None -> raise (ResolveLayoutException <| sprintf "Unknown column in unique constraint: %O" name)

    { columns = Array.map checkColumn constr.columns }

let private resolveCheckConstraint (entity : SourceEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
    let checkExpr =
        match parse tokenizeFunQL fieldExpr constr.expression with
        | Ok r -> r
        | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing check constraint expression: %s" msg)
    { expression = resolveLocalExpr entity checkExpr }

let private resolveComputedField (entity : SourceEntity) (comp : SourceComputedField) : ResolvedComputedField =
    let computedExpr =
        match parse tokenizeFunQL fieldExpr comp.expression with
        | Ok r -> r
        | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing computed field expression '%s': %s" comp.expression msg)

    { expression = resolveLocalExpr entity computedExpr
    }

let private resolveEntityRef : EntityRef -> ResolvedEntityRef = function
    | { schema = Some schema; name = name } -> { schema = schema; name = name }
    | ref -> raise (ResolveLayoutException <| sprintf "Unspecified schema for entity %O" ref.name)

type private LayoutResolver (layout : SourceLayout) =
    let resolveFieldType (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference (entityRef, where) ->
            let resolvedRef = resolveEntityRef entityRef
            let refEntity =
                match layout.FindEntity(resolvedRef) with
                | None -> raise (ResolveLayoutException <| sprintf "Cannot find entity %O from reference type" resolvedRef)
                | Some refEntity -> refEntity
            let resolvedWhere = Option.map (resolveReferenceExpr entity refEntity) where
            FTReference (resolvedRef, resolvedWhere)
        | FTEnum vals ->
            if Set.isEmpty vals then
                raise (ResolveLayoutException "Enums must not be empty")
            FTEnum vals

    let resolveColumnField (entity : SourceEntity) (col : SourceColumnField) : ResolvedColumnField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.fieldType with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing column field type: %s" msg)
        let defaultValue =
            match col.defaultValue with
            | None -> None
            | Some def ->
                match parse tokenizeFunQL fieldExpr def with
                | Ok r ->
                    match reduceDefaultExpr r with
                    | Some v -> Some v
                    | None -> raise (ResolveLayoutException <| sprintf "Default expression is not trivial: %s" def)
                | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing column field default expression: %s" msg)
        let fieldType = resolveFieldType entity fieldType

        { fieldType = fieldType
          valueType = compileFieldType fieldType
          defaultValue = defaultValue
          isNullable = col.isNullable
        }

    let resolveEntity (entity : SourceEntity) : ResolvedEntity =
        let columnFields = Map.map (fun name field -> checkFieldName name; resolveColumnField entity field) entity.columnFields
        let computedFields = Map.map (fun name field -> checkFieldName name; resolveComputedField entity field) entity.computedFields
        let uniqueConstraints = Map.map (fun name constr -> checkName name; resolveUniqueConstraint entity constr) entity.uniqueConstraints
        let checkConstraints = Map.map (fun name constr -> checkName name; resolveCheckConstraint entity constr) entity.checkConstraints

        let fields =
            try
                Set.ofSeqUnique <| Seq.append (Map.toSeq columnFields |> Seq.map fst) (Map.toSeq computedFields |> Seq.map fst)
            with
            | Failure msg -> raise (ResolveLayoutException <| sprintf "Clashing field names: %s" msg)
        if entity.mainField <> funId then
            if not <| Set.contains entity.mainField fields then
                raise (ResolveLayoutException <| sprintf "Nonexistent main field: %O" entity.mainField)

        { columnFields = columnFields
          computedFields = computedFields
          uniqueConstraints = uniqueConstraints
          checkConstraints = checkConstraints
          mainField = entity.mainField
        }

    let resolveEntities (entitiesMap : Map<EntityName, SourceEntity>) : Map<EntityName, ResolvedEntity> =
        entitiesMap |> Map.map (fun entityName entity -> checkName entityName; resolveEntity entity)

    let resolveSchema (schema : SourceSchema) : ResolvedSchema =
        { entities = resolveEntities schema.entities }

    member this.ResolveEntities = resolveEntities
    member this.ResolveSchema = resolveSchema

let resolveLayout (layout : SourceLayout) : Layout =
    let resolver = LayoutResolver layout
    { schemas = Map.map (fun schemaName schema -> checkSchemaName schemaName; resolver.ResolveSchema(schema)) layout.schemas
    }
