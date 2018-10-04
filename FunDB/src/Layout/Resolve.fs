module FunWithFlags.FunDB.Layout.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source

exception ResolveLayoutError of info : string with
    override this.Message = this.info

let private checkName : FunQLName -> unit = function
    | FunQLName name when name.Contains("__") -> raise (ResolveLayoutError <| sprintf "Name contains double underscore: %s" name)
    | FunQLName "" -> raise (ResolveLayoutError <| sprintf "Name is empty")
    | _ -> ()

let private checkFieldName (name : FunQLName) : unit =
    if name = funId
    then raise (ResolveLayoutError <| sprintf "Name is forbidden: %O" name)
    else checkName name

let private ensurePureFieldExpr : ParsedFieldExpr -> PureFieldExpr =
    let voidReference colName =
        raise (ResolveLayoutError <| sprintf "Column references are not supported in pure values: %O" colName)
    let voidPlaceholder name =
        raise (ResolveLayoutError <| sprintf "Placeholders are not allowed in pure values: %s" name)
    mapFieldExpr voidReference voidPlaceholder

let private resolveLocalExpr (entity : SourceEntity) : ParsedFieldExpr -> LocalFieldExpr =
    let resolveColumn = function
        | { entity = None; name = fieldName } ->
            // Ids are allowed in computed fields and in check constraints, but not in unique constraints.
            match entity.FindField(fieldName) with
                | Some _ -> fieldName
                | None when fieldName = funId -> fieldName
                | None -> raise (ResolveLayoutError <| sprintf "Column not found in local expression: %s" (fieldName.ToFunQLString()))
        | ref -> raise (ResolveLayoutError <| sprintf "Local expression cannot contain qualified field references: %O" ref)
    let voidPlaceholder name =
        raise (ResolveLayoutError <| sprintf "Placeholders are not allowed in local expressions: %s" name)
    mapFieldExpr resolveColumn voidPlaceholder

let private resolveReferenceExpr (thisEntity : SourceEntity) (refEntity : SourceEntity) : ParsedFieldExpr -> ResolvedReferenceFieldExpr =
    let resolveColumn = function
        | { entity = Some { schema = None; name = FunQLName "this" }; name = thisName } ->
            match thisEntity.FindField(thisName) with
                | Some _ -> RThis thisName
                | None when thisName = funId -> RThis thisName
                | None -> raise (ResolveLayoutError <| sprintf "Local column not found in reference condition: %s" (thisName.ToFunQLString()))
        | { entity = Some { schema = None; name = FunQLName "ref" }; name = refName } ->
            match refEntity.FindField(refName) with
                | Some _ -> RRef refName
                | None when refName = funId -> RRef refName
                | None -> raise (ResolveLayoutError <| sprintf "Referenced column not found in reference condition: %s" (refName.ToFunQLString()))
        | ref -> raise (ResolveLayoutError <| sprintf "Invalid column reference in reference condition: %O" ref)
    let voidPlaceholder name =
        raise (ResolveLayoutError <| sprintf "Placeholders are not allowed in reference conditions: %s" name)
    mapFieldExpr resolveColumn voidPlaceholder

let private resolveUniqueConstraint (entity : SourceEntity) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.columns then
        raise <| ResolveLayoutError "Empty unique constraint"
    
    let checkColumn name =
        match entity.FindField(name) with
            | Some _ -> name
            | None -> raise (ResolveLayoutError <| sprintf "Unknown column in unique constraint: %O" name)

    { columns = Array.map checkColumn constr.columns }

let private resolveCheckConstraint (entity : SourceEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
    let checkExpr =
        match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutError <| sprintf "Error parsing check constraint expression: %s" msg)
    { expression = resolveLocalExpr entity checkExpr }

let private resolveComputedField (entity : SourceEntity) (comp : SourceComputedField) : ResolvedComputedField =
    let computedExpr =
        match parse tokenizeFunQL fieldExpr comp.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutError <| sprintf "Error parsing computed field expression: %s" msg)

    { expression = resolveLocalExpr entity computedExpr
    }

type private LayoutResolver (layout : SourceLayout) =
    let resolveFieldType (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference (entityName, where) ->
            let refEntity =
                match layout.FindEntity(entityName) with
                    | None -> raise (ResolveLayoutError <| sprintf "Cannot find entity %O from reference type" entityName)
                    | Some refEntity -> refEntity
            let resolvedWhere = Option.map (resolveReferenceExpr entity refEntity) where
            FTReference (entityName, resolvedWhere)
        | FTEnum vals -> FTEnum vals

    let resolveColumnField (entity : SourceEntity) (col : SourceColumnField) : ResolvedColumnField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.fieldType with
                | Ok r -> r
                | Error msg -> raise (ResolveLayoutError <| sprintf "Error parsing column field type: %s" msg)
        let defaultExpr =
            match col.defaultExpr with
                | None -> None
                | Some def ->
                    match parse tokenizeFunQL fieldExpr def with
                        | Ok r -> Some r
                        | Error msg -> raise (ResolveLayoutError <| sprintf "Error parsing column field default expression: %s" msg)

        { fieldType = resolveFieldType entity fieldType
          defaultExpr = Option.map ensurePureFieldExpr defaultExpr
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
                | Failure msg -> raise (ResolveLayoutError <| sprintf "Clashing field names: %s" msg)
        if entity.mainField <> funId then
            if not <| Set.contains entity.mainField fields then
                raise (ResolveLayoutError <| sprintf "Nonexistent main field: %O" entity.mainField)

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
    { schemas = Map.map (fun schemaName schema -> resolver.ResolveSchema(schema)) layout.schemas
      systemEntities = resolver.ResolveEntities(layout.systemEntities)
    }
