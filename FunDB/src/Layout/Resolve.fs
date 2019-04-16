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

let private resolveReferenceExpr (thisEntity : SourceEntity) (refEntity : SourceEntity) : ParsedFieldExpr -> ResolvedReferenceFieldExpr =
    let resolveColumn : LinkedFieldRef -> ReferenceRef = function
        | { ref = { entity = Some { schema = None; name = FunQLName "this" }; name = thisName }; path = [||] } ->
            match thisEntity.FindField thisName with
            | Some _ -> RThis thisName
            | None when thisName = funId -> RThis thisName
            | None -> raise (ResolveLayoutException <| sprintf "Local column not found in reference condition: %s" (thisName.ToFunQLString()))
        | { ref = { entity = Some { schema = None; name = FunQLName "ref" }; name = refName }; path = [||] } ->
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

let private resolveEntityRef : EntityRef -> ResolvedEntityRef = function
    | { schema = Some schema; name = name } -> { schema = schema; name = name }
    | ref -> raise (ResolveLayoutException <| sprintf "Unspecified schema for entity %O" ref.name)

[<NoComparison>]
type private HalfResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, SourceComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
    }

[<NoComparison>]
type private HalfResolvedSchema =
    { entities : Map<EntityName, HalfResolvedEntity>
    }

[<NoComparison>]
type private HalfResolvedLayout =
    { schemas : Map<SchemaName, HalfResolvedSchema>
    }

type private Phase1Resolver (layout : SourceLayout) =
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

    let resolveEntity (entity : SourceEntity) : HalfResolvedEntity =
        let columnFields = Map.map (fun name field -> checkFieldName name; resolveColumnField entity field) entity.columnFields
        let uniqueConstraints = Map.map (fun name constr -> checkName name; resolveUniqueConstraint entity constr) entity.uniqueConstraints

        Map.iter (fun name constr -> checkName name) entity.checkConstraints
        Map.iter (fun name field -> checkFieldName name) entity.computedFields

        let fields =
            try
                Set.ofSeqUnique <| Seq.append (Map.toSeq columnFields |> Seq.map fst) (Map.toSeq entity.computedFields |> Seq.map fst)
            with
            | Failure msg -> raise (ResolveLayoutException <| sprintf "Clashing field names: %s" msg)
        if entity.mainField <> funId then
            if not <| Set.contains entity.mainField fields then
                raise (ResolveLayoutException <| sprintf "Nonexistent main field: %O" entity.mainField)

        { columnFields = columnFields
          computedFields = entity.computedFields
          uniqueConstraints = uniqueConstraints
          checkConstraints = entity.checkConstraints
          mainField = entity.mainField
        }

    let resolveSchema (schema : SourceSchema) : HalfResolvedSchema =
        { entities = schema.entities |> Map.map (fun entityName entity -> checkName entityName; resolveEntity entity)
        }

    let resolveLayout () : HalfResolvedLayout =
        { schemas = Map.map (fun schemaName schema -> checkSchemaName schemaName; resolveSchema schema) layout.schemas
        }

    member this.ResolveLayout = resolveLayout

type private Phase2Resolver (layout : HalfResolvedLayout) =
    let rec checkPath (entity : HalfResolvedEntity) (fieldName : FieldName) = function
        | [] ->
            // Should be in sync with ResolvedEntity.FindField
            if fieldName = funId then
                ()
            else if fieldName = funMain then
                ()
            else
                match Map.tryFind fieldName entity.columnFields with
                | Some col -> ()
                | None ->
                    match Map.tryFind fieldName entity.computedFields with
                    | Some comp -> ()           
                    | None -> raise (ResolveLayoutException <| sprintf "Column field not found in path: %O" fieldName)
        | (ref :: refs) ->
            match Map.tryFind fieldName entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity.name (Map.find refEntity.schema layout.schemas).entities
                checkPath newEntity ref refs
            | _ -> raise (ResolveLayoutException <| sprintf "Invalid dereference in path: %O" ref)

    let resolveComputedExpr (entity : HalfResolvedEntity) (expr : ParsedFieldExpr) : bool * LinkedLocalFieldExpr =
        let mutable isLocal = true
        let resolveColumn : LinkedFieldRef -> LinkedFieldName = function
            | { ref = { entity = None; name = name }; path = path } ->
                checkPath entity name (Array.toList path)
                if not <| Array.isEmpty path then
                    isLocal <- false
                { ref = name; path = path }
            | ref ->
                raise (ResolveLayoutException <| sprintf "Invalid reference in computed column: %O" ref)
        let voidPlaceholder name =
            raise (ResolveLayoutException <| sprintf "Placeholders are not allowed in computed columns: %O" name)
        let voidQuery query =
            raise (ResolveLayoutException <| sprintf "Queries are not allowed in computed columns: %O" query)
        
        let exprRes = mapFieldExpr resolveColumn voidPlaceholder voidQuery expr
        (isLocal, exprRes)

    let resolveComputedField (entity : HalfResolvedEntity) (comp : SourceComputedField) : ResolvedComputedField =
        let computedExpr =
            match parse tokenizeFunQL fieldExpr comp.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing computed field expression '%s': %s" comp.expression msg)
        let (isLocal, expr) = resolveComputedExpr entity computedExpr    
        { expression = expr
          isLocal = isLocal
        }

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
        
        mapFieldExpr resolveColumn voidPlaceholder voidQuery

    let resolveCheckConstraint (entity : ResolvedEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let checkExpr =
            match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise (ResolveLayoutException <| sprintf "Error parsing check constraint expression: %s" msg)
        { expression = resolveCheckExpr entity checkExpr }

    let resolveEntity (entity : HalfResolvedEntity) : ResolvedEntity =
        let computedFields = Map.map (fun name field -> resolveComputedField entity field) entity.computedFields
        let tempEntity =
            { columnFields = entity.columnFields
              computedFields = computedFields
              uniqueConstraints = entity.uniqueConstraints
              checkConstraints = Map.empty
              mainField = entity.mainField
            } : ResolvedEntity
        let checkConstraints = Map.map (fun name constr -> resolveCheckConstraint tempEntity constr) entity.checkConstraints

        { columnFields = entity.columnFields
          computedFields = computedFields
          uniqueConstraints = entity.uniqueConstraints
          checkConstraints = checkConstraints
          mainField = entity.mainField
        }

    let resolveSchema (schema : HalfResolvedSchema) : ResolvedSchema =
        { entities = schema.entities |> Map.map (fun entityName entity -> resolveEntity entity)
        }

    let resolveLayout () =
        { schemas = Map.map (fun schemaName schema -> resolveSchema schema) layout.schemas
        } : Layout

    member this.ResolveLayout = resolveLayout

type private CycleChecker (layout : Layout) =
    let rec checkComputedFieldCycles (stack : Set<ResolvedFieldRef>) (fieldRef : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedComputedField) =
        if Set.contains fieldRef stack then
            raise (ResolveLayoutException <| sprintf "Cycle detected: %O" stack)
        else
            let newStack = Set.add fieldRef stack
        
            let checkColumn (linkedField : LinkedFieldName) =
                checkPathCycles stack fieldRef.entity entity (linkedField.ref :: Array.toList linkedField.path)
            let noopPlaceholder name = ()
            let noopQuery query = ()
            iterFieldExpr checkColumn noopPlaceholder noopQuery field.expression

    and checkPathCycles (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : FieldName list -> unit = function
        | [] -> failwith "Unexpected empty list of references"
        | [ref] ->
            match entity.FindField ref |> Option.get with
            | (_, RId)
            | (_, RColumnField _) -> ()
            | (_, RComputedField comp) ->
                checkComputedFieldCycles stack { entity = entityRef; name = ref } entity comp
        | (ref :: refs) ->
            match entity.FindField ref |> Option.get with
            | (_, RColumnField { fieldType = FTReference (newEntityRef, _) }) ->
                let newEntity = layout.FindEntity newEntityRef |> Option.get
                checkPathCycles stack newEntityRef newEntity refs
            | _ -> failwith "Unexpected invalid reference"

    and checkComputedCycles (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) =
        for KeyValue(fieldName, field) in entity.computedFields do
            checkComputedFieldCycles stack { entity = entityRef; name = fieldName } entity field

    and runCheck () =
        layout.schemas |> Map.iter (fun schemaName schema -> schema.entities |> Map.iter (fun entityName entity -> checkComputedCycles Set.empty { schema = schemaName; name = entityName } entity))            

    member this.RunCheck = runCheck

let resolveLayout (layout : SourceLayout) : Layout =
    let phase1 = Phase1Resolver layout
    let halfLayout = phase1.ResolveLayout ()
    let phase2 = Phase2Resolver halfLayout
    let layout = phase2.ResolveLayout ()
    let cycleChecker = CycleChecker layout
    cycleChecker.RunCheck ()
    layout