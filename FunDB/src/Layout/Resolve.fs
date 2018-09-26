module FunWithFlags.FunDB.Layout.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source

exception ResolveLayoutError of info : string with
    override this.Message = this.info

let private checkName : FunQLName -> () = function
    | FunQLName name when name.Contains("__") -> raise <| ResolveLayoutError <| sprintf "Name contains double underscore: %s" name
    | FunQLName "" -> raise <| ResolveLayoutError <| sprintf "Name is empty"
    | _ -> ()

let private checkFieldName (name : FunQLName) : () =
    if name = funId or name = funSubEntity
    then raise <| ResolveLayoutError <| sprintf "Name is forbidden: %O" name
    else checkName name

let private ensurePureFieldExpr : ParsedFieldExpr -> LocalFieldExpr =
    let voidReference colName =
        raise <| ResolveLayoutError <| sprintf "Column references are not supported in pure values: %O" colName
    let voidPlaceholder name =
        raise <| ResolveLayoutError <| sprintf "Placeholders are not allowed in pure values: %s" name
    foreachFieldExpr voidReference voidPlaceholder

let private resolveLocalExpr (entity : SourceEntity) : ParsedFieldExpr -> LocalFieldExpr =
    let resolveColumn = function
        | { entity = None; name = fieldName } ->
            // Ids are allowed in computed fields and in check constraints, but not in unique constraints.
            match entity.FindField(fieldName) with
                | Some _ -> fieldName
                | None when fieldName = funId -> fieldName
                | None -> raise <| ResolveLayoutError <| sprintf "Column not found in local expression: %s" (fieldName.ToFunQLName())
        | ref -> raise <| ResolveLayoutError <| sprintf "Local expression cannot contain qualified field references: %O" ref
    let voidPlaceholder name =
        raise <| ResolveLayoutError <| sprintf "Placeholders are not allowed in local expressions: %s" name
    mapFieldExpr resolveColumn voidPlaceholder

let private resolveReferenceExpr (thisEntity : SourceEntity) (refEntity : SourceEntity) : ParsedFieldExpr -> ResolvedReferenceFieldExpr =
    let resolveColumn = function
        | { entity = Some { schema = None; table = FunQLName "this" }; name = thisName } ->
            match thisEntity.FindField(thisName) with
                | Some _ -> RThis thisName
                | None when thisName = funId -> RThis thisName
                | None -> raise <| ResolveLayoutError <| sprintf "Local column not found in reference condition: %s" (thisName.ToFunQLName())
        | { entity = Some { schema = None; table = FunQLName "ref" }; name = refName } ->
            match refEntity.FindField(refName) with
                | Some _ -> RRef refName
                | None when refName = funId -> RRef refName
                | None -> raise <| ResolveLayoutError <| sprintf "Referenced column not found in reference condition: %s" (refName.ToFunQLName())
        | ref -> raise <| ResolveLayoutError <| sprintf "Invalid column reference in reference condition: %O" ref
    let voidPlaceholder name =
        raise <| ResolveLayoutError <| sprintf "Placeholders are not allowed in reference conditions: %s" name
    mapFieldExpr resolveColumn voidPlaceholder

let private resolveUniqueConstraint (entity : SourceEntity) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    let checkColumn name =
        match entity.FindField(name) with
            | Some _ -> name
            | None -> raise <| ResolveLayoutError <| sprintf "Unknown column in unique constraint: %O" name

    { columns = Array.map checkColumn constr.columns }

let private resolveCheckConstraint (entity : SourceEntity) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
    let checkExpr =
        match parse tokenizeFunQL fieldExpr constr.expression with
            | Ok r -> r
            | Error msg -> raise <| ResolveLayoutError <| sprintf "Error parsing check constraint expression: %s" msg
    { expression = resolveLocalExpr entity checkExpr }

let private resolveFieldType (layout : SourceLayout) (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
    | FTType ft -> FTType ft
    | FTReference (entityName, where) ->
        let refEntity =
            match layout.FindField(entityName) with
                | None -> raise <| ResolveLayoutError <| sprintf "Cannot find entity %O from reference type" entityName
                | Some refEntity -> refEntity
        let resolvedWhere = Option.map (resolveReferenceExpr entity refEntity) where
        FTReference (entityName, resolvedWhere)
    | FTEnum vals -> FTEnum vals

let private resolveComputedField (entity : SourceEntity) (comp : SourceComputedField) : ResolvedComputedField =
    let computedExpr =
        match parse tokenizeFunQL fieldExpr comp.expression with
            | Ok r -> r
            | Error msg -> raise <| ResolveLayoutError <| sprintf "Error parsing computed field expression: %s" msg

    { expression = resolveLocalExpr entity computedExpr
    }

let private checkUniqueDescendantFields (entitiesMap : Map<EntityName, ResolvedEntity>) (entityName : EntityName) : () =
    let Same entity = Map.find entityName entitiesMap
    let descendants = entity.descendantsClosure |> Set.toSeq |> Seq.map (fun name -> Map.find name entitiesMap)
    Seq.append (Seq.singleton entity) descendants |> Seq.map (fun entity -> Set.union (mapKeys entity.columnFields) (mapKeys entity.computedFields)) |> Seq.fold setUnionUnique Set.empty

type private EntityContext =
    { ancestors : EntityName array
      descendants : Set<EntityName>
      descendantsClosure : Set<EntityName>
    }

type private LayoutResolver (layout : Layout) =
    let resolveColumnField (col : SourceColumnField) : ResolvedComputedField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.fieldType with
                | Ok r -> r
                | Error msg -> raise <| ResolveLayoutError <| sprintf "Error parsing column field type: %s" msg
        let defaultExpr =
            match col.defaultExpr with
                | None -> None
                | Some def ->
                    match parse tokenizeFunQL fieldExpr def with
                        | Ok r -> Some r
                        | Error msg -> raise <| ResolveLayoutError <| sprintf "Error parsing column field default expression: %s" msg

        { fieldType = resolveFieldType layout fieldType
          defaultExpr = Option.map ensurePureFieldExpr defaultExpr
          isNullable = comp.Nullable
        }

    let resolveEntity (entitiesMap : Map<EntityName, SourceEntity>) (context : EntityContext) (entity : SourceEntity) : ResolvedEntity =
        let columnFields = Map.map (fun name field -> checkFieldName name; resolveColumnField schemasMap field) entity.columnFields
        let computedFields = Map.map (fun name field -> checkFieldName name; resolveComputedField schemasMap field) entity.computedFields
        let uniqueConstraints = Map.map (fun name constr -> checkName name; resolveUniqueConstraint fieldsMap constr) entity.uniqueConstraints
        let checkConstraints = Map.map (fun name constr -> checkName name; resolveCheckConstraint fieldsMap constr) entity.checkConstraints

        // Check uniqueness of field names
        ignore <| setOfSeqUnique <| Seq.append (Map.toSeq columnFields |> Seq.map fst) (Map.toSeq computedFields |> Seq.map fst)

        { isAbstract = entity.isAbstract
          ancestors = context.ancestors
          descendants = context.descendants
          descendantsClosure = context.descendantsClosure
          possibleSubEntities = Set.add entityName context.descendantsClosure |> Set.filter (fun currName -> not (Map.find currName entitiesMap).isAbstract)
          columnFields = columnFields
          computedFields = computedFields
          uniqueConstraints = uniqueConstraints
          checkConstraints = checkConstraints
        }

    let resolveEntities (entitiesMap : Map<EntityName, SourceEntity>) : Map<EntityName, ResolvedEntity> =
        // Build immediate descendants
        let addImmediateDescendant descs (name, entity) =
            match entity.ancestor with
                | Some ancestorName ->
                    if not <| Map.contains ancestorName entitiesMap then
                        raise <| ResolveLayoutError <| sprintf "Ancestor not found: %O" ancestorName
                    let newNames =
                        match Map.tryFind ancestorName descs with
                            | Some names -> Set.insert name names
                            | None -> Set.singleton name
                    Map.add ancestorName newNames descs
                | None -> descs

        let descendants = entitiesMap |> Map.toSeq |> Seq.fold addImmediateDescendant Map.empty

        // Build descendants closure
        let reachDescendants stack newDescs name =
            let (descendants, descendantsClosure, newDescs_) =
                match Map.tryFind name descendants with
                    | None -> (Set.empty, Set.empty, newDescs)
                    | Some descs ->
                        let newStack = name :: stack
                        let newDescs_ = reachDescendantsFor newStack newDescs (Set.toSeq descs)
                        let addDescendants currSet descName = Set.union currSet (Map.find descName newDescs_).descendantsClosure
                        let closure = Set.toSeq descs |> Seq.fold addDesccendants descs
                        (descs, closure, newDescs_)
            let res =
                { descendants = descendants
                  descendantsClosure = descendantsClosure
                  ancestors = stack |> List.toArray |> Array.reverse
                }
            Map.insert name res newDescs_
        and reachDescendantsFor stack newDescs descs =
            Seq.fold (reachDescendants stack) newDescs descs
       
        let rootEntities = entitiesMap |> Map.toSeq |> Seq.filter (fun (name, entity) -> Option.none entity.ancestor) |> Seq.map fst |> Seq.cache
        let contexts = reachDescendantsFor [] Map.empty rootEntities

        let unreachable = Set.difference (mapKeys entitiesMap) (mapKeys contexts)
        if not <| Set.isEmpty unreachable then
            raise <| ResolveLayoutError <| sprintf "Unreachable entities found: %O" unreachable

        let res = entitiesMap |> Map.map (fun entityName entity -> checkName entityName; resolveEntity entitiesMap (Map.find entityName contexts) entity)
        Seq.foreach (checkUniqueDescendantFields res) rootEntities

    let resolveSchema (schema : SourceSchema) : ResolvedSchema =
        { entities = resolveEntities entitiesMap }

    member this.ResolveEntities = resolveEntities
    member this.ResolveSchema = resolveSchema

let resolveLayout (layout : SourceLayout) : Layout =
    let resolver = LayoutResolver layout
    { schemas = Map.map (fun schemaName schema -> resolver.ResolveSchema(schema)) layout.schemas
      systemEntities = resolver.ResolveEntities(layout.systemEntities)
    }
