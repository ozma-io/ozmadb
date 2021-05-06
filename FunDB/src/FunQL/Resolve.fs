module FunWithFlags.FunDB.FunQL.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.
// ...that is, except checking references to other views.

// Unique id of this FROM. Used to distinguish between same entities; fields with same id correspond to same rows.
type FromEntityId = int
type FromId =
    | FIEntity of FromEntityId
    | FIPlaceholder of Placeholder

type FromFieldKey =
    { FromId : FromId
      Path : FieldName[]
    }

type ViewResolveException (message : string) =
    inherit Exception(message)

[<NoEquality; NoComparison>]
type private BoundFieldHeader =
    { Ref : ResolvedFieldRef
      Key : FromFieldKey
      // Means that field is selected directly from an entity and not from a subexpression.
      Immediate : bool
    }

[<NoEquality; NoComparison>]
type private BoundField =
    { Header : BoundFieldHeader
      Entity : IEntityBits
      Field : ResolvedFieldBits
      // If set, forces user to explicitly name the field.
      // Used for `__main`: user cannot use `__main` without renaming in SELECT.
      ForceRename : bool
      // Different from `Header.Ref.Name` in case of `__main`.
      Name : FieldName
    }

// `BoundField` exists only for fields that are bound to one and only one field.
// Any set operation which merges fields with different bound fields discards that.
// It's used for:
// * checking dereferences;
// * finding bound columns for main entity.
// Should not be used for anything else - different cells can be bound to different
// fields.
type private QSubqueryFieldsMap = Map<FieldName, BoundField option>
type private QSubqueryFields = (FieldName option * BoundField option)[]

type private ResolvedFieldMapping =
    // Means that field exists in one of entity's children but is inaccessible without type narrowing.
    | FMTypeRestricted of BoundFieldHeader
    | FMBound of BoundField
    | FMUnbound of FromEntityId

type private FieldMappingInfo =
    { Entity : EntityRef option
      Mapping : ResolvedFieldMapping
      // Used for injected expressions which need to use existing column names.
      // For example, chunk WHERE expressions use column names from original user view,
      // but need to use SQL column names from it.
      ForceSQLName : SQL.ColumnName option
    }

type private FieldMappingValue =
    | FVAmbiguous of Set<EntityRef>
    | FVResolved of FieldMappingInfo

type private FieldMapping = Map<FieldRef, FieldMappingValue>

// None in EntityRef is used only for offset/limit expressions in set operations.
let private getAmbiguousMapping = function
    | FVAmbiguous set -> set
    | FVResolved info -> Set.ofSeq (Option.toSeq info.Entity)

let private unionFieldMappingValue (a : FieldMappingValue) (b : FieldMappingValue) : FieldMappingValue =
    let setA = getAmbiguousMapping a
    let setB = getAmbiguousMapping b
    FVAmbiguous <| Set.union setA setB

type CustomFromField =
    { Bound : ResolvedFieldRef option
      ForceSQLName : SQL.ColumnName option
    }

type CustomFromMapping = Map<FieldRef, CustomFromField>

type SingleFromMapping =
    | SFEntity of ResolvedEntityRef
    | SFCustom of CustomFromMapping

let private explodeFieldRef (fieldRef : FieldRef) : FieldRef seq =
    seq {
        let fieldName = fieldRef.Name
        yield ({ Entity = None; Name = fieldName } : FieldRef)
        match fieldRef.Entity with
        | None -> ()
        | Some { Schema = maybeSchema; Name = entityName } ->
            yield ({ Entity = Some { Schema = None; Name = entityName }; Name = fieldName } : FieldRef)
            match maybeSchema with
            | None -> ()
            | Some schemaName ->
                yield ({ Entity = Some { Schema = Some schemaName; Name = entityName }; Name = fieldName } : FieldRef)
    }

let private explodeResolvedFieldRef (fieldRef : ResolvedFieldRef) : FieldRef seq =
    seq {
        let ({ Entity = { Schema = schemaName; Name = entityName }; Name = fieldName } : ResolvedFieldRef) = fieldRef
        yield { Entity = None; Name = fieldName }
        yield { Entity = Some { Schema = None; Name = entityName }; Name = fieldName }
        yield { Entity = Some { Schema = Some schemaName; Name = entityName }; Name = fieldName }
    }

let private fieldsToFieldMapping (fromEntityId : FromEntityId) (maybeEntityRef : EntityRef option) (fields : QSubqueryFieldsMap) : FieldMapping =
    let explodeVariations (fieldName, maybeBoundField) =
        let mapping =
            match maybeBoundField with
            | None -> FMUnbound fromEntityId
            | Some bound -> FMBound bound
        let info =
            { Entity = maybeEntityRef
              Mapping = mapping
              ForceSQLName = None
            }
        let value = FVResolved info
        explodeFieldRef { Entity = maybeEntityRef; Name = fieldName } |> Seq.map (fun x -> (x, value))
    fields |> Map.toSeq |> Seq.collect explodeVariations |> Map.ofSeq

let private typeRestrictedFieldsToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (maybeEntityRef : EntityRef option) (parentRef : ResolvedEntityRef) (children : ResolvedEntityRef seq) : FieldMapping =
    let filterField (name : FieldName, field : ResolvedFieldBits) =
        match field with
        | RColumnField f when Option.isNone f.InheritedFrom -> Some name
        | RComputedField f when Option.isNone f.InheritedFrom -> Some name
        | _ -> None
    let mapEntity (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let expandName name : (FieldRef * BoundFieldHeader) seq =
            let key =
                { FromId = FIEntity fromEntityId
                  Path = [||]
                }
            let header =
                { Ref = { Entity = parentRef; Name = name }
                  Key = key
                  Immediate = true
                }
            explodeResolvedFieldRef { Entity = parentRef; Name = name } |> Seq.map (fun x -> (x, header))
        let headerToResolved header =
            let info =
                { Entity = maybeEntityRef
                  Mapping = FMTypeRestricted header
                  ForceSQLName = None
                }
            FVResolved info
        entity.Fields |> Seq.mapMaybe filterField |> Seq.collect expandName |> Seq.map (fun (ref, header) -> (ref, headerToResolved header))
    children |> Seq.collect mapEntity |> Map.ofSeq

let private customToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (mapping : CustomFromMapping) : FieldMapping =
    let makeBoundField (boundRef : ResolvedFieldRef) =
        let entity = layout.FindEntity boundRef.Entity |> Option.get
        let field = entity.FindField boundRef.Name |> Option.get
        let key =
            { FromId = FIEntity fromEntityId
              Path = [||]
            }
        let header =
            { Ref = boundRef
              Key = key
              Immediate = true
            }
        { Header = header
          Entity = entity
          Field = resolvedFieldToBits field.Field
          ForceRename = field.ForceRename
          Name = field.Name
        }
    let mapField (fieldRef : FieldRef, info : CustomFromField) =
        let fieldMapping =
            match info.Bound with
            | None -> FMUnbound fromEntityId
            | Some field -> FMBound <| makeBoundField field
        let info =
            { Entity = fieldRef.Entity
              Mapping = fieldMapping
              ForceSQLName = info.ForceSQLName
            }
        let value = FVResolved info
        explodeFieldRef fieldRef |> Seq.map (fun x -> (x, value))
    mapping |> Map.toSeq |> Seq.collect mapField |> Map.ofSeq

type private SomeArgumentsMap = Map<ArgumentName, ParsedFieldType>

type private RecursiveValue<'a> =
    | RValue of 'a
    | RRecursive of EntityName

let private mapRecursiveValue (f : 'a -> 'b) = function
    | RValue a -> RValue (f a)
    | RRecursive r -> RRecursive r

let private renameFields (names : FieldName[]) (fields : QSubqueryFields) : QSubqueryFields =
    if Array.length names <> Array.length fields then
        raisef ViewResolveException "Inconsistent number of columns"
    Array.map2 (fun newName (oldName, info) -> (Some newName, info)) names fields

let private getFieldsMap (fields : QSubqueryFields) : QSubqueryFieldsMap =
    fields |> Seq.mapMaybe (fun (mname, info) -> Option.map (fun name -> (name, info)) mname) |> Map.ofSeq

// If a query has a main entity it satisfies two properties:
// 1. All rows can be bound to some entry of that entity;
// 2. Columns contain all required fields of that entity.
type ResolvedMainEntity =
    { Entity : ResolvedEntityRef
      ColumnsToFields : Map<FieldName, FieldName>
      FieldsToColumns : Map<FieldName, FieldName>
    }

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>

[<NoEquality; NoComparison>]
type ResolvedViewExpr =
    { Arguments : ResolvedArgumentsMap
      Select : ResolvedSelectExpr
      MainEntity : ResolvedMainEntity option
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let selectStr = this.Select.ToFunQLString()
            if Map.isEmpty this.Arguments
            then selectStr
            else
                let printArgument (name : Placeholder, arg : ResolvedArgument) =
                    match name with
                    | PGlobal _ -> None
                    | PLocal _ -> Some <| sprintf "%s %s" (name.ToFunQLString()) (arg.ToFunQLString())
                let argStr = this.Arguments |> Map.toSeq |> Seq.mapMaybe printArgument |> String.concat ", "
                sprintf "(%s): %s" argStr selectStr

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

let private unionBoundFieldInfo (f1 : BoundField option) (f2 : BoundField option) : BoundField option =
    match (f1, f2) with
    | (Some bf1, Some bf2) when bf1.Header.Ref = bf2.Header.Ref -> Some bf1
    | _ -> None

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ViewResolveException "Invalid name: %s" name

// Copy of that in Layout.Resolve but with a different exception.
let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ViewResolveException "Unspecified schema in name: %O" name

let resolveArgumentFieldType (layout : ILayoutBits) : ParsedFieldType -> ResolvedFieldType = function
    | FTType ft -> FTType ft
    | FTReference entityRef ->
        let resolvedRef = resolveEntityRef entityRef
        let refEntity =
            match layout.FindEntity resolvedRef with
            | Some refEntity when not refEntity.IsHidden -> refEntity
            | _ -> raisef ViewResolveException "Cannot find entity %O from reference type" resolvedRef
        FTReference resolvedRef
    | FTEnum vals ->
        if Set.isEmpty vals then
            raisef ViewResolveException "Enums must not be empty"
        FTEnum vals

let resolveArgument (layout : ILayoutBits) (arg : ParsedArgument) : ResolvedArgument =
    { ArgType = resolveArgumentFieldType layout arg.ArgType
      Optional = arg.Optional
    }

let getGlobalArgument = function
    | PGlobal arg -> Some arg
    | PLocal _ -> None

type private ResolvedExprInfo =
    { IsLocal : bool
      HasAggregates : bool
    }

type private ResolvedResultInfo =
    { InnerField : BoundField option
      HasAggregates : bool
    }

type ResolvedSingleSelectMeta =
    { HasAggregates : bool
    }

// When this metadata is absent, any subtype is possible at this point.
type PossibleSubtypesMeta =
    { PossibleSubtypes : Set<ResolvedEntityRef>
    }

// `None` means "don't check at all".
type SubEntityMeta =
    { CheckForTypes : Set<ResolvedEntityRef> option
    }

type ReferencePlaceholderMeta =
    { // Of length `ref.Path`, contains reference entities given current type context. Guaranteed to be non-empty.
      Path : ResolvedEntityRef[]
    }

type BoundFieldMeta =
    { Ref : ResolvedFieldRef
      // Set if field references value from a table directly, not via a subexpression.
      Immediate : bool
      // Of length `ref.Path`, contains reference entities given current type context.
      Path : ResolvedEntityRef[]
    }

type FieldMeta =
    { Bound : BoundFieldMeta option
      FromEntityId : FromEntityId
      ForceSQLName : SQL.ColumnName option
    }

type ResolvedCommonTableExprsMap = Map<EntityName, ResolvedCommonTableExpr>

type private ResolvedCommonTableExprTempMeta =
    { mutable MainEntity : bool
    }

type private ResolvedCommonTableExprsTempMeta =
    { Bindings : ResolvedCommonTableExprsMap
    }

type ResolvedCommonTableExprInfo =
    { MainEntity : bool
    }

let private cteBindings (select : ResolvedSelectExpr) : ResolvedCommonTableExprsMap =
    match select.CTEs with
    | Some ctes ->
        (ObjectMap.findType<ResolvedCommonTableExprsTempMeta> ctes.Extra).Bindings
    | None ->
        Map.empty

let rec private findMainEntityFromExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedFromExpr -> RecursiveValue<ResolvedEntityRef> = function
    | FEntity (pun, { Schema = Some schemaName; Name = name }) -> RValue { Schema = schemaName; Name = name }
    | FEntity (pun, { Schema = None; Name = name }) ->
        let cte = Map.find name ctes
        let extra = ObjectMap.findType<ResolvedCommonTableExprTempMeta> cte.Extra
        if extra.MainEntity then
            RRecursive <| Option.get currentCte
        else
            extra.MainEntity <- true
            mapRecursiveValue fst <| findMainEntityExpr ctes (Some name) cte.Expr
    | FJoin join ->
        match join.Type with
        | Left -> findMainEntityFromExpr ctes currentCte join.A
        | Right -> findMainEntityFromExpr ctes currentCte join.B
        | _ -> raisef ViewResolveException "Unsupported JOIN type when validating main entity"
    | FSubExpr (name, subExpr) ->
        mapRecursiveValue fst <| findMainEntityExpr ctes currentCte subExpr

and private findMainEntityTreeExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedSelectTreeExpr -> RecursiveValue<ResolvedEntityRef * ResolvedQueryResult[] > = function
    | SSelect sel when not (Array.isEmpty sel.GroupBy) -> raisef ViewResolveException "Queries with GROUP BY cannot use FOR INSERT INTO"
    | SSelect { Results = results; From = Some from } ->
        mapRecursiveValue (fun ref -> (ref, results)) <| findMainEntityFromExpr ctes currentCte from
    | SSelect _ -> raisef ViewResolveException "FROM clause is not found when validating main entity"
    | SValues _ -> raisef ViewResolveException "VALUES clause found when validating main entity"
    | SSetOp setOp ->
        let ret1 = findMainEntityExpr ctes currentCte setOp.A
        let ret2 = findMainEntityExpr ctes currentCte setOp.B
        match (ret1, ret2) with
        | (RValue ((refA, resultsA) as ret), RValue (refB, resultsB)) ->
            if refA <> refB then
                raisef ViewResolveException "Conflicting main entities found in set operation"
            RValue ret
        | (RRecursive cte, RValue ret)
        | (RValue ret, RRecursive cte) ->
            assert (cte = Option.get currentCte)
            RValue ret
        | (RRecursive r1, RRecursive r2) ->
            assert (r1 = r2)
            RRecursive r1

and private findMainEntityExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) (select : ResolvedSelectExpr) : RecursiveValue<ResolvedEntityRef * ResolvedQueryResult[]> =
    let ctes = Map.union ctes (cteBindings select)
    findMainEntityTreeExpr ctes currentCte select.Tree

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private findMainEntity (select : ResolvedSelectExpr) : (ResolvedEntityRef * ResolvedQueryResult[]) =
    let getValue = function
    | RValue (ref, results) -> (ref, results)
    | RRecursive r -> failwithf "Impossible recursion detected in %O" r
    getValue <| findMainEntityExpr Map.empty None select

type private ResolveCTE = EntityName -> QSubqueryFieldsMap

type private Context =
    { ResolveCTE : ResolveCTE
      FieldMaps : FieldMapping list
    }

let private failCte (name : EntityName) =
    raisef ViewResolveException "Table %O not found" name

let private emptyContext =
    { ResolveCTE = failCte
      FieldMaps = []
    }

type private TypeContext =
    { AllowedSubtypes : Set<ResolvedEntityRef>
      Type : ResolvedEntityRef
    }

type private TypeContextsMap = Map<FromFieldKey, TypeContext>

let rec private foldCommonAncestor (layout : ILayoutBits) (baseRef : ResolvedEntityRef) (refA : ResolvedEntityRef) (entityA : IEntityBits) (refB : ResolvedEntityRef) (entityB : IEntityBits) : ResolvedEntityRef * IEntityBits =
    if Map.containsKey refB entityA.Children then
        (refA, entityA)
    else if Map.containsKey refA entityB.Children then
        (refB, entityB)
    else
        let newRefA = Option.get entityA.Parent
        let newEntityA = layout.FindEntity newRefA |> Option.get
        if newRefA = baseRef then
            (newRefA, newEntityA)
        else
            let newRefB = Option.get entityB.Parent
            let newEntityB = layout.FindEntity newRefB |> Option.get
            if newRefB = baseRef then
                (newRefB, newEntityB)
            else
                foldCommonAncestor layout baseRef newRefA newEntityA newRefB newEntityB

let private findCommonAncestor (layout : ILayoutBits) (baseRef : ResolvedEntityRef) (entities : ResolvedEntityRef seq) : (ResolvedEntityRef * IEntityBits) option =
    let foldPair (prev : (ResolvedEntityRef * IEntityBits) option) (currRef : ResolvedEntityRef) =
        let (newRef, newEntity) =
            match prev with
            | None -> (currRef, layout.FindEntity currRef |> Option.get)
            | Some (prevRef, prevEntity) ->
                let currEntity = layout.FindEntity currRef |> Option.get
                foldCommonAncestor layout baseRef prevRef prevEntity currRef currEntity
        if newRef = baseRef then
            None
        else
            Some (Some (newRef, newEntity))
    match Seq.foldOption foldPair None entities with
    | None ->
        let baseEntity = layout.FindEntity baseRef |> Option.get
        Some (baseRef, baseEntity)
    | Some None -> None
    | Some (Some ret) -> Some ret

// Find uppest possible entity in the hierarchy given current type context that has a required field, or None if no such entity exists.
let private findFieldWithContext (layout : ILayoutBits) (typeCtxs : TypeContextsMap) (key : FromFieldKey) (fieldRef : ResolvedFieldRef) : (ResolvedEntityRef * IEntityBits * ResolvedFieldInfo) option =
    // First try to find within the given entity itself.
    let parentEntity = layout.FindEntity fieldRef.Entity |> Option.get
    match parentEntity.FindField fieldRef.Name with
    | Some field -> Some (fieldRef.Entity, parentEntity, field)
    | None ->
        match Map.tryFind key typeCtxs with
        | None -> None
        | Some typeCtx ->
            assert (fieldRef.Entity = typeCtx.Type)
            match findCommonAncestor layout typeCtx.Type typeCtx.AllowedSubtypes with
            | Some (newRef, newEntity) when newRef <> fieldRef.Entity ->
                match newEntity.FindField fieldRef.Name with
                | None -> None
                | Some field ->
                    // Upcast it as much as possible to avoid unnecessary restrictions due to used entities.
                    let inheritedFrom =
                        match field.Field with
                        // These should have been found in parent entity.
                        | RId | RSubEntity -> failwith "Impossible"
                        | RColumnField col -> col.InheritedFrom
                        | RComputedField comp -> comp.InheritedFrom
                    match inheritedFrom with
                    | None -> Some (newRef, newEntity, field)
                    | Some realParentRef ->
                        let realParent = layout.FindEntity realParentRef |> Option.get
                        let realField = realParent.FindField fieldRef.Name |> Option.get
                        Some (realParentRef, realParent, realField)
            | _ -> None

type private OldBoundField =
    | OBField of BoundField
    | OBHeader of BoundFieldHeader

let private resolvePath (layout : ILayoutBits) (typeCtxs : TypeContextsMap) (firstOldBoundField : OldBoundField) (fullPath : FieldName list) : BoundField list =
    let getBoundField : OldBoundField -> BoundField = function
        | OBField bound -> bound
        | OBHeader header ->
            let (entityRef, entity, field) =
                match findFieldWithContext layout typeCtxs header.Key header.Ref with
                | Some ret -> ret
                | None -> raisef ViewResolveException "Field not found: %O" header.Ref
            { Header = { header with Ref = { header.Ref with Entity = entityRef } }
              Field = resolvedFieldToBits field.Field
              Entity = entity
              ForceRename = field.ForceRename
              Name = field.Name
            }

    let rec traverse (boundField : BoundField) : FieldName list -> BoundField list = function
        | [] -> [boundField]
        | (ref :: refs) ->
            match boundField.Field with
            | RColumnField { FieldType = FTReference entityRef } ->
                let refKey =
                    { FromId = boundField.Header.Key.FromId
                      Path = Array.append boundField.Header.Key.Path [|ref|]
                    }
                let refHeader =
                    { Key = refKey
                      Ref = { Entity = entityRef; Name = ref }
                      Immediate = false
                    }
                let nextBoundField = getBoundField (OBHeader refHeader)
                let boundFields = traverse nextBoundField refs
                boundField :: boundFields
            | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

    traverse (getBoundField firstOldBoundField) fullPath

let private boundFieldInfo (typeCtxs : TypeContextsMap) (inner : BoundField) (extras : obj seq) : ObjectMap =
    let commonExtras =
        seq {
            let addSubtypesInfo =
                match inner.Field with
                | RComputedField comp -> comp.IsVirtual
                | RSubEntity -> true
                | _ -> false
            if addSubtypesInfo then
                match Map.tryFind inner.Header.Key typeCtxs with
                | None -> ()
                | Some typeCtx -> yield ({ PossibleSubtypes = typeCtx.AllowedSubtypes } : PossibleSubtypesMeta) :> obj
        }
    ObjectMap.ofSeq (Seq.append commonExtras extras)

let private resolveSubEntity (layout : ILayoutBits) (outerTypeCtxs : TypeContextsMap) (ctx : SubEntityContext) (field : LinkedBoundFieldRef) (subEntityInfo : SubEntityRef) : TypeContextsMap * SubEntityRef =
    let fieldInfo = ObjectMap.findType<FieldMeta> field.Extra
    let boundInfo =
        match fieldInfo.Bound with
        | Some bound -> bound
        | _ -> raisef ViewResolveException "Unbound field in a type assertion"
    let fieldRef =
        if Array.isEmpty field.Ref.Path then
            boundInfo.Ref
        else
            let entityRef = Array.last boundInfo.Path
            let fieldName = Array.last field.Ref.Path
            { Entity = entityRef; Name = fieldName }
    let fields = layout.FindEntity fieldRef.Entity |> Option.get
    match fields.FindField fieldRef.Name with
    | Some { Field = RSubEntity } -> ()
    | _ -> raisef ViewResolveException "Bound field in a type assertion is not a 'sub_entity' field: %O" field
    let subEntityRef = { Schema = Option.defaultValue fieldRef.Entity.Schema subEntityInfo.Ref.Schema; Name = subEntityInfo.Ref.Name }
    let subEntity =
        match layout.FindEntity subEntityRef with
        | None -> raisef ViewResolveException "Couldn't find subentity %O" subEntityInfo.Ref
        | Some r -> r
    let typeCtxKey =
        { FromId = FIEntity fieldInfo.FromEntityId
          Path = field.Ref.Path
        }

    let neededTypes =
        match ctx with
        | SECInheritedFrom ->
            if checkInheritance layout subEntityRef fieldRef.Entity then
                None
            else if checkInheritance layout fieldRef.Entity subEntityRef then
                allPossibleEntities layout subEntityRef |> Seq.map fst |> Set.ofSeq |> Some
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"
        | SECOfType ->
            if subEntity.IsAbstract then
                raisef ViewResolveException "Instances of abstract entity %O do not exist" subEntityRef
            if subEntityRef = fieldRef.Entity then
                None
            else if checkInheritance layout fieldRef.Entity subEntityRef then
                Set.singleton subEntityRef |> Some
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"

    let checkForTypes =
        match neededTypes with
        | None -> None
        | Some needed ->
            match Map.tryFind typeCtxKey outerTypeCtxs with
            | None -> Some needed
            | Some ctx ->
                let otherTypes = Set.difference ctx.AllowedSubtypes needed
                if Set.isEmpty otherTypes then
                    None
                else
                    let possibleTypes = Set.intersect ctx.AllowedSubtypes needed
                    Some possibleTypes

    let innerTypeCtxs =
        match neededTypes with
        | None -> Map.empty
        | Some needed ->
            let ctx =
                { Type = fieldRef.Entity
                  AllowedSubtypes = needed
                }
            Map.singleton typeCtxKey ctx
    let info = { CheckForTypes = checkForTypes } : SubEntityMeta
    let ret = { Ref = relaxEntityRef subEntityRef; Extra = ObjectMap.singleton info } : SubEntityRef
    (innerTypeCtxs, ret)

// Public helper functions.

let replaceEntityRefInExpr (localRef : EntityRef option) : ResolvedFieldExpr -> ResolvedFieldExpr =
    let resolveReference : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
    | { Ref = { Ref = VRColumn col } } as ref -> 
        { ref with Ref = { ref.Ref with Ref = VRColumn { col with Entity = localRef } } }
    | ref -> ref
    let mapper = idFieldExprMapper resolveReference id
    mapFieldExpr mapper

let getFromEntityId = function
    | FIEntity id -> id
    | from -> failwithf "Unexpected non-entity from id: %O" from

let filterCasesWithSubtypes (extra : ObjectMap) (cases : VirtualFieldCase seq) : VirtualFieldCase seq =
    match ObjectMap.tryFindType<PossibleSubtypesMeta> extra with
    | None -> cases
    | Some meta ->
        let filterCase case =
            let possibleCases = Set.intersect meta.PossibleSubtypes case.PossibleEntities
            if Set.isEmpty possibleCases then
                None
            else
                Some { case with PossibleEntities = possibleCases }
        cases |> Seq.mapMaybe filterCase

let computedFieldCases (layout : ILayoutBits) (extra : ObjectMap) (name : FieldName) (cases : VirtualFieldCase seq) : (VirtualFieldCase * ResolvedFieldExpr) seq =
    let filteredCases = filterCasesWithSubtypes extra cases

    let compileCase (case : VirtualFieldCase) =
        let caseEntity = layout.FindEntity case.Ref |> Option.get
        let caseField =
            match caseEntity.FindField name with
            | Some { Field = RComputedField comp } -> comp
            | _ -> failwithf "Unexpected non-column field %O" name
        (case, caseField.Expression)

    Seq.map compileCase filteredCases

type private SelectFlags =
    { OneColumn : bool
      RequireNames : bool
      NoAttributes : bool
    }

type private ReferenceInfo =
    { // `Path` may be there and `OuterField` absent in case of placeholders with paths.
      InnerField : BoundField option
      OuterField : BoundField option
      Ref : LinkedBoundFieldRef
    }

let private viewExprSelectFlags =
    { NoAttributes = false
      OneColumn = false
      RequireNames = true
    }

let private subExprSelectFlags =
    { NoAttributes = true
      OneColumn = true
      RequireNames = false
    }

type private QueryResolver (layout : ILayoutBits, arguments : ResolvedArgumentsMap) =
    let mutable lastFromEntityId = 0
    let nextFromEntityId () =
        let ret = lastFromEntityId
        lastFromEntityId <- lastFromEntityId + 1
        ret

    let notTypeContext (ctx : TypeContext) : TypeContext option =
        let allEntities = allPossibleEntities layout ctx.Type |> Seq.map fst |> Set.ofSeq
        let newAllowed = Set.difference allEntities ctx.AllowedSubtypes
        if Set.count newAllowed = Set.count allEntities then
            None
        else
            Some
                { AllowedSubtypes = newAllowed
                  Type = ctx.Type
                }
    
    let notTypeContexts = Map.mapMaybe (fun name -> notTypeContext)

    let orTypeContext (ctxA : TypeContext) (ctxB : TypeContext) : TypeContext option =
        assert (ctxA.Type = ctxB.Type)
        let allEntities = allPossibleEntities layout ctxA.Type |> Seq.map fst |> Set.ofSeq
        let newAllowed = Set.union ctxA.AllowedSubtypes ctxB.AllowedSubtypes
        if Set.count newAllowed = Set.count allEntities then
            None
        else
            Some
                { AllowedSubtypes = newAllowed
                  Type = ctxA.Type
                }

    let orTypeContexts = Map.intersectWithMaybe (fun name -> orTypeContext)

    let andTypeContext (ctxA : TypeContext) (ctxB : TypeContext) : TypeContext =
        assert (ctxA.Type = ctxB.Type)
        { AllowedSubtypes = Set.intersect ctxA.AllowedSubtypes ctxB.AllowedSubtypes
          Type = ctxA.Type
        }

    let andTypeContexts = Map.unionWith (fun name -> andTypeContext)

    let createFromMapping (fromEntityId : FromEntityId) (ref : ResolvedEntityRef) (pun : (EntityRef option) option) (allowHidden : bool) : FieldMapping =
        let entity =
            match layout.FindEntity ref with
            | Some entity when not entity.IsHidden || allowHidden -> entity
            | _ -> raisef ViewResolveException "Entity not found: %O" ref
        let key =
            { FromId = FIEntity fromEntityId
              Path = [||]
            }

        let makeBoundField (name : FieldName) (field : ResolvedFieldBits) =
            let ref = { Entity = ref; Name = name } : ResolvedFieldRef

            let header =
                { Key = key
                  Ref = ref
                  Immediate = true
                }
            Some
                { Header = header
                  Entity = entity
                  Field = field
                  ForceRename = false
                  Name = name
                }

        let realFields = mapAllFields makeBoundField entity
        let mainFieldInfo = Option.get <| entity.FindFieldBits funMain
        let mainHeader =
            { Key = key
              Ref = { Entity = ref; Name = entity.MainField }
              Immediate = true
            }
        let mainBoundField =
            { Header = mainHeader
              Entity = entity
              Field = mainFieldInfo.Field
              ForceRename = true
              Name = mainFieldInfo.Name
            }
        let fields = Map.add funMain (Some mainBoundField) realFields
        let mappingRef =
            match pun with
            | None -> Some <| relaxEntityRef ref
            | Some punRef -> punRef
        let mapping = fieldsToFieldMapping fromEntityId mappingRef fields
        let extraMapping = typeRestrictedFieldsToFieldMapping layout fromEntityId mappingRef ref (entity.Children |> Map.keys)
        Map.union extraMapping mapping

    let resolveReference (mappings : FieldMapping list) (typeCtxs : TypeContextsMap) (f : LinkedFieldRef) : ReferenceInfo =
        match f.Ref with
        | VRColumn ref ->
            let rec findInMappings = function
                | [] -> raisef ViewResolveException "Unknown reference: %O" ref
                | mapping :: mappings ->
                    match Map.tryFind ref mapping with
                    | Some (FVResolved info) -> info
                    | Some (FVAmbiguous entities) -> raisef ViewResolveException "Ambiguous reference %O. Possible values: %O" ref entities
                    | None -> findInMappings mappings
            let info = findInMappings mappings

            let oldBoundField =
                match info.Mapping with
                | FMBound bound -> Some <| OBField bound
                | FMTypeRestricted header -> Some <| OBHeader header
                | _ when Array.isEmpty f.Path -> None
                | _ -> raisef ViewResolveException "Dereference on an unbound field in %O" f

            let boundFields = Option.map (fun old -> resolvePath layout typeCtxs old (Array.toList f.Path)) oldBoundField

            let fromEntityId =
                match info.Mapping with
                | FMBound bound -> getFromEntityId bound.Header.Key.FromId
                | FMTypeRestricted header -> getFromEntityId header.Key.FromId
                | FMUnbound id -> id

            let (outerBoundField, innerBoundInfo) =
                match boundFields with
                | Some fields ->
                    assert (List.length fields = Array.length f.Path + 1)
                    let (outer, remainingFields) =
                        match fields with
                        | outer :: path -> (outer, path)
                        | _ -> failwith "Impossible"
                    let inner = List.last fields
                    let boundPath =
                        fields
                        |> Seq.skip 1
                        |> Seq.map (fun p -> p.Header.Ref.Entity)
                        |> Seq.toArray
                    let boundInfo =
                        { Ref = outer.Header.Ref
                          Immediate = outer.Header.Immediate
                          Path = boundPath
                        }
                    (Some outer, Some (inner, boundInfo))
                | None -> (None, None)
            let fieldInfo =
                { Bound = Option.map snd innerBoundInfo
                  FromEntityId = fromEntityId
                  ForceSQLName = info.ForceSQLName
                } : FieldMeta
            let extra =
                match innerBoundInfo with
                | Some (inner, _) -> boundFieldInfo typeCtxs inner (Seq.singleton (fieldInfo :> obj))
                | None -> ObjectMap.singleton fieldInfo

            let newRef = { Entity = info.Entity; Name = ref.Name } : FieldRef
            { InnerField = Option.map fst innerBoundInfo
              OuterField = outerBoundField
              Ref = { Ref = { Ref = VRColumn newRef; Path = f.Path }; Extra = extra }
            }
        | VRPlaceholder arg ->
            let argInfo =
                match Map.tryFind arg arguments with
                | None -> raisef ViewResolveException "Unknown argument: %O" arg
                | Some argInfo -> argInfo
            let (innerBoundField, boundInfo) =
                if Array.isEmpty f.Path then
                    (None, ObjectMap.empty)
                else
                    match argInfo.ArgType with
                    | FTReference entityRef ->
                        let (name, remainingPath) =
                            match Array.toList f.Path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "Impossible"
                        let key =
                            { FromId = FIPlaceholder arg
                              Path = [||]
                            }
                        let (entityRef, argEntity, argField) =
                            match findFieldWithContext layout typeCtxs key { Entity = entityRef; Name = name } with
                            | Some ret -> ret
                            | None -> raisef ViewResolveException "Field doesn't exist in %O: %O" entityRef name
                        let argHeader =
                            { Key = key
                              Ref = { Entity = entityRef; Name = name }
                              Immediate = false
                            }
                        let outer : BoundField =
                            { Header = argHeader
                              Entity = argEntity
                              Field = resolvedFieldToBits argField.Field
                              ForceRename = argField.ForceRename
                              Name = argField.Name
                            }
                        let fields = resolvePath layout typeCtxs (OBField outer) remainingPath
                        assert (List.length fields = Array.length f.Path)
                        let inner = List.last fields
                        let boundPath =
                            fields
                                |> Seq.map (fun p -> p.Header.Ref.Entity)
                                |> Seq.toArray
                        let boundInfo =
                            { Path = boundPath
                            } : ReferencePlaceholderMeta
                        let info = boundFieldInfo typeCtxs inner (Seq.singleton (boundInfo :> obj))
                        (Some inner, info)
                    | _ -> raisef ViewResolveException "Argument is not a reference: %O" ref
            { InnerField = innerBoundField
              OuterField = None
              Ref = { Ref = { Ref = VRPlaceholder arg; Path = f.Path }; Extra = boundInfo }
            }

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { Ref = VRPlaceholder name; Path = [||] } ->
                if Map.containsKey name arguments then
                    { Ref = { Ref = VRPlaceholder name; Path = [||] }; Extra = ObjectMap.empty }
                else
                    raisef ViewResolveException "Undefined placeholder: %O" name
            | ref -> raisef ViewResolveException "Invalid reference in LIMIT or OFFSET: %O" ref
        let voidQuery query = raisef ViewResolveException "Forbidden subquery in LIMIT or OFFSET: %O" query
        let voidAggr aggr = raisef ViewResolveException "Forbidden aggregate function in LIMIT or OFFSET"
        let voidSubEntity field subEntity = raisef ViewResolveException "Forbidden type assertion in LIMIT or OFFSET"
        let mapper =
            { idFieldExprMapper resolveReference voidQuery with
                  Aggregate = voidAggr
                  SubEntity = voidSubEntity
            }
        mapFieldExpr mapper expr

    let applyAlias (alias : EntityAlias) (results : QSubqueryFields) : QSubqueryFields =
        checkName alias.Name
        match alias.Fields with
        | None -> results
        | Some fieldNames ->
            fieldNames |> Seq.iter checkName
            try
                fieldNames |> Set.ofSeqUnique |> ignore
            with
                | Failure msg -> raisef ViewResolveException "Clashing names: %s" msg
            renameFields fieldNames results

    let rec resolveResult (flags : SelectFlags) (ctx : Context) : ParsedQueryResult -> ResolvedResultInfo * ResolvedQueryResult = function
        | QRAll alias -> raisef ViewResolveException "Wildcard SELECTs are not yet supported"
        | QRExpr result ->
            let (exprInfo, newResult) = resolveResultColumn flags ctx result
            (exprInfo, QRExpr newResult)


    and resolveResultColumn (flags : SelectFlags) (ctx : Context) (result : ParsedQueryColumnResult) : ResolvedResultInfo * ResolvedQueryColumnResult =
        if not (Map.isEmpty result.Attributes) && flags.NoAttributes then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        let (exprInfo, expr) = resolveResultExpr flags ctx result.Alias result.Result
        let ret =
            { Alias = result.Alias
              Attributes = resolveAttributes ctx result.Attributes
              Result = expr
            } : ResolvedQueryColumnResult
        (exprInfo, ret)

    // Should be in sync with resultField
    and resolveResultExpr (flags : SelectFlags) (ctx : Context) (name : FieldName option) : ParsedFieldExpr -> ResolvedResultInfo * ResolvedFieldExpr = function
        | FERef f ->
            Option.iter checkName name
            let ref = resolveReference ctx.FieldMaps Map.empty f
            let innerField =
                match ref.InnerField with
                | Some field when field.ForceRename && Option.isNone name && flags.RequireNames -> raisef ViewResolveException "Field should be explicitly named in result expression: %s" (f.ToFunQLString())
                | Some field ->
                    match field.Field with
                    // We erase field information for computed fields from results, as they would be expanded at this point.
                    | RComputedField comp -> None
                    | _ ->
                        let header = { field.Header with Immediate = false }
                        let ret =
                            { field with
                                Header = header
                                ForceRename = false
                            }
                        Some ret
                | None -> None
            let info =
                { InnerField = innerField
                  HasAggregates = false
                }
            (info, FERef ref.Ref)
        | e ->
            match name with
            | Some n -> checkName n
            | None when flags.RequireNames -> raisef ViewResolveException "Unnamed results are allowed only inside expression queries"
            | None -> ()
            let (exprInfo, expr) = resolveFieldExpr ctx e
            let info =
                { InnerField = None
                  HasAggregates = exprInfo.HasAggregates
                }
            (info, expr)

    and resolveAttributes (ctx : Context) (attributes : ParsedAttributeMap) : ResolvedAttributeMap =
        Map.map (fun name expr -> resolveNonaggrFieldExpr ctx expr) attributes

    and resolveNonaggrFieldExpr (ctx : Context) (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let (info, res) = resolveFieldExpr ctx expr
        if info.HasAggregates then
            raisef ViewResolveException "Aggregate functions are not allowed here"
        res

    and resolveFieldExpr (ctx : Context) (expr : ParsedFieldExpr) : ResolvedExprInfo * ResolvedFieldExpr =
        let mutable isLocal = true
        let mutable hasAggregates = false

        let resolveExprReference typeCtxs col =
            let ref = resolveReference ctx.FieldMaps typeCtxs col

            if not <| Array.isEmpty col.Path then
                isLocal <- false
            else
                match ref.OuterField with
                | Some ({ Field = RComputedField _ } as outer) ->
                    match outer.Entity.FindField outer.Header.Ref.Name with
                    | Some { Field = RComputedField comp } ->
                        if not comp.IsLocal then
                            isLocal <- false
                    | _ -> failwith "Impossible"
                | _ -> ()

            ref.Ref

        let resolveQuery query =
            // FIXME: we don't support propagating field names into subqueries yet.
            let ctx = { ctx with FieldMaps = [] }
            let (_, res) = resolveSelectExpr ctx subExprSelectFlags query
            isLocal <- false
            res

        let rec traverse outerTypeCtxs = function
            | FEValue value -> (Map.empty, FEValue value)
            | FERef r -> (Map.empty, FERef (resolveExprReference outerTypeCtxs r))
            | FENot e ->
                let (innerTypeCtxs, e) = traverse outerTypeCtxs e
                (notTypeContexts innerTypeCtxs, FENot e)
            | FEAnd (a, b) ->
                let (innerTypeCtxs1, newA) = traverse outerTypeCtxs a
                let (innerTypeCtxs2, newB) = traverse (andTypeContexts outerTypeCtxs innerTypeCtxs1) b
                (andTypeContexts innerTypeCtxs1 innerTypeCtxs2, FEAnd (newA, newB))
            | FEOr (a, b) ->
                let (typeCtxsA, newA) = traverse outerTypeCtxs a
                let (typeCtxsB, newB) = traverse outerTypeCtxs b
                (orTypeContexts typeCtxsA typeCtxsB, FEOr (newA, newB))
            | FECase (es, els) ->
                let mutable curTypeCtxs = Map.empty
                let applyOne (cond, e) =
                    let mergedTypeCtxs = andTypeContexts outerTypeCtxs curTypeCtxs
                    let (condTypeCtxs, newCond) = traverse mergedTypeCtxs cond
                    let (typeCtxs_, newE) = traverse (andTypeContexts mergedTypeCtxs condTypeCtxs) e
                    curTypeCtxs <- andTypeContexts curTypeCtxs (notTypeContexts condTypeCtxs)
                    (newCond, newE)
                let newEs = Array.map applyOne es
                let applyElse e =
                    let mergedTypeCtxs = andTypeContexts outerTypeCtxs curTypeCtxs
                    let (typeCtxs_, newE) = traverse mergedTypeCtxs e
                    newE
                let newEls = Option.map applyElse els
                (Map.empty, FECase (newEs, newEls))
            | FEInheritedFrom (f, nam) ->
                let newF = resolveExprReference outerTypeCtxs f
                let (inerTypeCtxs, newNam) = resolveSubEntity layout outerTypeCtxs SECInheritedFrom (resolveExprReference outerTypeCtxs f) nam
                (inerTypeCtxs, FEInheritedFrom (newF, newNam))
            | FEOfType (f, nam) ->
                let newF = resolveExprReference outerTypeCtxs f
                let (inerTypeCtxs, newNam) = resolveSubEntity layout outerTypeCtxs SECOfType (resolveExprReference outerTypeCtxs f) nam
                (inerTypeCtxs, FEOfType (newF, newNam))
            | FEDistinct (a, b) ->
                let (typeCtxs_, newA) = traverse outerTypeCtxs a
                let (typeCtxs_, newB) = traverse outerTypeCtxs b
                (Map.empty, FEDistinct (newA, newB))
            | FENotDistinct (a, b) ->
                let (typeCtxs_, newA) = traverse outerTypeCtxs a
                let (typeCtxs_, newB) = traverse outerTypeCtxs b
                (Map.empty, FENotDistinct (newA, newB))
            | FEBinaryOp (a, op, b) ->
                let (typeCtxs_, newA) = traverse outerTypeCtxs a
                let (typeCtxs_, newB) = traverse outerTypeCtxs b
                (Map.empty, FEBinaryOp (newA, op, newB))
            | FESimilarTo (e, pat) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let (typeCtxs_, newPat) = traverse outerTypeCtxs pat
                (Map.empty, FESimilarTo (newE, newPat))
            | FENotSimilarTo (e, pat) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let (typeCtxs_, newPat) = traverse outerTypeCtxs pat
                (Map.empty, FENotSimilarTo (newE, newPat))
            | FEIn (e, vals) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (Map.empty, FEIn (newE, newVals))
            | FENotIn (e, vals) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (Map.empty, FENotIn (newE, newVals))
            | FEInQuery (e, query) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let newQuery = resolveQuery query
                (Map.empty, FEInQuery (newE, newQuery))
            | FENotInQuery (e, query) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let newQuery = resolveQuery query
                (Map.empty, FENotInQuery (newE, newQuery))
            | FEAny (e, op, arr) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let (typeCtxs_, newArr) = traverse outerTypeCtxs arr
                (Map.empty, FEAny (newE, op, newArr))
            | FEAll (e, op, arr) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                let (typeCtxs_, newArr) = traverse outerTypeCtxs arr
                (Map.empty, FEAll (newE, op, newArr))
            | FECast (e, typ) ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                (Map.empty, FECast (newE, typ))
            | FEIsNull e ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                (Map.empty, FEIsNull newE)
            | FEIsNotNull e ->
                let (typeCtxs_, newE) = traverse outerTypeCtxs e
                (Map.empty, FEIsNotNull newE)
            | FEJsonArray vals ->
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (Map.empty, FEJsonArray newVals)
            | FEJsonObject obj ->
                let newObj = Map.map (fun name -> traverse outerTypeCtxs >> snd) obj
                (Map.empty, FEJsonObject newObj)
            | FEFunc (name, args) ->
                let newArgs = Array.map (traverse outerTypeCtxs >> snd) args
                (Map.empty, FEFunc (name, newArgs))
            | FEAggFunc (name, args) ->
                hasAggregates <- true
                let newArgs = mapAggExpr (traverse outerTypeCtxs >> snd) args
                (Map.empty, FEAggFunc (name, newArgs))
            | FESubquery query ->
                let newQuery = resolveQuery query
                (Map.empty, FESubquery newQuery)

        let (_, ret) = traverse Map.empty expr
        let info =
            { IsLocal = isLocal
              HasAggregates = hasAggregates
            }
        (info, ret)

    and resolveOrderLimitClause (ctx : Context) (limits : ParsedOrderLimitClause) : bool * ResolvedOrderLimitClause =
        let mutable isLocal = true
        let resolveOrderBy (ord, expr) =
            let (info, ret) = resolveFieldExpr ctx expr
            if info.HasAggregates then
                raisef ViewResolveException "Aggregates are not allowed here"
            if not info.IsLocal then
                isLocal <- false
            (ord, ret)
        let ret =
            { OrderBy = Array.map resolveOrderBy limits.OrderBy
              Limit = Option.map resolveLimitFieldExpr limits.Limit
              Offset = Option.map resolveLimitFieldExpr limits.Offset
            }
        (isLocal, ret)

    and resolveCommonTableExpr (ctx : Context) (allowRecursive : bool) (name : EntityName) (cte : ParsedCommonTableExpr) : QSubqueryFieldsMap * ResolvedCommonTableExpr =
        let (ctx, newCtes) =
            match cte.Expr.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (resolveCte, newCtes) = resolveCommonTableExprs ctx ctes
                let ctx = { ctx with ResolveCTE = resolveCte }
                (ctx, Some newCtes)

        let cteFlags =
            { OneColumn = false
              RequireNames = Option.isNone cte.Fields
              NoAttributes = false
            }

        let (results, tree) =
            match cte.Expr.Tree with
            | SSetOp setOp when allowRecursive ->
                // We trait left side of outer UNION as a non-recursive side.
                let (results1, a') = resolveSelectExpr ctx cteFlags setOp.A
                let recResults =
                    match cte.Fields with
                    | None -> results1
                    | Some names -> renameFields names results1
                let fieldsMap = getFieldsMap recResults
                let newResolveCte (currName : EntityName) =
                    if currName = name then
                        fieldsMap
                    else
                        ctx.ResolveCTE currName
                let newCtx = { ctx with ResolveCTE = newResolveCte }
                let (results2, b') = resolveSelectExpr newCtx { cteFlags with RequireNames = false } setOp.B
                finishResolveSetOpExpr ctx setOp results1 a' results2 b' cteFlags
            | _ -> resolveSelectTreeExpr ctx cteFlags cte.Expr.Tree

        let results =
            match cte.Fields with
            | None -> results
            | Some names -> renameFields names results

        let newSelect =
            { CTEs = newCtes
              Tree = tree
              Extra = ObjectMap.empty
            }
        let info =
            { MainEntity = false
            } : ResolvedCommonTableExprTempMeta
        let newCte =
            { Fields = cte.Fields
              Expr = newSelect
              Extra = ObjectMap.singleton info
            }
        (getFieldsMap results, newCte)

    and resolveCommonTableExprs (ctx : Context) (ctes : ParsedCommonTableExprs) : ResolveCTE * ResolvedCommonTableExprs =
        let resolveCte = ctx.ResolveCTE
        let exprsMap =
            try
                Map.ofSeqUnique ctes.Exprs
            with
            | Failure msg -> raisef ViewResolveException "Clashing names in WITH binding: %s" msg
        let mutable resultsMap : Map<EntityName, QSubqueryFieldsMap * ResolvedCommonTableExpr> = Map.empty
        let mutable resolved : (EntityName * ResolvedCommonTableExpr) list = []
        let resolveOne (name : EntityName, cte : ParsedCommonTableExpr) =
            if not (Map.containsKey name resultsMap) then
                let mutable processing = Set.singleton name

                let rec tmpResolveCte (currName : EntityName) =
                    match Map.tryFind currName resultsMap with
                    | None when ctes.Recursive ->
                        match Map.tryFind currName exprsMap with
                        | None -> resolveCte currName
                        | Some expr when name = currName ->
                            raisef ViewResolveException "Invalid recursive statement"
                        | Some expr when Set.contains currName processing ->
                            raisef ViewResolveException "Mutual recursion in WITH statement"
                        | Some cte ->
                            processing <- Set.add currName processing
                            let (results, newCte) = resolveCommonTableExpr tmpCtx true currName cte
                            resultsMap <- Map.add currName (results, newCte) resultsMap
                            resolved <- (currName, newCte) :: resolved
                            results
                    | None -> resolveCte currName
                    | Some (results, cte) -> results
                and tmpCtx = { ctx with ResolveCTE = tmpResolveCte }

                let (results, newCte) = resolveCommonTableExpr tmpCtx ctes.Recursive name cte
                resultsMap <- Map.add name (results, newCte) resultsMap
                resolved <- (name, newCte) :: resolved

        Array.iter resolveOne ctes.Exprs

        let newResolveCte (name : EntityName) =
            match Map.tryFind name resultsMap with
            | None -> resolveCte name
            | Some (results, cte) -> results
        let info =
            { Bindings = Map.map (fun name (results, cte) -> cte) resultsMap
            } : ResolvedCommonTableExprsTempMeta
        let ret =
            { Recursive = ctes.Recursive
              Exprs = resolved |> Seq.rev |> Array.ofSeq
              Extra = ObjectMap.singleton info
            }
        (newResolveCte, ret)

    and resolveSelectExpr (ctx : Context) (flags : SelectFlags) (select : ParsedSelectExpr) : QSubqueryFields * ResolvedSelectExpr =
        let (ctx, newCtes) =
            match select.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (resolveCte, newCtes) = resolveCommonTableExprs ctx ctes
                let ctx = { ctx with ResolveCTE = resolveCte }
                (ctx, Some newCtes)
        let (results, tree) = resolveSelectTreeExpr ctx flags select.Tree
        let newSelect =
            { CTEs = newCtes
              Tree = tree
              Extra = ObjectMap.empty
            }
        (results, newSelect)

    and finishResolveSetOpExpr (ctx : Context) (setOp : ParsedSetOperationExpr) (results1 : QSubqueryFields) (a : ResolvedSelectExpr) (results2 : QSubqueryFields) (b : ResolvedSelectExpr) (flags : SelectFlags) =
        if Array.length results1 <> Array.length results2 then
            raisef ViewResolveException "Different number of columns in a set operation expression"
        if flags.RequireNames then
            for (name, info) in results1 do
                if Option.isNone name then
                    raisef ViewResolveException "Name is required for column"
        let newResults = Array.map2 (fun (name1, info1) (name2, info2) -> (name1, unionBoundFieldInfo info1 info2)) results1 results2
        // Bogus entity id for incompatible fields that are merged together.
        let setOpEntityId = nextFromEntityId ()
        let mapField (fieldName, fieldInfo) =
            let ref = { Entity = None; Name = fieldName } : FieldRef
            let mapping =
                match fieldInfo with
                | None -> FMUnbound setOpEntityId
                | Some bound -> FMBound bound
            let info =
                { Entity = None
                  Mapping = mapping
                  ForceSQLName = None
                }
            (ref, FVResolved info)
        let orderLimitMapping =
            getFieldsMap newResults
            |> Map.toSeq
            |> Seq.map mapField
            |> Map.ofSeq
        let orderLimitCtx = { ctx with FieldMaps = orderLimitMapping :: ctx.FieldMaps }
        let (limitsAreLocal, resolvedLimits) = resolveOrderLimitClause orderLimitCtx setOp.OrderLimit
        if not limitsAreLocal then
            raisef ViewResolveException "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.OrderBy
        let ret =
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = a
              B = b
              OrderLimit = resolvedLimits
            }
        (newResults, SSetOp ret)

    and resolveSelectTreeExpr (ctx : Context) (flags : SelectFlags) : ParsedSelectTreeExpr -> QSubqueryFields * ResolvedSelectTreeExpr = function
        | SSelect query ->
            let (results, res) = resolveSingleSelectExpr ctx flags query
            (results, SSelect res)
        | SValues values ->
            if flags.RequireNames then
                raisef ViewResolveException "Column aliases are required for this VALUES entry"
            let valuesLength = Array.length values.[0]
            let mapEntry entry =
                if Array.length entry <> valuesLength then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map (resolveFieldExpr ctx >> snd)
            let newValues = values |> Array.map mapEntry
            let fields = Seq.init valuesLength (fun i -> (None, None)) |> Array.ofSeq
            (fields, SValues newValues)
        | SSetOp setOp ->
            let (results1, a') = resolveSelectExpr ctx flags setOp.A
            let (results2, b') = resolveSelectExpr ctx { flags with RequireNames = false } setOp.B
            finishResolveSetOpExpr ctx setOp results1 a' results2 b' flags

    and resolveSingleSelectExpr (ctx : Context) (flags : SelectFlags) (query : ParsedSingleSelectExpr) : QSubqueryFields * ResolvedSingleSelectExpr =
        if flags.NoAttributes && not (Map.isEmpty query.Attributes) then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        if flags.OneColumn && Array.length query.Results <> 1 then
            raisef ViewResolveException "Expression queries must have only one resulting column"
        let (ctx, qFrom) =
            match query.From with
            | None -> (ctx, None)
            | Some from ->
                let (newMapping, _, res) = resolveFromExpr ctx flags from
                let ctx = { ctx with FieldMaps = newMapping :: ctx.FieldMaps }
                (ctx, Some res)
        let qWhere = Option.map (resolveNonaggrFieldExpr ctx) query.Where
        let qGroupBy = Array.map (resolveNonaggrFieldExpr ctx) query.GroupBy
        let rawResults = Array.map (resolveResult flags ctx) query.Results
        let hasAggregates = Array.exists (fun (info : ResolvedResultInfo, expr) -> info.HasAggregates) rawResults || not (Array.isEmpty qGroupBy)
        let results = Array.map snd rawResults
        let getFields : ResolvedResultInfo * ResolvedQueryResult -> FieldName option * BoundField option = function
            | (_, QRAll alias) -> failwith "Impossible QRAll"
            | (info, QRExpr result) -> (result.TryToName (), if hasAggregates then None else info.InnerField)
        let newFields = rawResults |> Array.map getFields
        try
            newFields |> Seq.mapMaybe fst |> Set.ofSeqUnique |> ignore
        with
            | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

        let extra =
            { HasAggregates = hasAggregates
            } : ResolvedSingleSelectMeta

        let newQuery = {
            Attributes = resolveAttributes ctx query.Attributes
            From = qFrom
            Where = qWhere
            GroupBy = qGroupBy
            Results = results
            OrderLimit = snd <| resolveOrderLimitClause ctx query.OrderLimit
            Extra = ObjectMap.singleton extra
        }
        (newFields, newQuery)

    // Set<EntityName> here is used to check uniqueness of puns.
    and resolveFromExpr (ctx : Context) (flags : SelectFlags) : ParsedFromExpr -> FieldMapping * Set<EntityRef> * ResolvedFromExpr = function
        | FEntity (pun, ({ Schema = Some schemaName; Name = entityName } as ref)) ->
            let resRef = { Schema = schemaName; Name = entityName }
            let fromEntityId = nextFromEntityId ()
            let punRef =
                match pun with
                | None -> None
                | Some punName -> Some ({ Schema = None; Name = punName } : EntityRef)
            let mapping = createFromMapping fromEntityId resRef (Option.map Some punRef) false
            let mappingRef = Option.defaultValue ref punRef
            (mapping, Set.singleton mappingRef, FEntity (pun, ref))
        | FEntity (pun, ({ Schema = None; Name = entityName } as ref)) ->
            let fields = ctx.ResolveCTE entityName
            let newName = Option.defaultValue entityName pun
            let mappingRef = { Schema = None; Name = newName } : EntityRef
            let fromEntityId = nextFromEntityId ()
            let mapping = fieldsToFieldMapping fromEntityId (Some mappingRef) fields
            (mapping, Set.singleton mappingRef, FEntity (pun, ref))
        | FJoin join ->
            let (newMapping1, namesA, newA) = resolveFromExpr ctx flags join.A
            let (newMapping2, namesB, newB) = resolveFromExpr ctx flags join.B

            let names =
                try
                    Set.unionUnique namesA namesB
                with
                | Failure msg -> raisef ViewResolveException "Clashing entity names in a join: %s" msg
            let newMapping = Map.unionWith (fun _ -> unionFieldMappingValue) newMapping1 newMapping2
            let newCtx = { ctx with FieldMaps = newMapping :: ctx.FieldMaps }

            let (info, newFieldExpr) = resolveFieldExpr newCtx join.Condition
            if not info.IsLocal then
                raisef ViewResolveException "Cannot use dereferences in join expressions: %O" join.Condition
            if info.HasAggregates then
                raisef ViewResolveException "Cannot use aggregate functions in join expression"
            let ret =
                { Type = join.Type
                  A = newA
                  B = newB
                  Condition = newFieldExpr
                }
            (newMapping, names, FJoin ret)
        | FSubExpr (alias, q) ->
            let (info, newQ) = resolveSelectExpr ctx { flags with RequireNames = Option.isNone alias.Fields; OneColumn = false } q
            let info = applyAlias alias info
            let fields = getFieldsMap info
            let mappingRef = { Schema = None; Name = alias.Name } : EntityRef
            let fromEntityId = nextFromEntityId ()
            let mapping = fieldsToFieldMapping fromEntityId (Some mappingRef) fields
            (mapping, Set.singleton mappingRef, FSubExpr (alias, newQ))

    let resolveMainEntity (fields : QSubqueryFieldsMap) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity =
        let ref = resolveEntityRef main.Entity
        let entity =
            match layout.FindEntity ref with
            | None -> raisef ViewResolveException "Entity not found: %O" main.Entity
            | Some e -> e :?> ResolvedEntity
        if entity.IsAbstract then
            raisef ViewResolveException "Entity is abstract: %O" main.Entity
        let (foundRef, columns) = findMainEntity query
        if foundRef <> ref then
            raisef ViewResolveException "Cannot map main entity to the expression: %O, possible value: %O" ref foundRef

        let getField : ResolvedQueryResult -> (FieldName * FieldName) option = function
            | QRAll _ -> failwith "Impossible QRAll"
            | QRExpr result ->
                let name = result.TryToName () |> Option.get
                match Map.tryFind name fields with
                | Some (Some { Header = header; Field = RColumnField _ }) when header.Ref.Entity = ref -> Some (name, header.Ref.Name)
                | _ -> None
        let mappedResults = columns |> Seq.mapMaybe getField |> Map.ofSeqUnique

        let checkField fieldName (field : ResolvedColumnField) =
            if Option.isNone field.DefaultValue && not field.IsNullable then
                if not (Map.containsKey fieldName mappedResults) then
                    raisef ViewResolveException "Required inserted entity field is not in the view expression: %O" fieldName
        entity.ColumnFields |> Map.iter checkField

        { Entity = ref
          FieldsToColumns = Map.reverse mappedResults
          ColumnsToFields = mappedResults
        }

    member this.ResolveSelectExpr flags select = resolveSelectExpr emptyContext flags select
    member this.ResolveMainEntity fields select main = resolveMainEntity fields select main

    member this.ResolveSingleFieldExpr (fromEntityId : FromEntityId) (fromMapping : SingleFromMapping) expr =
        let mapping =
            match fromMapping with
            | SFEntity ref -> createFromMapping fromEntityId ref None true
            | SFCustom custom -> customToFieldMapping layout fromEntityId custom
        let context =
            { ResolveCTE = failCte
              FieldMaps = [mapping]
            }
        resolveFieldExpr context expr

// Remove and replace unneeded Extra fields.
let rec private relabelSelectExpr (select : ResolvedSelectExpr) : ResolvedSelectExpr =
    { CTEs = Option.map relabelCommonTableExprs select.CTEs
      Tree = relabelSelectTreeExpr select.Tree
      Extra = select.Extra
    }

and private relabelCommonTableExprs (ctes : ResolvedCommonTableExprs) : ResolvedCommonTableExprs =
    { Recursive = ctes.Recursive
      Exprs = Array.map (fun (name, cte) -> (name, relabelCommonTableExpr cte)) ctes.Exprs
      Extra = ObjectMap.empty
    }

and private relabelCommonTableExpr (cte : ResolvedCommonTableExpr) : ResolvedCommonTableExpr =
    let extra = ObjectMap.findType<ResolvedCommonTableExprTempMeta> cte.Extra
    { Fields = cte.Fields
      Expr = relabelSelectExpr cte.Expr
      Extra = ObjectMap.singleton ({ MainEntity = extra.MainEntity } : ResolvedCommonTableExprInfo)
    }

and private relabelSelectTreeExpr : ResolvedSelectTreeExpr -> ResolvedSelectTreeExpr = function
    | SSelect sel -> SSelect (relabelSingleSelectExpr sel)
    | SValues values -> SValues (Array.map (Array.map relabelFieldExpr) values)
    | SSetOp setOp ->
        SSetOp
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = relabelSelectExpr setOp.A
              B = relabelSelectExpr setOp.B
              OrderLimit = relabelOrderLimitClause setOp.OrderLimit
            }

and private relabelSingleSelectExpr (select : ResolvedSingleSelectExpr) : ResolvedSingleSelectExpr =
    { Attributes = Map.map (fun name -> relabelFieldExpr) select.Attributes
      Results = Array.map relabelQueryResult select.Results
      From  = Option.map relabelFromExpr select.From
      Where = Option.map relabelFieldExpr select.Where
      GroupBy = Array.map relabelFieldExpr select.GroupBy
      OrderLimit = relabelOrderLimitClause select.OrderLimit
      Extra = select.Extra
    }

and private relabelFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
    | FEntity (pun, ref) -> FEntity (pun, ref)
    | FJoin join ->
        FJoin
            { Type = join.Type
              A = relabelFromExpr join.A
              B = relabelFromExpr join.B
              Condition = relabelFieldExpr join.Condition
            }
    | FSubExpr (name, subExpr) -> FSubExpr (name, relabelSelectExpr subExpr)

and private relabelFieldExpr (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
    let mapper = idFieldExprMapper id relabelSelectExpr
    mapFieldExpr mapper expr

and private relabelOrderLimitClause (clause : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
    { Offset = Option.map relabelFieldExpr clause.Offset
      Limit = Option.map relabelFieldExpr clause.Limit
      OrderBy = Array.map (fun (order, expr) -> (order, relabelFieldExpr expr)) clause.OrderBy
    }

and private relabelQueryResult : ResolvedQueryResult -> ResolvedQueryResult = function
    | QRAll alias -> QRAll alias
    | QRExpr result -> QRExpr <| relabelQueryColumnResult result

and private relabelQueryColumnResult (result : ResolvedQueryColumnResult) : ResolvedQueryColumnResult =
    { Attributes = Map.map (fun name -> relabelFieldExpr) result.Attributes
      Result = relabelFieldExpr result.Result
      Alias = result.Alias
    }

let private buildAllArguments (layout : ILayoutBits) (rawArguments : ParsedArgumentsMap) : ResolvedArgumentsMap * ResolvedArgumentsMap =
    let arguments = rawArguments |> Map.map (fun name -> resolveArgument layout)
    let localArguments = Map.mapKeys PLocal arguments
    let allArguments = Map.union localArguments globalArgumentsMap
    (localArguments, allArguments)

let resolveSelectExpr (layout : ILayoutBits) (arguments : ParsedArgumentsMap) (select : ParsedSelectExpr) : ResolvedArgumentsMap * ResolvedSelectExpr =
    let (localArguments, allArguments) = buildAllArguments layout arguments
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr subExprSelectFlags select
    (localArguments, relabelSelectExpr qQuery)

let resolveSingleFieldExpr (layout : ILayoutBits) (arguments : ParsedArgumentsMap) (fromEntityId : FromEntityId) (fromMapping : SingleFromMapping) (expr : ParsedFieldExpr) : ResolvedArgumentsMap * ResolvedFieldExpr =
    let (localArguments, allArguments) = buildAllArguments layout arguments
    let qualifier = QueryResolver (layout, allArguments)
    let (info, qExpr) = qualifier.ResolveSingleFieldExpr fromEntityId fromMapping expr
    (localArguments, relabelFieldExpr qExpr)

let resolveViewExpr (layout : ILayoutBits) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let (localArguments, allArguments) = buildAllArguments layout viewExpr.Arguments
    let qualifier = QueryResolver (layout, allArguments)
    let (results, qQuery) = qualifier.ResolveSelectExpr viewExprSelectFlags viewExpr.Select
    let mainEntity = Option.map (qualifier.ResolveMainEntity (getFieldsMap results) qQuery) viewExpr.MainEntity
    { Arguments = localArguments
      Select = relabelSelectExpr qQuery
      MainEntity = mainEntity
    }