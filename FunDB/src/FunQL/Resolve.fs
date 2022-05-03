module FunWithFlags.FunDB.FunQL.Resolve

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Typecheck

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.
// ...that is, except checking references to other views.

type ViewResolveException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ViewResolveException (message, innerException, isUserException innerException)

    new (message : string) = ViewResolveException (message, null, true)

// Unique id of this FROM. Used to distinguish between same entities; fields with same id correspond to same rows.
type FromEntityId = int
type FromId =
    | FIEntity of FromEntityId
    | FIArgument of ArgumentRef

type FromFieldKey =
    { FromId : FromId
      Path : FieldName list
    }

type private ResolvedExprInfo =
    { Flags : ResolvedExprFlags
      ExternalEntities : Set<FromEntityId>
    }

let private emptyResolvedExprInfo : ResolvedExprInfo =
    { Flags = emptyResolvedExprFlags
      ExternalEntities = Set.empty
    }

let private unionResolvedExprInfo (a : ResolvedExprInfo) (b : ResolvedExprInfo) =
    { Flags = unionResolvedExprFlags a.Flags b.Flags
      ExternalEntities = Set.union a.ExternalEntities b.ExternalEntities
    }

let private fieldResolvedExprInfo : ResolvedExprInfo =
    { emptyResolvedExprInfo with Flags = fieldResolvedExprFlags }

[<NoEquality; NoComparison>]
type private BoundFieldHeader =
    { ParentEntity : ResolvedEntityRef // Parent entity which may or may not contain field `Name`. Headers are refined with type contexts, yielding `BoundField`s.
      Name : FieldName
      Key : FromFieldKey
      // Means that field is selected directly from an entity and not from a subexpression.
      Immediate : bool
      IsInner : bool
      // Arrows are not allowed in subexpressions -- we use this flag to track this.
    }

[<NoEquality; NoComparison>]
type private BoundField =
    { Header : BoundFieldHeader
      Ref : ResolvedFieldRef // Real field ref.
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

type private SubqueryExprInfo =
    { HasArguments : bool
      HasFetches : bool
      ExternalEntities : Set<FromEntityId>
    }

let private emptySubqueryExprInfo : SubqueryExprInfo =
    { HasArguments = false
      HasFetches = false
      ExternalEntities = Set.empty
    }

let private unionSubqueryExprInfo (a : SubqueryExprInfo) (b : SubqueryExprInfo) : SubqueryExprInfo =
    { HasArguments = a.HasArguments || b.HasArguments
      HasFetches = a.HasFetches || b.HasFetches
      ExternalEntities = Set.union a.ExternalEntities b.ExternalEntities
    }

let private resolvedToSubqueryExprInfo (info : ResolvedExprInfo) : SubqueryExprInfo =
    { HasArguments = info.Flags.HasArguments
      HasFetches = info.Flags.HasFetches
      ExternalEntities = info.ExternalEntities
    }

type private SubqueryInfo =
    { Fields : QSubqueryFields
      ExprInfo : SubqueryExprInfo
    }

type private ResolvedFieldMapping =
    // Means that field exists in one of entity's children but is inaccessible without type narrowing.
    | FMTypeRestricted of BoundFieldHeader
    | FMBound of BoundField
    | FMUnbound

type private FieldMappingInfo =
    { Entity : EntityRef option
      EntityId : FromEntityId
      Mapping : ResolvedFieldMapping
      // Used for injected expressions which need to use existing column names.
      // For example, chunk WHERE expressions use column names from original user view,
      // but need to use SQL column names from it.
      ForceSQLName : SQL.ColumnName option
    }

type private NameMappingValue<'k, 'v> when 'k : comparison =
    | FVAmbiguous of Set<'k>
    | FVResolved of 'v

type private NameMapping<'k, 'v> when 'k : comparison = Map<'k, NameMappingValue<'k, 'v>>
type private FieldMapping = NameMapping<FieldRef, FieldMappingInfo>

let private getFromEntityId = function
    | FIEntity id -> id
    | from -> failwithf "Unexpected non-entity from id: %O" from

// None in EntityRef is used only for offset/limit expressions in set operations.
let private getAmbiguousMapping k = function
    | FVAmbiguous set -> set
    | FVResolved info -> Set.singleton k

let private unionNameMappingValue (k : 'k) (a : NameMappingValue<'k, 'v>) (b : NameMappingValue<'k, 'v>) : NameMappingValue<'k, 'v> =
    let setA = getAmbiguousMapping k a
    let setB = getAmbiguousMapping k b
    FVAmbiguous <| Set.union setA setB

let private unionNameMapping (a : NameMapping<'k, 'v>) (b : NameMapping<'k, 'v>) : NameMapping<'k, 'v> =
    Map.unionWithKey unionNameMappingValue a b

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
            | None -> FMUnbound
            | Some bound -> FMBound bound
        let info =
            { Entity = maybeEntityRef
              EntityId = fromEntityId
              Mapping = mapping
              ForceSQLName = None
            }
        let value = FVResolved info
        explodeFieldRef { Entity = maybeEntityRef; Name = fieldName } |> Seq.map (fun x -> (x, value))
    fields |> Map.toSeq |> Seq.collect explodeVariations |> Map.ofSeq

let private typeRestrictedFieldsToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (isInner : bool) (maybeEntityRef : EntityRef option) (parentRef : ResolvedEntityRef) (children : ResolvedEntityRef seq) : FieldMapping =
    let filterField (name : FieldName, field : ResolvedFieldBits) =
        match field with
        | RColumnField { InheritedFrom = None } -> Some name
        | RComputedField f when Option.isNone f.InheritedFrom ->
            // Filter out children virtual fields.
            match f.Virtual with
            | None -> Some name
            | Some v when Option.isNone v.InheritedFrom -> Some name
            | _ -> None
        | _ -> None
    let mapEntity (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let expandName name : (FieldRef * BoundFieldHeader) seq =
            let key =
                { FromId = FIEntity fromEntityId
                  Path = []
                }
            let header =
                { ParentEntity = parentRef
                  Name = name
                  Key = key
                  Immediate = true
                  IsInner = isInner
                }
            explodeResolvedFieldRef { Entity = parentRef; Name = name } |> Seq.map (fun x -> (x, header))
        let headerToResolved header =
            let info =
                { Entity = maybeEntityRef
                  EntityId = fromEntityId
                  Mapping = FMTypeRestricted header
                  ForceSQLName = None
                }
            FVResolved info
        entity.Fields |> Seq.mapMaybe filterField |> Seq.collect expandName |> Seq.map (fun (ref, header) -> (ref, headerToResolved header))
    children |> Seq.collect mapEntity |> Map.ofSeq

let private customToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (isInner : bool) (mapping : CustomFromMapping) : FieldMapping =
    let makeBoundField (boundRef : ResolvedFieldRef) =
        let entity = layout.FindEntity boundRef.Entity |> Option.get
        let field = entity.FindField boundRef.Name |> Option.get
        let key =
            { FromId = FIEntity fromEntityId
              Path = []
            }
        let header =
            { ParentEntity = boundRef.Entity
              Name = boundRef.Name
              Key = key
              Immediate = true
              IsInner = isInner
            }
        { Header = header
          Ref = boundRef
          Entity = entity
          Field = resolvedFieldToBits field.Field
          ForceRename = field.ForceRename
          Name = field.Name
        }
    let mapField (fieldRef : FieldRef, info : CustomFromField) =
        let fieldMapping =
            match info.Bound with
            | None -> FMUnbound
            | Some field -> FMBound <| makeBoundField field
        let info =
            { Entity = fieldRef.Entity
              EntityId = fromEntityId
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

let private renameFieldsInSubquery (names : FieldName[]) (info : SubqueryInfo) : SubqueryInfo =
    { info with Fields = renameFields names info.Fields }

let private getFieldsMap (fields : QSubqueryFields) : QSubqueryFieldsMap =
    fields |> Seq.mapMaybe (fun (mname, info) -> Option.map (fun name -> (name, info)) mname) |> Map.ofSeq

// If a query has a main entity it satisfies two properties:
// 1. All rows can be bound to some entry of that entity;
// 2. Columns contain all required fields of that entity.
type ResolvedMainEntity =
    { Entity : ResolvedEntityRef
      ColumnsToFields : Map<FieldName, FieldName>
      FieldsToColumns : Map<FieldName, FieldName>
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            sprintf "FOR INSERT INTO %s" (this.Entity.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

type ResolvedArgumentsMap = OrderedMap<ArgumentRef, ResolvedArgument>

let private renderFunQLArguments (arguments : ResolvedArgumentsMap) : string =
    if OrderedMap.isEmpty arguments then
        ""
    else
        let printArgument (name : ArgumentRef, arg : ResolvedArgument) =
            match name with
            | PGlobal _ -> None
            | PLocal _ -> Some <| sprintf "%s %s" (name.ToFunQLString()) (arg.ToFunQLString())
        arguments |> OrderedMap.toSeq |> Seq.mapMaybe printArgument |> String.concat ", " |> sprintf "(%s):"

[<NoEquality; NoComparison>]
type ResolvedViewExpr =
    { Pragmas : PragmasMap
      Arguments : ResolvedArgumentsMap
      Select : ResolvedSelectExpr
      MainEntity : ResolvedMainEntity option
      Privileged : bool
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            String.concatWithWhitespaces
                [ renderFunQLArguments this.Arguments
                  this.Select.ToFunQLString()
                  optionToFunQLString this.MainEntity
                ]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

[<NoEquality; NoComparison>]
type ResolvedCommandExpr =
    { Pragmas : PragmasMap
      Arguments : ResolvedArgumentsMap
      Command : ResolvedDataExpr
      Privileged : bool
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            String.concatWithWhitespaces
                [ renderFunQLArguments this.Arguments
                  this.Command.ToFunQLString()
                ]

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

let private unionBoundFieldInfo (f1 : BoundField option) (f2 : BoundField option) : BoundField option =
    match (f1, f2) with
    | (Some bf1, Some bf2) when bf1.Ref = bf2.Ref -> Some bf1
    | _ -> None

let private unionSubqueryInfo (a : SubqueryInfo) (b : SubqueryInfo) =
    if Array.length a.Fields <> Array.length b.Fields then
        raisef ViewResolveException "Different number of columns in a set operation expression"
    let newFields = Array.map2 (fun (name1, info1) (name2, info2) -> (name1, unionBoundFieldInfo info1 info2)) a.Fields b.Fields
    { Fields = newFields
      ExprInfo = unionSubqueryExprInfo a.ExprInfo b.ExprInfo
    }

let private subqueryToResolvedExprInfo (localEntities : Set<FromEntityId>) (info : SubqueryInfo) : ResolvedExprInfo =
    let subLocalEntities = Set.intersect info.ExprInfo.ExternalEntities localEntities
    // We add entities that are not local to our own external entities.
    let subExternalEntities = Set.difference info.ExprInfo.ExternalEntities localEntities
    let newFlags =
        { emptyResolvedExprFlags with
            HasArguments = info.ExprInfo.HasArguments
            HasFetches = info.ExprInfo.HasFetches
            HasFields = not <| Set.isEmpty subLocalEntities
        }
    { emptyResolvedExprInfo with
        Flags = newFlags
        ExternalEntities = subExternalEntities
    }

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ViewResolveException "Invalid name: %s" name

// Copy of that in Layout.Resolve but with a different exception.
let private resolveEntityRef (name : EntityRef) : ResolvedEntityRef =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ViewResolveException "Unspecified schema in name: %O" name

let resolveScalarFieldType (layout : IEntitiesSet) (allowReferenceOptions : bool) : ParsedScalarFieldType -> ResolvedScalarFieldType = function
    | SFTInt -> SFTInt
    | SFTDecimal -> SFTDecimal
    | SFTString -> SFTString
    | SFTBool -> SFTBool
    | SFTDateTime -> SFTDateTime
    | SFTDate -> SFTDate
    | SFTInterval -> SFTInterval
    | SFTJson -> SFTJson
    | SFTUserViewRef -> SFTUserViewRef
    | SFTUuid -> SFTUuid
    | SFTReference (entityRef, mopts) ->
        match mopts with
        | Some opts when not allowReferenceOptions -> raisef ViewResolveException "Reference delete options are not allowed here"
        | _ -> ()
        let resolvedRef = resolveEntityRef entityRef
        if not <| layout.HasVisibleEntity resolvedRef then
            raisef ViewResolveException "Cannot find entity %O from reference type" resolvedRef
        SFTReference (resolvedRef, mopts)
    | SFTEnum vals ->
        if OrderedSet.isEmpty vals then
            raisef ViewResolveException "Enums must not be empty"
        SFTEnum vals

let resolveFieldType (layout : IEntitiesSet) (allowReferenceOptions : bool) : ParsedFieldType -> ResolvedFieldType = function
    | FTArray typ -> FTArray <| resolveScalarFieldType layout allowReferenceOptions typ
    | FTScalar typ -> FTScalar <| resolveScalarFieldType layout allowReferenceOptions typ

let resolveCastScalarFieldType : ScalarFieldType<_> -> ScalarFieldType<_> = function
    | SFTInt -> SFTInt
    | SFTDecimal -> SFTDecimal
    | SFTString -> SFTString
    | SFTBool -> SFTBool
    | SFTDateTime -> SFTDateTime
    | SFTDate -> SFTDate
    | SFTInterval -> SFTInterval
    | SFTJson -> SFTJson
    | SFTUserViewRef -> SFTUserViewRef
    | SFTUuid -> SFTUuid
    | SFTReference (entityRef, opts) -> raisef ViewResolveException "Cannot cast to reference type"
    | SFTEnum vals -> raisef ViewResolveException "Cannot cast to enum type"

let resolveCastFieldType : FieldType<_> -> FieldType<_> = function
    | FTArray typ -> FTArray <| resolveCastScalarFieldType typ
    | FTScalar typ -> FTScalar <| resolveCastScalarFieldType typ

let fieldValueType : FieldValue -> ResolvedFieldType option = function
    | FInt _ -> Some <| FTScalar SFTInt
    | FDecimal _ -> Some <| FTScalar SFTDecimal
    | FString _ -> Some <| FTScalar SFTString
    | FBool _ -> Some <| FTScalar SFTBool
    | FDateTime _-> Some <| FTScalar SFTDateTime
    | FDate _ -> Some <| FTScalar SFTDate
    | FInterval _ -> Some <| FTScalar SFTInterval
    | FJson _ -> Some <| FTScalar SFTJson
    | FUserViewRef _ -> Some <| FTScalar SFTUserViewRef
    | FUuid _ -> Some <| FTScalar SFTUuid
    | FIntArray _ -> Some <| FTArray SFTInt
    | FDecimalArray _ -> Some <| FTArray SFTDecimal
    | FStringArray _ -> Some <| FTArray SFTString
    | FBoolArray _ -> Some <| FTArray SFTBool
    | FDateTimeArray _ -> Some <| FTArray SFTDateTime
    | FDateArray _ -> Some <| FTArray SFTDate
    | FIntervalArray _ -> Some <| FTArray SFTInterval
    | FJsonArray _ -> Some <| FTArray SFTJson
    | FUserViewRefArray _ -> Some <| FTArray SFTUserViewRef
    | FUuidArray _ -> Some <| FTArray SFTUuid
    | FNull -> None

let getGlobalArgument = function
    | PGlobal arg -> Some arg
    | PLocal _ -> None

type TypeContext =
    { AllowedSubtypes : Set<ResolvedEntityRef>
      Type : ResolvedEntityRef
    }

// Fields that are not listed in `TypeContextsMap` are assumed to be unrestricted.
type private TypeContextsMap = Map<FromFieldKey, TypeContext>

type private TypeContexts =
    { Map : TypeContextsMap
      ExtraConditionals : bool
    }

type ResolvedSingleSelectMeta =
    { HasAggregates : bool
    }

// When this metadata is absent, any subtype is possible at this point.
type PossibleSubtypesMeta =
    { PossibleSubtypes : Set<ResolvedEntityRef>
    }

type SubEntityMeta =
    { PossibleEntities : PossibleEntities<Set<ResolvedEntityRef>>
    }

type ReferenceArgumentMeta =
    { // Of length `ref.Path`, contains reference entities given current type context, starting from the first referenced entity. Guaranteed to be non-empty.
      Path : ResolvedEntityRef[]
    }

type BoundFieldMeta =
    { Ref : ResolvedFieldRef
      // Set if field references value from a table directly, not via a subexpression.
      Immediate : bool
      // Of length `ref.Path`, contains reference entities given current type context, starting from the first referenced entity.
      // `[Ref] + Path` is the full list of used references, starting from the first one.
      Path : ResolvedEntityRef[]
      // Set if source entity is joined as INNER (that is, it's impossible for its `id` to be NULL).
      IsInner : bool
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
    | FEntity { Ref = { Schema = Some schemaName; Name = name } } -> RValue { Schema = schemaName; Name = name }
    | FEntity { Ref = { Schema = None; Name = name } } ->
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
    | FSubExpr subsel ->
        mapRecursiveValue fst <| findMainEntityExpr ctes currentCte subsel.Select

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
    let ctes = Map.union (cteBindings select) ctes
    findMainEntityTreeExpr ctes currentCte select.Tree

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private findMainEntity (select : ResolvedSelectExpr) : (ResolvedEntityRef * ResolvedQueryResult[]) =
    let getValue = function
    | RValue (ref, results) -> (ref, results)
    | RRecursive r -> failwithf "Impossible recursion detected in %O" r
    getValue <| findMainEntityExpr Map.empty None select

type private ResolveCTE = EntityName -> SubqueryInfo

type private CTEsInfo =
    { ResolveCTE : ResolveCTE
      ExprInfo : SubqueryExprInfo
    }

type private FromEntityInfo =
    { Schema : SchemaName option
      Id : FromEntityId
      EntityAttributes : Set<AttributeName>
    }

type private Context =
    { ResolveCTE : ResolveCTE
      FieldMaps : FieldMapping list
      Entities : Map<EntityName, FromEntityInfo>
      // Used for tracking external dependencies.
      LocalEntities : Set<FromEntityId>
      Types : TypeContextsMap
      NoArguments : bool
    }

type private InnerResolvedExprInfo =
    { Info : ResolvedExprInfo
      Types : TypeContextsMap
    }

let private exprDependency (info : ResolvedExprInfo) : DependencyStatus =
    if not info.Flags.HasFields then
        if info.Flags.HasArguments then
            DSSingle
        else
            DSConst
    else
        DSPerRow

type private FromEntityIdMeta =
    { Id : FromEntityId
      IsInner : bool
    }

let private failCte (name : EntityName) =
    raisef ViewResolveException "Table %O not found" name

let private emptyContext : Context =
    { ResolveCTE = failCte
      FieldMaps = []
      Entities = Map.empty
      LocalEntities = Set.empty
      Types = Map.empty
      NoArguments = false
    }

let private emptyTypeContexts : TypeContexts =
    { Map = Map.empty
      ExtraConditionals = false
    }

let private emptyCondTypeContexts =
    { Map = Map.empty
      ExtraConditionals = true
    }

let private subSelectContext (ctx : Context) =
    { ctx with
        LocalEntities = Set.empty
        Entities = Map.empty
    }

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

let private findFieldWithoutContext (layout : ILayoutBits) (entityRef : ResolvedEntityRef) (fieldName : FieldName) : (ResolvedEntityRef * IEntityBits * ResolvedFieldInfo) option =
    let parentEntity = layout.FindEntity entityRef |> Option.get
    match parentEntity.FindField fieldName with
    | Some field -> Some (entityRef, parentEntity, field)
    | None -> None

// Find uppest possible entity in the hierarchy given current type context that has a required field, or None if no such entity exists.
let private findFieldWithContext (layout : ILayoutBits) (typeCtxs : TypeContextsMap) (key : FromFieldKey) (parentEntity : ResolvedEntityRef) (fieldName : FieldName) : (ResolvedEntityRef * IEntityBits * ResolvedFieldInfo) option =
    // First try to find within the given entity itself.
    match Map.tryFind key typeCtxs with
    | None -> findFieldWithoutContext layout parentEntity fieldName
    | Some typeCtx ->
        assert (parentEntity = typeCtx.Type)
        match findCommonAncestor layout typeCtx.Type typeCtx.AllowedSubtypes with
        | Some (newRef, newEntity) when newRef <> parentEntity ->
            match newEntity.FindField fieldName with
            | None -> None
            | Some field ->
                // Upcast it as much as possible to avoid unnecessary restrictions due to used entities.
                let inheritedFrom =
                    match field.Field with
                    // These should have been found in parent entity.
                    | RId | RSubEntity -> None
                    | RColumnField col -> col.InheritedFrom
                    | RComputedField comp -> comp.InheritedFrom
                match inheritedFrom with
                | None -> Some (newRef, newEntity, field)
                | Some realParentRef ->
                    let realParent = layout.FindEntity realParentRef |> Option.get
                    let realField = realParent.FindField fieldName |> Option.get
                    Some (realParentRef, realParent, realField)
        | _ -> findFieldWithoutContext layout parentEntity fieldName

type private OldBoundField =
    | OBField of BoundField
    | OBHeader of BoundFieldHeader

let private boundFieldInfo (typeCtxs : TypeContextsMap) (inner : BoundField) (extras : obj seq) : ObjectMap =
    let commonExtras =
        seq {
            let addSubtypesInfo =
                match inner.Field with
                | RComputedField comp -> Option.isSome comp.Virtual
                | RSubEntity -> true
                | _ -> false
            if addSubtypesInfo then
                match Map.tryFind inner.Header.Key typeCtxs with
                | None -> ()
                | Some typeCtx -> yield ({ PossibleSubtypes = typeCtx.AllowedSubtypes } : PossibleSubtypesMeta) :> obj
        }
    ObjectMap.ofSeq (Seq.append commonExtras extras)

let private resolveSubEntity (layout : ILayoutBits) (outerTypeCtxs : TypeContextsMap) (ctx : SubEntityContext) (field : LinkedBoundFieldRef) (subEntityInfo : SubEntityRef) : TypeContextsMap * SubEntityRef =
    let (fieldRef, typeCtxKey) =
        match field.Ref.Ref with
        | VRColumn col ->
            let fieldInfo = ObjectMap.findType<FieldMeta> field.Extra
            let boundInfo =
                match fieldInfo.Bound with
                | Some bound -> bound
                | _ -> raisef ViewResolveException "Unbound field in a type assertion"
            let arrowNames = field.Ref.Path |> Seq.map (fun arr -> arr.Name) |> Seq.toList
            let typeCtxKey =
                { FromId = FIEntity fieldInfo.FromEntityId
                  Path = List.exceptLast (boundInfo.Ref.Name :: arrowNames)
                }
            let fieldRef =
                if Array.isEmpty field.Ref.Path then
                    boundInfo.Ref
                else
                    let entityRef = Array.last boundInfo.Path
                    let fieldArrow = Array.last field.Ref.Path
                    { Entity = entityRef; Name = fieldArrow.Name }
            (fieldRef, typeCtxKey)
        | VRArgument arg ->
            let pathInfo =
                match ObjectMap.tryFindType<ReferenceArgumentMeta> field.Extra with
                | Some info -> info
                | None -> raisef ViewResolveException "Unbound field in a type assertion"
            let arrowNames = field.Ref.Path |> Seq.map (fun arr -> arr.Name) |> Seq.toList
            let typeCtxKey =
                { FromId = FIArgument arg
                  Path = List.exceptLast arrowNames
                }
            let fieldRef =
                    let entityRef = Array.last pathInfo.Path
                    let fieldArrow = Array.last field.Ref.Path
                    { Entity = entityRef; Name = fieldArrow.Name }
            (fieldRef, typeCtxKey)

    let fields = layout.FindEntity fieldRef.Entity |> Option.get
    match fields.FindField fieldRef.Name with
    | Some { Field = RSubEntity } -> ()
    | _ -> raisef ViewResolveException "Bound field in a type assertion is not a 'sub_entity' field: %O" field
    let subEntityRef = { Schema = Option.defaultValue fieldRef.Entity.Schema subEntityInfo.Ref.Schema; Name = subEntityInfo.Ref.Name }
    let subEntity =
        match layout.FindEntity subEntityRef with
        | None -> raisef ViewResolveException "Couldn't find subentity %O" subEntityInfo.Ref
        | Some r -> r

    let neededTypes =
        match ctx with
        | SECInheritedFrom ->
            if checkInheritance layout subEntityRef fieldRef.Entity then
                PEAny
            else if checkInheritance layout fieldRef.Entity subEntityRef then
                allPossibleEntities layout subEntityRef |> mapPossibleEntities (Seq.map fst >> Set.ofSeq)
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"
        | SECOfType ->
            if subEntity.IsAbstract then
                raisef ViewResolveException "Instances of abstract entity %O do not exist" subEntityRef
            if subEntityRef = fieldRef.Entity then
                PEAny
            else if checkInheritance layout fieldRef.Entity subEntityRef then
                PEList (Set.singleton subEntityRef)
            else
                raisef ViewResolveException "Entities in a type assertion are not in the same hierarchy"

    let checkForTypes =
        match neededTypes with
        | PEAny -> PEAny
        | PEList needed ->
            match Map.tryFind typeCtxKey outerTypeCtxs with
            | None -> PEList needed
            | Some ctx ->
                let otherTypes = Set.difference ctx.AllowedSubtypes needed
                if Set.isEmpty otherTypes then
                    PEAny
                else
                    let possibleTypes = Set.intersect ctx.AllowedSubtypes needed
                    PEList possibleTypes

    let innerTypeCtxs =
        match neededTypes with
        | PEAny -> Map.empty
        | PEList needed ->
            let ctx =
                { Type = fieldRef.Entity
                  AllowedSubtypes = needed
                }
            Map.singleton typeCtxKey ctx
    let info = { PossibleEntities = checkForTypes } : SubEntityMeta
    let ret = { Ref = relaxEntityRef subEntityRef; Extra = ObjectMap.singleton info } : SubEntityRef
    (innerTypeCtxs, ret)

// Public helper functions.

let replaceFieldRefInExpr (updateEntityRef : LinkedBoundFieldRef -> FieldRef -> LinkedBoundFieldRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
    let resolveReference : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
    | { Ref = { Ref = VRColumn col } } as ref -> updateEntityRef ref col
    | ref -> ref
    let mapper = { idFieldExprMapper with FieldReference = resolveReference }
    mapFieldExpr mapper

let replaceEntityRefInField (localRef : EntityRef option) (ref : LinkedBoundFieldRef) (col : FieldRef) : LinkedBoundFieldRef =
    { ref with Ref = { ref.Ref with Ref = VRColumn { col with Entity = localRef } } }

let replaceEntityRefInExpr (localRef : EntityRef option) = replaceFieldRefInExpr (replaceEntityRefInField localRef)

let replacePathInField (layout : ILayoutBits) (localRef : ValueRef<FieldRef>) (asRoot : bool) (path : PathArrow seq) (extra : ObjectMap) (replaceLastPrefixArrow : bool) (ref : LinkedBoundFieldRef) (col : FieldRef) : LinkedBoundFieldRef =
    let newFieldArrow =
        { Name = col.Name
          AsRoot = ref.Ref.AsRoot
        }
    let pathPrefix =
        if replaceLastPrefixArrow then
            Seq.exceptLast path
        else
            path
    let newPath = Seq.concat [pathPrefix; Seq.singleton newFieldArrow; Array.toSeq ref.Ref.Path] |> Array.ofSeq

    let replaceBoundPath (boundPath : ResolvedEntityRef[]) =
        let boundPrefix =
            if replaceLastPrefixArrow then
                Seq.exceptLast boundPath
            else
                boundPath
        let newBoundEntity =
            if replaceLastPrefixArrow then
                Seq.last boundPath
            else
                let lastEntityRef = Seq.last boundPath
                let lastEntity = layout.FindEntity lastEntityRef |> Option.get
                match lastEntity.FindField col.Name with
                | Some { Field = RColumnField { FieldType = FTScalar (SFTReference (refEntityRef, _)) } } -> refEntityRef
                | _ -> failwith "Impossible"
        let fieldMeta = ObjectMap.findType<FieldMeta> ref.Extra
        let boundMeta = Option.get fieldMeta.Bound
        Seq.concat [boundPrefix; Seq.singleton newBoundEntity; Array.toSeq boundMeta.Path] |> Array.ofSeq

    let newExtra =
        match localRef with
        | VRArgument _ ->
            let argInfo = ObjectMap.findType<ReferenceArgumentMeta> extra
            let newBoundPath = replaceBoundPath argInfo.Path
            let newArgInfo = { argInfo with Path = newBoundPath }
            ObjectMap.add newArgInfo extra
        | VRColumn _ ->
            let localFieldMeta = ObjectMap.findType<FieldMeta> extra
            let localBoundMeta = Option.get localFieldMeta.Bound
            let newBoundPath = replaceBoundPath localBoundMeta.Path
            let newBoundMeta = { localBoundMeta with Path = newBoundPath }
            let newFieldMeta = { localFieldMeta with Bound = Some newBoundMeta }
            ObjectMap.add newFieldMeta extra

    { Ref = { Ref = localRef; Path = newPath; AsRoot = asRoot }; Extra = newExtra }

let replacePathInExpr (layout : ILayoutBits) (localRef : ValueRef<FieldRef>) (asRoot : bool) (pathPrefix : PathArrow seq) (extra : ObjectMap) (replaceLastPrefixArrow : bool) = replaceFieldRefInExpr (replacePathInField layout localRef asRoot pathPrefix extra replaceLastPrefixArrow)

let private filterCasesWithSubtypes (extra : ObjectMap) (cases : VirtualFieldCase seq) : VirtualFieldCase seq =
    match ObjectMap.tryFindType<PossibleSubtypesMeta> extra with
    | None -> cases
    | Some meta ->
        let filterCase (case : VirtualFieldCase) =
            let possibleCases = Set.intersect meta.PossibleSubtypes case.PossibleEntities
            if Set.isEmpty possibleCases then
                None
            else
                Some { case with PossibleEntities = possibleCases }
        cases |> Seq.mapMaybe filterCase

let computedFieldCases (layout : ILayoutBits) (extra : ObjectMap) (ref : ResolvedFieldRef) (comp : ResolvedComputedField) : (VirtualFieldCase * ResolvedComputedField) seq =
    match comp.Virtual with
    | None ->
        let nonvirtualCase =
            { PossibleEntities = Set.singleton ref.Entity
              Ref = ref.Entity
            }
        Seq.singleton (nonvirtualCase, comp)
    | Some virtInfo ->
        let filteredCases = filterCasesWithSubtypes extra virtInfo.Cases

        let compileCase (case : VirtualFieldCase) =
            let caseEntity = layout.FindEntity case.Ref |> Option.get
            let caseField =
                match caseEntity.FindField ref.Name with
                | Some { Field = RComputedField comp } -> comp
                | _ -> failwithf "Unexpected non-computed field %O" ref.Name
            (case, caseField)

        Seq.map compileCase filteredCases

let private resolveMainEntity (layout : ILayoutBits) (fields : QSubqueryFieldsMap) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity =
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
            | Some (Some { Ref = fieldRef; Field = RColumnField _ }) when fieldRef.Entity = ref -> Some (name, fieldRef.Name)
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

type private SelectFlags =
    { RequireNames : bool
      NoAttributes : bool
    }

type private OuterField =
    { Bound : BoundField option
      IsExternal : bool
      EntityId : FromEntityId
    }

type private ReferenceInfo =
    { InnerField : BoundField option
      ExprInfo : ResolvedExprInfo
    }

type private ResultInfo =
    { Field : BoundField option
      ExprInfo : ResolvedExprInfo
    }

let private viewExprSelectFlags =
    { NoAttributes = false
      RequireNames = true
    }

let private subExprSelectFlags =
    { NoAttributes = true
      RequireNames = false
    }

type ExprResolutionFlags =
    { Privileged : bool
    }

let emptyExprResolutionFlags =
    { Privileged = false
    }

type private FromExprInfo =
    { Fields : FieldMapping
      Entities : Map<EntityName, FromEntityInfo>
      ExprInfo : SubqueryExprInfo
      Types : TypeContextsMap
    }

let private emptyFromExprInfo : FromExprInfo =
    { Fields = Map.empty
      Entities = Map.empty
      ExprInfo = emptySubqueryExprInfo
      Types = Map.empty
    }

let private unionFromExprInfo (a : FromExprInfo) (b : FromExprInfo) =
    let entities =
        try
            Map.unionUnique a.Entities b.Entities
        with
        | Failure msg -> raisef ViewResolveException "Clashing entity names: %s" msg
    { Fields = unionNameMapping a.Fields b.Fields
      Entities = entities
      Types = Map.unionUnique a.Types b.Types
      ExprInfo = unionSubqueryExprInfo a.ExprInfo b.ExprInfo
    }

let private addFromToContext (fromInfo : FromExprInfo) (ctx : Context) =
    { ctx with
        FieldMaps = fromInfo.Fields :: ctx.FieldMaps
        Entities = fromInfo.Entities
        Types = Map.unionUnique ctx.Types fromInfo.Types
        LocalEntities = fromInfo.Entities |> Map.values |> Seq.map (fun info -> info.Id) |> Set.ofSeq
    }

type private FromEntityMappingInfo =
    { SingleFrom : FromExprInfo
      Id : FromEntityId
    }

type FromEntityMeta =
    { TypeContext : TypeContext option
      Id : int
      IsInner : bool
    }

type private FromMappingFlags =
    { WithoutChildren : bool
      AllowHidden : bool
      IsInner : bool
    }

let rec private relabelFromExprType (typeContexts : TypeContextsMap) = function
    | FEntity entity ->
        let idMeta = ObjectMap.findType<FromEntityIdMeta> entity.Extra
        let key =
            { FromId = FIEntity idMeta.Id
              Path = []
            }
        let ctxMeta =
            { TypeContext = Map.tryFind key typeContexts
              Id = idMeta.Id
              IsInner = idMeta.IsInner
            } : FromEntityMeta
        FEntity { entity with Extra = ObjectMap.singleton ctxMeta }
    | FJoin join ->
        let a = relabelFromExprType typeContexts join.A
        let b = relabelFromExprType typeContexts join.B
        FJoin { join with A = a; B = b }
    | FSubExpr expr -> FSubExpr expr

type private QueryResolver (layout : ILayoutBits, arguments : Map<ArgumentRef, ResolvedArgument>, resolveFlags : ExprResolutionFlags) =
    let mutable isPrivileged = false

    let mutable lastFromEntityId : FromEntityId = 0
    let nextFromEntityId () =
        let ret = lastFromEntityId
        lastFromEntityId <- lastFromEntityId + 1
        ret

    let notTypeContext (ctx : TypeContext) : PossibleEntities<TypeContext> =
        if Set.isEmpty ctx.AllowedSubtypes then
            PEAny
        else
            let allEntities = allPossibleEntitiesList layout ctx.Type |> Seq.map fst |> Set.ofSeq
            let newAllowed = Set.difference allEntities ctx.AllowedSubtypes
            PEList
                { AllowedSubtypes = newAllowed
                  Type = ctx.Type
                }

    let notTypeContexts ctx =
        if ctx.ExtraConditionals then
            emptyTypeContexts
        else
            let mapOne name ctx =
                match notTypeContext ctx with
                | PEAny -> None
                | PEList list -> Some list
            { Map = Map.mapMaybe mapOne ctx.Map
              ExtraConditionals = false
            }

    let orTypeContext (ctxA : TypeContext) (ctxB : TypeContext) : TypeContext option =
        assert (ctxA.Type = ctxB.Type)
        let allEntities = allPossibleEntitiesList layout ctxA.Type |> Seq.map fst |> Set.ofSeq
        let newAllowed = Set.union ctxA.AllowedSubtypes ctxB.AllowedSubtypes
        if Set.count newAllowed = Set.count allEntities then
            None
        else
            Some
                { AllowedSubtypes = newAllowed
                  Type = ctxA.Type
                }

    let orTypeContexts (ctxA : TypeContexts) (ctxB : TypeContexts) =
        { Map = Map.intersectWithMaybe (fun name -> orTypeContext) ctxA.Map ctxB.Map
          ExtraConditionals = ctxA.ExtraConditionals || ctxB.ExtraConditionals
        }

    let andTypeContext (ctxA : TypeContext) (ctxB : TypeContext) : TypeContext =
        assert (ctxA.Type = ctxB.Type)
        { AllowedSubtypes = Set.intersect ctxA.AllowedSubtypes ctxB.AllowedSubtypes
          Type = ctxA.Type
        }

    let andTypeContexts (ctxA : TypeContexts) (ctxB : TypeContexts) =
        { Map = Map.unionWith andTypeContext ctxA.Map ctxB.Map
          ExtraConditionals = ctxA.ExtraConditionals || ctxB.ExtraConditionals
        }

    let findEntityByRef (entityRef : ResolvedEntityRef) (allowHidden : bool) : IEntityBits =
        match layout.FindEntity entityRef with
        | Some entity when not entity.IsHidden || allowHidden -> entity
        | _ -> raisef ViewResolveException "Entity not found: %O" ref

    let createFromMapping (fromEntityId : FromEntityId) (entityRef : ResolvedEntityRef) (pun : (EntityRef option) option) (flags : FromMappingFlags) : FieldMapping =
        let entity = findEntityByRef entityRef flags.AllowHidden
        if flags.WithoutChildren && entity.IsAbstract then
            raisef ViewResolveException "Abstract entity cannot be selected as ONLY"

        let key =
            { FromId = FIEntity fromEntityId
              Path = []
            }

        let makeBoundField (name : FieldName) (field : ResolvedFieldBits) =
            let header =
                { ParentEntity = entityRef
                  Name = name
                  Key = key
                  Immediate = true
                  IsInner = flags.IsInner
                }
            Some
                { Header = header
                  Ref = { Entity = entityRef; Name = name }
                  Entity = entity
                  Field = field
                  ForceRename = false
                  Name = name
                }

        let realFields = mapAllFields makeBoundField entity
        let mainFieldInfo = Option.get <| entity.FindFieldBits funMain
        let mainHeader =
            { Key = key
              ParentEntity = entityRef
              Name = entity.MainField
              Immediate = true
              IsInner = flags.IsInner
            }
        let mainBoundField =
            { Header = mainHeader
              Ref = { Entity = entityRef; Name = entity.MainField }
              Entity = entity
              Field = mainFieldInfo.Field
              ForceRename = true
              Name = mainFieldInfo.Name
            }
        let fields = Map.add funMain (Some mainBoundField) realFields
        let mappingRef =
            match pun with
            | None -> Some <| relaxEntityRef entityRef
            | Some punRef -> punRef
        let mapping = fieldsToFieldMapping fromEntityId mappingRef fields
        if flags.WithoutChildren then
            mapping
        else
            let extraMapping = typeRestrictedFieldsToFieldMapping layout fromEntityId flags.IsInner mappingRef entityRef (entity.Children |> Map.keys)
            Map.unionUnique extraMapping mapping

    let resolvePath (typeCtxs : TypeContextsMap) (firstOldBoundField : OldBoundField) (fullPath : PathArrow list) : BoundField list =
        let getBoundField (oldBoundField : OldBoundField) : BoundField =
            let (cachedBound, header) =
                match oldBoundField with
                | OBField bound -> (Some bound, bound.Header)
                | OBHeader header -> (None, header)
            match findFieldWithContext layout typeCtxs header.Key header.ParentEntity header.Name with
            | None ->
                assert Option.isNone cachedBound
                raisef ViewResolveException "Field not found: %O" { Entity = header.ParentEntity; Name = header.Name }
            | Some (entityRef, entity, field) ->
                match cachedBound with
                | Some cached when cached.Ref.Entity = entityRef -> cached
                | _ ->
                    { Header = header
                      Ref = { Entity = entityRef; Name = header.Name }
                      Field = resolvedFieldToBits field.Field
                      Entity = entity
                      ForceRename = field.ForceRename
                      Name = field.Name
                    }

        let rec traverse (boundField : BoundField) : PathArrow list -> BoundField list = function
            | [] -> [boundField]
            | (ref :: refs) ->

                if ref.AsRoot then
                    if not resolveFlags.Privileged then
                        raisef ViewResolveException "Cannot specify roles in non-privileged user views"
                    isPrivileged <- true
                match boundField.Field with
                | RColumnField { FieldType = FTScalar (SFTReference (entityRef, opts)) } ->
                    let refKey =
                        { FromId = boundField.Header.Key.FromId
                          Path = List.append boundField.Header.Key.Path [boundField.Ref.Name]
                        }
                    let refHeader =
                        { Key = refKey
                          ParentEntity = entityRef
                          Name = ref.Name
                          Immediate = false
                          IsInner = boundField.Header.IsInner
                        }
                    let nextBoundField = getBoundField (OBHeader refHeader)
                    let boundFields = traverse nextBoundField refs
                    boundField :: boundFields
                | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

        traverse (getBoundField firstOldBoundField) fullPath

    let resolveReference (ctx : Context) (typeCtxs : TypeContextsMap) (f : LinkedFieldRef) : ReferenceInfo * LinkedBoundFieldRef =
        if f.AsRoot then
            if not resolveFlags.Privileged then
                raisef ViewResolveException "Cannot specify roles in non-privileged user views"
            isPrivileged <- true

        let (refInfo, newRef) =
            match f.Ref with
            | VRColumn ref ->
                let rec findInMappings = function
                    | [] -> raisef ViewResolveException "Unknown reference: %O" ref
                    | mapping :: mappings ->
                        match Map.tryFind ref mapping with
                        | Some (FVResolved info) -> info
                        | Some (FVAmbiguous entities) -> raisef ViewResolveException "Ambiguous reference %O. Possible values: %O" ref entities
                        | None -> findInMappings mappings
                let info = findInMappings ctx.FieldMaps

                let isExternal = not <| Set.contains info.EntityId ctx.LocalEntities
                if not (Array.isEmpty f.Path) && isExternal then
                    raisef ViewResolveException "Arrows for external fields are currently not supported"

                let oldBoundField =
                    match info.Mapping with
                    | FMBound bound -> Some <| OBField bound
                    | FMTypeRestricted header -> Some <| OBHeader header
                    | _ when Array.isEmpty f.Path -> None
                    | _ -> raisef ViewResolveException "Dereference of an unbound field in %O" f

                let boundFields = Option.map (fun old -> resolvePath typeCtxs old (Array.toList f.Path)) oldBoundField

                let boundFields =
                    match boundFields with
                    | Some fields ->
                        let (outer, remainingFields) =
                            match fields with
                            | outer :: path -> (outer, path)
                            | _ -> failwith "Impossible"
                        let inner = List.last fields
                        let boundPath =
                            remainingFields
                            |> Seq.map (fun p -> p.Ref.Entity)
                            |> Seq.toArray
                        assert (Array.length boundPath = Array.length f.Path)
                        let meta =
                            { Ref = outer.Ref
                              Immediate = outer.Header.Immediate
                              Path = boundPath
                              IsInner = outer.Header.IsInner
                            }
                        Some (inner, meta)
                    | None -> None
                let fieldInfo =
                    { Bound = Option.map (fun (inner, meta) -> meta) boundFields
                      FromEntityId = info.EntityId
                      ForceSQLName = info.ForceSQLName
                    } : FieldMeta
                let extra =
                    match boundFields with
                    | Some (inner, meta) -> boundFieldInfo typeCtxs inner (Seq.singleton (fieldInfo :> obj))
                    | None -> ObjectMap.singleton fieldInfo

                let newFieldRef = { Entity = info.Entity; Name = ref.Name } : FieldRef
                let newRef = { Ref = { f with Ref = VRColumn newFieldRef }; Extra = extra }

                let exprInfo =
                    if isExternal then
                        { emptyResolvedExprInfo with ExternalEntities = Set.singleton info.EntityId }
                    else
                        fieldResolvedExprInfo
                let refInfo =
                    { ExprInfo = exprInfo
                      InnerField = Option.map (fun (inner, meta) -> inner) boundFields
                    }
                (refInfo, newRef)
            | VRArgument arg ->
                if ctx.NoArguments then
                    raisef ViewResolveException "Arguments are not allowed here: %O" arg
                let argInfo =
                    match Map.tryFind arg arguments with
                    | None -> raisef ViewResolveException "Unknown argument: %O" arg
                    | Some argInfo -> argInfo
                let (innerBoundField, boundInfo) =
                    if Array.isEmpty f.Path then
                        (None, ObjectMap.empty)
                    else
                        match argInfo.ArgType with
                        | FTScalar (SFTReference (parentEntity, opts)) ->
                            let (firstArrow, remainingPath) =
                                match Array.toList f.Path with
                                | head :: tail -> (head, tail)
                                | _ -> failwith "Impossible"
                            let key =
                                { FromId = FIArgument arg
                                  Path = []
                                }
                            let (entityRef, argEntity, argField) =
                                match findFieldWithContext layout typeCtxs key parentEntity firstArrow.Name with
                                | Some ret -> ret
                                | None -> raisef ViewResolveException "Field doesn't exist in %O: %O" parentEntity firstArrow.Name
                            let argHeader =
                                { Key = key
                                  ParentEntity = parentEntity
                                  Name = firstArrow.Name
                                  Immediate = false
                                  IsInner = false
                                }
                            let outer : BoundField =
                                { Header = argHeader
                                  Ref = { Entity = entityRef; Name = firstArrow.Name }
                                  Entity = argEntity
                                  Field = resolvedFieldToBits argField.Field
                                  ForceRename = argField.ForceRename
                                  Name = argField.Name
                                }
                            let fields = resolvePath typeCtxs (OBField outer) remainingPath
                            assert (List.length fields = Array.length f.Path)
                            let inner = List.last fields
                            let boundPath =
                                fields
                                    |> Seq.map (fun p -> p.Ref.Entity)
                                    |> Seq.toArray
                            let boundInfo =
                                { Path = boundPath
                                } : ReferenceArgumentMeta
                            let info = boundFieldInfo typeCtxs inner (Seq.singleton (boundInfo :> obj))
                            (Some inner, info)
                        | _ -> raisef ViewResolveException "Argument is not a reference: %O" ref

                let exprInfo = { emptyResolvedExprInfo with Flags = { emptyResolvedExprFlags with HasArguments = true } }
                let refInfo =
                    { ExprInfo = exprInfo
                      InnerField = innerBoundField
                    }
                let newRef = { Ref = { f with Ref = VRArgument arg }; Extra = boundInfo }

                (refInfo, newRef)

        let refInfo =
            match refInfo.InnerField with
            | Some ({ Field = RComputedField _; Ref = fieldRef } as outer) ->
                // Find full field, we only have IComputedFieldBits in OuterField...
                match outer.Entity.FindField outer.Ref.Name with
                | Some { Field = RComputedField comp; Name = realName } ->
                    let exprFlags =
                        if comp.IsMaterialized then
                            fieldResolvedExprFlags
                        else
                            computedFieldCases layout newRef.Extra { fieldRef with Name = realName } comp
                            |> Seq.map (fun (case, caseComp) -> caseComp.Flags)
                            |> Seq.fold unionResolvedExprFlags refInfo.ExprInfo.Flags
                    { refInfo with ExprInfo = { refInfo.ExprInfo with Flags = exprFlags } }
                | _ -> failwith "Impossible"
            | _ -> refInfo
        let refInfo =
            if not <| Array.isEmpty f.Path then
                { refInfo with ExprInfo = { refInfo.ExprInfo with Flags = { refInfo.ExprInfo.Flags with HasArrows = true } } }
            else
                refInfo

        (refInfo, newRef)

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let resolveRef : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { Ref = VRArgument name; Path = [||] } as ref ->
                if Map.containsKey name arguments then
                    { Ref = ref; Extra = ObjectMap.empty }
                else
                    raisef ViewResolveException "Undefined placeholder: %O" name
            | ref -> raisef ViewResolveException "Invalid reference in LIMIT or OFFSET: %O" ref
        let voidSubEntity field subEntity = raisef ViewResolveException "Forbidden type assertion in LIMIT or OFFSET"
        let mapper =
            { onlyFieldExprMapper resolveRef with
                  SubEntity = voidSubEntity
            }
        try
            mapFieldExpr mapper expr
        with
        | :? UnexpectedExprException as e -> raisefWithInner ViewResolveException e ""

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

    let fromEntityMapping (ctx : Context) (isInner : bool) (entity : ParsedFromEntity) : FromEntityMappingInfo =
        let fromEntityId = nextFromEntityId ()

        let (mapping, types, name, exprInfo) =
            match entity.Ref with
            | { Schema = Some schemaName; Name = entityName } ->
                if entity.AsRoot then
                    if not resolveFlags.Privileged then
                        raisef ViewResolveException "Cannot specify roles in non-privileged user views"
                    isPrivileged <- true
                let resRef = { Schema = schemaName; Name = entityName }
                let punRef =
                    match entity.Alias with
                    | None -> None
                    | Some punName -> Some ({ Schema = None; Name = punName } : EntityRef)
                let mappingFlags =
                    { WithoutChildren = entity.Only
                      AllowHidden = false
                      IsInner = isInner
                    }
                let mapping = createFromMapping fromEntityId resRef (Option.map Some punRef) mappingFlags
                let mappingRef = Option.defaultValue entity.Ref punRef
                let typeContextsMap =
                    if entity.Only then
                        let ctx =
                            { AllowedSubtypes = Set.singleton resRef
                              Type = resRef
                            }
                        let key =
                            { FromId = FIEntity fromEntityId
                              Path = []
                            }
                        Map.singleton key ctx
                    else
                        Map.empty
                let exprInfo = { emptySubqueryExprInfo with HasFetches = true }
                (mapping, typeContextsMap, mappingRef, exprInfo)
            | { Schema = None; Name = entityName } ->
                if entity.AsRoot then
                    raisef ViewResolveException "Roles can only be specified for entities"
                if entity.Only then
                    raisef ViewResolveException "ONLY can only be specified for entities"
                let cteInfo = ctx.ResolveCTE entityName
                let newName = Option.defaultValue entityName entity.Alias
                let mappingRef = { Schema = None; Name = newName } : EntityRef
                let mapping = fieldsToFieldMapping fromEntityId (Some mappingRef) (getFieldsMap cteInfo.Fields)
                (mapping, Map.empty, mappingRef, cteInfo.ExprInfo)

        let (name, maybeSchema) =
            match entity.Alias with
            | None -> (name.Name, name.Schema)
            | Some alias -> (alias, None)
        let entityInfo =
            { Schema = maybeSchema
              EntityAttributes = Set.empty
              Id = fromEntityId
            } : FromEntityInfo
        let fromInfo =
            { Entities = Map.singleton name entityInfo
              Fields = mapping
              ExprInfo = exprInfo
              Types = types
            }
        { SingleFrom = fromInfo
          Id = fromEntityId
        }

    let rec resolveResult (flags : SelectFlags) (ctx : Context) : ParsedQueryResult -> ResultInfo * ResolvedQueryResult = function
        | QRAll alias -> raisef ViewResolveException "Wildcard SELECTs are not yet supported"
        | QRExpr result ->
            let (exprInfo, newResult) = resolveResultColumn flags ctx result
            (exprInfo, QRExpr newResult)

    and resolveResultColumn (flags : SelectFlags) (ctx : Context) (result : ParsedQueryColumnResult) : ResultInfo * ResolvedQueryColumnResult =
        if not (Map.isEmpty result.Attributes) && flags.NoAttributes then
            raisef ViewResolveException "Attributes are not allowed in query expressions"
        let (resultInfo, expr) = resolveResultExpr flags ctx result.Alias result.Result
        let (attrRefs, attributes) = resolveBoundAttributesMap ctx resultInfo.ExprInfo result.Attributes
        let ret =
            { Alias = result.Alias
              Attributes = attributes
              Result = expr
            } : ResolvedQueryColumnResult
        let resultInfo =
            { resultInfo with ExprInfo = unionResolvedExprInfo resultInfo.ExprInfo attrRefs }
        (resultInfo, ret)

    // Should be in sync with resultField
    and resolveResultExpr (flags : SelectFlags) (ctx : Context) (name : FieldName option) : ParsedFieldExpr -> ResultInfo * ResolvedFieldExpr = function
        | FERef f ->
            Option.iter checkName name
            let (refInfo, ref) = resolveReference ctx Map.empty f
            // Field as seen outside of this query.
            let resultField =
                match refInfo.InnerField with
                | None -> None
                | Some field ->
                    if field.ForceRename && Option.isNone name && flags.RequireNames then
                        raisef ViewResolveException "Field should be explicitly named in result expression: %s" (f.ToFunQLString())
                    match field.Field with
                    // We erase field information for computed fields from results, as they would be expanded at this point.
                    | RComputedField comp -> None
                    | _ ->
                        // Field is no longer immediate, and rename is not needed because a name is assigned here.
                        let header = { field.Header with Immediate = false }
                        let ret =
                            { field with
                                Header = header
                                ForceRename = false
                            }
                        Some ret
            let info =
                { Field = resultField
                  ExprInfo = refInfo.ExprInfo
                }
            (info, FERef ref)
        | e ->
            match name with
            | Some n -> checkName n
            | None when flags.RequireNames -> raisef ViewResolveException "Unnamed results are allowed only inside expression queries"
            | None -> ()
            let (exprInfo, expr) = resolveFieldExpr ctx e
            let info =
                { Field = None
                  ExprInfo = exprInfo.Info
                }
            (info, expr)

    and resolveAttributeExpr (ctx : Context) (expr : ParsedFieldExpr) : ResolvedExprInfo * ResolvedFieldExpr =
        let (info, newExpr) = resolveFieldExpr ctx expr
        if info.Info.Flags.HasAggregates then
            raisef ViewResolveException "Aggregate functions are not allowed here"
        (info.Info, newExpr)

    and resolveAttribute (ctx : Context) (attr : ParsedAttribute) : ResolvedExprInfo * ResolvedAttribute =
        let newCtx =
            match attr.Dependency with
            | DSPerRow -> ctx
            | DSSingle -> { ctx with FieldMaps = [] }
            | DSConst -> { ctx with FieldMaps = []; NoArguments = true }
        let (info, expr) = resolveFieldExpr newCtx attr.Expression
        let newAttr =
            { Expression = expr
              Dependency = exprDependency info.Info
            }
        (info.Info, newAttr)

    and resolveBoundAttribute (ctx : Context) (boundExprInfo : ResolvedExprInfo) (attr : ParsedBoundAttribute) : ResolvedExprInfo * ResolvedBoundAttribute =
        match attr.Expression with
        | BAExpr expr ->
            let (info, newAttr) = resolveAttribute ctx { Expression = expr; Dependency = attr.Dependency }
            (info, { Expression = BAExpr newAttr.Expression; Dependency = newAttr.Dependency })
        | BAMapping mapping ->
            (boundExprInfo, { Expression = BAMapping mapping; Dependency = exprDependency boundExprInfo })

    and resolveAttributesMap (ctx : Context) (attributes : ParsedAttributesMap) : ResolvedExprInfo * ResolvedAttributesMap =
        let mutable exprInfo = emptyResolvedExprInfo
        let mapOne name attr =
            let (newInfo, newAttr) = resolveAttribute ctx attr
            exprInfo <- unionResolvedExprInfo exprInfo newInfo
            newAttr
        let ret = Map.map mapOne attributes
        (exprInfo, ret)

    and resolveBoundAttributesMap (ctx : Context) (boundExprInfo : ResolvedExprInfo) (attributes : ParsedBoundAttributesMap) : ResolvedExprInfo * ResolvedBoundAttributesMap =
        let mutable exprInfo = emptyResolvedExprInfo
        let mapOne name attr =
            let (newInfo, newAttr) = resolveBoundAttribute ctx boundExprInfo attr
            exprInfo <- unionResolvedExprInfo exprInfo newInfo
            newAttr
        let ret = Map.map mapOne attributes
        (exprInfo, ret)

    and resolveNonaggrFieldExpr (ctx : Context) (expr : ParsedFieldExpr) : InnerResolvedExprInfo * ResolvedFieldExpr =
        let (info, res) = resolveFieldExpr ctx expr
        if info.Info.Flags.HasAggregates then
            raisef ViewResolveException "Aggregate functions are not allowed here"
        (info, res)

    and resolveEntityReference (ctx : Context) (entityRef : EntityRef) : EntityRef =
        match Map.tryFind entityRef.Name ctx.Entities with
        | None -> raisef ViewResolveException "Entity %O not found" entityRef
        | Some entityInfo ->
            match entityRef.Schema with
            | Some _ as schema when entityInfo.Schema <> schema -> raisef ViewResolveException "Entity %O not found" entityRef
            | _ -> ()
            { Schema = entityInfo.Schema; Name = entityRef.Name }

    and resolveFieldExpr (ctx : Context) (expr : ParsedFieldExpr) : InnerResolvedExprInfo * ResolvedFieldExpr =
        let mutable exprInfo = emptyResolvedExprInfo

        let resolveExprReference typeCtxs col =
            let (refInfo, ref) = resolveReference ctx typeCtxs col
            exprInfo <- unionResolvedExprInfo exprInfo refInfo.ExprInfo
            ref

        let resolveQuery query =
            let (selectInfo, res) = resolveSelectExpr (subSelectContext ctx) subExprSelectFlags query
            // We only consider an expression to have references if it's for a local entity.
            let localEntities = Set.intersect selectInfo.ExprInfo.ExternalEntities ctx.LocalEntities
            // We add entities that are not local to our own external entities.
            let externalEntities = Set.difference selectInfo.ExprInfo.ExternalEntities ctx.LocalEntities
            let newFlags =
                { emptyResolvedExprFlags with
                    HasArguments = selectInfo.ExprInfo.HasArguments
                    HasFetches = selectInfo.ExprInfo.HasFetches
                    HasFields = not <| Set.isEmpty localEntities
                }
            let newInfo =
                { emptyResolvedExprInfo with
                    Flags = newFlags
                    ExternalEntities = externalEntities
                }
            exprInfo <- unionResolvedExprInfo exprInfo newInfo
            res

        let rec traverse (outerTypeCtxs : TypeContexts) = function
            | FEValue value -> (emptyCondTypeContexts, FEValue value)
            | FERef r -> (emptyCondTypeContexts, FERef (resolveExprReference outerTypeCtxs.Map r))
            | FEEntityAttr (eref, attr) ->
                let newEref = resolveEntityReference ctx eref
                // We consider entity attribute references non-local because they can change based on inner parts of the query and default attributes.
                exprInfo <- { exprInfo with Flags = unionResolvedExprFlags exprInfo.Flags unknownResolvedExprFlags }
                (emptyCondTypeContexts, FEEntityAttr (newEref, attr))
            | FEFieldAttr (fref, attr) ->
                // We consider field attribute references non-local because they can change based on inner parts of the query and default attributes.
                exprInfo <- { exprInfo with Flags = unionResolvedExprFlags exprInfo.Flags unknownResolvedExprFlags }
                (emptyCondTypeContexts, FEFieldAttr (resolveExprReference outerTypeCtxs.Map fref, attr))
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
                let mutable curTypeCtxs = emptyTypeContexts
                let applyOne (cond, e) =
                    let mergedTypeCtxs = andTypeContexts outerTypeCtxs curTypeCtxs
                    let (condTypeCtxs, newCond) = traverse mergedTypeCtxs cond
                    let (typeCtxs, newE) = traverse (andTypeContexts mergedTypeCtxs condTypeCtxs) e
                    curTypeCtxs <- andTypeContexts curTypeCtxs (notTypeContexts condTypeCtxs)
                    (newCond, newE)
                let newEs = Array.map applyOne es
                let applyElse e =
                    let mergedTypeCtxs = andTypeContexts outerTypeCtxs curTypeCtxs
                    let (typeCtxs, newE) = traverse mergedTypeCtxs e
                    newE
                let newEls = Option.map applyElse els
                (emptyCondTypeContexts, FECase (newEs, newEls))
            | FEMatch (expr, es, els) ->
                let (typeCtxExpr, newExpr) = traverse outerTypeCtxs expr
                let applyOne (matchExpr, e) =
                    let (typeCtxsMatch, newMatchExpr) = traverse outerTypeCtxs matchExpr
                    let (typeCtxs, newE) = traverse outerTypeCtxs e
                    (newMatchExpr, newE)
                let newEs = Array.map applyOne es
                let applyElse e =
                    let (typeCtxs, newE) = traverse outerTypeCtxs e
                    newE
                let newEls = Option.map applyElse els
                (emptyCondTypeContexts, FEMatch (newExpr, newEs, newEls))
            | FEInheritedFrom (f, nam) ->
                let newF = resolveExprReference outerTypeCtxs.Map f
                let (innerTypeCtxs, newNam) = resolveSubEntity layout outerTypeCtxs.Map SECInheritedFrom newF nam
                let ctx =
                    { Map = innerTypeCtxs
                      ExtraConditionals = false
                    }
                (ctx, FEInheritedFrom (newF, newNam))
            | FEOfType (f, nam) ->
                let newF = resolveExprReference outerTypeCtxs.Map f
                let (innerTypeCtxs, newNam) = resolveSubEntity layout outerTypeCtxs.Map SECOfType newF nam
                let ctx =
                    { Map = innerTypeCtxs
                      ExtraConditionals = false
                    }
                (ctx, FEOfType (newF, newNam))
            | FEDistinct (a, b) ->
                let (typeCtxs, newA) = traverse outerTypeCtxs a
                let (typeCtxs, newB) = traverse outerTypeCtxs b
                (emptyCondTypeContexts, FEDistinct (newA, newB))
            | FENotDistinct (a, b) ->
                let (typeCtxs, newA) = traverse outerTypeCtxs a
                let (typeCtxs, newB) = traverse outerTypeCtxs b
                (emptyCondTypeContexts, FENotDistinct (newA, newB))
            | FEBinaryOp (a, op, b) ->
                let (typeCtxs, newA) = traverse outerTypeCtxs a
                let (typeCtxs, newB) = traverse outerTypeCtxs b
                (emptyCondTypeContexts, FEBinaryOp (newA, op, newB))
            | FESimilarTo (e, pat) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let (typeCtxs, newPat) = traverse outerTypeCtxs pat
                (emptyCondTypeContexts, FESimilarTo (newE, newPat))
            | FENotSimilarTo (e, pat) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let (typeCtxs, newPat) = traverse outerTypeCtxs pat
                (emptyCondTypeContexts, FENotSimilarTo (newE, newPat))
            | FEIn (e, vals) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (emptyCondTypeContexts, FEIn (newE, newVals))
            | FENotIn (e, vals) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (emptyCondTypeContexts, FENotIn (newE, newVals))
            | FEInQuery (e, query) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let newQuery = resolveQuery query
                (emptyCondTypeContexts, FEInQuery (newE, newQuery))
            | FENotInQuery (e, query) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let newQuery = resolveQuery query
                (emptyCondTypeContexts, FENotInQuery (newE, newQuery))
            | FEAny (e, op, arr) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let (typeCtxs, newArr) = traverse outerTypeCtxs arr
                (emptyCondTypeContexts, FEAny (newE, op, newArr))
            | FEAll (e, op, arr) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                let (typeCtxs, newArr) = traverse outerTypeCtxs arr
                (emptyCondTypeContexts, FEAll (newE, op, newArr))
            | FECast (e, typ) ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                (emptyCondTypeContexts, FECast (newE, resolveCastFieldType typ))
            | FEIsNull e ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                (emptyCondTypeContexts, FEIsNull newE)
            | FEIsNotNull e ->
                let (typeCtxs, newE) = traverse outerTypeCtxs e
                (emptyCondTypeContexts, FEIsNotNull newE)
            | FEJsonArray vals ->
                let newVals = Array.map (traverse outerTypeCtxs >> snd) vals
                (emptyCondTypeContexts, FEJsonArray newVals)
            | FEJsonObject obj ->
                let newObj = Map.map (fun name -> traverse outerTypeCtxs >> snd) obj
                (emptyCondTypeContexts, FEJsonObject newObj)
            | FEFunc (name, args) ->
                let newArgs = Array.map (traverse outerTypeCtxs >> snd) args
                (emptyCondTypeContexts, FEFunc (name, newArgs))
            | FEAggFunc (name, args) ->
                exprInfo <- { exprInfo with Flags = { exprInfo.Flags with HasAggregates = true } }
                let newArgs = mapAggExpr (traverse outerTypeCtxs >> snd) args
                (emptyCondTypeContexts, FEAggFunc (name, newArgs))
            | FESubquery query ->
                let newQuery = resolveQuery query
                (emptyCondTypeContexts, FESubquery newQuery)

        let typeContexts =
            { Map = ctx.Types
              ExtraConditionals = false
            }
        let (typeContexts, ret) = traverse typeContexts expr
        let info =
            { Types = typeContexts.Map
              Info = exprInfo
            } : InnerResolvedExprInfo
        (info, ret)

    and resolveOrderLimitClause (ctx : Context) (limits : ParsedOrderLimitClause) : ResolvedExprInfo * ResolvedOrderLimitClause =
        let mutable exprInfo = emptyResolvedExprInfo

        let resolveOrderBy (ord : ParsedOrderColumn) =
            let (info, ret) = resolveFieldExpr ctx ord.Expr
            if info.Info.Flags.HasAggregates then
                raisef ViewResolveException "Aggregates are not allowed here"
            exprInfo <- unionResolvedExprInfo exprInfo info.Info
            { Expr = ret
              Order = ord.Order
              Nulls = ord.Nulls
            }

        let ret =
            { OrderBy = Array.map resolveOrderBy limits.OrderBy
              Limit = Option.map resolveLimitFieldExpr limits.Limit
              Offset = Option.map resolveLimitFieldExpr limits.Offset
            }
        (exprInfo, ret)

    and resolveCommonTableExpr (ctx : Context) (flags : SelectFlags) (allowRecursive : bool) (name : EntityName) (cte : ParsedCommonTableExpr) : SubqueryInfo * ResolvedCommonTableExpr =
        let (ctx, exprInfo, newCtes) =
            match cte.Expr.CTEs with
            | None -> (ctx, emptySubqueryExprInfo, None)
            | Some ctes ->
                let (info, newCtes) = resolveCommonTableExprs ctx flags ctes
                let ctx = { ctx with ResolveCTE = info.ResolveCTE }
                (ctx, info.ExprInfo, Some newCtes)

        let cteFlags =
            { viewExprSelectFlags with
                  NoAttributes = flags.NoAttributes
                  RequireNames = Option.isNone cte.Fields
            }

        let (results, tree) =
            match cte.Expr.Tree with
            | SSetOp setOp when allowRecursive ->
                // We trait left side of outer UNION as a non-recursive side.
                let (recResults, a') = resolveSelectExpr ctx cteFlags setOp.A
                let recResults =
                    match cte.Fields with
                    | None -> recResults
                    | Some names -> renameFieldsInSubquery names recResults
                let newResolveCte (currName : EntityName) =
                    if currName = name then
                        recResults
                    else
                        ctx.ResolveCTE currName
                let newCtx = { ctx with ResolveCTE = newResolveCte }
                let (results2, b') = resolveSelectExpr newCtx { cteFlags with RequireNames = false } setOp.B
                finishResolveSetOpExpr ctx setOp recResults a' results2 b' cteFlags
            | _ -> resolveSelectTreeExpr ctx cteFlags cte.Expr.Tree

        let results =
            match cte.Fields with
            | None -> results
            | Some names -> renameFieldsInSubquery names results
        let results = { results with ExprInfo = unionSubqueryExprInfo results.ExprInfo exprInfo }

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
              Materialized = cte.Materialized
              Extra = ObjectMap.singleton info
            }
        (results, newCte)

    and resolveCommonTableExprs (ctx : Context) (flags : SelectFlags) (ctes : ParsedCommonTableExprs) : CTEsInfo * ResolvedCommonTableExprs =
        let resolveCte = ctx.ResolveCTE
        let exprsMap =
            try
                Map.ofSeqUnique ctes.Exprs
            with
            | Failure msg -> raisef ViewResolveException "Clashing names in WITH binding: %s" msg
        let mutable resultsMap : Map<EntityName, SubqueryInfo * ResolvedCommonTableExpr> = Map.empty
        let mutable resolved : (EntityName * ResolvedCommonTableExpr) list = []
        let mutable exprInfo = emptySubqueryExprInfo

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
                            let (results, newCte) = resolveCommonTableExpr tmpCtx flags true currName cte
                            resultsMap <- Map.add currName (results, newCte) resultsMap
                            resolved <- (currName, newCte) :: resolved
                            exprInfo <- unionSubqueryExprInfo exprInfo results.ExprInfo
                            results
                    | None -> resolveCte currName
                    | Some (cteResults, cte) -> cteResults
                and tmpCtx = { ctx with ResolveCTE = tmpResolveCte }

                let (results, newCte) = resolveCommonTableExpr tmpCtx flags ctes.Recursive name cte
                resultsMap <- Map.add name (results, newCte) resultsMap
                exprInfo <- unionSubqueryExprInfo exprInfo results.ExprInfo
                resolved <- (name, newCte) :: resolved

        Array.iter resolveOne ctes.Exprs

        let newResolveCte (name : EntityName) =
            match Map.tryFind name resultsMap with
            | None -> resolveCte name
            | Some (results, cte) -> results
        let meta =
            { Bindings = Map.map (fun name (results, cte) -> cte) resultsMap
            } : ResolvedCommonTableExprsTempMeta
        let ctes =
            { Recursive = ctes.Recursive
              Exprs = resolved |> Seq.rev |> Array.ofSeq
              Extra = ObjectMap.singleton meta
            }
        let info =
            { ResolveCTE = newResolveCte
              ExprInfo = exprInfo
            }
        (info, ctes)

    and resolveSelectExpr (ctx : Context) (flags : SelectFlags) (select : ParsedSelectExpr) : SubqueryInfo * ResolvedSelectExpr =
        let (ctx, ctesInfo, newCtes) =
            match select.CTEs with
            | None -> (ctx, emptySubqueryExprInfo, None)
            | Some ctes ->
                let (info, newCtes) = resolveCommonTableExprs ctx flags ctes
                let ctx = { ctx with ResolveCTE = info.ResolveCTE }
                (ctx, info.ExprInfo, Some newCtes)
        let (results, tree) = resolveSelectTreeExpr ctx flags select.Tree
        let newSelect =
            { CTEs = newCtes
              Tree = tree
              Extra = ObjectMap.empty
            }
        let results = { results with ExprInfo = unionSubqueryExprInfo results.ExprInfo ctesInfo }
        (results, newSelect)

    and finishResolveSetOpExpr (ctx : Context) (setOp : ParsedSetOperationExpr) (results1 : SubqueryInfo) (a : ResolvedSelectExpr) (results2 : SubqueryInfo) (b : ResolvedSelectExpr) (flags : SelectFlags) : SubqueryInfo * ResolvedSelectTreeExpr =
        if flags.RequireNames then
            for (name, info) in results1.Fields do
                if Option.isNone name then
                    raisef ViewResolveException "Name is required for column"
        let newResults = unionSubqueryInfo results1 results2
        // Bogus entity id for incompatible fields that are merged together.
        let setOpEntityId = nextFromEntityId ()
        let mapField (fieldName, fieldInfo) =
            let ref = { Entity = None; Name = fieldName } : FieldRef
            let mapping =
                match fieldInfo with
                | None -> FMUnbound
                | Some bound -> FMBound bound
            let info =
                { Entity = None
                  Mapping = mapping
                  ForceSQLName = None
                  EntityId = setOpEntityId
                }
            (ref, FVResolved info)
        let orderLimitMapping =
            getFieldsMap newResults.Fields
            |> Map.toSeq
            |> Seq.map mapField
            |> Map.ofSeq
        let orderLimitCtx = { ctx with FieldMaps = orderLimitMapping :: ctx.FieldMaps }
        let (limitsInfo, resolvedLimits) = resolveOrderLimitClause orderLimitCtx setOp.OrderLimit
        if limitsInfo.Flags.HasArrows then
            raisef ViewResolveException "Dereferences are not allowed in ORDER BY clauses for set expressions: %O" resolvedLimits.OrderBy
        let newResults = { newResults with ExprInfo = unionSubqueryExprInfo newResults.ExprInfo (resolvedToSubqueryExprInfo limitsInfo) }
        let ret =
            { Operation = setOp.Operation
              AllowDuplicates = setOp.AllowDuplicates
              A = a
              B = b
              OrderLimit = resolvedLimits
            }
        (newResults, SSetOp ret)

    and resolveSelectTreeExpr (ctx : Context) (flags : SelectFlags) : ParsedSelectTreeExpr -> SubqueryInfo * ResolvedSelectTreeExpr = function
        | SSelect query ->
            let (results, res) = resolveSingleSelectExpr ctx flags query
            (results, SSelect res)
        | SValues values ->
            if flags.RequireNames then
                raisef ViewResolveException "Column aliases are required for this VALUES entry"
            let valuesLength = Array.length values.[0]

            let mutable exprInfo = emptySubqueryExprInfo
            let mapValue expr =
                let (info, newExpr) = resolveFieldExpr ctx expr
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
                newExpr
            let mapEntry entry =
                if Array.length entry <> valuesLength then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map mapValue

            let newValues = values |> Array.map mapEntry
            let fields = Seq.init valuesLength (fun i -> (None, None)) |> Array.ofSeq
            let info =
                { Fields = fields
                  ExprInfo = exprInfo
                }
            (info, SValues newValues)
        | SSetOp setOp ->
            let (results1, a') = resolveSelectExpr ctx flags setOp.A
            let (results2, b') = resolveSelectExpr ctx { flags with RequireNames = false } setOp.B
            finishResolveSetOpExpr ctx setOp results1 a' results2 b' flags

    and resolveSingleSelectExpr (ctx : Context) (flags : SelectFlags) (query : ParsedSingleSelectExpr) : SubqueryInfo * ResolvedSingleSelectExpr =
        if flags.NoAttributes && not (Map.isEmpty query.Attributes) then
            raisef ViewResolveException "Attributes are not allowed in query expressions"

        let mutable exprInfo = emptySubqueryExprInfo

        let (ctx, qFrom) =
            match query.From with
            | None -> (ctx, None)
            | Some from ->
                let (info, res) = resolveFromExpr ctx emptyFromExprInfo true flags from
                exprInfo <- unionSubqueryExprInfo exprInfo info.ExprInfo
                let ctx = addFromToContext info ctx
                (ctx, Some res)

        let myResolveNonaggrFieldExpr expr =
            let (info, newExpr) = resolveNonaggrFieldExpr ctx expr
            exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
            (info, newExpr)

        let (whereTypes, qWhere) =
            match query.Where with
            | None -> (Map.empty, None)
            | Some where ->
                let (info, res) = myResolveNonaggrFieldExpr where
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
                (info.Types, Some res)
        let qGroupBy = Array.map (myResolveNonaggrFieldExpr >> snd) query.GroupBy

        let (attrInfo, attributes) = resolveAttributesMap ctx query.Attributes
        exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo attrInfo)

        let (orderLimitInfo, orderLimit) = resolveOrderLimitClause ctx query.OrderLimit
        exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo orderLimitInfo)

        let rawResults = Array.map (resolveResult flags ctx) query.Results
        let hasAggregates = Array.exists (fun (info : ResultInfo, expr) -> info.ExprInfo.Flags.HasAggregates) rawResults || not (Array.isEmpty qGroupBy)
        let results = Array.map snd rawResults
        let getFields : ResultInfo * ResolvedQueryResult -> FieldName option * BoundField option = function
            | (_, QRAll alias) -> failwith "Impossible QRAll"
            | (info, QRExpr result) -> (result.TryToName (), if hasAggregates then None else info.Field)
        let newFields = rawResults |> Array.map getFields
        try
            newFields |> Seq.mapMaybe fst |> Set.ofSeqUnique |> ignore
        with
            | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

        let info =
            { Fields = newFields
              ExprInfo = exprInfo
            }

        let extra =
            { HasAggregates = hasAggregates
            } : ResolvedSingleSelectMeta

        let qFrom = Option.map (relabelFromExprType whereTypes) qFrom

        let newQuery =
            { Attributes = attributes
              From = qFrom
              Where = qWhere
              GroupBy = qGroupBy
              Results = results
              OrderLimit = orderLimit
              Extra = ObjectMap.singleton extra
            }
        (info, newQuery)

    // Set<EntityName> here is used to check uniqueness of puns.
    and resolveFromExpr (ctx : Context) (fromInfo : FromExprInfo) (isInner : bool) (flags : SelectFlags) : ParsedFromExpr -> FromExprInfo * ResolvedFromExpr = function
        | FEntity entity ->
            let entityInfo = fromEntityMapping ctx isInner entity
            let fromInfo = unionFromExprInfo fromInfo entityInfo.SingleFrom
            let extra =
                { Id = entityInfo.Id
                  IsInner = isInner
                } : FromEntityIdMeta
            let newEntity = { entity with Extra = ObjectMap.singleton extra }
            (fromInfo, FEntity newEntity)
        | FJoin join ->
            let isInner1 =
                match join.Type with
                | Left | Inner -> isInner
                | _ -> false
            let (infoA, newA) = resolveFromExpr ctx fromInfo isInner1 flags join.A
            let isInner2 =
                match join.Type with
                | Right | Inner -> isInner
                | _ -> false
            let (infoB, newB) = resolveFromExpr ctx infoA isInner2 flags join.B

            let localCtx = addFromToContext infoB ctx
            let (innerInfo, newFieldExpr) = resolveFieldExpr localCtx join.Condition
            if innerInfo.Info.Flags.HasAggregates then
                raisef ViewResolveException "Cannot use aggregate functions in join expression"
            let retInfo =
                { infoB with ExprInfo = unionSubqueryExprInfo infoB.ExprInfo (resolvedToSubqueryExprInfo innerInfo.Info)
                }
            let newJoin =
                { Type = join.Type
                  A = newA
                  B = newB
                  Condition = newFieldExpr
                }
            (retInfo, FJoin newJoin)
        | FSubExpr subsel ->
            let localCtx =
                if subsel.Lateral then
                    addFromToContext fromInfo ctx
                else
                    ctx
            // TODO: Currently we unbind context because we don't support arrows in sub-expressions.
            let (info, newQ) = resolveSelectExpr (subSelectContext localCtx) { flags with RequireNames = Option.isNone subsel.Alias.Fields } subsel.Select
            let fields = applyAlias subsel.Alias info.Fields
            let fieldsMap = getFieldsMap fields
            let mappingRef = { Schema = None; Name = subsel.Alias.Name } : EntityRef
            let fromEntityId = nextFromEntityId ()
            let newMapping = fieldsToFieldMapping fromEntityId (Some mappingRef) fieldsMap
            let newSubsel = { Alias = subsel.Alias; Lateral = subsel.Lateral; Select = newQ }
            let entityInfo =
                { Schema = None
                  EntityAttributes = Set.empty
                  Id = fromEntityId
                }
            let myFromInfo =
                { Entities = Map.singleton subsel.Alias.Name entityInfo
                  Fields = newMapping
                  ExprInfo = info.ExprInfo
                  // TODO: pass type contexts from sub-selects.
                  Types = Map.empty
                }
            let fromInfo = unionFromExprInfo fromInfo myFromInfo
            (fromInfo, FSubExpr newSubsel)

    and resolveInsertValue (ctx : Context) : ParsedInsertValue -> ResolvedExprInfo * ResolvedInsertValue = function
        | IVDefault ->
            (emptyResolvedExprInfo, IVDefault)
        | IVValue expr ->
            let (info, newExpr) = resolveFieldExpr ctx expr
            (info.Info, IVValue newExpr)

    and resolveInsertExpr (ctx : Context) (flags : SelectFlags) (insert : ParsedInsertExpr) : SubqueryInfo * ResolvedInsertExpr =
        let mutable exprInfo = emptySubqueryExprInfo

        let (ctx, newCtes) =
            match insert.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (cteInfo, newCtes) = resolveCommonTableExprs ctx flags ctes
                exprInfo <- unionSubqueryExprInfo exprInfo cteInfo.ExprInfo
                let ctx = { ctx with ResolveCTE = cteInfo.ResolveCTE }
                (ctx, Some newCtes)

        let entityRef = resolveEntityRef insert.Entity.Ref
        let entity = findEntityByRef entityRef false
        if entity.IsAbstract then
            raisef ViewResolveException "Entity %O is abstract" entityRef

        let fields =
            try
                Set.ofSeqUnique insert.Fields
            with
            | Failure err -> raisef ViewResolveException "Clashing insert field: %s" err
        for fieldName in insert.Fields do
            match entity.FindField fieldName with
            | Some { ForceRename = false; Field = RColumnField field } -> ()
            | _ -> raisef ViewResolveException "Field not found: %O" { Entity = entityRef; Name = fieldName }
        for (fieldName, field) in entity.ColumnFields do
            if not (fieldIsOptional field) && not (Set.contains fieldName fields) then
                raisef ViewResolveException "Required field not provided: %O" fieldName

        let myResolveInsertValue value =
            let (info, newVal) = resolveInsertValue ctx value
            exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info)
            newVal

        let resolveInsertValues vals =
            if Array.length vals <> Array.length insert.Fields then
                raisef ViewResolveException "Number of values is not consistent"
            Array.map myResolveInsertValue vals

        let source =
            match insert.Source with
            | ISDefaultValues -> ISDefaultValues
            | ISValues allVals -> ISValues <| Array.map resolveInsertValues allVals
            | ISSelect select ->
                let (selInfo, resSelect) = resolveSelectExpr ctx subExprSelectFlags select
                if Array.length selInfo.Fields <> Array.length insert.Fields then
                    raisef ViewResolveException "Queries in INSERT must have the same number of columns as number of inserted columns"
                exprInfo <- unionSubqueryExprInfo exprInfo selInfo.ExprInfo
                ISSelect resSelect
        let newExpr =
            { CTEs = newCtes
              Entity = insert.Entity
              Fields = insert.Fields
              Source = source
              Extra = ObjectMap.empty
            }
        let subqueryInfo =
            { Fields = [||]
              ExprInfo = exprInfo
            }
        (subqueryInfo, newExpr)

    and resolveUpdateExpr (ctx : Context) (flags : SelectFlags) (update : ParsedUpdateExpr) : SubqueryInfo * ResolvedUpdateExpr =
        let mutable exprInfo = emptySubqueryExprInfo

        let (ctx, newCtes) =
            match update.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (ctesInfo, newCtes) = resolveCommonTableExprs ctx flags ctes
                exprInfo <- unionSubqueryExprInfo exprInfo ctesInfo.ExprInfo
                let ctx = { ctx with ResolveCTE = ctesInfo.ResolveCTE }
                (ctx, Some newCtes)
        let entityFromInfo = fromEntityMapping emptyContext true update.Entity

        let entityRef = resolveEntityRef update.Entity.Ref
        let entity = findEntityByRef entityRef false

        let (entityInfo, from) =
            match update.From with
            | None -> (entityFromInfo.SingleFrom, None)
            | Some from ->
                let (newEntityInfo, newFrom) = resolveFromExpr ctx entityFromInfo.SingleFrom true flags from
                (newEntityInfo, Some newFrom)
        exprInfo <- unionSubqueryExprInfo exprInfo entityInfo.ExprInfo
        let ctx = addFromToContext entityInfo ctx

        let mutable columns = Set.empty

        let checkField fieldName =
            match entity.FindField fieldName with
            | Some { ForceRename = false; Field = RColumnField field } ->
                if field.IsImmutable then
                    raisef ViewResolveException "Field %O is immutable" fieldName
            | _ -> raisef ViewResolveException "Field not found: %O" { Entity = entityRef; Name = fieldName }
            if Set.contains fieldName columns then
                raisef ViewResolveException "Field %O is assigned multiple times" fieldName
            columns <- Set.add fieldName columns

        let resolveFieldUpdate = function
            | UAESet (fieldName, expr) ->
                checkField fieldName
                let (info, newExpr) = resolveInsertValue ctx expr
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info)
                UAESet (fieldName, newExpr)
            | UAESelect (cols, select) ->
                for col in cols do
                    checkField col
                let (selectInfo, newSelect) = resolveSelectExpr ctx subExprSelectFlags select
                exprInfo <- unionSubqueryExprInfo exprInfo selectInfo.ExprInfo
                if Array.length selectInfo.Fields <> Array.length cols then
                    raisef ViewResolveException "Queries in UPDATE must have the same number of columns as number of updated columns"
                UAESelect (cols, newSelect)
        let assigns = Array.map resolveFieldUpdate update.Assignments

        let (whereTypes, where) =
            match update.Where with
            | None -> (Map.empty, None)
            | Some expr ->
                let (info, newExpr) = resolveFieldExpr ctx expr
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
                (info.Types, Some newExpr)

        let from = Option.map (relabelFromExprType whereTypes) from

        let newExpr =
            { CTEs = newCtes
              Entity = update.Entity
              Assignments = assigns
              From = from
              Where = where
              Extra = ObjectMap.empty
            }
        let subqueryInfo =
            { Fields = [||]
              ExprInfo = exprInfo
            }
        (subqueryInfo, newExpr)

    and resolveDeleteExpr (ctx : Context) (flags : SelectFlags) (delete : ParsedDeleteExpr) : SubqueryInfo * ResolvedDeleteExpr =
        let mutable exprInfo = emptySubqueryExprInfo

        let (ctx, newCtes) =
            match delete.CTEs with
            | None -> (ctx, None)
            | Some ctes ->
                let (ctesInfo, newCtes) = resolveCommonTableExprs ctx flags ctes
                exprInfo <- unionSubqueryExprInfo exprInfo ctesInfo.ExprInfo
                let ctx = { ctx with ResolveCTE = ctesInfo.ResolveCTE }
                (ctx, Some newCtes)
        let entityFromInfo = fromEntityMapping emptyContext true delete.Entity

        let (entityInfo, using) =
            match delete.Using with
            | None -> (entityFromInfo.SingleFrom, None)
            | Some from ->
                let (newEntityInfo, newFrom) = resolveFromExpr ctx entityFromInfo.SingleFrom true flags from
                (newEntityInfo, Some newFrom)
        exprInfo <- unionSubqueryExprInfo exprInfo entityInfo.ExprInfo
        let ctx = addFromToContext entityInfo ctx

        let (whereTypes, where) =
            match delete.Where with
            | None -> (Map.empty, None)
            | Some expr ->
                let (info, newExpr) = resolveFieldExpr ctx expr
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
                (info.Types, Some newExpr)

        let using = Option.map (relabelFromExprType whereTypes) using

        let newExpr =
            { CTEs = newCtes
              Entity = delete.Entity
              Using = using
              Where = where
              Extra = ObjectMap.empty
            }
        let subqueryInfo =
            { Fields = [||]
              ExprInfo = exprInfo
            }
        (subqueryInfo, newExpr)

    and resolveDataExpr (ctx : Context) (flags : SelectFlags) (dataExpr : ParsedDataExpr) : SubqueryInfo * ResolvedDataExpr =
        match dataExpr with
        | DESelect select ->
            let (fields, newSelect) = resolveSelectExpr ctx flags select
            (fields, DESelect newSelect)
        | DEInsert insert ->
            let (fields, newInsert) = resolveInsertExpr ctx flags insert
            (fields, DEInsert newInsert)
        | DEUpdate update ->
            let (fields, newUpdate) = resolveUpdateExpr ctx flags update
            (fields, DEUpdate newUpdate)
        | DEDelete delete ->
            let (fields, newDelete) = resolveDeleteExpr ctx flags delete
            (fields, DEDelete newDelete)

    member this.ResolveSelectExpr flags select = resolveSelectExpr emptyContext flags select

    member this.ResolveDataExpr flags dataExpr = resolveDataExpr emptyContext flags dataExpr

    member this.ResolveSingleFieldExpr (fromEntityId : FromEntityId) (fromMapping : SingleFromMapping) expr =
        let mapping =
            match fromMapping with
            | SFEntity ref ->
                let flags =
                    { IsInner = true
                      AllowHidden = true
                      WithoutChildren = false
                    }
                createFromMapping fromEntityId ref None flags
            | SFCustom custom -> customToFieldMapping layout fromEntityId true custom
        let context =
            { ResolveCTE = failCte
              FieldMaps = [mapping]
              // Not exactly right, but should't be a problem for simple expressions.
              Entities = Map.empty
              Types = Map.empty
              LocalEntities = Set.singleton fromEntityId
              NoArguments = false
            }
        resolveFieldExpr context expr

    member this.ResolveArgumentAttributesMap (attrs : ParsedBoundAttributesMap) : ResolvedBoundAttributesMap =
        let boundExprInfo =
            { emptyResolvedExprInfo with
                Flags = { emptyResolvedExprFlags with HasArguments = true }
            }
        let resolveOne name (expr : ParsedBoundAttribute) : ResolvedBoundAttribute =
            let (info, res) = resolveBoundAttribute emptyContext boundExprInfo expr
            res

        Map.map resolveOne attrs

    member this.ResolveEntityAttributesMap (entityRef : ResolvedEntityRef) (attrs : ParsedBoundAttributesMap) : ResolvedBoundAttributesMap =
        let flags =
            { IsInner = true
              AllowHidden = true
              WithoutChildren = false
            }
        let mapping = createFromMapping 0 entityRef None flags
        let context =
            { ResolveCTE = failCte
              FieldMaps = [mapping]
              // Not exactly right, but should't be a problem for simple expressions.
              Entities = Map.empty
              Types = Map.empty
              LocalEntities = Set.singleton 0
              NoArguments = false
            }
        let (info, ret) = resolveBoundAttributesMap context fieldResolvedExprInfo attrs
        ret

    member this.Privileged = isPrivileged

// Remove and replace unneeded Extra fields.
let rec private relabelSelectExpr (select : ResolvedSelectExpr) : ResolvedSelectExpr =
    { CTEs = Option.map relabelCommonTableExprs select.CTEs
      Tree = relabelSelectTreeExpr select.Tree
      Extra = select.Extra
    }

and private relabelInsertValue = function
    | IVDefault -> IVDefault
    | IVValue expr -> IVValue <| relabelFieldExpr expr

and private relabelInsertExpr (insert : ResolvedInsertExpr) : ResolvedInsertExpr =
    let source =
        match insert.Source with
        | ISDefaultValues -> ISDefaultValues
        | ISValues allVals -> ISValues <| Array.map (Array.map relabelInsertValue) allVals
        | ISSelect select -> ISSelect <| relabelSelectExpr select
    { CTEs = Option.map relabelCommonTableExprs insert.CTEs
      Entity = insert.Entity
      Fields = insert.Fields
      Source = source
      Extra = insert.Extra
    }

and private relabelUpdateAssignExpr = function
    | UAESet (name, expr) -> UAESet (name, expr)
    | UAESelect (cols, select) -> UAESelect (cols, relabelSelectExpr select)

and private relabelUpdateExpr (update : ResolvedUpdateExpr) : ResolvedUpdateExpr =
    { CTEs = Option.map relabelCommonTableExprs update.CTEs
      Entity = update.Entity
      Assignments = Array.map relabelUpdateAssignExpr update.Assignments
      From = Option.map relabelFromExpr update.From
      Where = Option.map relabelFieldExpr update.Where
      Extra = update.Extra
    }

and private relabelDeleteExpr (delete : ResolvedDeleteExpr) : ResolvedDeleteExpr =
    { CTEs = Option.map relabelCommonTableExprs delete.CTEs
      Entity = delete.Entity
      Using = Option.map relabelFromExpr delete.Using
      Where = Option.map relabelFieldExpr delete.Where
      Extra = delete.Extra
    }

and private relabelDataExpr = function
    | DESelect select -> DESelect <| relabelSelectExpr select
    | DEInsert insert -> DEInsert <| relabelInsertExpr insert
    | DEUpdate update -> DEUpdate <| relabelUpdateExpr update
    | DEDelete delete -> DEDelete <| relabelDeleteExpr delete

and private relabelCommonTableExprs (ctes : ResolvedCommonTableExprs) : ResolvedCommonTableExprs =
    { Recursive = ctes.Recursive
      Exprs = Array.map (fun (name, cte) -> (name, relabelCommonTableExpr cte)) ctes.Exprs
      Extra = ObjectMap.empty
    }

and private relabelCommonTableExpr (cte : ResolvedCommonTableExpr) : ResolvedCommonTableExpr =
    let extra = ObjectMap.findType<ResolvedCommonTableExprTempMeta> cte.Extra
    { Fields = cte.Fields
      Expr = relabelSelectExpr cte.Expr
      Materialized = cte.Materialized
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

and private relabelBoundAttributeExpr : ResolvedBoundAttributeExpr -> ResolvedBoundAttributeExpr = function
    | BAExpr expr -> BAExpr (relabelFieldExpr expr)
    | BAMapping _ as mapping -> mapping

and private relabelAttribute (attr : ResolvedAttribute) : ResolvedAttribute =
    { Dependency = attr.Dependency
      Expression = relabelFieldExpr attr.Expression
    }

and private relabelBoundAttribute (attr : ResolvedBoundAttribute) : ResolvedBoundAttribute =
    { Dependency = attr.Dependency
      Expression = relabelBoundAttributeExpr attr.Expression
    }

and private relabelSingleSelectExpr (select : ResolvedSingleSelectExpr) : ResolvedSingleSelectExpr =
    { Attributes = Map.map (fun name -> relabelAttribute) select.Attributes
      Results = Array.map relabelQueryResult select.Results
      From  = Option.map relabelFromExpr select.From
      Where = Option.map relabelFieldExpr select.Where
      GroupBy = Array.map relabelFieldExpr select.GroupBy
      OrderLimit = relabelOrderLimitClause select.OrderLimit
      Extra = select.Extra
    }

and private relabelFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
    | FEntity ent -> FEntity ent
    | FJoin join ->
        FJoin
            { Type = join.Type
              A = relabelFromExpr join.A
              B = relabelFromExpr join.B
              Condition = relabelFieldExpr join.Condition
            }
    | FSubExpr subsel -> FSubExpr { subsel with Select = relabelSelectExpr subsel.Select }

and private relabelFieldExpr (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
    let mapper = { idFieldExprMapper with Query = relabelSelectExpr }
    mapFieldExpr mapper expr

and private relabelOrderLimitClause (clause : ResolvedOrderLimitClause) : ResolvedOrderLimitClause =
    { Offset = Option.map relabelFieldExpr clause.Offset
      Limit = Option.map relabelFieldExpr clause.Limit
      OrderBy = Array.map relabelOrderColumn clause.OrderBy
    }

and private relabelOrderColumn (ord : ResolvedOrderColumn) : ResolvedOrderColumn =
    { Expr = relabelFieldExpr ord.Expr
      Order = ord.Order
      Nulls = ord.Nulls
    }

and private relabelQueryResult : ResolvedQueryResult -> ResolvedQueryResult = function
    | QRAll alias -> QRAll alias
    | QRExpr result -> QRExpr <| relabelQueryColumnResult result

and private relabelQueryColumnResult (result : ResolvedQueryColumnResult) : ResolvedQueryColumnResult =
    { Attributes = Map.map (fun name -> relabelBoundAttribute) result.Attributes
      Result = relabelFieldExpr result.Result
      Alias = result.Alias
    }

let compileScalarType : ScalarFieldType<_> -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate
    | SFTInterval -> SQL.STInterval
    | SFTJson -> SQL.STJson
    | SFTUserViewRef -> SQL.STJson
    | SFTUuid -> SQL.STUuid
    | SFTReference (ent, opts) -> SQL.STInt
    | SFTEnum vals -> SQL.STString

let compileFieldType : FieldType<_> -> SQL.SimpleValueType = function
    | FTScalar stype -> SQL.VTScalar <| compileScalarType stype
    | FTArray stype -> SQL.VTArray <| compileScalarType stype

let isScalarValueSubtype (layout : ILayoutBits) (wanted : ResolvedScalarFieldType) (given : ResolvedScalarFieldType) : bool =
    match (wanted, given) with
    | (a, b) when a = b -> true
    | (SFTReference (wantedRef, optsA), SFTReference (givenRef, optsB)) ->
        let wantedEntity = layout.FindEntity wantedRef |> Option.get
        Map.containsKey givenRef wantedEntity.Children
    | (SFTEnum wantedVals, SFTEnum givenVals) -> OrderedSet.difference givenVals wantedVals |> OrderedSet.isEmpty
    | (SFTInt, SFTReference _) -> true
    | (SFTString, SFTEnum _) -> true
    | (a, b) -> SQL.tryImplicitCasts (compileScalarType a) (compileScalarType b)
    | _ -> false

let isSubtype (layout : ILayoutBits) (wanted : ResolvedFieldType) (given : ResolvedFieldType) : bool =
    match (wanted, given) with
    | (FTScalar wantedTyp, FTScalar givenTyp)
    | (FTArray wantedTyp, FTArray givenTyp) -> isScalarValueSubtype layout wantedTyp givenTyp
    | _ -> false

let isMaybeSubtype (layout : ILayoutBits) (wanted : ResolvedFieldType) (maybeGiven : ResolvedFieldType option) : bool =
    match maybeGiven with
    | None -> true
    | Some given -> isSubtype layout wanted given

let private emptyLayoutBits =
    { new ILayoutBits with
          member this.FindEntity ent = None
          member this.HasVisibleEntity ent = false
    }

let isValueOfSubtype (wanted : ResolvedFieldType) (value : FieldValue) : bool =
    match (wanted, value) with
    | (FTScalar (SFTEnum wantedVals), FString str) -> wantedVals.Contains str
    | (FTArray (SFTEnum wantedVals), FStringArray strs) -> Seq.forall (fun str -> wantedVals.Contains str) wantedVals
    | _ -> isMaybeSubtype emptyLayoutBits wanted (fieldValueType value)

let private resolveArgument (layout : ILayoutBits) (arg : ParsedArgument) : ResolvedArgument =
    let argType = resolveFieldType layout false arg.ArgType
    match arg.DefaultValue with
    | None -> ()
    | Some def ->
        match def with
        | FNull when not arg.Optional ->
            raisef ViewResolveException "Invalid default value, expected %O" argType
        | _ -> ()
        if not (isValueOfSubtype argType def) then
            raisef ViewResolveException "Invalid default value, expected %O" argType
    { ArgType = argType
      Optional = arg.Optional
      DefaultValue = arg.DefaultValue
      Attributes = Map.empty
    }

let private resolveArgumentsMap (layout : ILayoutBits) (rawArguments : ParsedArgumentsMap) : ResolvedArgumentsMap * Map<ArgumentRef, ResolvedArgument> =
    let halfLocalArguments = rawArguments |> OrderedMap.map (fun name arg -> resolveArgument layout arg)
    let halfAllArguments = Map.unionUnique (halfLocalArguments |> OrderedMap.toMap |> Map.mapKeys PLocal) globalArgumentsMap

    let attrsQualifier = QueryResolver (layout, halfAllArguments, emptyExprResolutionFlags)

    let resolveAttrs name (arg : ResolvedArgument) =
        let rawArg = OrderedMap.find name rawArguments
        let newArgs = attrsQualifier.ResolveArgumentAttributesMap rawArg.Attributes
        (PLocal name, { arg with Attributes = newArgs })

    let localArguments = OrderedMap.mapWithKeys resolveAttrs halfLocalArguments
    let allArguments = Map.unionUnique (OrderedMap.toMap localArguments) globalArgumentsMap

    (localArguments, allArguments)

let resolveSelectExpr (layout : ILayoutBits) (arguments : ParsedArgumentsMap) (flags : ExprResolutionFlags) (select : ParsedSelectExpr) : ResolvedArgumentsMap * ResolvedSelectExpr =
    let (localArguments, allArguments) = resolveArgumentsMap layout arguments
    let qualifier = QueryResolver (layout, allArguments, flags)
    let (info, qQuery) = qualifier.ResolveSelectExpr subExprSelectFlags select
    if Array.length info.Fields <> 1 then
        raisef ViewResolveException "Expression queries must have only one resulting column"
    (localArguments, relabelSelectExpr qQuery)

type SingleFieldExprInfo =
    { LocalArguments : ResolvedArgumentsMap
      Flags : ResolvedExprFlags
    }

let resolveSingleFieldExpr (layout : ILayoutBits) (arguments : ParsedArgumentsMap) (fromEntityId : FromEntityId) (flags : ExprResolutionFlags) (fromMapping : SingleFromMapping) (expr : ParsedFieldExpr) : SingleFieldExprInfo * ResolvedFieldExpr =
    let (localArguments, allArguments) = resolveArgumentsMap layout arguments
    let qualifier = QueryResolver (layout, allArguments, flags)
    let (exprInfo, qExpr) = qualifier.ResolveSingleFieldExpr fromEntityId fromMapping expr
    let singleInfo =
        { LocalArguments = localArguments
          Flags = exprInfo.Info.Flags
        }
    (singleInfo, relabelFieldExpr qExpr)

let resolveEntityAttributesMap (layout : ILayoutBits) (flags : ExprResolutionFlags) (entityRef : ResolvedEntityRef) (attrs : ParsedBoundAttributesMap) : ResolvedBoundAttributesMap =
    let qualifier = QueryResolver (layout, globalArgumentsMap, flags)
    qualifier.ResolveEntityAttributesMap entityRef attrs

let resolveViewExpr (layout : ILayoutBits) (flags : ExprResolutionFlags) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let (localArguments, allArguments) = resolveArgumentsMap layout viewExpr.Arguments
    let qualifier = QueryResolver (layout, allArguments, flags)
    let (info, qQuery) = qualifier.ResolveSelectExpr viewExprSelectFlags viewExpr.Select
    let mainEntity = Option.map (resolveMainEntity layout (getFieldsMap info.Fields) qQuery) viewExpr.MainEntity
    { Pragmas = Map.filter (fun name v -> Set.contains name allowedPragmas) viewExpr.Pragmas
      Arguments = localArguments
      Select = relabelSelectExpr qQuery
      MainEntity = mainEntity
      Privileged = qualifier.Privileged
    }

let resolveCommandExpr (layout : ILayoutBits) (flags : ExprResolutionFlags) (cmdExpr : ParsedCommandExpr) : ResolvedCommandExpr =
    let (localArguments, allArguments) = resolveArgumentsMap layout cmdExpr.Arguments
    let qualifier = QueryResolver (layout, allArguments, flags)
    let (results, qCommand) = qualifier.ResolveDataExpr viewExprSelectFlags cmdExpr.Command
    { Pragmas = Map.filter (fun name v -> Set.contains name allowedPragmas) cmdExpr.Pragmas
      Arguments = localArguments
      Command = relabelDataExpr qCommand
      Privileged = qualifier.Privileged
    }

let makeSingleFieldExpr (boundEntityRef : ResolvedEntityRef) (isInner : bool) (fieldRef : FieldRef) : ResolvedFieldExpr =
    let boundInfo =
        { Ref =
            { Entity = boundEntityRef
              Name = fieldRef.Name
            }
          Immediate = true
          Path = [||]
          IsInner = isInner
        } : BoundFieldMeta
    let fieldInfo =
        { Bound = Some boundInfo
          FromEntityId = localExprFromEntityId
          ForceSQLName = None
        } : FieldMeta
    let column =
        { Ref = VRColumn fieldRef
          Path = [||]
          AsRoot = false
        } : LinkedFieldRef
    let resultColumn =
        { Ref = column
          Extra = ObjectMap.singleton fieldInfo
        }
    FERef resultColumn