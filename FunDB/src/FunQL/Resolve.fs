module FunWithFlags.FunDB.FunQL.Resolve

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Typecheck

// Validates fields and expressions to the point where database can catch all the remaining problems
// and all further processing code can avoid any checks.
// ...that is, except checking references to other views.

type ViewResolveException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ViewResolveException (message, innerException, isUserException innerException)

    new (message : string) = ViewResolveException (message, null, true)

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

let decompileScalarType : SQL.SimpleType -> ScalarFieldType<_> = function
    | SQL.STInt -> SFTInt
    | SQL.STBigInt -> failwith "Unexpected bigint encountered"
    | SQL.STRegclass -> failwith "Unexpected regclass encountered"
    | SQL.STDecimal -> SFTDecimal
    | SQL.STString -> SFTString
    | SQL.STBool -> SFTBool
    | SQL.STDateTime -> SFTDateTime
    | SQL.STLocalDateTime -> failwith "Unexpected timestamp encountered"
    | SQL.STDate -> SFTDate
    | SQL.STInterval -> SFTInterval
    | SQL.STJson -> SFTJson
    | SQL.STUuid -> SFTUuid

let decompileFieldType : SQL.SimpleValueType -> FieldType<_> = function
    | SQL.VTScalar typ -> FTScalar <| decompileScalarType typ
    | SQL.VTArray typ -> FTArray <| decompileScalarType typ

let compileBinaryOp = function
    | BOLess -> SQL.BOLess
    | BOLessEq -> SQL.BOLessEq
    | BOGreater -> SQL.BOGreater
    | BOGreaterEq -> SQL.BOGreaterEq
    | BOEq -> SQL.BOEq
    | BONotEq -> SQL.BONotEq
    | BOConcat -> SQL.BOConcat
    | BOLike -> SQL.BOLike
    | BOILike -> SQL.BOILike
    | BONotLike -> SQL.BONotLike
    | BONotILike -> SQL.BONotILike
    | BOMatchRegex -> SQL.BOMatchRegex
    | BOMatchRegexCI -> SQL.BOMatchRegexCI
    | BONotMatchRegex -> SQL.BONotMatchRegex
    | BONotMatchRegexCI -> SQL.BONotMatchRegexCI
    | BOPlus -> SQL.BOPlus
    | BOMinus -> SQL.BOMinus
    | BOMultiply -> SQL.BOMultiply
    | BODivide -> SQL.BODivide
    | BOJsonArrow -> SQL.BOJsonArrow
    | BOJsonTextArrow -> SQL.BOJsonTextArrow

let unionTypes (args : (ResolvedFieldType option) seq) : ResolvedFieldType option =
    let sqlArgs = args |> Seq.map (Option.map compileFieldType)
    SQL.unionTypes sqlArgs |> Option.map decompileFieldType

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

let isSubtype (layout : ILayoutBits) (wanted : ResolvedFieldType) (given : ResolvedFieldType) : bool =
    match (wanted, given) with
    | (FTScalar wantedTyp, FTScalar givenTyp)
    | (FTArray wantedTyp, FTArray givenTyp) -> isScalarValueSubtype layout wantedTyp givenTyp
    | _ -> false

let isMaybeSubtype (layout : ILayoutBits) (wanted : ResolvedFieldType) (maybeGiven : ResolvedFieldType option) : bool =
    match maybeGiven with
    | None -> true
    | Some given -> isSubtype layout wanted given

let isValueOfSubtype (wanted : ResolvedFieldType) (value : FieldValue) : bool =
    match (wanted, value) with
    | (FTScalar (SFTEnum wantedVals), FString str) -> wantedVals.Contains str
    | (FTArray (SFTEnum wantedVals), FStringArray strs) -> Seq.forall (fun str -> wantedVals.Contains str) wantedVals
    | _ -> isMaybeSubtype emptyLayoutBits wanted (fieldValueType value)

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
type private BoundColumnHeader =
    { ParentEntity : ResolvedEntityRef // Parent entity which may or may not contain field `Name`. Headers are refined with type contexts, yielding `BoundValue`s.
      // May be "__main".
      Name : FieldName
      Key : FromFieldKey
      // These flags are described in `BoundColumnMeta`.
      Immediate : bool
      Single : bool
      IsInner : bool
    }

// `BoundColumn` exists only for fields that are bound to one and only one column field.
// Any set operation which merges fields with different bound fields discards that.
// It's used for:
// * checking dereferences;
// * finding bound columns for main entity.
// Should not be used for anything else - different cells can be bound to different
// fields.
[<NoEquality; NoComparison>]
type private BoundColumn =
    { Header : BoundColumnHeader
      Ref : ResolvedFieldRef // Real field ref.
      Entity : IEntityBits
      Field : ResolvedFieldBits
      // If set, forces user to explicitly name the field.
      // Used for `__main`: user cannot use `__main` without renaming in SELECT.
      ForceRename : bool
    }

[<NoEquality; NoComparison>]
type private BoundArgument =
    { Ref : ArgumentRef
      Immediate : bool
    }

type private BoundValue =
    | BVColumn of BoundColumn
    | BVArgument of BoundArgument

[<NoEquality; NoComparison>]
type private SubqueryField =
    { Bound : BoundValue option
      // Here and after `Bound` and `Type` track outermost field.
      // TODO: rename them to `OuterType`/`OuterBound`?
      Type : ResolvedFieldType option
      FieldAttributes : Set<AttributeName>
    }

let private emptySubqueryField : SubqueryField =
    { Bound = None
      Type = None
      FieldAttributes = Set.empty
    }

type private SubqueryFieldsMap = Map<FieldName, SubqueryField>

type private SubqueryFields = (FieldName option * SubqueryField)[]

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
    { Fields : SubqueryFields
      EntityAttributes : Set<AttributeName>
      ExprInfo : SubqueryExprInfo
    }

type private ResolvedFieldMapping =
    // Means that field exists in one of entity's children but is inaccessible without type narrowing.
    | FMTypeRestricted of BoundColumnHeader
    | FMBound of BoundValue
    | FMUnbound

type private FieldMappingInfo =
    { // Resolved entity ref.
      Entity : EntityRef option
      EntityId : FromEntityId
      Type : ResolvedFieldType option
      FieldAttributes : Set<AttributeName>
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

type CustomFromBound =
    | CFBoundColumn of ResolvedFieldRef
    | CFTyped of ResolvedFieldType
    | CFUnbound

type CustomFromField =
    { Bound : CustomFromBound
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

let private boundValueToMapping : BoundValue option -> ResolvedFieldMapping = function
    | None -> FMUnbound
    | Some bound -> FMBound bound

let private resolvedFieldForcedSQLName : GenericResolvedField<ResolvedColumnField, 'comp> -> SQL.ColumnName option when 'comp :> IComputedFieldBits = function
    | RColumnField col -> Some col.ColumnName
    | RComputedField comp when comp.IsMaterialized -> Some comp.ColumnName
    | _ -> None

let private boundValueForcedSQLName : BoundValue -> SQL.ColumnName option = function
    | BVColumn { Field = field; Header = { Immediate = true; Single = false } } -> resolvedFieldForcedSQLName field
    | _ -> None

let private fieldsToFieldMapping (fromEntityId : FromEntityId) (maybeEntityRef : EntityRef option) (fields : SubqueryFieldsMap) : FieldMapping =
    let explodeVariations (fieldName, subqueryField : SubqueryField) =
        let mapping = boundValueToMapping subqueryField.Bound
        let info =
            { Entity = maybeEntityRef
              EntityId = fromEntityId
              Type = subqueryField.Type
              Mapping = mapping
              ForceSQLName = subqueryField.Bound |> Option.bind boundValueForcedSQLName
              FieldAttributes = subqueryField.FieldAttributes
            }
        let value = FVResolved info
        explodeFieldRef { Entity = maybeEntityRef; Name = fieldName } |> Seq.map (fun x -> (x, value))
    fields |> Map.toSeq |> Seq.collect explodeVariations |> Map.ofSeq

let private typeRestrictedFieldsToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (isInner : bool) (maybeEntityRef : EntityRef option) (parentRef : ResolvedEntityRef) (children : ResolvedEntityRef seq) : FieldMapping =
    let filterField (name : FieldName, field : ResolvedFieldBits) =
        match field with
        | RColumnField { InheritedFrom = None } -> true
        | RComputedField f when Option.isNone f.InheritedFrom ->
            // Filter out children virtual fields.
            match f.Virtual with
            | None -> true
            | Some v -> Option.isNone v.InheritedFrom
        | _ -> false
    let mapEntity (entityRef : ResolvedEntityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let expandName (name : FieldName, field : ResolvedFieldBits) : (FieldRef * NameMappingValue<FieldRef, FieldMappingInfo>) seq =
            let key =
                { FromId = FIEntity fromEntityId
                  Path = []
                }
            let header =
                { ParentEntity = parentRef
                  Name = name
                  Key = key
                  Immediate = true
                  Single = false
                  IsInner = isInner
                }
            let info =
                { Entity = maybeEntityRef
                  EntityId = fromEntityId
                  Type = resolvedFieldBitsType field
                  Mapping = FMTypeRestricted header
                  ForceSQLName = resolvedFieldForcedSQLName field
                  FieldAttributes = Set.empty
                }
            let resolved = FVResolved info
            explodeResolvedFieldRef { Entity = parentRef; Name = name } |> Seq.map (fun ref -> (ref, resolved))
        entity.Fields |> Seq.filter filterField |> Seq.collect expandName
    children |> Seq.collect mapEntity |> Map.ofSeq

let private customToFieldMapping (layout : ILayoutBits) (fromEntityId : FromEntityId) (isInner : bool) (mapping : CustomFromMapping) : FieldMapping =
    let mapField (fieldRef : FieldRef, info : CustomFromField) =
        let (typ, fieldMapping) =
            match info.Bound with
            | CFUnbound -> (None, FMUnbound)
            | CFTyped typ -> (Some typ, FMUnbound)
            | CFBoundColumn boundRef ->
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
                      Single = false
                      IsInner = isInner
                    }
                let bound =
                    { Header = header
                      Ref = { boundRef with Name = field.Name }
                      Entity = entity
                      Field = resolvedFieldToBits field.Field
                      ForceRename = field.ForceRename
                    }
                (resolvedFieldType field.Field, FMBound (BVColumn bound))
        let info =
            { Entity = fieldRef.Entity
              EntityId = fromEntityId
              Mapping = fieldMapping
              Type = typ
              ForceSQLName = info.ForceSQLName
              FieldAttributes = Set.empty
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

let private renameFields (names : FieldName[]) (fields : SubqueryFields) : SubqueryFields =
    if Array.length names <> Array.length fields then
        raisef ViewResolveException "Inconsistent number of columns"
    Array.map2 (fun newName (oldName, info) -> (Some newName, info)) names fields

let private renameFieldsInSubquery (names : FieldName[]) (info : SubqueryInfo) : SubqueryInfo =
    { info with Fields = renameFields names info.Fields }

let private getFieldsMap (fields : SubqueryFields) : SubqueryFieldsMap =
    fields |> Seq.mapMaybe (fun (mname, info) -> Option.map (fun name -> (name, info)) mname) |> Map.ofSeq

type MainEntityMeta =
    { ColumnsToFields : Map<FieldName, FieldName>
      FieldsToColumns : Map<FieldName, FieldName>
    }

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

let private unionSubqueryField (a : SubqueryField) (b : SubqueryField) : SubqueryField =
    let bound =
        match (a.Bound, b.Bound) with
        | (Some (BVColumn bf1 as bound), Some (BVColumn bf2)) when bf1.Ref = bf2.Ref -> Some bound
        | (Some (BVArgument arg1 as bound), Some (BVArgument arg2)) when arg1.Ref = arg2.Ref -> Some bound
        | _ -> None
    let typ =
        match (a.Type, b.Type) with
        | ((Some _ as t1), (Some _ as t2)) -> unionTypes (seq { t1; t2 })
        | _ -> None
    { Bound = bound
      Type = typ
      FieldAttributes = Set.union a.FieldAttributes b.FieldAttributes
    }

let private unionSubqueryInfo (a : SubqueryInfo) (b : SubqueryInfo) =
    if Array.length a.Fields <> Array.length b.Fields then
        raisef ViewResolveException "Different number of columns in a set operation expression"
    let newFields = Array.map2 (fun (name1, info1) (name2, info2) -> (name1, unionSubqueryField info1 info2)) a.Fields b.Fields
    { Fields = newFields
      ExprInfo = unionSubqueryExprInfo a.ExprInfo b.ExprInfo
      EntityAttributes = Set.union a.EntityAttributes b.EntityAttributes
    }

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ViewResolveException "Invalid name: %s" name

let private selfSchema : SchemaName = FunQLName "__self"

let tryResolveEntityRef (homeSchema : SchemaName option) (forceSchema : bool) (ref : EntityRef) : ResolvedEntityRef option =
    match ref.Schema with
    | Some someSchema when someSchema = selfSchema ->
        match homeSchema with
        | Some schema -> Some { Schema = schema; Name = ref.Name }
        | None -> None
    | None ->
        match homeSchema with
        | _ when forceSchema -> None
        | Some schema -> Some { Schema = schema; Name = ref.Name }
        | None -> None
    | Some schema -> Some { Schema = schema; Name = ref.Name }

// Copy of that in Layout.Resolve but with a different exception.
let private resolveEntityRef (homeSchema : SchemaName option) (ref : EntityRef) : ResolvedEntityRef =
    match tryResolveEntityRef homeSchema false ref with
    | Some newRef -> newRef
    | None -> raisef ViewResolveException "Unspecified schema in reference: %O" ref

let resolveScalarFieldType (layout : IEntitiesSet) (homeSchema : SchemaName option) (allowReferenceOptions : bool) : ParsedScalarFieldType -> ResolvedScalarFieldType = function
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
        let resolvedRef = resolveEntityRef homeSchema entityRef
        if not <| layout.HasVisibleEntity resolvedRef then
            raisef ViewResolveException "Cannot find entity %O from reference type" resolvedRef
        SFTReference (resolvedRef, mopts)
    | SFTEnum vals ->
        if OrderedSet.isEmpty vals then
            raisef ViewResolveException "Enums must not be empty"
        SFTEnum vals

let resolveFieldType (layout : IEntitiesSet) (homeSchema : SchemaName option) (allowReferenceOptions : bool) : ParsedFieldType -> ResolvedFieldType = function
    | FTArray typ -> FTArray <| resolveScalarFieldType layout homeSchema allowReferenceOptions typ
    | FTScalar typ -> FTScalar <| resolveScalarFieldType layout homeSchema allowReferenceOptions typ

let relaxScalarFieldType : ResolvedScalarFieldType -> ParsedScalarFieldType = function
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
    | SFTReference (entityRef, mopts) -> SFTReference (relaxEntityRef entityRef, mopts)
    | SFTEnum vals -> SFTEnum vals

let relaxFieldType : ResolvedFieldType -> ParsedFieldType = function
    | FTArray typ -> FTArray <| relaxScalarFieldType typ
    | FTScalar typ -> FTScalar <| relaxScalarFieldType typ

let getResolvedScalarFieldType : ParsedScalarFieldType -> ResolvedScalarFieldType = function
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
    | SFTReference (entityRef, mopts) -> SFTReference (getResolvedEntityRef entityRef, mopts)
    | SFTEnum vals -> SFTEnum vals

let getResolvedFieldType : ParsedFieldType -> ResolvedFieldType = function
    | FTArray typ -> FTArray <| getResolvedScalarFieldType typ
    | FTScalar typ -> FTScalar <| getResolvedScalarFieldType typ

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

type BoundColumnMeta =
    { Ref : ResolvedFieldRef
      // Set if field references value from a table directly, not via a subexpression.
      Immediate : bool
      // Set if source entity is not expected to have other fields related to this table (e.g. `id`).
      Single : bool
      // Set if source entity is joined as INNER (that is, it's impossible for its `id` to be NULL).
      IsInner : bool
    }

let private boundColumnMeta (bound : BoundColumn) : BoundColumnMeta =
    { Ref = bound.Ref
      Immediate = bound.Header.Immediate
      Single = bound.Header.Single
      IsInner = bound.Header.IsInner
    }

type BoundArgumentMeta =
    { Ref : ArgumentRef
      // Set if field references bound value directly.
      // Can be true even when the reference is to a column for `d1.value` in `SELECT d1.value DOMAIN OF $foo AS d1`.
      Immediate : bool
    }

type BoundValueMeta =
    | BMColumn of BoundColumnMeta
    | BMArgument of BoundArgumentMeta

let private boundValueMeta : BoundValue -> BoundValueMeta = function
    | BVColumn col -> BMColumn (boundColumnMeta col)
    | BVArgument arg -> BMArgument { Ref = arg.Ref; Immediate = arg.Immediate }

type ColumnMeta =
    { EntityId : FromEntityId
      // Use specific SQL name (often ColumnField's `ColumnName`).
      ForceSQLName : SQL.ColumnName option
    }

type FieldRefMeta =
    { Column : ColumnMeta option
      Bound : BoundValueMeta option
      // Of length `ref.Path`, contains reference entities given current type context, starting from the first referenced entity.
      // `[Bound.Ref] + Path` is the full list of used references, starting from the first one.
      Type : ResolvedFieldType option
      Path : ResolvedEntityRef[]
    }

let emptyFieldRefMeta =
    { Column = None
      Bound = None
      Type = None
      Path = [||]
    } : FieldRefMeta

let getFieldRefBoundColumn (info : FieldRefMeta) : BoundColumnMeta =
    match info.Bound with
    | Some (BMColumn col) -> col
    | _ -> failwith "No bound column"

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

type private MainEntityResult =
    { Entity : ResolvedEntityRef
      Results : ResolvedQueryResult[]
    }

let rec private tryFindMainEntityFromExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedFromExpr -> RecursiveValue<ResolvedEntityRef> option = function
    | FEntity { Ref = { Schema = Some schemaName; Name = name } } -> Some <| RValue { Schema = schemaName; Name = name }
    | FEntity { Ref = { Schema = None; Name = name } } ->
        let cte = Map.find name ctes
        let extra = ObjectMap.findType<ResolvedCommonTableExprTempMeta> cte.Extra
        if extra.MainEntity then
            Option.get currentCte |> RRecursive |> Some
        else
            extra.MainEntity <- true
            tryFindMainEntityExpr ctes (Some name) cte.Expr |> Option.map (mapRecursiveValue (fun res -> res.Entity))
    | FJoin join ->
        match join.Type with
        | Left -> tryFindMainEntityFromExpr ctes currentCte join.A
        | Right -> tryFindMainEntityFromExpr ctes currentCte join.B
        | _ -> None
    | FTableExpr tabExpr -> tryFindMainEntityTableExpr ctes currentCte tabExpr.Expression

and private tryFindMainEntityTableExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedTableExpr -> RecursiveValue<ResolvedEntityRef> option = function
    | TESelect sel -> tryFindMainEntityExpr ctes currentCte sel |> Option.map (mapRecursiveValue (fun res -> res.Entity))
    | TEDomain (f, info) -> None
    | TEFieldDomain (e, f, info) -> None
    | TETypeDomain (typ, info) -> None

and private tryFindMainEntityTreeExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) : ResolvedSelectTreeExpr -> RecursiveValue<MainEntityResult> option = function
    | SSelect ({ From = Some from } as sel) ->
        let info = ObjectMap.findType<ResolvedSingleSelectMeta> sel.Extra
        if info.HasAggregates || not <| Array.isEmpty sel.GroupBy then
            None
        else
            let refResults ref =
                { Entity = ref
                  Results = sel.Results
                }
            tryFindMainEntityFromExpr ctes currentCte from |> Option.map (mapRecursiveValue refResults)
    | SSelect _ -> None
    | SValues _ -> None
    | SSetOp setOp ->
        monad' {
            let! ret1 = tryFindMainEntityExpr ctes currentCte setOp.A
            let! ret2 = tryFindMainEntityExpr ctes currentCte setOp.B
            match (ret1, ret2) with
            | (RValue mainA, RValue mainB) ->
                if mainA.Entity <> mainB.Entity then
                    return! None
                else
                    return RValue mainA
            | (RRecursive cte, RValue ret)
            | (RValue ret, RRecursive cte) ->
                assert (cte = Option.get currentCte)
                return RValue ret
            | (RRecursive r1, RRecursive r2) ->
                assert (r1 = r2)
                return RRecursive r1
        }

and private tryFindMainEntityExpr (ctes : ResolvedCommonTableExprsMap) (currentCte : EntityName option) (select : ResolvedSelectExpr) : RecursiveValue<MainEntityResult> option =
    let ctes = Map.union (cteBindings select) ctes
    tryFindMainEntityTreeExpr ctes currentCte select.Tree

// Returns map from query column names to fields in main entity
// Be careful as changes here also require changes to main id propagation in the compiler.
let private tryFindMainEntity (select : ResolvedSelectExpr) : MainEntityResult option =
    let getValue = function
    | RValue ret -> ret
    | RRecursive r -> failwithf "Impossible recursion detected in %O" r
    tryFindMainEntityExpr Map.empty None select |> Option.map getValue

type private ResolveCTE = EntityName -> SubqueryInfo option

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
      NoFetches : bool
    }

type private InnerResolvedExprInfo =
    { Info : ResolvedExprInfo
      Types : TypeContextsMap
    }

let private exprDependency (info : ResolvedExprInfo) : DependencyStatus =
    if not info.Flags.HasFields then
        if info.Flags.HasArguments || info.Flags.HasFetches then
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
      NoFetches = false
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

let private boundEntityFieldInfo (typeCtxs : TypeContextsMap) (inner : BoundColumn) (extras : obj seq) : ObjectMap =
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

let private fieldRefFromId (fieldInfo : FieldRefMeta) : FromId =
    match fieldInfo.Bound with
    | Some (BMArgument arg) -> FIArgument arg.Ref
    | _ ->
        let columnInfo = Option.get fieldInfo.Column
        FIEntity columnInfo.EntityId

let private resolveSubEntity (layout : ILayoutBits) (outerTypeCtxs : TypeContextsMap) (ctx : SubEntityContext) (field : LinkedBoundFieldRef) (subEntityInfo : SubEntityRef) : TypeContextsMap * SubEntityRef =
    let (fieldRef, typeCtxKey) =
        let fieldInfo = ObjectMap.findType<FieldRefMeta> field.Extra
        let arrowNames = field.Ref.Path |> Seq.map (fun arr -> arr.Name)
        match field.Ref.Ref with
        | VRColumn col ->
            let typeCtxKey =
                { FromId = fieldRefFromId fieldInfo
                  Path = seq { col.Name; yield! arrowNames } |> Seq.exceptLast |> Seq.toList
                }
            let fieldRef =
                if Array.isEmpty field.Ref.Path then
                    match fieldInfo.Bound with
                    | Some (BMColumn boundInfo) -> boundInfo.Ref
                    | _ -> raisef ViewResolveException "Unbound field in a type assertion"
                else
                    let entityRef = Array.last fieldInfo.Path
                    let fieldArrow = Array.last field.Ref.Path
                    { Entity = entityRef; Name = fieldArrow.Name }
            (fieldRef, typeCtxKey)
        | VRArgument arg ->
            if Array.isEmpty field.Ref.Path then
                raisef ViewResolveException "Unbound field in a type assertion"
            let typeCtxKey =
                { FromId = FIArgument arg
                  Path = arrowNames |> Seq.exceptLast |> Seq.toList
                }
            let fieldRef =
                    let entityRef = Array.last fieldInfo.Path
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

let replaceFieldRefInExpr (updateEntityRef : LinkedBoundFieldRef -> LinkedBoundFieldRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
    let resolveReference : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
    | { Ref = { Ref = VRColumn col } } as ref -> updateEntityRef ref
    | ref -> ref
    let mapper = { idFieldExprMapper with FieldReference = resolveReference }
    mapFieldExpr mapper

let replaceEntityRefInField (localRef : EntityRef option) (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
    match ref.Ref.Ref with
    | VRColumn col -> { ref with Ref = { ref.Ref with Ref = VRColumn { col with Entity = localRef } } }
    | _ -> failwith "Not a column reference"

let replaceEntityRefInExpr (localRef : EntityRef option) = replaceFieldRefInExpr (replaceEntityRefInField localRef)

// Takes `sourceRef` as prefix reference and appends path from another given reference, constructing combined one.
// For example, if local reference is `foo=>bar` and given reference is `quz=>qux`, then:
// If `replacelastPrefixArrow` is true, then `foo=>quz=>qux`;
// else `foo=>bar=>quz=>qux`.
let replacePathInField (layout : ILayoutBits) (sourceRef : LinkedBoundFieldRef) (replaceLastPrefixArrow : bool) (ref : LinkedBoundFieldRef) : LinkedBoundFieldRef =
    let col =
        match ref.Ref.Ref with
        | VRColumn col -> col
        | _ -> failwith "Not a column reference"
    let newFieldArrow =
        { Name = col.Name
          AsRoot = ref.Ref.AsRoot
        }
    let pathPrefix =
        if replaceLastPrefixArrow then
            Seq.exceptLast sourceRef.Ref.Path
        else
            sourceRef.Ref.Path
    let newPath = Seq.concat [pathPrefix; Seq.singleton newFieldArrow; Array.toSeq ref.Ref.Path] |> Array.ofSeq

    let sourceMeta = ObjectMap.findType<FieldRefMeta> sourceRef.Extra

    let boundPrefix =
        if replaceLastPrefixArrow then
            Seq.exceptLast sourceMeta.Path
        else
            sourceMeta.Path
    let newBoundEntity =
        if replaceLastPrefixArrow then
            Array.last sourceMeta.Path
        else
            let lastEntityRef = Array.last sourceMeta.Path
            let lastEntity = layout.FindEntity lastEntityRef |> Option.get
            match lastEntity.FindField col.Name with
            | Some { Field = RColumnField { FieldType = FTScalar (SFTReference (refEntityRef, _)) } } -> refEntityRef
            | _ -> failwith "Impossible"

    let destMeta = ObjectMap.findType<FieldRefMeta> ref.Extra
    let newBoundPath = Seq.concat [boundPrefix; Seq.singleton newBoundEntity; destMeta.Path] |> Array.ofSeq

    assert (Array.length newPath = Array.length newBoundPath)

    let newFieldMeta = { sourceMeta with Path = newBoundPath }
    let newExtra = ObjectMap.add newFieldMeta ref.Extra
    { Ref = { Ref = sourceRef.Ref.Ref; Path = newPath; AsRoot = ref.Ref.AsRoot }; Extra = newExtra }

let replacePathInExpr (layout : ILayoutBits) (sourceRef : LinkedBoundFieldRef) (replaceLastPrefixArrow : bool) = replaceFieldRefInExpr (replacePathInField layout sourceRef replaceLastPrefixArrow)

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

let private mainEntityColumnsMeta (fields : SubqueryFieldsMap) (ref : ResolvedEntityRef) (columns : ResolvedQueryResult[]) : MainEntityMeta =
    let getField : ResolvedQueryResult -> (FieldName * FieldName) option = function
        | QRAll _ -> failwith "Impossible QRAll"
        | QRExpr result ->
            let name = result.TryToName () |> Option.get
            match Map.tryFind name fields with
            | Some { Bound = Some (BVColumn { Ref = fieldRef; Field = RColumnField _ }) } when fieldRef.Entity = ref -> Some (name, fieldRef.Name)
            | _ -> None
    let mappedResults = columns |> Seq.mapMaybe getField |> Map.ofSeqUnique
    { FieldsToColumns = Map.reverse mappedResults
      ColumnsToFields = mappedResults
    }

let private detectMainEntity (fields : SubqueryFieldsMap) (query : ResolvedSelectExpr) : ResolvedMainEntity option =
    match tryFindMainEntity query with
    | None -> None
    | Some found ->
        let meta = mainEntityColumnsMeta fields found.Entity found.Results
        Some
            { Entity = relaxEntityRef found.Entity
              ForInsert = false
              Extra = ObjectMap.singleton meta
            }

let private resolveMainEntity (layout : ILayoutBits) (homeSchema : SchemaName option) (fields : SubqueryFieldsMap) (query : ResolvedSelectExpr) (main : ParsedMainEntity) : ResolvedMainEntity option =
    match tryFindMainEntity query with
    | None -> raisef ViewResolveException "Main entity not matched in the expression"
    | Some found ->
        let requestedRef = resolveEntityRef homeSchema main.Entity
        if found.Entity <> requestedRef then
            raisef ViewResolveException "Main entity %O is specified, but %O is detected" requestedRef ref

        let meta = mainEntityColumnsMeta fields found.Entity found.Results

        if main.ForInsert then
            let entity = layout.FindEntity found.Entity |> Option.get
            if entity.IsAbstract then
                raisef ViewResolveException "Entity is abstract: %O" main.Entity
            let checkField (fieldName, field : ResolvedColumnField) =
                if Option.isNone field.DefaultValue && not field.IsNullable then
                    if not (Map.containsKey fieldName meta.ColumnsToFields) then
                        raisef ViewResolveException "Required inserted entity field is not in the view expression: %O" fieldName
            entity.ColumnFields |> Seq.iter checkField

        Some
            { Entity = relaxEntityRef found.Entity
              ForInsert = main.ForInsert
              Extra = ObjectMap.singleton meta
            }

let enumDomainValuesColumn = FunQLName "value"

type private SelectFlags =
    { RequireNames : bool
      NoAttributes : bool
    }

type private OuterField =
    { Bound : BoundValue option
      IsExternal : bool
      EntityId : FromEntityId
    }

type private ReferenceInfo =
    { InnerValue : BoundValue option
      Type : ResolvedFieldType option
      ExprInfo : ResolvedExprInfo
      FieldAttributes : Set<AttributeName>
    }

type private ResultInfo =
    { Bound : BoundValue option
      Type : ResolvedFieldType option
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
      // Resolved entity ref.
      Entity : EntityRef
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
    | FTableExpr expr -> FTableExpr expr
    | FJoin join ->
        let a = relabelFromExprType typeContexts join.A
        let b = relabelFromExprType typeContexts join.B
        FJoin { join with A = a; B = b }

type private FindArgument = ArgumentRef -> ResolvedArgument option
type HasUserView = ResolvedUserViewRef -> bool

type GetDefaultAttribute = ResolvedFieldRef -> AttributeName -> DefaultAttribute option

type ResolveCallbacks =
    { Layout : ILayoutBits
      HomeSchema : SchemaName option
      GetDefaultAttribute : GetDefaultAttribute
      HasUserView : HasUserView
    }

let emptyGetDefaultAttribute (fieldRef : ResolvedFieldRef) (name : AttributeName) : DefaultAttribute option = None
let emptyHasUserView (ref : ResolvedUserViewRef) = false

let resolveCallbacks (layout : ILayoutBits) : ResolveCallbacks =
    { Layout = layout
      HomeSchema = None
      GetDefaultAttribute = emptyGetDefaultAttribute
      HasUserView = emptyHasUserView
    }

let private boundValueToDomain : BoundValue -> BoundValue = function
    | BVArgument arg -> BVArgument { arg with Immediate = true }
    | BVColumn col -> BVColumn { col with Header = { col.Header with Single = true; Immediate = true } }

type private UnresolvedBoundColumn =
    | UBCColumn of BoundColumn
    | UBCHeader of BoundColumnHeader

type private QueryResolver (callbacks : ResolveCallbacks, findArgument : FindArgument, resolveFlags : ExprResolutionFlags) =
    let { Layout = layout } = callbacks

    let mutable isPrivileged = false

    let mutable lastFromEntityId : FromEntityId = 0
    let nextFromEntityId () =
        let ret = lastFromEntityId
        lastFromEntityId <- lastFromEntityId + 1
        ret

    let getArgument arg =
        match findArgument arg with
        | None -> raisef ViewResolveException "Unknown argument: %O" arg
        | Some argInfo -> argInfo

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

    let createFromMapping (fromEntityId : FromEntityId) (entityRef : ResolvedEntityRef) (alias : (EntityRef option) option) (flags : FromMappingFlags) : FieldMapping =
        let entity = findEntityByRef entityRef flags.AllowHidden
        if flags.WithoutChildren && entity.IsAbstract then
            raisef ViewResolveException "Abstract entity cannot be selected as ONLY"

        let key =
            { FromId = FIEntity fromEntityId
              Path = []
            }

        let makeSubqueryField (name : FieldName) (field : ResolvedFieldBits) : SubqueryField =
            let header =
                { ParentEntity = entityRef
                  Name = name
                  Key = key
                  Immediate = true
                  Single = false
                  IsInner = flags.IsInner
                }
            let bound =
                { Header = header
                  Ref = { Entity = entityRef; Name = name }
                  Entity = entity
                  Field = field
                  ForceRename = false
                }
            { Bound = Some (BVColumn bound)
              Type = resolvedFieldBitsType field
              FieldAttributes = Set.empty
            }

        let realFields = mapAllFields makeSubqueryField entity
        let mainFieldInfo = Option.get <| entity.FindFieldBits funMain
        let mainHeader =
            { Key = key
              ParentEntity = entityRef
              Name = funMain
              Immediate = true
              Single = false
              IsInner = flags.IsInner
            }
        let mainBoundField =
            { Header = mainHeader
              Ref = { Entity = entityRef; Name = mainFieldInfo.Name }
              Entity = entity
              Field = mainFieldInfo.Field
              ForceRename = true
            }
        let mainSubqueryField =
            { Bound = Some (BVColumn mainBoundField)
              Type = resolvedFieldBitsType mainFieldInfo.Field
              FieldAttributes = Set.empty
            }
        let fields = Map.add funMain mainSubqueryField realFields
        let mappingRef =
            match alias with
            | None -> Some <| relaxEntityRef entityRef
            | Some aliasRef -> aliasRef
        let mapping = fieldsToFieldMapping fromEntityId mappingRef fields
        if flags.WithoutChildren then
            mapping
        else
            let extraMapping = typeRestrictedFieldsToFieldMapping layout fromEntityId flags.IsInner mappingRef entityRef (entity.Children |> Map.keys)
            Map.unionUnique extraMapping mapping

    let domainFromInfo (fromEntityId : FromEntityId) (boundValue : BoundValue option) (fieldType : ResolvedFieldType) (tableName : EntityName) (flags : DomainExprInfo) : FromExprInfo =
        if flags.AsRoot then
            if not resolveFlags.Privileged then
                raisef ViewResolveException "Cannot specify roles in non-privileged user views"
            isPrivileged <- true

        let fields =
            match fieldType with
            // TODO: Not implemented yet.
            // | FTScalar (SFTReference (entityRef, opts)) -> ()
            | FTScalar (SFTEnum vals) ->
                let fieldInfo =
                    { Bound = Option.map boundValueToDomain boundValue
                      Type = Some fieldType
                      FieldAttributes = Set.empty
                    }
                Map.singleton enumDomainValuesColumn fieldInfo

            | typ -> raisef ViewResolveException "Field %O doesn't have a domain" typ

        let alias = { Schema = None; Name = tableName } : EntityRef
        let mapping = fieldsToFieldMapping fromEntityId (Some alias) fields
        let entityInfo =
            { Schema = None
              EntityAttributes = Set.empty
              Id = fromEntityId
            }
        { Entities = Map.singleton tableName entityInfo
          Fields = mapping
          ExprInfo = emptySubqueryExprInfo
          Types = Map.empty
        }

    let resolvePath (typeCtxs : TypeContextsMap) (firstHeader : UnresolvedBoundColumn) (fullPath : PathArrow list) : BoundColumn list =
        let getBoundField (unresolved : UnresolvedBoundColumn) : BoundColumn =
            let header =
                match unresolved with
                | UBCColumn col -> col.Header
                | UBCHeader header -> header
            match findFieldWithContext layout typeCtxs header.Key header.ParentEntity header.Name with
            | None ->
                raisef ViewResolveException "Field not found: %O" { Entity = header.ParentEntity; Name = header.Name }
            | Some (entityRef, entity, field) ->
                // It's important to inherit `ForceRename` from the unresolved field,
                // as it may already have a proper name.
                let forceRename =
                    match unresolved with
                    | UBCColumn col -> col.ForceRename
                    | UBCHeader header -> field.ForceRename
                { Header = header
                  Ref = { Entity = entityRef; Name = field.Name }
                  Field = resolvedFieldToBits field.Field
                  Entity = entity
                  ForceRename = forceRename
                }

        let rec traverse (boundField : BoundColumn) : PathArrow list -> BoundColumn list = function
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
                          Single = false
                          IsInner = boundField.Header.IsInner
                        }
                    let nextBoundField = getBoundField (UBCHeader refHeader)
                    let boundFields = traverse nextBoundField refs
                    boundField :: boundFields
                | _ -> raisef ViewResolveException "Invalid dereference: %O" ref

        traverse (getBoundField firstHeader) fullPath

    let resolvePathByType (typeCtxs : TypeContextsMap) (fromId : FromId) (fieldType : ResolvedFieldType) (path : PathArrow[]) : BoundColumn list =
        let parentEntity =
            match fieldType with
            | FTScalar (SFTReference (parentEntity, opts)) -> parentEntity
            | _ -> raisef ViewResolveException "Value is not a reference"
        let (firstArrow, remainingPath) =
            match Array.toList path with
            | head :: tail -> (head, tail)
            | _ -> failwith "Impossible"
        let key =
            { FromId = fromId
              Path = []
            }
        let argHeader =
            { Key = key
              ParentEntity = parentEntity
              Name = firstArrow.Name
              Immediate = false
              Single = false
              IsInner = false
            }
        resolvePath typeCtxs (UBCHeader argHeader) remainingPath

    let resolveReference (ctx : Context) (typeCtxs : TypeContextsMap) (f : LinkedFieldRef) : ReferenceInfo * LinkedBoundFieldRef =
        if f.AsRoot then
            if not resolveFlags.Privileged then
                raisef ViewResolveException "Cannot specify roles in non-privileged user views"
            isPrivileged <- true

        let (refInfo, newRef) =
            match f.Ref with
            | VRColumn ref ->
                let rec findInMappings = function
                    | [] -> raisef ViewResolveException "Unknown field: %O" ref
                    | mapping :: mappings ->
                        match Map.tryFind ref mapping with
                        | Some (FVResolved info) -> info
                        | Some (FVAmbiguous entities) -> raisef ViewResolveException "Ambiguous reference %O. Possible values: %O" ref entities
                        | None -> findInMappings mappings
                let info = findInMappings ctx.FieldMaps

                let isExternal = not <| Set.contains info.EntityId ctx.LocalEntities
                if not (Array.isEmpty f.Path) && isExternal then
                    raisef ViewResolveException "Arrows for external fields are currently not supported"

                let getFromCurrentBoundField bound =
                    let (outer, remaining) = resolvePath typeCtxs bound (Array.toList f.Path) |> List.snoc
                    let innerEntityField =
                        if not <| List.isEmpty remaining then
                            List.last remaining
                        else
                            outer
                    (Some (BVColumn outer), Some (BVColumn innerEntityField), remaining)

                let (outerValue, innerValue, boundFields) =
                    match info.Mapping with
                    | FMBound (BVColumn bound) -> getFromCurrentBoundField (UBCColumn bound)
                    | FMTypeRestricted header -> getFromCurrentBoundField (UBCHeader header)
                    | FMBound (BVArgument arg) ->
                        if Array.isEmpty f.Path then
                            (Some (BVArgument arg), Some (BVArgument arg), [])
                        else
                            let argInfo = getArgument arg.Ref
                            let path = resolvePathByType typeCtxs (FIEntity info.EntityId) argInfo.ArgType f.Path
                            (Some (BVArgument arg), Some (BVColumn <| List.last path), path)
                    | FMUnbound ->
                        if Array.isEmpty f.Path then
                            (None, None, [])
                        else
                            let fieldType =
                                match info.Type with
                                | Some typ -> typ
                                | None -> raisef ViewResolveException "Dereference of an unbound field in %O" f
                            let path = resolvePathByType typeCtxs (FIEntity info.EntityId) fieldType f.Path
                            (None, Some (BVColumn <| List.last path), path)

                let newType =
                    match info.Type with
                    | Some _ as typ -> typ
                    | None ->
                        match outerValue with
                        // We delay acquiring computed field types until they are actually used.
                        | Some (BVColumn ({ Field = RComputedField _ } as col)) ->
                            match col.Entity.FindField col.Ref.Name with
                            | Some { Field = RComputedField comp } -> comp.Type
                            | _ -> failwith "Impossible"
                        | _ -> None

                let columnInfo =
                    { EntityId = info.EntityId
                      ForceSQLName = info.ForceSQLName
                    }
                let fieldInfo =
                    { Path = boundFields |> Seq.map (fun x -> x.Ref.Entity) |> Array.ofSeq
                      Type = newType
                      Column = Some columnInfo
                      Bound = Option.map boundValueMeta outerValue
                    } : FieldRefMeta
                let extra =
                    match innerValue with
                    | Some (BVColumn inner) -> boundEntityFieldInfo typeCtxs inner (Seq.singleton (fieldInfo :> obj))
                    | _ -> ObjectMap.singleton fieldInfo

                let newFieldRef = { Entity = info.Entity; Name = ref.Name } : FieldRef
                let newRef = { Ref = { f with Ref = VRColumn newFieldRef }; Extra = extra }

                let exprInfo =
                    if isExternal then
                        { emptyResolvedExprInfo with ExternalEntities = Set.singleton info.EntityId }
                    else
                        fieldResolvedExprInfo
                let exprInfo =
                    match innerValue with
                    | Some (BVColumn ({ Field = RComputedField _ } as inner)) ->
                        // Find full field, we only have IComputedFieldBits in the inner field...
                        match inner.Entity.FindField inner.Ref.Name with
                        | Some { Field = RComputedField comp } ->
                            let newExprFlags =
                                if comp.IsMaterialized then
                                    { exprInfo.Flags with HasFields = true }
                                else
                                    computedFieldCases layout newRef.Extra inner.Ref comp
                                    |> Seq.map (fun (case, caseComp) -> caseComp.Flags)
                                    |> Seq.fold unionResolvedExprFlags exprInfo.Flags
                            { exprInfo with Flags = newExprFlags }
                        | _ -> failwith "Impossible"
                    | _ -> exprInfo
                let refInfo =
                    { ExprInfo = exprInfo
                      InnerValue = innerValue
                      Type = info.Type
                      FieldAttributes = info.FieldAttributes
                    }
                (refInfo, newRef)
            | VRArgument argRef ->
                if ctx.NoArguments then
                    raisef ViewResolveException "Arguments are not allowed here: %O" argRef
                let argInfo = getArgument argRef
                let hasSubquery = not <| Array.isEmpty f.Path
                let boundMeta =
                    { Ref = argRef
                      Immediate = true
                    } : BoundArgumentMeta
                let (innerValue, boundInfo) =
                    if not hasSubquery then
                        let boundInfo =
                            { Path = [||]
                              Column = None
                              Bound = Some (BMArgument boundMeta)
                              Type = Some argInfo.ArgType
                            } : FieldRefMeta
                        let innerBound =
                            { Ref = argRef
                              Immediate = true
                            } : BoundArgument
                        (BVArgument innerBound, ObjectMap.singleton boundInfo)
                    else
                        let boundFields = resolvePathByType typeCtxs (FIArgument argRef) argInfo.ArgType f.Path
                        let inner = List.last boundFields
                        let boundInfo =
                            { Path = boundFields |> Seq.map (fun f -> f.Ref.Entity) |> Seq.toArray
                              Column = None
                              Bound = Some (BMArgument boundMeta)
                              Type = Some argInfo.ArgType
                            } : FieldRefMeta
                        let info = boundEntityFieldInfo typeCtxs inner (Seq.singleton (boundInfo :> obj))
                        (BVColumn inner, info)
                let exprFlags =
                    { emptyResolvedExprFlags with
                        HasArguments = true
                        HasSubqueries = hasSubquery
                        HasFetches = hasSubquery
                    }
                let exprInfo = { emptyResolvedExprInfo with Flags = exprFlags }
                let refInfo =
                    { ExprInfo = exprInfo
                      Type = Some argInfo.ArgType
                      InnerValue = Some innerValue
                      FieldAttributes = Set.empty
                    }
                let newRef = { Ref = { f with Ref = VRArgument argRef }; Extra = boundInfo }

                (refInfo, newRef)

        let refInfo =
            if not <| Array.isEmpty f.Path then
                { refInfo with ExprInfo = { refInfo.ExprInfo with Flags = { refInfo.ExprInfo.Flags with HasArrows = true } } }
            else
                refInfo

        (refInfo, newRef)

    let resolveLimitFieldExpr (expr : ParsedFieldExpr) : ResolvedFieldExpr =
        let resolveRef : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { Ref = VRArgument name; Path = [||] } as ref ->
                let argInfo = getArgument name
                { Ref = ref; Extra = ObjectMap.empty }
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

    let resolveMyEntityRef = resolveEntityRef callbacks.HomeSchema
    let resolveMyFieldType = resolveFieldType layout callbacks.HomeSchema false

    let resolveUserViewRef (ref : UserViewRef) : UserViewRef =
        let fullRef = resolveMyEntityRef ref
        if not <| callbacks.HasUserView fullRef then
            raisef ViewResolveException "Referenced user view not found: %O" fullRef
        relaxEntityRef fullRef

    let resolveFieldValue : FieldValue -> FieldValue = function
        | FUserViewRef ref -> FUserViewRef (resolveUserViewRef ref)
        | FUserViewRefArray vals -> FUserViewRefArray (Array.map resolveUserViewRef vals)
        | v -> v

    let applyAlias (alias : EntityAlias) (results : SubqueryFields) : SubqueryFields =
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

        let getEntityInfo schemaName entityName =
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
            (mapping, typeContextsMap, relaxEntityRef resRef, mappingRef, exprInfo)

        let (mapping, types, resolvedRef, name, exprInfo) =
            match entity.Ref with
            | { Schema = Some schemaName; Name = entityName } -> getEntityInfo schemaName entityName
            | { Schema = None; Name = entityName } ->
                match ctx.ResolveCTE entityName with
                | Some cteInfo ->
                    if entity.AsRoot then
                        raisef ViewResolveException "Roles can only be specified for entities"
                    if entity.Only then
                        raisef ViewResolveException "ONLY can only be specified for entities"
                    let newName = Option.defaultValue entityName entity.Alias
                    let mappingRef = { Schema = None; Name = newName } : EntityRef
                    let mapping = fieldsToFieldMapping fromEntityId (Some mappingRef) (getFieldsMap cteInfo.Fields)
                    (mapping, Map.empty, entity.Ref, mappingRef, cteInfo.ExprInfo)
                | None ->
                    match callbacks.HomeSchema with
                    | Some schemaName -> getEntityInfo schemaName entityName
                    | None -> raisef ViewResolveException "Entity not found: %O" entity.Ref

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
          Entity = resolvedRef
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
            let (resultBound, innerType) =
                match refInfo.InnerValue with
                | None -> (None, None)
                | Some (BVArgument arg) ->
                    let argInfo = getArgument arg.Ref
                    let newArg = { arg with Immediate = false }
                    (Some (BVArgument newArg), Some argInfo.ArgType)
                | Some (BVColumn field) ->
                    if field.ForceRename && Option.isNone name && flags.RequireNames then
                        raisef ViewResolveException "Field should be explicitly named in result expression: %s" (f.ToFunQLString())
                    match field.Field with
                    // We erase field information for computed fields from results, as they would be expanded at this point.
                    | RComputedField comp ->
                        match field.Entity.FindField field.Ref.Name with
                        | Some { Field = RComputedField comp } -> (None, comp.Type)
                        | _ -> failwith "Impossible"
                    | _ ->
                        // Field is no longer immediate, and rename is not needed because a name is assigned here.
                        let header = { field.Header with Immediate = false }
                        let newField =
                            { field with
                                Header = header
                                ForceRename = false
                            }
                        (Some (BVColumn newField), resolvedFieldBitsType field.Field)
            let info =
                { Bound = resultBound
                  Type = innerType
                  ExprInfo = refInfo.ExprInfo
                } : ResultInfo
            (info, FERef ref)
        | e ->
            match name with
            | Some n -> checkName n
            | None when flags.RequireNames -> raisef ViewResolveException "Unnamed results are allowed only inside expression queries"
            | None -> ()
            let (exprInfo, expr) = resolveFieldExpr ctx e
            let info =
                { Bound = None
                  Type = None
                  ExprInfo = exprInfo.Info
                } : ResultInfo
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
            | DSConst -> { ctx with FieldMaps = []; NoArguments = true; NoFetches = true }
        let (info, expr) = resolveFieldExpr newCtx attr.Expression
        let newDep = exprDependency info.Info
        assert (newDep <= attr.Dependency)
        let newAttr =
            { Expression = expr
              Dependency = newDep
              Internal = attr.Internal
            }
        (info.Info, newAttr)

    and resolveBoundAttribute (ctx : Context) (boundExprInfo : ResolvedExprInfo) (attr : ParsedBoundAttribute) : ResolvedExprInfo * ResolvedBoundAttribute =
        match attr.Expression with
        | BAExpr expr ->
            let (info, newAttr) = resolveAttribute ctx { Internal = attr.Internal; Expression = expr; Dependency = attr.Dependency }
            (info, { Internal = newAttr.Internal; Expression = BAExpr newAttr.Expression; Dependency = newAttr.Dependency })
        | BAMapping mapping ->
            (boundExprInfo, { Internal = attr.Internal; Expression = BAMapping mapping; Dependency = exprDependency boundExprInfo })
        | BAArrayMapping mapping ->
            (boundExprInfo, { Internal = attr.Internal; Expression = BAArrayMapping mapping; Dependency = exprDependency boundExprInfo })

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

    and resolveEntityReference (ctx : Context) (entityRef : EntityRef) : FromEntityInfo * EntityRef =
        match Map.tryFind entityRef.Name ctx.Entities with
        | None -> raisef ViewResolveException "Entity %O not found" entityRef
        | Some entityInfo ->
            match entityRef.Schema with
            | Some _ as schema when entityInfo.Schema <> schema -> raisef ViewResolveException "Entity %O not found" entityRef
            | _ -> ()
            let newRef = { Schema = entityInfo.Schema; Name = entityRef.Name } : EntityRef
            (entityInfo, newRef)

    and fieldAttributeExists (refInfo : ReferenceInfo) (attrName : AttributeName) =
            if Set.contains attrName refInfo.FieldAttributes then
                true
            else
                match refInfo.InnerValue with
                | Some (BVColumn innerField) ->
                    match callbacks.GetDefaultAttribute innerField.Ref attrName with
                    | Some attr -> not innerField.Header.Single || attr.Single
                    | None -> false
                | Some (BVArgument arg) ->
                    let argInfo = getArgument arg.Ref
                    Map.containsKey attrName argInfo.Attributes
                | None -> false

    and resolveFieldExpr (ctx : Context) (expr : ParsedFieldExpr) : InnerResolvedExprInfo * ResolvedFieldExpr =
        let mutable exprInfo = emptyResolvedExprInfo

        let resolveExprReference typeCtxs ref =
            let (refInfo, newRef) = resolveReference ctx typeCtxs ref
            exprInfo <- unionResolvedExprInfo exprInfo refInfo.ExprInfo
            (refInfo, newRef)

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
            | FEValue value ->
                let newValue = resolveFieldValue value
                (emptyCondTypeContexts, FEValue newValue)
            | FERef r ->
                let (refInfo, newRef) = resolveExprReference outerTypeCtxs.Map r
                (emptyCondTypeContexts, FERef newRef)
            | FEEntityAttr (eref, attr) ->
                let (entityInfo, newEref) = resolveEntityReference ctx eref
                if not <| Set.contains attr entityInfo.EntityAttributes then
                    raisef ViewResolveException "Entity attribute %O is not specified for %O" attr eref
                // We consider entity attribute references non-local because they can change based on inner parts of the query and default attributes.
                exprInfo <- { exprInfo with Flags = unionResolvedExprFlags exprInfo.Flags unknownResolvedExprFlags }
                (emptyCondTypeContexts, FEEntityAttr (newEref, attr))
            | FEFieldAttr (fref, attr) ->
                let (refInfo, newRef) = resolveExprReference outerTypeCtxs.Map fref
                if not <| fieldAttributeExists refInfo attr then
                    raisef ViewResolveException "Field attribute %O is not specified for field %O" attr newRef
                // We consider field attribute references non-local because they can change based on inner parts of the query and default attributes.
                // That is, unless reference is an argument, in which case it's basically a subquery and this is already accounted for
                // in argument flags.
                let newFlags =
                    match newRef.Ref.Ref with
                    | VRColumn _ -> unknownResolvedExprFlags
                    | VRArgument _ -> refInfo.ExprInfo.Flags
                exprInfo <- { exprInfo with Flags = unionResolvedExprFlags exprInfo.Flags newFlags }
                (emptyCondTypeContexts, FEFieldAttr (newRef, attr))
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
                let (refInfo, newF) = resolveExprReference outerTypeCtxs.Map f
                let (innerTypeCtxs, newNam) = resolveSubEntity layout outerTypeCtxs.Map SECInheritedFrom newF nam
                let ctx =
                    { Map = innerTypeCtxs
                      ExtraConditionals = false
                    }
                (ctx, FEInheritedFrom (newF, newNam))
            | FEOfType (f, nam) ->
                let (refInfo, newF) = resolveExprReference outerTypeCtxs.Map f
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
                        Some recResults
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
                    | Some (cteResults, cte) -> Some cteResults
                    | None when ctes.Recursive ->
                        match Map.tryFind currName exprsMap with
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
                            Some results
                        | None -> resolveCte currName
                    | None -> resolveCte currName
                and tmpCtx = { ctx with ResolveCTE = tmpResolveCte }

                let (results, newCte) = resolveCommonTableExpr tmpCtx flags ctes.Recursive name cte
                resultsMap <- Map.add name (results, newCte) resultsMap
                exprInfo <- unionSubqueryExprInfo exprInfo results.ExprInfo
                resolved <- (name, newCte) :: resolved

        Array.iter resolveOne ctes.Exprs

        let newResolveCte (name : EntityName) =
            match Map.tryFind name resultsMap with
            | Some (results, cte) -> Some results
            | None -> resolveCte name
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
        let mapField (fieldName, fieldInfo : SubqueryField) =
            let ref = { Entity = None; Name = fieldName } : FieldRef
            let info =
                { Entity = None
                  Mapping = boundValueToMapping fieldInfo.Bound
                  Type = fieldInfo.Type
                  ForceSQLName = None
                  EntityId = setOpEntityId
                  FieldAttributes = fieldInfo.FieldAttributes
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
            let mapValue = function
                | VVDefault -> raisef ViewResolveException "DEFAULT is not allowed here"
                | VVExpr expr ->
                    let (info, newExpr) = resolveFieldExpr ctx expr
                    exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.Info)
                    VVExpr newExpr
            let mapEntry entry =
                if Array.length entry <> valuesLength then
                    raisef ViewResolveException "Invalid number of items in VALUES entry: %i" (Array.length entry)
                entry |> Array.map mapValue

            let newValues = values |> Array.map mapEntry
            let fields = Seq.init valuesLength (fun i -> (None, emptySubqueryField)) |> Array.ofSeq
            let info =
                { Fields = fields
                  ExprInfo = exprInfo
                  EntityAttributes = Set.empty
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
        let getFields : ResultInfo * ResolvedQueryResult -> FieldName option * SubqueryField = function
            | (_, QRAll alias) -> failwith "Impossible QRAll"
            | (info, QRExpr result) ->
                let subInfo =
                    { Bound = if hasAggregates then None else info.Bound
                      Type = info.Type
                      FieldAttributes = Map.keysSet result.Attributes
                    }
                exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info.ExprInfo)
                (result.TryToName (), subInfo)
        let newFields = rawResults |> Array.map getFields
        try
            newFields |> Seq.mapMaybe fst |> Set.ofSeqUnique |> ignore
        with
            | Failure msg -> raisef ViewResolveException "Clashing result names: %s" msg

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

        let info =
            { Fields = newFields
              ExprInfo = exprInfo
              EntityAttributes = Map.keysSet attributes
            }
        (info, newQuery)

    // Set<EntityName> here is used to check uniqueness of puns.
    and resolveFromExpr (ctx : Context) (fromInfo : FromExprInfo) (isInner : bool) (flags : SelectFlags) : ParsedFromExpr -> FromExprInfo * ResolvedFromExpr = function
        | FEntity entity ->
            if ctx.NoFetches then
                raisef ViewResolveException "Fetches are not allowed here"
            let entityInfo = fromEntityMapping ctx isInner entity
            let fromInfo = unionFromExprInfo fromInfo entityInfo.SingleFrom
            let extra =
                { Id = entityInfo.Id
                  IsInner = isInner
                } : FromEntityIdMeta
            let newEntity =
                { entity with
                    Ref = entityInfo.Entity
                    Extra = ObjectMap.singleton extra
                }
            (fromInfo, FEntity newEntity)
        | FTableExpr tab ->
            let localCtx =
                if tab.Lateral then
                    addFromToContext fromInfo ctx
                else
                    ctx
            let fromEntityId = nextFromEntityId ()
            let (fromInfo, newExpr) =
                match tab.Expression with
                | TESelect sel ->
                    // TODO: Currently we unbind context because we don't support arrows in sub-expressions.
                    let (info, newQ) = resolveSelectExpr (subSelectContext localCtx) { flags with RequireNames = Option.isNone tab.Alias.Fields } sel
                    let fields = applyAlias tab.Alias info.Fields
                    let fieldsMap = getFieldsMap fields
                    let mappingRef = { Schema = None; Name = tab.Alias.Name } : EntityRef
                    let newMapping = fieldsToFieldMapping fromEntityId (Some mappingRef) fieldsMap
                    let entityInfo =
                        { Schema = None
                          EntityAttributes = info.EntityAttributes
                          Id = fromEntityId
                        }
                    let myFromInfo =
                        { Entities = Map.singleton tab.Alias.Name entityInfo
                          Fields = newMapping
                          ExprInfo = info.ExprInfo
                          // TODO: pass type contexts from sub-selects.
                          Types = Map.empty
                        }
                    let fromInfo = unionFromExprInfo fromInfo myFromInfo
                    (fromInfo, TESelect newQ)
                | TEDomain (fieldRef, flags) ->
                    let (info, newRef) = resolveReference localCtx Map.empty fieldRef
                    let fieldType =
                        match info.Type with
                        | Some typ -> typ
                        | _ -> raisef ViewResolveException "Field %O has unknown type" fieldRef
                    if Option.isSome tab.Alias.Fields then
                        raisef ViewResolveException "Can't rename fields from domain"
                    let myFromInfo = domainFromInfo fromEntityId info.InnerValue fieldType tab.Alias.Name flags
                    let fromInfo = unionFromExprInfo fromInfo myFromInfo
                    (fromInfo, TEDomain (newRef, flags))
                | TEFieldDomain (entityRef, fieldName, flags) ->
                    let resRef = resolveMyEntityRef entityRef
                    let entity = findEntityByRef resRef false
                    let field =
                        match entity.FindField fieldName with
                        | None -> raisef ViewResolveException "Field doesn't exist in %O: %O" resRef fieldName
                        | Some f -> f
                    let fieldType =
                        match field.Field with
                        | RColumnField { FieldType = typ } -> typ
                        | _ -> raisef ViewResolveException "Field %O has unknown type" { Entity = resRef; Name = fieldName }
                    let key =
                        { FromId = FIEntity <| nextFromEntityId ()
                          Path = []
                        }
                    let header =
                        { ParentEntity = resRef
                          Name = fieldName
                          Key = key
                          Immediate = true
                          Single = false
                          IsInner = isInner
                        }
                    let bound =
                        { Header = header
                          Ref = { Entity = resRef; Name = field.Name }
                          Entity = entity
                          Field = resolvedFieldToBits field.Field
                          ForceRename = field.ForceRename
                        }
                    let myFromInfo = domainFromInfo fromEntityId (Some <| BVColumn bound) fieldType tab.Alias.Name flags
                    let fromInfo = unionFromExprInfo fromInfo myFromInfo
                    (fromInfo, TEFieldDomain (relaxEntityRef resRef, fieldName, flags))
                | TETypeDomain (fieldType, flags) ->
                    let resType = resolveMyFieldType fieldType
                    let myFromInfo = domainFromInfo fromEntityId None resType tab.Alias.Name flags
                    let fromInfo = unionFromExprInfo fromInfo myFromInfo
                    (fromInfo, TETypeDomain (relaxFieldType resType, flags))
            let newTab = { Alias = tab.Alias; Lateral = tab.Lateral; Expression = newExpr }
            (fromInfo, FTableExpr newTab)
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

    and resolveInsertValue (ctx : Context) : ParsedValuesValue -> ResolvedExprInfo * ResolvedValuesValue = function
        | VVDefault ->
            (emptyResolvedExprInfo, VVDefault)
        | VVExpr expr ->
            let (info, newExpr) = resolveFieldExpr ctx expr
            (info.Info, VVExpr newExpr)

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

        let entityRef = resolveMyEntityRef insert.Entity.Ref
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

        let myresolveInsertValue value =
            let (info, newValue) = resolveInsertValue ctx value
            exprInfo <- unionSubqueryExprInfo exprInfo (resolvedToSubqueryExprInfo info)
            newValue

        let resolveInsertValues vals =
            if Array.length vals <> Array.length insert.Fields then
                raisef ViewResolveException "Number of values is not consistent"
            Array.map myresolveInsertValue vals

        let source =
            match insert.Source with
            | ISDefaultValues -> ISDefaultValues
            | ISSelect { CTEs = None; Tree = SValues allVals } ->
                let newVals = Array.map resolveInsertValues allVals
                ISSelect { CTEs = None; Tree = SValues newVals; Extra = ObjectMap.empty }
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
              EntityAttributes = Set.empty
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

        let entityRef = resolveMyEntityRef update.Entity.Ref
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
              EntityAttributes = Set.empty
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
              EntityAttributes = Set.empty
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
            { emptyContext with
                  FieldMaps = [mapping]
                  LocalEntities = Set.singleton fromEntityId
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
        let mapping = createFromMapping localExprFromEntityId entityRef None flags
        let context =
            { emptyContext with
                  FieldMaps = [mapping]
                  LocalEntities = Set.singleton localExprFromEntityId
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

and private relabelValuesValue = function
    | VVDefault -> VVDefault
    | VVExpr expr -> VVExpr <| relabelFieldExpr expr

and private relabelInsertExpr (insert : ResolvedInsertExpr) : ResolvedInsertExpr =
    let source =
        match insert.Source with
        | ISDefaultValues -> ISDefaultValues
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
    | SValues values -> SValues (Array.map (Array.map relabelValuesValue) values)
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
    | BAArrayMapping _ as mapping -> mapping

and private relabelAttribute (attr : ResolvedAttribute) : ResolvedAttribute =
    { Dependency = attr.Dependency
      Internal = attr.Internal
      Expression = relabelFieldExpr attr.Expression
    }

and private relabelBoundAttribute (attr : ResolvedBoundAttribute) : ResolvedBoundAttribute =
    { Dependency = attr.Dependency
      Internal = attr.Internal
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

and private relabelTableExpr : ResolvedTableExpr -> ResolvedTableExpr = function
    | TESelect subsel -> TESelect <| relabelSelectExpr subsel
    | TEDomain (dom, flags) -> TEDomain (dom, flags)
    | TEFieldDomain (entity, field, flags) -> TEFieldDomain (entity, field, flags)
    | TETypeDomain (ftype, flags) -> TETypeDomain (ftype, flags)

and private relabelFromTableExpr (expr : ResolvedFromTableExpr) : ResolvedFromTableExpr =
    { Expression = relabelTableExpr expr.Expression
      Alias = expr.Alias
      Lateral = expr.Lateral
    }

and private relabelFromExpr : ResolvedFromExpr -> ResolvedFromExpr = function
    | FEntity ent -> FEntity ent
    | FTableExpr expr -> FTableExpr <| relabelFromTableExpr expr
    | FJoin join ->
        FJoin
            { Type = join.Type
              A = relabelFromExpr join.A
              B = relabelFromExpr join.B
              Condition = relabelFieldExpr join.Condition
            }

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

let private resolveArgument (callbacks : ResolveCallbacks) (arg : ParsedArgument) : ResolvedArgument =
    let argType = resolveFieldType callbacks.Layout callbacks.HomeSchema false arg.ArgType
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

let private resolveArgumentsMap (callbacks : ResolveCallbacks) (rawArguments : ParsedArgumentsMap) : ResolvedArgumentsMap * FindArgument =
    let halfLocalArguments = rawArguments |> OrderedMap.map (fun name arg -> resolveArgument callbacks arg)

    let getHalfArgument = function
        | PLocal name -> OrderedMap.tryFind name halfLocalArguments
        | PGlobal name -> Map.tryFind name globalArgumentTypes

    let attrsQualifier = QueryResolver (callbacks, getHalfArgument, emptyExprResolutionFlags)

    let resolveAttrs name (arg : ResolvedArgument) =
        let rawArg = OrderedMap.find name rawArguments
        let newArgs = attrsQualifier.ResolveArgumentAttributesMap rawArg.Attributes
        (PLocal name, { arg with Attributes = newArgs })

    let localArguments = OrderedMap.mapWithKeys resolveAttrs halfLocalArguments

    let getArgument = function
        | PLocal _ as arg -> OrderedMap.tryFind arg localArguments
        | PGlobal name -> Map.tryFind name globalArgumentTypes

    (localArguments, getArgument)

let private getArgumentsQualifier (callbacks : ResolveCallbacks) (arguments : ParsedArgumentsMap) (flags : ExprResolutionFlags) =
    let (localArguments, getArgument) = resolveArgumentsMap callbacks arguments
    let qualifier = QueryResolver (callbacks, getArgument, flags)
    (localArguments, qualifier)

let resolveSelectExpr (callbacks : ResolveCallbacks) (arguments : ParsedArgumentsMap) (flags : ExprResolutionFlags) (select : ParsedSelectExpr) : ResolvedArgumentsMap * ResolvedSelectExpr =
    let (localArguments, qualifier) = getArgumentsQualifier callbacks arguments flags
    let (info, qQuery) = qualifier.ResolveSelectExpr subExprSelectFlags select
    if Array.length info.Fields <> 1 then
        raisef ViewResolveException "Expression queries must have only one resulting column"
    (localArguments, relabelSelectExpr qQuery)

type SingleFieldExprInfo =
    { LocalArguments : ResolvedArgumentsMap
      Flags : ResolvedExprFlags
    }

let resolveSingleFieldExpr (callbacks : ResolveCallbacks) (arguments : ParsedArgumentsMap) (fromEntityId : FromEntityId) (flags : ExprResolutionFlags) (fromMapping : SingleFromMapping) (expr : ParsedFieldExpr) : SingleFieldExprInfo * ResolvedFieldExpr =
    let (localArguments, qualifier) = getArgumentsQualifier callbacks arguments flags
    let (exprInfo, qExpr) = qualifier.ResolveSingleFieldExpr fromEntityId fromMapping expr
    let singleInfo =
        { LocalArguments = localArguments
          Flags = exprInfo.Info.Flags
        }
    (singleInfo, relabelFieldExpr qExpr)

let resolveEntityAttributesMap (callbacks : ResolveCallbacks) (flags : ExprResolutionFlags) (entityRef : ResolvedEntityRef) (attrs : ParsedBoundAttributesMap) : ResolvedBoundAttributesMap =
    let getArgument = function
        | PLocal name -> None
        | PGlobal name -> Map.tryFind name globalArgumentTypes

    let qualifier = QueryResolver (callbacks, getArgument, flags)
    qualifier.ResolveEntityAttributesMap entityRef attrs

let resolveViewExpr (callbacks : ResolveCallbacks) (flags : ExprResolutionFlags) (viewExpr : ParsedViewExpr) : ResolvedViewExpr =
    let (localArguments, qualifier) = getArgumentsQualifier callbacks viewExpr.Arguments flags
    let (info, qQuery) = qualifier.ResolveSelectExpr viewExprSelectFlags viewExpr.Select
    let fieldsMap = getFieldsMap info.Fields
    let mainEntity =
        match viewExpr.MainEntity with
        | None -> detectMainEntity fieldsMap qQuery
        | Some main -> resolveMainEntity callbacks.Layout callbacks.HomeSchema fieldsMap qQuery main
    { Pragmas = Map.filter (fun name v -> Set.contains name allowedPragmas) viewExpr.Pragmas
      Arguments = localArguments
      Select = relabelSelectExpr qQuery
      MainEntity = mainEntity
      Privileged = qualifier.Privileged
    }

let resolveCommandExpr (callbacks : ResolveCallbacks) (flags : ExprResolutionFlags) (cmdExpr : ParsedCommandExpr) : ResolvedCommandExpr =
    let (localArguments, qualifier) = getArgumentsQualifier callbacks cmdExpr.Arguments flags
    let (results, qCommand) = qualifier.ResolveDataExpr viewExprSelectFlags cmdExpr.Command
    { Pragmas = Map.filter (fun name v -> Set.contains name allowedPragmas) cmdExpr.Pragmas
      Arguments = localArguments
      Command = relabelDataExpr qCommand
      Privileged = qualifier.Privileged
    }

type SimpleColumnMeta =
    { IsInner : bool
      BoundEntity : ResolvedEntityRef
      EntityId : FromEntityId
      Path : PathArrow[]
      PathEntities : ResolvedEntityRef[]
    }

let simpleColumnMeta (boundEntity : ResolvedEntityRef) : SimpleColumnMeta =
    { IsInner = true
      BoundEntity = boundEntity
      EntityId = localExprFromEntityId
      Path = [||]
      PathEntities = [||]
    }

let makeColumnReference (layout : Layout) (meta : SimpleColumnMeta) (fieldRef : FieldRef) : LinkedBoundFieldRef =
    assert (Array.length meta.Path = Array.length meta.PathEntities)
    let field = layout.FindField meta.BoundEntity fieldRef.Name |> Option.get
    let boundInfo =
        { Ref =
            { Entity = meta.BoundEntity
              Name = fieldRef.Name
            }
          Immediate = true
          Single = false
          IsInner = meta.IsInner
        } : BoundColumnMeta
    let columnInfo =
        { ForceSQLName = resolvedFieldForcedSQLName field.Field
          EntityId = meta.EntityId
        } : ColumnMeta
    let fieldInfo =
        { Bound = Some (BMColumn boundInfo)
          Column = Some columnInfo
          Type = resolvedFieldType field.Field
          Path = meta.PathEntities
        } : FieldRefMeta
    let column =
        { Ref = VRColumn fieldRef
          Path = meta.Path
          AsRoot = false
        } : LinkedFieldRef
    { Ref = column
      Extra = ObjectMap.singleton fieldInfo
    }

let makeSingleFieldExpr (layout : Layout) (meta : SimpleColumnMeta) (fieldRef : FieldRef) : ResolvedFieldExpr =
    let resultColumn = makeColumnReference layout meta fieldRef
    FERef resultColumn
