module OzmaDB.Layout.Types

open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.Utils
open OzmaDB.OzmaQL.AST

module SQL = OzmaDB.SQL.AST

open OzmaDB.Layout.Source
open OzmaDB.Objects.Types

type ResolvedConstraintRef = ResolvedFieldRef
type ResolvedIndexRef = ResolvedFieldRef

type ReferenceRef =
    | RThis of FieldName
    | RRef of FieldName

    override this.ToString() = this.ToOzmaQLString()

    member this.ToOzmaQLString() =
        match this with
        | RThis name -> sprintf "%s.%s" (renderOzmaQLName "this") (name.ToOzmaQLString())
        | RRef name -> sprintf "%s.%s" (renderOzmaQLName "ref") (name.ToOzmaQLString())

    interface IOzmaQLString with
        member this.ToOzmaQLString() = this.ToOzmaQLString()

    member this.ToName() =
        match this with
        | RThis name -> name
        | RRef name -> name

    interface IOzmaQLName with
        member this.ToName() = this.ToName()

// Used instead of names in system generated identifiers to ensure that they do not exceed PostgreSQL identifier length restrictions (63 bytes).
type HashName = string

type ResolvedReferenceFieldExpr = FieldExpr<ResolvedEntityRef, ReferenceRef>

[<NoEquality; NoComparison>]
type ResolvedUniqueConstraint =
    { Columns: FieldName[]
      IsAlternateKey: bool
      InheritedFrom: ResolvedEntityRef option
      HashName: HashName } // Guaranteed to be unique in an entity.

[<NoEquality; NoComparison>]
type ResolvedCheckConstraint =
    { Expression: ResolvedFieldExpr
      IsLocal: bool // No arrows or subqueries.
      UsedDatabase: UsedDatabase // Needed for domains.
      HashName: HashName } // Guaranteed to be unique in an entity.

[<NoEquality; NoComparison>]
type ResolvedIndex =
    { Expressions: ResolvedIndexColumn[]
      IncludedExpressions: ResolvedFieldExpr[]
      HashName: HashName // Guaranteed to be unique in an entity.
      IsUnique: bool
      Predicate: ResolvedFieldExpr option
      Type: IndexType }

[<NoEquality; NoComparison>]
type ResolvedColumnField =
    { FieldType: ResolvedFieldType
      ValueType: SQL.SimpleValueType
      DefaultValue: FieldValue option
      IsNullable: bool
      IsImmutable: bool
      InheritedFrom: ResolvedEntityRef option
      ColumnName: SQL.ColumnName
      HashName: HashName } // Guaranteed to be unique for any own field (column or computed) in an entity.

let fieldIsOptional (field: ResolvedColumnField) =
    Option.isSome field.DefaultValue || field.IsNullable

[<NoEquality; NoComparison>]
type VirtualFieldCase =
    { Ref: ResolvedEntityRef
      PossibleEntities: Set<ResolvedEntityRef> }

type IVirtualFieldBits =
    // Root virtual field in hierarchy.
    abstract member InheritedFrom: ResolvedEntityRef option

[<NoEquality; NoComparison>]
type VirtualField =
    { Cases: VirtualFieldCase[]
      InheritedFrom: ResolvedEntityRef option }

    interface IVirtualFieldBits with
        member this.InheritedFrom = this.InheritedFrom

// These fields are available without fully resolving a computed field.
type IComputedFieldBits =
    abstract member ColumnName: SQL.ColumnName
    abstract member IsMaterialized: bool
    abstract member InheritedFrom: ResolvedEntityRef option
    abstract member AllowBroken: bool
    abstract member Virtual: IVirtualFieldBits option

[<NoEquality; NoComparison>]
type RootComputedField =
    { Type: ResolvedFieldType
      UsedDatabase: UsedDatabase
      Flags: ResolvedExprFlags }

[<NoEquality; NoComparison>]
type ResolvedComputedField =
    { Expression: ResolvedFieldExpr
      InheritedFrom: ResolvedEntityRef option
      AllowBroken: bool
      ColumnName: SQL.ColumnName
      HashName: HashName // Guaranteed to be unique for any own field (column or computed) in an entity.
      Virtual: VirtualField option
      IsMaterialized: bool
      Type: ResolvedFieldType
      // These don't take virtual field cases into account.
      UsedDatabase: UsedDatabase
      Flags: ResolvedExprFlags
      // But this one does.
      Root: RootComputedField option }

    interface IComputedFieldBits with
        member this.ColumnName = this.ColumnName
        member this.IsMaterialized = this.IsMaterialized
        member this.InheritedFrom = this.InheritedFrom
        member this.AllowBroken = this.AllowBroken
        member this.Virtual = Option.map (fun x -> upcast x) this.Virtual

[<NoEquality; NoComparison>]
type GenericResolvedField<'col, 'comp> =
    | RColumnField of 'col
    | RComputedField of 'comp
    | RId
    | RSubEntity

let mapResolvedField
    (colFunc: 'col1 -> 'col2)
    (compFunc: 'comp1 -> 'comp2)
    : GenericResolvedField<'col1, 'comp1> -> GenericResolvedField<'col2, 'comp2> =
    function
    | RColumnField col -> RColumnField(colFunc col)
    | RComputedField comp -> RComputedField(compFunc comp)
    | RId -> RId
    | RSubEntity -> RSubEntity

type ResolvedField = GenericResolvedField<ResolvedColumnField, ResolvedComputedField>

type ResolvedFieldBits = GenericResolvedField<ResolvedColumnField, IComputedFieldBits>

let resolvedFieldToBits: ResolvedField -> ResolvedFieldBits =
    mapResolvedField id (fun x -> x :> IComputedFieldBits)

let resolvedFieldBitsType: ResolvedFieldBits -> ResolvedFieldType option =
    function
    | RId -> Some(FTScalar SFTInt)
    | RSubEntity -> Some(FTScalar SFTJson)
    | RColumnField { FieldType = typ } -> Some typ
    | RComputedField _ -> None

let resolvedFieldType: ResolvedField -> ResolvedFieldType =
    function
    | RId -> FTScalar SFTInt
    | RSubEntity -> FTScalar SFTJson
    | RColumnField { FieldType = typ } -> typ
    | RComputedField { Type = typ } -> typ

type FieldInfo<'col, 'comp> =
    { Name: FieldName
      // If a field is considered to not have an implicit name (so an explicit one is required in `SELECT`).
      ForceRename: bool
      Field: GenericResolvedField<'col, 'comp> }

type ResolvedFieldInfo = FieldInfo<ResolvedColumnField, ResolvedComputedField>

type ResolvedFieldBitsInfo = FieldInfo<ResolvedColumnField, IComputedFieldBits>

let resolvedFieldInfoToBits (info: ResolvedFieldInfo) : ResolvedFieldBitsInfo =
    { Name = info.Name
      ForceRename = info.ForceRename
      Field = mapResolvedField id (fun x -> x :> IComputedFieldBits) info.Field }

[<NoEquality; NoComparison>]
type ChildEntity = { Direct: bool }

type IEntityBits =
    abstract member FindField: FieldName -> ResolvedFieldInfo option
    abstract member FindFieldBits: FieldName -> ResolvedFieldBitsInfo option
    abstract member Fields: (FieldName * ResolvedFieldBits) seq
    abstract member ColumnFields: (FieldName * ResolvedColumnField) seq
    abstract member MainField: FieldName
    abstract member TypeName: string
    abstract member Parent: ResolvedEntityRef option
    abstract member IsAbstract: bool
    abstract member IsHidden: bool
    abstract member Children: Map<ResolvedEntityRef, ChildEntity>

let hasSubType (entity: IEntityBits) =
    Option.isSome entity.Parent
    || entity.IsAbstract
    || not (Seq.isEmpty entity.Children)

let inline genericFindField
    ([<InlineIfLambda>] getColumnField: FieldName -> 'col option)
    ([<InlineIfLambda>] getComputedField: FieldName -> 'comp option)
    (fields: IEntityBits)
    : FieldName -> FieldInfo<'col, 'comp> option =
    let rec traverse (name: FieldName) =
        if name = funId then
            Some
                { Name = funId
                  ForceRename = false
                  Field = RId }
        else if name = funSubEntity && hasSubType fields then
            Some
                { Name = funSubEntity
                  ForceRename = false
                  Field = RSubEntity }
        else if name = funMain then
            // We force renaming main fields, otherwise the name may spontaneously clash in the future.
            Option.map (fun f -> { f with ForceRename = true }) <| traverse fields.MainField
        else
            match getColumnField name with
            | Some col ->
                Some
                    { Name = name
                      ForceRename = false
                      Field = RColumnField col }
            | None ->
                match getComputedField name with
                | Some comp ->
                    Some
                        { Name = name
                          ForceRename = false
                          Field = RComputedField comp }
                | None -> None

    traverse

let getColumnName (entity: IEntityBits) (name: FieldName) : SQL.ColumnName =
    match entity.FindField name |> Option.get with
    | { Field = RComputedField comp } when comp.IsMaterialized -> comp.ColumnName
    | { Field = RColumnField col } -> col.ColumnName
    | _ -> failwithf "No column name for field %O" name

[<NoEquality; NoComparison>]
type ResolvedEntity =
    { ColumnFields: Map<FieldName, ResolvedColumnField>
      ComputedFields: Map<FieldName, PossiblyBroken<ResolvedComputedField>>
      UniqueConstraints: Map<ConstraintName, ResolvedUniqueConstraint>
      CheckConstraints: Map<ConstraintName, ResolvedCheckConstraint>
      Indexes: Map<IndexName, ResolvedIndex>
      MainField: FieldName
      InsertedInternally: bool
      UpdatedInternally: bool
      DeletedInternally: bool
      TriggersMigration: bool
      IsHidden: bool
      SaveRestoreKey: ConstraintName option
      Parent: ResolvedEntityRef option
      Children: Map<ResolvedEntityRef, ChildEntity>
      TypeName: string // SubEntity value for this entity
      HashName: HashName // Guaranteed to be unique for any entity in a schema
      IsAbstract: bool
      IsFrozen: bool
      Root: ResolvedEntityRef // Hierarchy root
      ReferencingFields: Map<ResolvedFieldRef, ReferenceDeleteAction>
      RequiredFields: Set<FieldName>
      CascadeDeleted: bool } // If an entity with this type or any child of it can be CASCADE DELETE'd

    member this.FindField(name: FieldName) =
        genericFindField
            (fun name -> Map.tryFind name this.ColumnFields)
            (fun name -> Map.tryFind name this.ComputedFields |> Option.bind Result.getOption)
            this
            name

    interface IEntityBits with
        member this.FindField name = this.FindField name

        member this.FindFieldBits name =
            Option.map resolvedFieldInfoToBits <| this.FindField name

        member this.Fields =
            let id = Seq.singleton (funId, RId)

            let subentity =
                if hasSubType this then
                    Seq.singleton (funSubEntity, RSubEntity)
                else
                    Seq.empty

            let columns =
                this.ColumnFields
                |> Map.toSeq
                |> Seq.map (fun (name, col) -> (name, RColumnField col))

            let getComputed =
                function
                | (name, Error e) -> None
                | (name, Ok comp) -> Some(name, RComputedField(comp :> IComputedFieldBits))

            let computed = this.ComputedFields |> Map.toSeq |> Seq.mapMaybe getComputed
            Seq.concat [ id; subentity; columns; computed ]

        member this.ColumnFields = Map.toSeq this.ColumnFields
        member this.MainField = this.MainField
        member this.TypeName = this.TypeName
        member this.Parent = this.Parent
        member this.IsAbstract = this.IsAbstract
        member this.IsHidden = this.IsHidden
        member this.Children = this.Children

// Should be in sync with type names generation in Resolve
let parseTypeName (root: ResolvedEntityRef) (typeName: string) : ResolvedEntityRef =
    match typeName.Split("__") with
    | [| entityName |] ->
        { root with
            Name = OzmaQLName entityName }
    | [| schemaName; entityName |] ->
        { Schema = OzmaQLName schemaName
          Name = OzmaQLName entityName }
    | _ -> failwith "Invalid type name"

let uniqueConstraintSQLName (entityName: HashName) (constrName: HashName) =
    SQL.SQLName <| sprintf "__unique__%s__%s" entityName constrName

let checkConstraintSQLName (entityName: HashName) (constrName: HashName) =
    SQL.SQLName <| sprintf "__check__%s__%s" entityName constrName

let indexSQLName (entityName: HashName) (indexName: HashName) =
    SQL.SQLName <| sprintf "__index__%s__%s" entityName indexName

let foreignConstraintSQLName (entityName: HashName) (fieldName: HashName) =
    SQL.SQLName <| sprintf "__foreign__%s__%s" entityName fieldName

[<NoEquality; NoComparison>]
type ResolvedSchema =
    { Entities: Map<EntityName, ResolvedEntity>
      ForeignConstraintNames: Map<SQL.SQLName, ResolvedFieldRef>
      UniqueConstraintNames: Map<SQL.SQLName, ResolvedConstraintRef>
      CheckConstraintNames: Map<SQL.SQLName, ResolvedConstraintRef>
      IndexNames: Map<SQL.SQLName, ResolvedIndexRef> }

type IEntitiesSet = Source.IEntitiesSet

type ILayoutBits =
    inherit IEntitiesSet

    abstract member FindEntity: ResolvedEntityRef -> IEntityBits option

let emptyLayoutBits =
    { new ILayoutBits with
        member this.FindEntity ent = None
        member this.HasVisibleEntity ent = false }

[<NoEquality; NoComparison>]
type Layout =
    { Schemas: Map<SchemaName, ResolvedSchema>
      // Save/restored entities are sorted topologically by their dependencies.
      SaveRestoredEntities: ResolvedEntityRef[] }

    member this.FindEntity(ref: ResolvedEntityRef) =
        match Map.tryFind ref.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind ref.Name schema.Entities

    member this.FindField (entity: ResolvedEntityRef) (field: FieldName) =
        this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

    interface IEntitiesSet with
        member this.HasVisibleEntity ref =
            match this.FindEntity ref with
            | Some entity when not entity.IsHidden -> true
            | _ -> false

    interface ILayoutBits with
        member this.FindEntity ref =
            Option.map (fun e -> e :> IEntityBits) (this.FindEntity ref)

let mapAllFields (f: FieldName -> ResolvedFieldBits -> 'a) (entity: IEntityBits) : Map<FieldName, 'a> =
    entity.Fields
    |> Seq.map (fun (name, field) -> (name, f name field))
    |> Map.ofSeq

let rec checkInheritance (layout: ILayoutBits) (parentRef: ResolvedEntityRef) (childRef: ResolvedEntityRef) =
    if parentRef = childRef then
        true
    else
        let childEntity = layout.FindEntity childRef |> Option.get

        match childEntity.Parent with
        | None -> false
        | Some childParentRef -> checkInheritance layout parentRef childParentRef

let filterLayout (f: SchemaName -> bool) (layout: Layout) : Layout =
    { Schemas = layout.Schemas |> Map.filter (fun name schema -> f name)
      SaveRestoredEntities = layout.SaveRestoredEntities }

type PossibleEntities<'a> =
    | PEList of 'a
    | PEAny

let getPossibleEntitiesList =
    function
    | PEList a -> Some a
    | PEAny -> None

let mapPossibleEntities (f: 'a -> 'b) =
    function
    | PEList a -> PEList(f a)
    | PEAny -> PEAny

let private allPossibleEntitiesList'
    (layout: ILayoutBits)
    (parentRef: ResolvedEntityRef)
    (parentEntity: IEntityBits)
    : (ResolvedEntityRef * IEntityBits) seq =
    let getEntity ref =
        let entity = layout.FindEntity ref |> Option.get
        if entity.IsAbstract then None else Some(ref, entity)

    let childrenEntities = parentEntity.Children |> Map.keys |> Seq.mapMaybe getEntity

    let realEntity =
        if parentEntity.IsAbstract then
            Seq.empty
        else
            Seq.singleton (parentRef, parentEntity)

    Seq.append realEntity childrenEntities

let allPossibleEntitiesList
    (layout: ILayoutBits)
    (parentRef: ResolvedEntityRef)
    : (ResolvedEntityRef * IEntityBits) seq =
    let parentEntity = layout.FindEntity parentRef |> Option.get
    allPossibleEntitiesList' layout parentRef parentEntity

let allPossibleEntities
    (layout: ILayoutBits)
    (parentRef: ResolvedEntityRef)
    : PossibleEntities<(ResolvedEntityRef * IEntityBits) seq> =
    let parentEntity = layout.FindEntity parentRef |> Option.get

    if Option.isNone parentEntity.Parent then
        PEAny
    else
        PEList <| allPossibleEntitiesList' layout parentRef parentEntity

let allowedOpClasses =
    Map.ofSeq
        [ (OzmaQLName "trgm", Map.ofSeq [ (ITGIST, SQL.SQLName "gist_trgm_ops"); (ITGIN, SQL.SQLName "gin_trgm_ops") ]) ]

let schemasNameFieldRef: ResolvedFieldRef =
    { Entity =
        { Schema = funSchema
          Name = OzmaQLName "schemas" }
      Name = OzmaQLName "name" }

type IndexTypeInfo = { CanOrder: bool }

let getIndexTypeInfo =
    function
    | ITBTree -> { CanOrder = true }
    | ITGIST -> { CanOrder = false }
    | ITGIN -> { CanOrder = false }
