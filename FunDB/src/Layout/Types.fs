module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

type ResolvedConstraintRef = ResolvedFieldRef

type ReferenceRef =
    | RThis of FieldName
    | RRef of FieldName
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | RThis name -> sprintf "%s.%s" (renderFunQLName "this") (name.ToFunQLString())
            | RRef name -> sprintf "%s.%s" (renderFunQLName "ref") (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString() = this.ToFunQLString()

        member this.ToName () =
            match this with
            | RThis name -> name
            | RRef name -> name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

// Used instead of names in system generated identifiers to ensure that they do not exceed PostgreSQL identifier length restrictions (63 bytes).
type HashName = string

let hashNameLength = 20

type ResolvedReferenceFieldExpr = FieldExpr<ResolvedEntityRef, ReferenceRef>

[<NoEquality; NoComparison>]
type ResolvedUniqueConstraint =
    { Columns : FieldName[]
      HashName : HashName // Guaranteed to be unique in an entity
    }

[<NoEquality; NoComparison>]
type ResolvedCheckConstraint =
    { Expression : ResolvedFieldExpr
      IsLocal : bool
      HashName : HashName // Guaranteed to be unique in an entity
    }

[<NoEquality; NoComparison>]
type ResolvedIndex =
    { Expressions : LocalFieldExpr[]
      HashName : HashName // Guaranteed to be unique in an entity
      IsUnique : bool
    }

[<NoEquality; NoComparison>]
type ResolvedColumnField =
    { FieldType : ResolvedFieldType
      ValueType : SQL.SimpleValueType
      DefaultValue : FieldValue option
      IsNullable : bool
      IsImmutable : bool
      InheritedFrom : ResolvedEntityRef option
      ColumnName : SQL.ColumnName
      HashName : HashName // Guaranteed to be unique for any own field (column or computed) in an entity
    }

[<NoEquality; NoComparison>]
type VirtualFieldCase =
    { Check : SQL.ValueExpr
      Expression : ResolvedFieldExpr
      Ref : ResolvedFieldRef
    }

[<NoEquality; NoComparison>]
type ResolvedComputedField =
    { Expression : ResolvedFieldExpr
      // Set when there's no dereferences in the expression
      IsLocal : bool
      // Set when computed field uses Id
      HasId : bool
      UsedSchemas : UsedSchemas
      InheritedFrom : ResolvedEntityRef option
      AllowBroken : bool
      HashName : HashName // Guaranteed to be unique for any own field (column or computed) in an entity
      VirtualCases : (VirtualFieldCase array) option
    }

[<NoEquality; NoComparison>]
type EntityInheritance =
    { Parent : ResolvedEntityRef
      // Expression that verifies that given entry's sub_entity is valid for this type.
      // Column is used unqualified.
      CheckExpr : SQL.ValueExpr
    }

[<NoEquality; NoComparison>]
type GenericResolvedField<'col, 'comp> =
    | RColumnField of 'col
    | RComputedField of 'comp
    | RId
    | RSubEntity

type ResolvedField = GenericResolvedField<ResolvedColumnField, ResolvedComputedField>

type FieldInfo<'col, 'comp> =
    { Name : FieldName
      // If a field is considered to not have an implicit name (so an explicit one is required in `SELECT`).
      ForceRename : bool
      Field : GenericResolvedField<'col, 'comp>
    }

type ResolvedFieldInfo = FieldInfo<ResolvedColumnField, ResolvedComputedField>

[<NoEquality; NoComparison>]
type ChildEntity =
    { Direct : bool
    }

type IEntityFields =
    abstract member FindField : FieldName ->  ResolvedFieldInfo option
    abstract member Fields : (FieldName * ResolvedField) seq
    abstract member MainField : FieldName
    abstract member Parent : ResolvedEntityRef option
    abstract member IsAbstract : bool
    abstract member Children : (ResolvedEntityRef * ChildEntity) seq

let hasSubType (entity : IEntityFields) =
        Option.isSome entity.Parent || entity.IsAbstract || not (Seq.isEmpty entity.Children)

let inline genericFindField (getColumnField : FieldName -> 'col option) (getComputedField : FieldName -> 'comp option) (fields : IEntityFields) : FieldName -> FieldInfo<'col, 'comp> option =
    let rec traverse (name : FieldName) =
        if name = funId then
            Some { Name = funId; ForceRename = false; Field = RId }
        else if name = funSubEntity && hasSubType fields then
            Some { Name = funSubEntity; ForceRename = false; Field = RSubEntity }
        else if name = funMain then
            // We set name `main` by default for main fields, otherwise name may spontaneously clash in the future.
            Option.map (fun f -> { f with ForceRename = true }) <| traverse fields.MainField
        else
            match getColumnField name with
            | Some col -> Some { Name = name; ForceRename = false; Field = RColumnField col }
            | None ->
                match getComputedField name with
                | Some comp -> Some { Name = name; ForceRename = false; Field = RComputedField comp }
                | None -> None
    traverse

[<NoEquality; NoComparison>]
type ResolvedEntity =
    { ColumnFields : Map<FieldName, ResolvedColumnField>
      ComputedFields : Map<FieldName, Result<ResolvedComputedField, exn>>
      UniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      CheckConstraints : Map<ConstraintName, ResolvedCheckConstraint>
      Indexes : Map<IndexName, ResolvedIndex>
      MainField : FieldName
      ForbidExternalReferences : bool
      ForbidTriggers : bool
      TriggersMigration : bool
      IsHidden : bool
      Inheritance : EntityInheritance option
      SubEntityParseExpr : SQL.ValueExpr // Parses SubEntity field into JSON
      Children : Map<ResolvedEntityRef, ChildEntity>
      TypeName : string // SubEntity value for this entity
      HashName : HashName // Guaranteed to be unique for any entity in a schema
      IsAbstract : bool
      IsFrozen : bool
      // Hierarchy root
      Root : ResolvedEntityRef
    } with
        member this.FindField (name : FieldName) =
            genericFindField (fun name -> Map.tryFind name this.ColumnFields) (fun name -> Map.tryFind name this.ComputedFields |> Option.bind Result.getOption) this name

        interface IEntityFields with
            member this.FindField name = this.FindField name
            member this.Fields =
                let id = Seq.singleton (funId, RId)
                let subentity =
                    if hasSubType this then
                        Seq.singleton (funSubEntity, RSubEntity)
                    else
                        Seq.empty
                let columns = this.ColumnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
                let getComputed = function
                | (name, Error e) -> None
                | (name, Ok comp) -> Some (name, RComputedField comp)
                let computed = this.ComputedFields |> Map.toSeq |> Seq.mapMaybe getComputed
                Seq.concat [id; subentity; columns; computed]
            member this.MainField = this.MainField
            member this.Parent = this.Inheritance |> Option.map (fun i -> i.Parent)
            member this.IsAbstract = this.IsAbstract
            member this.Children = Map.toSeq this.Children

// Should be in sync with type names generation in Resolve
let parseTypeName (root : ResolvedEntityRef) (typeName : string) : ResolvedEntityRef =
    match typeName.Split("__") with
    | [| entityName |] -> { root with name = FunQLName entityName }
    | [| schemaName; entityName |] -> { schema = FunQLName schemaName; name = FunQLName entityName }
    | _ -> failwith "Invalid type name"

[<NoEquality; NoComparison>]
type ResolvedSchema =
    { Entities : Map<EntityName, ResolvedEntity>
      Roots : Set<EntityName>
    }

type ILayoutFields =
    abstract member FindEntity : ResolvedEntityRef -> IEntityFields option

[<NoEquality; NoComparison>]
type Layout =
    { Schemas : Map<SchemaName, ResolvedSchema>
    } with
        member this.FindEntity (ref : ResolvedEntityRef) =
            match Map.tryFind ref.schema this.Schemas with
            | None -> None
            | Some schema ->
                match Map.tryFind ref.name schema.Entities with
                | Some entity when not entity.IsHidden -> Some entity
                | _ -> None

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

        interface ILayoutFields with
            member this.FindEntity ref = Option.map (fun e -> e :> IEntityFields) (this.FindEntity ref)

let mapAllFields (f : FieldName -> ResolvedField -> 'a) (entity : IEntityFields) : Map<FieldName, 'a> =
    entity.Fields |> Seq.map (fun (name, field) -> (name, f name field)) |> Map.ofSeq

let rec checkInheritance (layout : ILayoutFields) (parentRef : ResolvedEntityRef) (childRef : ResolvedEntityRef) =
    if parentRef = childRef then
        true
    else
        let childEntity = layout.FindEntity childRef |> Option.get
        match childEntity.Parent with
        | None -> false
        | Some childParentRef -> checkInheritance layout parentRef childParentRef

let filterLayout (f : SchemaName -> bool) (layout : Layout) : Layout =
    { Schemas = Map.filter (fun name schema -> f name) layout.Schemas
    }

type ErroredEntity =
    { ComputedFields : Map<FieldName, exn>
    }
type ErroredSchema = Map<EntityName, ErroredEntity>
type ErroredLayout = Map<SchemaName, ErroredSchema>