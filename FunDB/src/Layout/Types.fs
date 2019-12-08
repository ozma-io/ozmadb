module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.Layout.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type ReferenceRef =
    | RThis of FieldName
    | RRef of FieldName
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | RThis name -> sprintf "%s.%s" (renderSqlName "this") (name.ToFunQLString())
            | RRef name -> sprintf "%s.%s" (renderSqlName "ref") (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString() = this.ToFunQLString()

        member this.ToName () =
            match this with
            | RThis name -> name
            | RRef name -> name

        interface IFunQLName with
            member this.ToName () = this.ToName ()

type ResolvedFieldType = FieldType<ResolvedEntityRef, ReferenceRef>

type ResolvedReferenceFieldExpr = FieldExpr<ResolvedEntityRef, ReferenceRef>

type ResolvedUniqueConstraint =
    { columns : FunQLName array
    }

[<NoComparison>]
type ResolvedCheckConstraint =
    { expression : LocalFieldExpr
    }

[<NoComparison>]
type ResolvedColumnField =
    { fieldType : ResolvedFieldType
      valueType : SQL.SimpleValueType
      defaultValue : FieldValue option
      isNullable : bool
      isImmutable : bool
      inheritedFrom : ResolvedEntityRef option
      columnName : SQL.ColumnName
    }

[<NoComparison>]
type ResolvedComputedField =
    { expression : ResolvedFieldExpr
      // Set when there's no dereferences in the expression
      isLocal : bool
      // Set when computed field uses Id
      hasId : bool
      usedSchemas : UsedSchemas
      inheritedFrom : ResolvedEntityRef option
      allowBroken : bool
    }

[<NoComparison>]
type EntityInheritance =
    { parent : ResolvedEntityRef
      // Expression that verifies that given entry's sub_entity is valid for this type.
      // Column is used unqualified.
      checkExpr : SQL.ValueExpr
    }

[<NoComparison>]
type GenericResolvedField<'col, 'comp> =
    | RColumnField of 'col
    | RComputedField of 'comp
    | RId
    | RSubEntity

[<NoComparison>]
type ResolvedField = GenericResolvedField<ResolvedColumnField, ResolvedComputedField>

[<NoComparison>]
type ComputedFieldError =
    { source : SourceComputedField
      inheritedFrom : ResolvedEntityRef option
      error : exn
    }

let inline genericFindField (getColumnField : FieldName -> 'col option) (getComputedField : FieldName -> 'comp option) (mainField : FieldName) =
    let rec traverse (name : FieldName) =
        if name = funId then
            Some (funId, RId)
        else if name = funSubEntity then
            Some (funSubEntity, RSubEntity)
        else if name = funMain then
            traverse mainField
        else
            match getColumnField name with
            | Some col -> Some (name, RColumnField col)
            | None ->
                match getComputedField name with
                | Some comp -> Some (name, RComputedField comp)
                | None -> None
    traverse

[<NoComparison>]
type ChildEntity =
    { direct : bool
    }

type IEntityFields =
    abstract member FindField : FieldName -> (FunQLName * ResolvedField) option
    abstract member Fields : (FieldName * ResolvedField) seq
    abstract member MainField : FieldName
    abstract member Parent : ResolvedEntityRef option
    abstract member IsAbstract : bool
    abstract member Children : (ResolvedEntityRef * ChildEntity) seq

let hasSubType (entity : IEntityFields) =
        Option.isSome entity.Parent || entity.IsAbstract || not (Seq.isEmpty entity.Children)

[<NoComparison>]
type ResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, Result<ResolvedComputedField, ComputedFieldError>>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, ResolvedCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
      hidden : bool
      inheritance : EntityInheritance option
      subEntityParseExpr : SQL.ValueExpr // Parses SubEntity field into JSON
      children : Map<ResolvedEntityRef, ChildEntity>
      typeName : string // SubEntity value for this entity
      isAbstract : bool
      // Hierarchy root
      root : ResolvedEntityRef
    } with
        member this.FindField (name : FieldName) =
            genericFindField (fun name -> Map.tryFind name this.columnFields) (fun name -> Map.tryFind name this.computedFields |> Option.bind Result.getOption) this.mainField name
        member this.Fields =
            let id = Seq.singleton (funId, RId)
            let subentity =
                if hasSubType this then
                    Seq.singleton (funSubEntity, RSubEntity)
                else
                    Seq.empty
            let columns = this.columnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
            let getComputed = function
            | (name, Error e) -> None
            | (name, Ok comp) -> Some (name, RComputedField comp)
            let computed = this.computedFields |> Map.toSeq |> Seq.mapMaybe getComputed
            Seq.concat [id; subentity; columns; computed]

        interface IEntityFields with
            member this.FindField name = this.FindField name
            member this.Fields = this.Fields
            member this.MainField = this.mainField
            member this.Parent = this.inheritance |> Option.map (fun i -> i.parent)
            member this.IsAbstract = this.isAbstract
            member this.Children = Map.toSeq this.children

// Should be in sync with type names generation in Resolve
let parseTypeName (root : ResolvedEntityRef) (typeName : string) : ResolvedEntityRef =
    match typeName.Split("__") with
    | [| entityName |] -> { root with name = FunQLName entityName }
    | [| schemaName; entityName |] -> { schema = FunQLName schemaName; name = FunQLName entityName }
    | _ -> failwith "Invalid type name"

[<NoComparison>]
type ResolvedSchema =
    { entities : Map<EntityName, ResolvedEntity>
      roots : Set<EntityName>
    }

type ILayoutFields =
    abstract member FindEntity : ResolvedEntityRef -> IEntityFields option

[<NoComparison>]
type Layout =
    { schemas : Map<SchemaName, ResolvedSchema>
    } with
        member this.FindEntity (ref : ResolvedEntityRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema ->
                match Map.tryFind ref.name schema.entities with
                | Some entity when not entity.hidden -> Some entity
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

type ErroredEntity =
    { computedFields : Map<FieldName, exn>
    }
type ErroredSchema = Map<EntityName, ErroredEntity>
type ErroredLayout = Map<SchemaName, ErroredSchema>