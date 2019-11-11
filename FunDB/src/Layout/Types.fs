module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Utils
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
    }

[<NoComparison>]
type EntityInheritance =
    { parent : ResolvedEntityRef
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

let inline genericFindField (columnFields : Map<FieldName, 'col>) (computedFields : Map<FieldName, 'comp>) (mainField : FieldName) =
    let rec traverse (name : FieldName) =
        if name = funId then
            Some (funId, RId)
        else if name = funSubEntity then
            Some (funSubEntity, RSubEntity)
        else if name = funMain then
            traverse mainField
        else
            match Map.tryFind name columnFields with
            | Some col -> Some (name, RColumnField col)
            | None ->
                match Map.tryFind name computedFields with
                | Some comp -> Some (name, RComputedField comp)
                | None -> None
    traverse

type IEntityFields =
    abstract member FindField : FieldName -> (FunQLName * ResolvedField) option
    abstract member Fields : (FieldName * ResolvedField) seq
    abstract member MainField : FieldName

[<NoComparison>]
type ResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, ResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, ResolvedCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
      hidden : bool
      inheritance : EntityInheritance option
      children : Set<ResolvedEntityRef>
      typeName : string // SubEntity value for this entity
      isAbstract : bool
      // Hierarchy root
      root : ResolvedEntityRef
    } with
        member this.FindField (name : FieldName) =
            genericFindField this.columnFields this.computedFields this.mainField name
        member this.Fields =
            let id = Seq.singleton (funId, RId)
            let subentity =
                if this.HasSubType then
                    Seq.singleton (funSubEntity, RSubEntity)
                else
                    Seq.empty
            let columns = this.columnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
            let computed = this.computedFields |> Map.toSeq |> Seq.map (fun (name, comp) -> (name, RComputedField comp))
            Seq.concat [id; subentity; columns; computed]

        member this.HasSubType =
            Option.isSome this.inheritance || this.isAbstract || not (Set.isEmpty this.children)

        member this.MainField = this.mainField

        interface IEntityFields with
            member this.FindField name = this.FindField name
            member this.Fields = this.Fields
            member this.MainField = this.MainField

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

let mapAllFields (f : FieldName -> ResolvedField -> 'a) (entity : IEntityFields) : Map<FieldName, 'a> =
    entity.Fields |> Seq.map (fun (name, field) -> (name, f name field)) |> Map.ofSeq
