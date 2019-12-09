module FunWithFlags.FunDB.Attributes.Merge

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Types

[<NoEquality; NoComparison>]
type MergedAttribute =
    { priority : int
      inherited : bool
      expression : ResolvedFieldExpr
    }

type MergedAttributeMap = Map<AttributeName, MergedAttribute>

[<NoEquality; NoComparison>]
type MergedAttributesEntity =
    { fields : Map<FieldName, MergedAttributeMap>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.fields

[<NoEquality; NoComparison>]
type MergedAttributesSchema =
    { entities : Map<EntityName, MergedAttributesEntity>
    }

[<NoEquality; NoComparison>]
type MergedDefaultAttributes =
    { schemas : Map<SchemaName, MergedAttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptyMergedDefaultAttributes =
    { schemas = Map.empty
    } : MergedDefaultAttributes

let private chooseAttribute (a : MergedAttribute) (b : MergedAttribute) : MergedAttribute =
    if not a.inherited && b.inherited then
        a
    else if a.inherited && not b.inherited then
        b
    else if a.priority < b.priority then
        a
    else
        b

let private mergeAttributeMap (a : MergedAttributeMap) (b : MergedAttributeMap) : MergedAttributeMap =
    Map.unionWith (fun name -> chooseAttribute) a b

let private mergeAttributesEntity (a : MergedAttributesEntity) (b : MergedAttributesEntity) : MergedAttributesEntity =
    { fields = Map.unionWith (fun name -> mergeAttributeMap) a.fields b.fields
    }

let private mergeAttributesSchema (a : MergedAttributesSchema) (b : MergedAttributesSchema) : MergedAttributesSchema =
    { entities = Map.unionWith (fun name -> mergeAttributesEntity) a.entities b.entities
    }

let private mergeAttributesPair (a : MergedDefaultAttributes) (b : MergedDefaultAttributes) : MergedDefaultAttributes =
    { schemas = Map.unionWith (fun name -> mergeAttributesSchema) a.schemas b.schemas
    }

let private makeMergedAttributesField = function
    | Ok field ->
        field.attributes |> Map.map (fun name expr -> { priority = field.priority; expression = expr; inherited = false })
    | Error _ -> Map.empty

let private makeOneMergedAttributesEntity (entityAttrs : AttributesEntity) : MergedAttributesEntity =
    let addNonEmpty name fieldAttrs =
        let ret = makeMergedAttributesField fieldAttrs
        if Map.isEmpty ret then
            None
        else
            Some ret
    { fields = Map.mapMaybe addNonEmpty entityAttrs.fields }

let private markMapInherited (attrs : MergedAttributeMap) : MergedAttributeMap =
    Map.map (fun name attr -> { attr with inherited = true }) attrs

let private markEntityInherited (entityAttrs : MergedAttributesEntity) : MergedAttributesEntity =
    { fields = Map.map (fun name field -> markMapInherited field) entityAttrs.fields }

type private AttributesMerger (layout : Layout, attrs : AttributesDatabase) =
    let mutable mergedAttrs = Map.empty

    let makeMergedAttributesEntity (entityRef : ResolvedEntityRef) (entityAttrs : AttributesEntity) =
        let entity = layout.FindEntity entityRef |> Option.get
        let retEntity = makeOneMergedAttributesEntity entityAttrs
        let retEntity =
            match Map.tryFind entityRef mergedAttrs with
            | None -> retEntity
            | Some old -> mergeAttributesEntity old retEntity
        mergedAttrs <- Map.add entityRef retEntity mergedAttrs
        if not (Map.isEmpty entity.children) then
            let inheritedEntity = markEntityInherited retEntity
            for KeyValue (childRef, childInfo) in entity.children do
                let newAttrs =
                    match Map.tryFind entityRef mergedAttrs with
                    | None -> inheritedEntity
                    | Some old -> mergeAttributesEntity inheritedEntity old
                mergedAttrs <- Map.add childRef newAttrs mergedAttrs

    let makeMergedAttributesSchema (schemaName : SchemaName) (schema : AttributesSchema) =
        let iterEntity name entity =
            match Map.tryFind name schema.entities with
            | None -> ()
            | Some entityAttrs -> makeMergedAttributesEntity { schema = schemaName; name = name } entityAttrs
        Map.iter iterEntity schema.entities

    let makeMergedAttributedDatabase () =
        let iterSchema schemaName schema =
            match Map.tryFind schemaName attrs.schemas with
            | None -> ()
            | Some schemaAttrs -> makeMergedAttributesSchema schemaName schemaAttrs
        Map.iter iterSchema layout.schemas

    member this.MergeDefaultAttributes () =
        makeMergedAttributedDatabase ()
        let schemas =
            mergedAttrs
                |> Map.toSeq
                |> Seq.map (fun (ref, attrs) -> (ref.schema, { entities = Map.singleton ref.name attrs }))
                |> Map.ofSeqWith (fun name -> mergeAttributesSchema)
        { schemas = schemas }

let mergeDefaultAttributes (layout: Layout) (attrs : DefaultAttributes) : MergedDefaultAttributes =
    let mergeOne attrs =
        let merger = AttributesMerger (layout, attrs)
        merger.MergeDefaultAttributes ()

    attrs.schemas |> Map.toSeq |> Seq.map (fun (name, db) -> mergeOne db) |> Seq.fold mergeAttributesPair emptyMergedDefaultAttributes