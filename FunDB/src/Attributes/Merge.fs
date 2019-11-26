module FunWithFlags.FunDB.Attributes.Merge

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Types

[<NoComparison>]
type MergedAttribute =
    { priority : int
      expression : ResolvedFieldExpr
    }

type MergedAttributeMap = Map<AttributeName, MergedAttribute>

[<NoComparison>]
type MergedAttributesEntity =
    { fields : Map<FieldName, MergedAttributeMap>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.fields

[<NoComparison>]
type MergedAttributesSchema =
    { entities : Map<EntityName, MergedAttributesEntity>
    }

[<NoComparison>]
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
    if a.priority < b.priority then
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
        field.attributes |> Map.map (fun name expr -> { priority = field.priority; expression = expr })
    | Error _ -> Map.empty

type private AttributesMerger (layout : Layout, attrs : AttributesDatabase) =
    let mutable mergedAttrs = Map.empty

    let rec makeOneMergedAttributesEntity (entityRef : ResolvedEntityRef) (entityAttrs : AttributesEntity) : MergedAttributesEntity =
        let entity = layout.FindEntity entityRef |> Option.get
        let addNonEmpty name fieldAttrs =
            let ret = makeMergedAttributesField fieldAttrs
            if Map.isEmpty ret then
                None
            else
                Some ret
        let myEntity = { fields = Map.mapMaybe addNonEmpty entityAttrs.fields }
        match entity.inheritance with
        | None -> myEntity
        | Some inher ->
            match attrs.FindEntity inher.parent with
            | None -> myEntity
            | Some parentAttrs ->
                let parentEntity = makeMergedAttributesEntity inher.parent parentAttrs
                mergeAttributesEntity parentEntity myEntity

    and makeMergedAttributesEntity (entityRef : ResolvedEntityRef) (entityAttrs : AttributesEntity) : MergedAttributesEntity =
        match Map.tryFind entityRef mergedAttrs with
        | Some ret -> ret
        | None ->
            let retEntity = makeOneMergedAttributesEntity entityRef entityAttrs
            mergedAttrs <- Map.add entityRef retEntity mergedAttrs
            retEntity

    let makeMergedAttributesSchema (schemaName : SchemaName) (schema : AttributesSchema) : MergedAttributesSchema =
        let addNonEmpty name entityAttrs =
            let ret = makeMergedAttributesEntity { schema = schemaName; name = name } entityAttrs
            if Map.isEmpty ret.fields then
                None
            else
                Some ret
        { entities = Map.mapMaybe addNonEmpty schema.entities
        }

    let makeMergedAttributedDatabase () : MergedDefaultAttributes =
        let addNonEmpty schemaName schemaAttrs =
            let ret = makeMergedAttributesSchema schemaName schemaAttrs
            if Map.isEmpty ret.entities then
                None
            else
                Some ret
        { schemas = Map.mapMaybe addNonEmpty attrs.schemas
        }

    member this.MergeDefaultAttributes = makeMergedAttributedDatabase

let mergeDefaultAttributes (layout: Layout) (attrs : DefaultAttributes) : MergedDefaultAttributes =
    let mergeOne attrs =
        let merger = AttributesMerger (layout, attrs)
        merger.MergeDefaultAttributes ()

    attrs.schemas |> Map.toSeq |> Seq.map (fun (name, db) -> mergeOne db) |> Seq.fold mergeAttributesPair emptyMergedDefaultAttributes