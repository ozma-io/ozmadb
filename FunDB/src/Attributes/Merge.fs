module FunWithFlags.FunDB.Attributes.Merge

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Layout.Types

[<NoEquality; NoComparison>]
type MergedAttribute =
    { SchemaName : SchemaName
      Priority : int
      Inherited : bool
      Expression : ResolvedFieldExpr
    }

type MergedAttributeMap = Map<AttributeName, MergedAttribute>

[<NoEquality; NoComparison>]
type MergedAttributesEntity =
    { Fields : Map<FieldName, MergedAttributeMap>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.Fields

[<NoEquality; NoComparison>]
type MergedAttributesSchema =
    { Entities : Map<EntityName, MergedAttributesEntity>
    }

[<NoEquality; NoComparison>]
type MergedDefaultAttributes =
    { Schemas : Map<SchemaName, MergedAttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.Entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptyMergedDefaultAttributes =
    { Schemas = Map.empty
    } : MergedDefaultAttributes

let private chooseAttribute (a : MergedAttribute) (b : MergedAttribute) : MergedAttribute =
    if not a.Inherited && b.Inherited then
        a
    else if a.Inherited && not b.Inherited then
        b
    else if a.Priority < b.Priority then
        a
    else if a.SchemaName < b.SchemaName then
        a
    else
        b

let private mergeAttributeMap (a : MergedAttributeMap) (b : MergedAttributeMap) : MergedAttributeMap =
    Map.unionWith (fun name -> chooseAttribute) a b

let private mergeAttributesEntity (a : MergedAttributesEntity) (b : MergedAttributesEntity) : MergedAttributesEntity =
    { Fields = Map.unionWith (fun name -> mergeAttributeMap) a.Fields b.Fields
    }

let private mergeAttributesSchema (a : MergedAttributesSchema) (b : MergedAttributesSchema) : MergedAttributesSchema =
    { Entities = Map.unionWith (fun name -> mergeAttributesEntity) a.Entities b.Entities
    }

let private mergeAttributesPair (a : MergedDefaultAttributes) (b : MergedDefaultAttributes) : MergedDefaultAttributes =
    { Schemas = Map.unionWith (fun name -> mergeAttributesSchema) a.Schemas b.Schemas
    }

let private makeMergedAttributesField (schemaName : SchemaName) = function
    | Ok field ->
        field.Attributes |> Map.map (fun name expr -> { SchemaName = schemaName; Priority = field.Priority; Expression = expr; Inherited = false })
    | Error _ -> Map.empty

let private makeOneMergedAttributesEntity (schemaName : SchemaName) (entityAttrs : AttributesEntity) : MergedAttributesEntity =
    let addNonEmpty name fieldAttrs =
        let ret = makeMergedAttributesField schemaName fieldAttrs
        if Map.isEmpty ret then
            None
        else
            Some ret
    { Fields = Map.mapMaybe addNonEmpty entityAttrs.Fields }

let private markMapInherited (attrs : MergedAttributeMap) : MergedAttributeMap =
    Map.map (fun name attr -> { attr with Inherited = true }) attrs

let private markEntityInherited (entityAttrs : MergedAttributesEntity) : MergedAttributesEntity =
    { Fields = Map.map (fun name field -> markMapInherited field) entityAttrs.Fields }

type private AttributesMerger (layout : Layout) =
    let emitMergedAttributesEntity (schemaName : SchemaName) (attrEntityRef : ResolvedEntityRef) (entityAttrs : AttributesEntity) =
        seq {
            let entity = layout.FindEntity attrEntityRef |> Option.get
            let retEntity = makeOneMergedAttributesEntity schemaName entityAttrs
            if not (Map.isEmpty retEntity.Fields) then
                yield (attrEntityRef, retEntity)
                if not (Map.isEmpty entity.children) then
                    let inheritedEntity = markEntityInherited retEntity
                    for KeyValue (childRef, childInfo) in entity.children do
                        yield (childRef, inheritedEntity)
        }

    let emitDefaultAttributes (attrs : DefaultAttributes) =
        seq {
            for KeyValue(schemaName, attrsDb) in attrs.Schemas do
                for KeyValue(attrSchemaName, schema) in attrsDb.Schemas do
                    for KeyValue(attrEntityName, entity) in schema.Entities do
                        yield! emitMergedAttributesEntity schemaName { schema = attrSchemaName; name = attrEntityName } entity
        }

    let mergeDefaultAttributes (attrs : DefaultAttributes) : MergedDefaultAttributes =
        let schemas =
            emitDefaultAttributes attrs
            |> Map.ofSeqWith (fun ref attrs1 attrs2 -> mergeAttributesEntity attrs1 attrs2)
            |> Map.toSeq
            |> Seq.map (fun (ref, attrs) -> (ref.schema, { Entities = Map.singleton ref.name attrs }))
            |> Map.ofSeqWith (fun name -> mergeAttributesSchema)
        { Schemas = schemas }

    member this.MergeDefaultAttributes attrs = mergeDefaultAttributes attrs

let mergeDefaultAttributes (layout: Layout) (attrs : DefaultAttributes) : MergedDefaultAttributes =
    let merger = AttributesMerger (layout)
    merger.MergeDefaultAttributes attrs