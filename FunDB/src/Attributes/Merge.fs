module FunWithFlags.FunDB.Attributes.Merge

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Types

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

let private makeMergedAttributesField = function
    | Ok field ->
        field.attributes |> Map.map (fun name expr -> { priority = field.priority; expression = expr })
    | Error _ -> Map.empty

let private makeMergedAttributesEntity (entity : AttributesEntity) : MergedAttributesEntity =
    { fields = Map.map (fun name -> makeMergedAttributesField) entity.fields |> Map.filter (fun name attrs -> not <| Map.isEmpty attrs)
    }

let private makeMergedAttributesSchema (schema : AttributesSchema) : MergedAttributesSchema =
    { entities = Map.map (fun name -> makeMergedAttributesEntity) schema.entities
    }

let private makeMergedAttributedDatabase (db : AttributesDatabase) : MergedDefaultAttributes =
    { schemas = Map.map (fun name -> makeMergedAttributesSchema) db.schemas
    }

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

let mergeDefaultAttributes (attrs : DefaultAttributes) : MergedDefaultAttributes =
    attrs.schemas |> Map.toSeq |> Seq.map (fun (name, db) -> makeMergedAttributedDatabase db) |> Seq.fold mergeAttributesPair emptyMergedDefaultAttributes