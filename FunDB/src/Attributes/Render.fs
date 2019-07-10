module FunWithFlags.FunDB.Attributes.Render

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

let private renderAttributesField = function
    | Ok field ->
        { allowBroken = field.allowBroken
          priority = field.priority
          attributes = renderAttributesMap field.attributes
        } : SourceAttributesField
    | Error (e : AttributesError) -> e.source

let private renderAttributesEntity (entity : AttributesEntity) : SourceAttributesEntity =
    { fields = Map.map (fun name -> renderAttributesField) entity.fields
    }

let private renderAttributesSchema (schema : AttributesSchema) : SourceAttributesSchema =
    { entities = Map.map (fun name -> renderAttributesEntity) schema.entities
    }

let renderAttributesDatabase (db : AttributesDatabase) : SourceAttributesDatabase =
    { schemas = Map.map (fun name -> renderAttributesSchema) db.schemas
    }

let renderDefaultAttributes (attrs : DefaultAttributes) : SourceDefaultAttributes =
    { schemas = Map.map (fun name -> renderAttributesDatabase) attrs.schemas
    }