module FunWithFlags.FunDB.Attributes.Render

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

let private renderAttributesField = function
    | Ok field ->
        { AllowBroken = field.allowBroken
          Priority = field.priority
          Attributes = renderAttributesMap field.attributes
        } : SourceAttributesField
    | Error (e : AttributesError) -> e.source

let private renderAttributesEntity (entity : AttributesEntity) : SourceAttributesEntity =
    { Fields = Map.map (fun name -> renderAttributesField) entity.fields
    }

let private renderAttributesSchema (schema : AttributesSchema) : SourceAttributesSchema =
    { Entities = Map.map (fun name -> renderAttributesEntity) schema.entities
    }

let renderAttributesDatabase (db : AttributesDatabase) : SourceAttributesDatabase =
    { Schemas = Map.map (fun name -> renderAttributesSchema) db.schemas
    }

let renderDefaultAttributes (attrs : DefaultAttributes) : SourceDefaultAttributes =
    { Schemas = Map.map (fun name -> renderAttributesDatabase) attrs.schemas
    }