module FunWithFlags.FunDB.Attributes.Render

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

let private renderAttributesField = function
    | Ok field ->
        { AllowBroken = field.AllowBroken
          Priority = field.Priority
          Attributes = renderAttributesMap field.Attributes
        } : SourceAttributesField
    | Error (e : AttributesError) -> e.Source

let private renderAttributesEntity (entity : AttributesEntity) : SourceAttributesEntity =
    { Fields = Map.map (fun name -> renderAttributesField) entity.Fields
    }

let private renderAttributesSchema (schema : AttributesSchema) : SourceAttributesSchema =
    { Entities = Map.map (fun name -> renderAttributesEntity) schema.Entities
    }

let renderAttributesDatabase (db : AttributesDatabase) : SourceAttributesDatabase =
    { Schemas = Map.map (fun name -> renderAttributesSchema) db.Schemas
    }

let renderDefaultAttributes (attrs : DefaultAttributes) : SourceDefaultAttributes =
    { Schemas = Map.map (fun name -> renderAttributesDatabase) attrs.Schemas
    }