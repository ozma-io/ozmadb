module FunWithFlags.FunDB.Layout.Json

open Newtonsoft.Json

open FunWithFlags.FunDB.Layout.Source

// Sanitize source layout after deserialization

let private sanitizeSourceEntity (entity : SourceEntity) : SourceEntity =
    { columnFields =
        if obj.ReferenceEquals(entity.columnFields, null) then
            Map.empty
        else entity.columnFields
      computedFields =
        if obj.ReferenceEquals(entity.computedFields, null) then
            Map.empty
        else entity.computedFields
      uniqueConstraints =
        if obj.ReferenceEquals(entity.uniqueConstraints, null) then
            Map.empty
        else entity.uniqueConstraints
      checkConstraints =
        if obj.ReferenceEquals(entity.checkConstraints, null) then
            Map.empty
        else entity.checkConstraints
      mainField = entity.mainField
    }

let private sanitizeSourceSchema (schema : SourceSchema) : SourceSchema =
    { entities =
        if obj.ReferenceEquals(schema.entities, null) then
            Map.empty
        else Map.map (fun name -> sanitizeSourceEntity) schema.entities
    }

let private sanitizeSourceLayout (layout : SourceLayout) : SourceLayout =
    { schemas =
        if obj.ReferenceEquals(layout.schemas, null) then
            Map.empty
        else Map.map (fun name -> sanitizeSourceSchema) layout.schemas
      systemEntities =
        if obj.ReferenceEquals(layout.systemEntities, null) then
            Map.empty
        else Map.map (fun name -> sanitizeSourceEntity) layout.systemEntities
    }

let parseJsonLayout (str : string) : SourceLayout =
    sanitizeSourceLayout <| JsonConvert.DeserializeObject<SourceLayout>(str)