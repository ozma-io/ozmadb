module FunWithFlags.FunDB.Attributes.Source

open Newtonsoft.Json

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST

type SourceAttributesField =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      allowBroken : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      priority : int
      attributes : string
    }

type SourceAttributesEntity =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      fields : Map<FieldName, SourceAttributesField>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.fields

let emptySourceAttributesEntity : SourceAttributesEntity =
    { fields = Map.empty }

let mergeSourceAttributesEntity (a : SourceAttributesEntity) (b : SourceAttributesEntity) : SourceAttributesEntity =
    { fields = Map.unionUnique a.fields b.fields
    }

type SourceAttributesSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceAttributesEntity>
    }

let mergeSourceAttributesSchema (a : SourceAttributesSchema) (b : SourceAttributesSchema) : SourceAttributesSchema =
    { entities = Map.unionWith (fun name -> mergeSourceAttributesEntity) a.entities b.entities
    }

type SourceAttributesDatabase =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceAttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities


        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptySourceAttributesDatabase : SourceAttributesDatabase =
    { schemas = Map.empty }

let mergeSourceAttributesDatabase (a : SourceAttributesDatabase) (b : SourceAttributesDatabase) : SourceAttributesDatabase =
    { schemas = Map.unionWith (fun name -> mergeSourceAttributesSchema) a.schemas b.schemas
    }

type SourceDefaultAttributes =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceAttributesDatabase>
    }

let emptySourceDefaultAttributes : SourceDefaultAttributes =
    { schemas = Map.empty }