module FunWithFlags.FunDB.Attributes.Source

open Newtonsoft.Json

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

type SourceAttributesSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceAttributesEntity>
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

let emptySourceAttributesSchema : SourceAttributesDatabase =
    { schemas = Map.empty }

type SourceDefaultAttributes =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceAttributesDatabase>
    }

let emptySourceDefaultAttributes : SourceDefaultAttributes =
    { schemas = Map.empty }