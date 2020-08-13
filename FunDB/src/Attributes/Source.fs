module FunWithFlags.FunDB.Attributes.Source

open System.ComponentModel

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST

type SourceAttributesField =
    { AllowBroken : bool
      [<DefaultValue(0)>]
      Priority : int
      Attributes : string
    }

type SourceAttributesEntity =
    { Fields : Map<FieldName, SourceAttributesField>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.Fields

let emptySourceAttributesEntity : SourceAttributesEntity =
    { Fields = Map.empty }

let mergeSourceAttributesEntity (a : SourceAttributesEntity) (b : SourceAttributesEntity) : SourceAttributesEntity =
    { Fields = Map.unionUnique a.Fields b.Fields
    }

type SourceAttributesSchema =
    { Entities : Map<EntityName, SourceAttributesEntity>
    }

let mergeSourceAttributesSchema (a : SourceAttributesSchema) (b : SourceAttributesSchema) : SourceAttributesSchema =
    { Entities = Map.unionWith (fun name -> mergeSourceAttributesEntity) a.Entities b.Entities
    }

type SourceAttributesDatabase =
    { Schemas : Map<SchemaName, SourceAttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.Entities


        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptySourceAttributesDatabase : SourceAttributesDatabase =
    { Schemas = Map.empty }

let mergeSourceAttributesDatabase (a : SourceAttributesDatabase) (b : SourceAttributesDatabase) : SourceAttributesDatabase =
    { Schemas = Map.unionWith (fun name -> mergeSourceAttributesSchema) a.Schemas b.Schemas
    }

type SourceDefaultAttributes =
    { Schemas : Map<SchemaName, SourceAttributesDatabase>
    }

let emptySourceDefaultAttributes : SourceDefaultAttributes =
    { Schemas = Map.empty }