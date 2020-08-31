module FunWithFlags.FunDB.Attributes.Types

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type DefaultAttributeRef =
    { Schema : SchemaName
      Field : ResolvedFieldRef
    } with
        override this.ToString () = sprintf "%s.%s" (this.Schema.ToFunQLString()) (this.Field.ToFunQLString())

[<NoEquality; NoComparison>]
type AttributesField =
    { AllowBroken : bool
      Priority : int
      Attributes : ResolvedAttributeMap
      GlobalArguments : Set<ArgumentName>
    }

[<NoEquality; NoComparison>]
type AttributesError =
    { Source : SourceAttributesField
      Error : exn
    }

[<NoEquality; NoComparison>]
type AttributesEntity =
    { Fields : Map<FieldName, Result<AttributesField, AttributesError>>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.Fields

[<NoEquality; NoComparison>]
type AttributesSchema =
    { Entities : Map<EntityName, AttributesEntity>
    }

[<NoEquality; NoComparison>]
type AttributesDatabase =
    { Schemas : Map<SchemaName, AttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.Entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

[<NoEquality; NoComparison>]
type DefaultAttributes =
    { Schemas : Map<SchemaName, AttributesDatabase>
    }

type ErroredAttributesEntity = Map<FieldName, exn>
type ErroredAttributesSchema = Map<EntityName, ErroredAttributesEntity>
type ErroredAttributesDatabase = Map<SchemaName, ErroredAttributesSchema>
type ErroredDefaultAttributes = Map<SchemaName, ErroredAttributesDatabase>