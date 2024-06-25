module OzmaDB.Attributes.Types

open OzmaDB.OzmaQL.AST
open OzmaDB.Objects.Types

module SQL = OzmaDB.SQL.AST

type DefaultAttributeRef =
    { Schema: SchemaName
      Field: ResolvedFieldRef }

    override this.ToString() =
        sprintf "%s.%s" (this.Schema.ToOzmaQLString()) (this.Field.ToOzmaQLString())

[<NoEquality; NoComparison>]
type DefaultAttribute =
    { Value: ResolvedBoundAttribute
      // Doesn't depend on any other fields except the bound one.
      Single: bool }

[<NoEquality; NoComparison>]
type AttributesField =
    { AllowBroken: bool
      Priority: int
      Attributes: Map<AttributeName, DefaultAttribute> }

[<NoEquality; NoComparison>]
type AttributesEntity =
    { Fields: Map<FieldName, PossiblyBroken<AttributesField>> }

    member this.FindField(name: FieldName) = Map.tryFind name this.Fields

[<NoEquality; NoComparison>]
type AttributesSchema =
    { Entities: Map<EntityName, AttributesEntity> }

[<NoEquality; NoComparison>]
type AttributesDatabase =
    { Schemas: Map<SchemaName, AttributesSchema> }

    member this.FindEntity(entity: ResolvedEntityRef) =
        match Map.tryFind entity.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind entity.Name schema.Entities

    member this.FindField (entity: ResolvedEntityRef) (field: FieldName) =
        this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

[<NoEquality; NoComparison>]
type DefaultAttributes =
    { Schemas: Map<SchemaName, AttributesDatabase> }
