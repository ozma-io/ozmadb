module FunWithFlags.FunDB.Attributes.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Attributes.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type DefaultFieldRef = ThisRef of FieldName
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
            | ThisRef fieldName -> sprintf "this.%O" fieldName

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type DefaultAttributeFieldExpr = FieldExpr<FunQLVoid, DefaultFieldRef>
type DefaultAttributeMap = AttributeMap<FunQLVoid, DefaultFieldRef>

[<NoComparison>]
type AttributesField =
    { allowBroken : bool
      priority : int
      attributes : DefaultAttributeMap
      globalArguments : Set<ArgumentName>
    }

[<NoComparison>]
type AttributesError =
    { source : SourceAttributesField
      error : exn
    }

[<NoComparison>]
type AttributesEntity =
    { fields : Map<FieldName, Result<AttributesField, AttributesError>>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.fields

[<NoComparison>]
type AttributesSchema =
    { entities : Map<EntityName, AttributesEntity>
    }

[<NoComparison>]
type AttributesDatabase =
    { schemas : Map<SchemaName, AttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

[<NoComparison>]
type DefaultAttributes =
    { schemas : Map<SchemaName, AttributesDatabase>
    }

type ErroredAttributesEntity = Map<FieldName, exn>
type ErroredAttributesSchema = Map<EntityName, ErroredAttributesEntity>
type ErroredAttributesDatabase = Map<SchemaName, ErroredAttributesSchema>
type ErroredDefaultAttributes = Map<SchemaName, ErroredAttributesDatabase>