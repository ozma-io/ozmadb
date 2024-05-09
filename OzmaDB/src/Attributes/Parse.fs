module OzmaDB.Attributes.Parse

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Parsing
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Lex
open OzmaDB.OzmaQL.Parse
open OzmaDB.Attributes.Source
open OzmaDB.Objects.Types

type ParseAttributesException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ParseAttributesException (message, innerException, isUserException innerException)

    new (message : string) = ParseAttributesException (message, null, true)

[<NoEquality; NoComparison>]
type ParsedAttributesField =
    { AllowBroken : bool
      Priority : int
      Attributes : Map<AttributeName, ParsedBoundAttribute>
    }

[<NoEquality; NoComparison>]
type ParsedAttributesEntity =
    { Fields : Map<FieldName, PossiblyBroken<ParsedAttributesField>>
    } with
        member this.FindField (name : FieldName) =
            Map.tryFind name this.Fields

[<NoEquality; NoComparison>]
type ParsedAttributesSchema =
    { Entities : Map<EntityName, ParsedAttributesEntity>
    }

[<NoEquality; NoComparison>]
type ParsedAttributesDatabase =
    { Schemas : Map<SchemaName, ParsedAttributesSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.Name schema.Entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

        member this.FindAttribute (entity : ResolvedEntityRef) (field : FieldName) (attr : AttributeName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

[<NoEquality; NoComparison>]
type ParsedDefaultAttributes =
    { Schemas : Map<SchemaName, ParsedAttributesDatabase>
    }

type private Phase1Resolver (forceAllowBroken : bool) =
    let parseAttributesField (fieldAttrs : SourceAttributesField) : ParsedAttributesField =
        let attrsMap =
            match parse tokenizeOzmaQL boundAttributesMap fieldAttrs.Attributes with
            | Ok r -> r
            | Error msg -> raisef ParseAttributesException "Error parsing attributes: %s" msg

        { AllowBroken = fieldAttrs.AllowBroken
          Priority = fieldAttrs.Priority
          Attributes = attrsMap
        }

    let parseAttributesEntity (entityAttrs : SourceAttributesEntity) : ParsedAttributesEntity =
        let mapField name (fieldAttrs : SourceAttributesField) =
            try
                try
                    Ok <| parseAttributesField fieldAttrs
                with
                | :? ParseAttributesException as e when fieldAttrs.AllowBroken || forceAllowBroken ->
                    Error { Error = e; AllowBroken = fieldAttrs.AllowBroken }
            with
            | e -> raisefWithInner ParseAttributesException e "In field %O" name

        { Fields = entityAttrs.Fields |> Map.map mapField
        }

    let parseAttributesSchema (schemaAttrs : SourceAttributesSchema) : ParsedAttributesSchema =
        let mapEntity name entityAttrs =
            try
                parseAttributesEntity entityAttrs
            with
            | e -> raisefWithInner ParseAttributesException e "In entity %O" name

        { Entities = schemaAttrs.Entities |> Map.map mapEntity
        }

    let parseAttributesDatabase (db : SourceAttributesDatabase) : ParsedAttributesDatabase =
        let mapSchema name schemaAttrs =
            try
                parseAttributesSchema schemaAttrs
            with
            | e -> raisefWithInner ParseAttributesException e "For schema %O" name

        { Schemas = db.Schemas |> Map.map mapSchema
        }

    let parseAttributes (defaultAttrs : SourceDefaultAttributes) : ParsedDefaultAttributes =
        let mapDatabase name db =
            try
                parseAttributesDatabase db
            with
            | e -> raisefWithInner ParseAttributesException e "In schema %O" name

        { Schemas = defaultAttrs.Schemas |> Map.map mapDatabase
        }

    member this.ParseAttributes defaultAttrs = parseAttributes defaultAttrs

let parseAttributes (forceAllowBroken : bool) (source : SourceDefaultAttributes) : ParsedDefaultAttributes =
    let phase1 = Phase1Resolver forceAllowBroken
    phase1.ParseAttributes source