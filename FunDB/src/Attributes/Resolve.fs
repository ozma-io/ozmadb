module FunWithFlags.FunDB.Attributes.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Parse
open FunWithFlags.FunDB.Attributes.Types
open FunWithFlags.FunDB.Objects.Types

type ResolveAttributesException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ResolveAttributesException (message, innerException, isUserException innerException)

    new (message : string) = ResolveAttributesException (message, null, true)

let private attrResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let callbacks =
        { Layout = layout
          // TODO: allow attributes to refer to each other.
          HasDefaultAttribute = emptyHasDefaultAttribute
        }

    let resolveAttributesField (fieldRef : ResolvedFieldRef) (fieldAttrs : ParsedAttributesField) : AttributesField =
        let resolvedMap =
            try
                resolveEntityAttributesMap callbacks attrResolutionFlags fieldRef.Entity fieldAttrs.Attributes
            with
            | :? ViewResolveException as e -> raisefWithInner ResolveAttributesException e ""

        { AllowBroken = fieldAttrs.AllowBroken
          Priority = fieldAttrs.Priority
          Attributes = resolvedMap
        }

    let resolveAttributesEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (entityAttrs : ParsedAttributesEntity) : AttributesEntity =
        let mapField name = function
            | Error e -> Error e
            | Ok fieldAttrs ->
                try
                    try
                        if entity.FindField name |> Option.isNone then
                            raisef ResolveAttributesException "Unknown field name"
                        let fieldRef = { Entity = entityRef; Name = name }
                        Ok <| resolveAttributesField fieldRef fieldAttrs
                    with
                    | :? ResolveAttributesException as e when fieldAttrs.AllowBroken || forceAllowBroken ->
                        Error { Error = e; AllowBroken = fieldAttrs.AllowBroken }
                with
                | e -> raisefWithInner ResolveAttributesException e "In field %O" name

        { Fields = entityAttrs.Fields |> Map.map mapField
        }

    let resolveAttributesSchema (schemaName : SchemaName) (schema : ResolvedSchema) (schemaAttrs : ParsedAttributesSchema) : AttributesSchema =
        let mapEntity name entityAttrs =
            try
                let entity =
                    match Map.tryFind name schema.Entities with
                    | Some entity when not entity.IsHidden -> entity
                    | _ -> raisef ResolveAttributesException "Unknown entity name"
                let ref = { Schema = schemaName; Name = name }
                resolveAttributesEntity ref entity entityAttrs
            with
            | e -> raisefWithInner ResolveAttributesException e "In entity %O" name

        { Entities = schemaAttrs.Entities |> Map.map mapEntity
        }

    let resolveAttributesDatabase (db : ParsedAttributesDatabase) : AttributesDatabase =
        let mapSchema name schemaAttrs =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolveAttributesException "Unknown schema name"
                    | Some schema -> schema
                resolveAttributesSchema name schema schemaAttrs
            with
            | e -> raisefWithInner ResolveAttributesException e "For schema %O" name

        { Schemas = db.Schemas |> Map.map mapSchema
        }

    let resolveAttributes (defaultAttrs : ParsedDefaultAttributes) : DefaultAttributes =
        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveAttributesException "Unknown schema name"
                resolveAttributesDatabase db
            with
            | e -> raisefWithInner ResolveAttributesException e "In schema %O" name

        { Schemas = defaultAttrs.Schemas |> Map.map mapDatabase
        }

    member this.ResolveAttributes defaultAttrs = resolveAttributes defaultAttrs

let resolveAttributes (layout : Layout) (forceAllowBroken : bool) (source : ParsedDefaultAttributes) : DefaultAttributes =
    let phase1 = Phase1Resolver (layout, forceAllowBroken)
    phase1.ResolveAttributes source