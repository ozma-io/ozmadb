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

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, hasUserView : HasUserView) =
    // TODO: allow attributes to refer to each other.
    let defaultCallbacks = { resolveCallbacks layout with HasUserView = hasUserView }

    let resolveAttributesField (homeSchema : SchemaName) (fieldRef : ResolvedFieldRef) (fieldAttrs : ParsedAttributesField) : AttributesField =
        let resolvedMap =
            try
                // TOOD: allow to specify `__self` as home schema.
                resolveEntityAttributesMap defaultCallbacks attrResolutionFlags fieldRef.Entity fieldAttrs.Attributes
            with
            | :? ViewResolveException as e -> raisefWithInner ResolveAttributesException e ""

        { AllowBroken = fieldAttrs.AllowBroken
          Priority = fieldAttrs.Priority
          Attributes = resolvedMap
        }

    let resolveAttributesEntity (homeSchema : SchemaName) (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (entityAttrs : ParsedAttributesEntity) : AttributesEntity =
        let mapField name = function
            | Error e -> Error e
            | Ok fieldAttrs ->
                try
                    try
                        if entity.FindField name |> Option.isNone then
                            raisef ResolveAttributesException "Field not found"
                        let fieldRef = { Entity = entityRef; Name = name }
                        Ok <| resolveAttributesField homeSchema fieldRef fieldAttrs
                    with
                    | :? ResolveAttributesException as e when fieldAttrs.AllowBroken || forceAllowBroken ->
                        Error { Error = e; AllowBroken = fieldAttrs.AllowBroken }
                with
                | e -> raisefWithInner ResolveAttributesException e "In field %O" name

        { Fields = entityAttrs.Fields |> Map.map mapField
        }

    let resolveAttributesSchema (homeSchema : SchemaName) (schemaName : SchemaName) (schema : ResolvedSchema) (schemaAttrs : ParsedAttributesSchema) : AttributesSchema =
        let mapEntity name entityAttrs =
            try
                let entity =
                    match Map.tryFind name schema.Entities with
                    | Some entity when not entity.IsHidden -> entity
                    | _ -> raisef ResolveAttributesException "Entity not found"
                let ref = { Schema = schemaName; Name = name }
                resolveAttributesEntity homeSchema ref entity entityAttrs
            with
            | e -> raisefWithInner ResolveAttributesException e "In entity %O" name

        { Entities = schemaAttrs.Entities |> Map.map mapEntity
        }

    let resolveAttributesDatabase (homeSchema : SchemaName) (db : ParsedAttributesDatabase) : AttributesDatabase =
        let mapSchema name schemaAttrs =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolveAttributesException "Schema not found"
                    | Some schema -> schema
                resolveAttributesSchema homeSchema name schema schemaAttrs
            with
            | e -> raisefWithInner ResolveAttributesException e "For schema %O" name

        { Schemas = db.Schemas |> Map.map mapSchema
        }

    let resolveAttributes (defaultAttrs : ParsedDefaultAttributes) : DefaultAttributes =
        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveAttributesException "Unknown schema name"
                resolveAttributesDatabase name db
            with
            | e -> raisefWithInner ResolveAttributesException e "In schema %O" name

        { Schemas = defaultAttrs.Schemas |> Map.map mapDatabase
        }

    member this.ResolveAttributes defaultAttrs = resolveAttributes defaultAttrs

let resolveAttributes (layout : Layout) (hasUserView : HasUserView) (forceAllowBroken : bool)  (source : ParsedDefaultAttributes) : DefaultAttributes =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, hasUserView)
    phase1.ResolveAttributes source