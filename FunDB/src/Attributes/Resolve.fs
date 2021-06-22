module FunWithFlags.FunDB.Attributes.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

type ResolveAttributesException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveAttributesException (message, null)

let private attrResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let resolveAttributesField (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (fieldAttrs : SourceAttributesField) : AttributesField =
        let attrsMap =
            match parse tokenizeFunQL attributeMap fieldAttrs.Attributes with
            | Ok r -> r
            | Error msg -> raisef ResolveAttributesException "Error parsing attributes: %s" msg

        let resolveAttribute name expr =
            try
                let entityInfo = SFEntity entityRef
                let (info, resolvedExpr) =
                    try
                        resolveSingleFieldExpr layout Map.empty localExprFromEntityId attrResolutionFlags entityInfo expr
                    with
                    | :? ViewResolveException as e -> raisefWithInner ResolveAttributesException e ""
                resolvedExpr
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e "In attribute %O" name
        let resolvedMap = Map.map resolveAttribute attrsMap

        { AllowBroken = fieldAttrs.AllowBroken
          Priority = fieldAttrs.Priority
          Attributes = resolvedMap
        }

    let resolveAttributesEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (entityAttrs : SourceAttributesEntity) : ErroredAttributesEntity * AttributesEntity =
        let mutable errors = Map.empty

        let mapField name (fieldAttrs : SourceAttributesField) =
            try
                try
                    match entity.FindField name with
                    | None -> raisef ResolveAttributesException "Unknown field name"
                    | Some field ->
                            Ok <| resolveAttributesField entityRef entity fieldAttrs
                with
                | :? ResolveAttributesException as e when fieldAttrs.AllowBroken || forceAllowBroken ->
                    if not fieldAttrs.AllowBroken then
                        errors <- Map.add name (e :> exn) errors
                    Error (e :> exn)
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e "In field %O" name

        let ret =
            { Fields = entityAttrs.Fields |> Map.map mapField
            }
        (errors, ret)

    let resolveAttributesSchema (schemaName : SchemaName) (schema : ResolvedSchema) (schemaAttrs : SourceAttributesSchema) : ErroredAttributesSchema * AttributesSchema =
        let mutable errors = Map.empty

        let mapEntity name entityAttrs =
            try
                let entity =
                    match Map.tryFind name schema.Entities with
                    | Some entity when not entity.IsHidden -> entity
                    | _ -> raisef ResolveAttributesException "Unknown entity name"
                let ref = { Schema = schemaName; Name = name }
                let (entityErrors, newEntity) = resolveAttributesEntity ref entity entityAttrs
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e "In entity %O" name

        let ret =
            { Entities = schemaAttrs.Entities |> Map.map mapEntity
            }
        (errors, ret)

    let resolveAttributesDatabase (db : SourceAttributesDatabase) : ErroredAttributesDatabase * AttributesDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaAttrs =
            try
                let schema =
                    match Map.tryFind name layout.Schemas with
                    | None -> raisef ResolveAttributesException "Unknown schema name"
                    | Some schema -> schema
                let (schemaErrors, newSchema) = resolveAttributesSchema name schema schemaAttrs
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e "For schema %O" name

        let ret =
            { Schemas = db.Schemas |> Map.map mapSchema
            } : AttributesDatabase
        (errors, ret)

    let resolveAttributes (defaultAttrs : SourceDefaultAttributes) : ErroredDefaultAttributes * DefaultAttributes =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveAttributesException "Unknown schema name"
                let (dbErrors, newDb) = resolveAttributesDatabase db
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newDb
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e "In schema %O" name

        let ret =
            { Schemas = defaultAttrs.Schemas |> Map.map mapDatabase
            }
        (errors, ret)

    member this.ResolveAttributes defaultAttrs = resolveAttributes defaultAttrs

let resolveAttributes (layout : Layout) (forceAllowBroken : bool) (source : SourceDefaultAttributes) : ErroredDefaultAttributes * DefaultAttributes =
    let phase1 = Phase1Resolver (layout, forceAllowBroken)
    phase1.ResolveAttributes source