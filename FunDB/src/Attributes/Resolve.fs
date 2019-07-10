module FunWithFlags.FunDB.Attributes.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Types

type ResolveAttributesException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveAttributesException (message, null)

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, defaultAttrs : SourceDefaultAttributes) =
    let resolveAttributesField (fieldAttrs : SourceAttributesField) : AttributesField =
        let attrsMap =
            match parse tokenizeFunQL attributeMap fieldAttrs.attributes with
            | Ok r -> r
            | Error msg -> raisef ResolveAttributesException "Error parsing attributes: %s" msg

        let mutable globalArguments = Set.empty

        let resolveColumn : LinkedFieldRef -> DefaultFieldRef = function
            | { ref = { entity = Some { schema = None; name = FunQLName "this" }; name = fieldName }; path = [||] } -> ThisRef fieldName
            | ref ->
                raisef ResolveAttributesException "Invalid reference: %O" ref
        let resolvePlaceholder = function
            | PLocal name -> raisef ResolveAttributesException "Local argument %O is not allowed" name
            | PGlobal name when Map.containsKey name globalArgumentTypes ->
                globalArguments <- Set.add name globalArguments
                PGlobal name
            | PGlobal name -> raisef ResolveAttributesException "Unknown global argument %O" name
        let voidQuery query =
            raisef ResolveAttributesException "Query is not allowed: %O" query

        let resolveAttribute name expr =
            try
                mapFieldExpr id resolveColumn resolvePlaceholder voidQuery expr
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "Error in attribute %O: %s" name e.Message
        let resolvedMap = Map.map resolveAttribute attrsMap

        { allowBroken = fieldAttrs.allowBroken
          priority = fieldAttrs.priority
          attributes = resolvedMap
          globalArguments = globalArguments
        }

    let resolveAttributesEntity (entity : ResolvedEntity) (entityAttrs : SourceAttributesEntity) : ErroredAttributesEntity * AttributesEntity =
        let mutable errors = Map.empty

        let mapField name (fieldAttrs : SourceAttributesField) =
            try
                try
                    match entity.FindField name with
                    | None -> raisef ResolveAttributesException "Unknown field name"
                    | Some field ->
                            Ok <| resolveAttributesField fieldAttrs
                with
                | :? ResolveAttributesException as e when fieldAttrs.allowBroken || forceAllowBroken ->
                    errors <- Map.add name (e :> exn) errors
                    Error { source = fieldAttrs; error = e }
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "Error in attributes field %O: %s" name e.Message

        let ret =
            { fields = entityAttrs.fields |> Map.map mapField
            }
        (errors, ret)

    let resolveAttributesSchema (schema : ResolvedSchema) (schemaAttrs : SourceAttributesSchema) : ErroredAttributesSchema * AttributesSchema =
        let mutable errors = Map.empty

        let mapEntity name entityAttrs =
            try
                let entity =
                    match Map.tryFind name schema.entities with
                    | None -> raisef ResolveAttributesException "Unknown entity name"
                    | Some entity -> entity
                let (entityErrors, newEntity) = resolveAttributesEntity entity entityAttrs
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "Error in attributes entity %O: %s" name e.Message

        let ret =
            { entities = schemaAttrs.entities |> Map.map mapEntity
            }
        (errors, ret)

    let resolveAttributesDatabase (db : SourceAttributesDatabase) : ErroredAttributesDatabase * AttributesDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaAttrs =
            try
                let schema =
                    match Map.tryFind name layout.schemas with
                    | None -> raisef ResolveAttributesException "Unknown schema name"
                    | Some schema -> schema
                let (schemaErrors, newSchema) = resolveAttributesSchema schema schemaAttrs
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "Error in attributes schema %O: %s" name e.Message

        let ret =
            { schemas = db.schemas |> Map.map mapSchema
            } : AttributesDatabase
        (errors, ret)

    let resolveAttributes () : ErroredDefaultAttributes * DefaultAttributes =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef ResolveAttributesException "Unknown schema name"
                let (dbErrors, newDb) = resolveAttributesDatabase db
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newDb
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { schemas = defaultAttrs.schemas |> Map.map mapDatabase
            }
        (errors, ret)

    member this.ResolveAttributes = resolveAttributes

let resolveAttributes (layout : Layout) (forceAllowBroken : bool) (source : SourceDefaultAttributes) : ErroredDefaultAttributes * DefaultAttributes =
    let phase1 = Phase1Resolver (layout, forceAllowBroken, source)
    phase1.ResolveAttributes ()