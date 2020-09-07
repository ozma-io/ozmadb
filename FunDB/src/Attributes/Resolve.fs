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

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let rec checkPath (entity : ResolvedEntity) (name : FieldName) (fields : FieldName list) : unit =
        match fields with
        | [] ->
            if Option.isNone <| entity.FindField name then
                raisef ResolveAttributesException "Column not found in default attribute: %O" name
        | (ref :: refs) ->
            match Map.tryFind name entity.columnFields with
            | Some { fieldType = FTReference (refEntity, _) } ->
                let newEntity = Map.find refEntity.name (Map.find refEntity.schema layout.schemas).entities
                checkPath newEntity ref refs
            | _ -> raisef ResolveAttributesException "Invalid dereference in path: %O" ref

    let resolveAttributesField (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (fieldAttrs : SourceAttributesField) : AttributesField =
        let attrsMap =
            match parse tokenizeFunQL attributeMap fieldAttrs.Attributes with
            | Ok r -> r
            | Error msg -> raisef ResolveAttributesException "Error parsing attributes: %s" msg

        let mutable globalArguments = Set.empty

        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { ref = VRColumn { entity = None; name = name }; path = path } ->
                checkPath entity name (Array.toList path)
                let bound =
                    { ref = { entity = entityRef; name = name }
                      immediate = true
                    }
                { ref = VRColumn { ref = ({ entity = None; name = name } : FieldRef); bound = Some bound }; path = path }
            | { ref = VRPlaceholder (PLocal name) } ->
                raisef ResolveAttributesException "Local argument %O is not allowed" name
            | { ref = VRPlaceholder (PGlobal name as arg); path = path } ->
                let argInfo =
                    match Map.tryFind name globalArgumentTypes with
                    | None -> raisef ResolveAttributesException "Unknown global argument: %O" ref
                    | Some argInfo -> argInfo
                if not <| Array.isEmpty path then
                    match argInfo.argType with
                    | FTReference (entityRef, where) ->
                        let (name, remainingPath) =
                            match Array.toList path with
                            | head :: tail -> (head, tail)
                            | _ -> failwith "impossible"
                        let argEntity = layout.FindEntity entityRef |> Option.get
                        checkPath argEntity name remainingPath
                    | _ -> raisef ResolveAttributesException "Argument is not a reference: %O" ref
                { ref = VRPlaceholder arg; path = path }
            | ref ->
                raisef ResolveAttributesException "Invalid reference: %O" ref
        let resolveQuery query =
            let (_, res) = resolveSelectExpr layout query
            res
        let voidAggr aggr =
            raisef ResolveAttributesException "Aggregate expressions are not allowed"

        let mapper =
            { idFieldExprMapper resolveReference resolveQuery with
                  aggregate = voidAggr
            }

        let resolveAttribute name expr =
            try
                mapFieldExpr mapper expr
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "In attribute %O: %s" name e.Message
        let resolvedMap = Map.map resolveAttribute attrsMap

        { AllowBroken = fieldAttrs.AllowBroken
          Priority = fieldAttrs.Priority
          Attributes = resolvedMap
          GlobalArguments = globalArguments
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
                    Error { Source = fieldAttrs; Error = e }
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "In field %O: %s" name e.Message

        let ret =
            { Fields = entityAttrs.Fields |> Map.map mapField
            }
        (errors, ret)

    let resolveAttributesSchema (schemaName : SchemaName) (schema : ResolvedSchema) (schemaAttrs : SourceAttributesSchema) : ErroredAttributesSchema * AttributesSchema =
        let mutable errors = Map.empty

        let mapEntity name entityAttrs =
            try
                let entity =
                    match Map.tryFind name schema.entities with
                    | None -> raisef ResolveAttributesException "Unknown entity name"
                    | Some entity -> entity
                let ref = { schema = schemaName; name = name }
                let (entityErrors, newEntity) = resolveAttributesEntity ref entity entityAttrs
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "In entity %O: %s" name e.Message

        let ret =
            { Entities = schemaAttrs.Entities |> Map.map mapEntity
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
                let (schemaErrors, newSchema) = resolveAttributesSchema name schema schemaAttrs
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "For schema %O: %s" name e.Message

        let ret =
            { Schemas = db.Schemas |> Map.map mapSchema
            } : AttributesDatabase
        (errors, ret)

    let resolveAttributes (defaultAttrs : SourceDefaultAttributes) : ErroredDefaultAttributes * DefaultAttributes =
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
            | :? ResolveAttributesException as e -> raisefWithInner ResolveAttributesException e.InnerException "In schema %O: %s" name e.Message

        let ret =
            { Schemas = defaultAttrs.Schemas |> Map.map mapDatabase
            }
        (errors, ret)

    member this.ResolveAttributes defaultAttrs = resolveAttributes defaultAttrs

let resolveAttributes (layout : Layout) (forceAllowBroken : bool) (source : SourceDefaultAttributes) : ErroredDefaultAttributes * DefaultAttributes =
    let phase1 = Phase1Resolver (layout, forceAllowBroken)
    phase1.ResolveAttributes source