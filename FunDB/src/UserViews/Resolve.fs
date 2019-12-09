module FunWithFlags.FunDB.UserViews.Resolve

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Dereference
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Attributes.Merge
module SQL = FunWithFlags.FunDB.SQL.AST

type UserViewResolveException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewResolveException (message, null)

let private checkName (FunQLName name) =
    if not (goodName name) then
        raisef UserViewResolveException "Invalid user view name"

let private getColumn : ColumnType -> FunQLName option = function
    | CTColumn c -> Some c
    | _ -> None

[<NoEquality; NoComparison>]
type private HalfResolvedView =
    { source : SourceUserView
      resolved : ResolvedViewExpr
      allowBroken : bool
    }

type private HalfResolvedSchema = Map<UserViewName, Result<HalfResolvedView, UserViewError>>
type private HalfResolvedViews = Map<SchemaName, HalfResolvedSchema>

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let resolveUserView (uv : SourceUserView) : HalfResolvedView =
        let parsed =
            match parse tokenizeFunQL viewExpr uv.query with
            | Error msg -> raisef UserViewResolveException "Parse error: %s" msg
            | Ok rawExpr -> rawExpr
        let resolved =
            try
                resolveViewExpr layout parsed
            with
            | :? ViewResolveException as err -> raisefWithInner UserViewResolveException err "Resolve error"
        { source = uv
          resolved = resolved
          allowBroken = uv.allowBroken
        }

    let resolveUserViewsSchema (schema : SourceUserViewsSchema) : HalfResolvedSchema =
        let mapUserView name uv =
            try
                checkName name
                try
                    Ok <| resolveUserView uv
                with
                | :? UserViewResolveException as e ->
                    if forceAllowBroken || uv.allowBroken then
                        let err =
                            { error = e :> exn
                              source = uv
                            }
                        Error err
                    else
                        reraise ()
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e.InnerException "Error in user view %O: %s" name e.Message
        schema.userViews |> Map.map mapUserView

    let resolveUserViews (uvs : SourceUserViews) : HalfResolvedViews =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef UserViewResolveException "Unknown schema name"
                resolveUserViewsSchema schema
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e.InnerException "Error in schema %O: %s" name e.Message
        uvs.schemas |> Map.map mapSchema

    member this.ResolveUserView = resolveUserView
    member this.ResolveUserViews = resolveUserViews

type FindExistingView = ResolvedUserViewRef -> Result<ResolvedUserView, UserViewError> option

type private Phase2Resolver (layout : Layout, defaultAttrs : MergedDefaultAttributes, findExistingView : FindExistingView, halfResolved : HalfResolvedViews, forceAllowBroken : bool) =
    let mutable cachedViews : Map<ResolvedUserViewRef, Result<ResolvedUserView, UserViewError>> = Map.empty

    let findCached (ref : ResolvedUserViewRef) =
        match Map.tryFind ref cachedViews with
        | Some r -> Some r
        | None -> findExistingView ref

    let rec resolveUserView (stack : Set<ResolvedUserViewRef>) (homeSchema : SchemaName option) (uv : HalfResolvedView) : ResolvedUserView =
        let checkView ref =
            let schema =
                match Map.tryFind ref.schema halfResolved with
                | None -> raisef UserViewResolveException "Referenced view not found: %O" ref
                | Some r -> r
            if not <| Map.containsKey ref.name schema then
                raisef UserViewResolveException "Referenced view not found: %O" ref

        let dereferenced =
            try
                dereferenceViewExpr checkView homeSchema uv.resolved
            with
            | :? ViewDereferenceException as err -> raisefWithInner UserViewResolveException err "Dereference error"
        let compiled = compileViewExpr layout defaultAttrs dereferenced

        { resolved = dereferenced
          compiled = compiled
          allowBroken = uv.allowBroken
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : HalfResolvedSchema) : ErroredUserViewsSchema * UserViewsSchema =
        let mutable errors = Map.empty

        let mapUserView name maybeUv =
            let ref = { schema = schemaName; name = name }
            match maybeUv with
            | Error e ->
                cachedViews <- Map.add ref (Error e) cachedViews
                if not e.source.allowBroken then
                    errors <- Map.add name e.error errors
                Error e
            | Ok uv ->
                match findCached ref with
                | Some (Error e) ->
                    if not e.source.allowBroken then
                        errors <- Map.add name e.error errors
                    Error e
                | Some (Ok uv) -> Ok uv
                | None ->
                    let r =
                        try
                            let r = resolveUserView (Set.singleton ref) (Some schemaName) uv
                            Ok r
                        with
                        | :? UserViewResolveException as e ->
                            if uv.allowBroken || forceAllowBroken then
                                if not uv.source.allowBroken then
                                    errors <- Map.add name (e :> exn) errors
                                let err =
                                    { error = e :> exn
                                      source = uv.source
                                    }
                                Error err
                            else
                                raisefWithInner UserViewResolveException e "Error in user view %O" ref
                    cachedViews <- Map.add ref r cachedViews
                    r

        let userViews = schema |> Map.map mapUserView
        let ret = { userViews = userViews } : UserViewsSchema
        (errors, ret)

    let resolveUserViews () : ErroredUserViews * UserViews =
        let mutable errors = Map.empty

        let mapSchema name schema =
            try
                let (schemaErrors, newSchema) = resolveUserViewsSchema name schema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? UserViewResolveException as e ->
                raisefWithInner UserViewResolveException e.InnerException "Error in schema %O: %s" name e.Message

        let schemas = halfResolved |> Map.map mapSchema
        let ret = { schemas = schemas } : UserViews
        (errors, ret)

    member this.ResolveAnonymousUserView uv =
        assert (Map.isEmpty halfResolved)
        resolveUserView Set.empty None uv
    member this.ResolveUserViews = resolveUserViews

// Warning: this should be executed outside of any transactions because of test runs.
let resolveUserViews (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (forceAllowBroken : bool) (userViews : SourceUserViews) : ErroredUserViews * UserViews =
    let phase1 = Phase1Resolver(layout, forceAllowBroken)
    let resolvedViews = phase1.ResolveUserViews userViews
    let phase2 = Phase2Resolver(layout, defaultAttrs, (fun ref -> None), resolvedViews, forceAllowBroken)
    let (errors, ret) = phase2.ResolveUserViews ()
    (errors, ret)

let resolveAnonymousUserView (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (findExistingView : FindExistingView) (q: string) : ResolvedUserView =
    let phase1 = Phase1Resolver(layout, false)
    let resolvedView = phase1.ResolveUserView { query = q; allowBroken = false }
    let phase2 = Phase2Resolver(layout, defaultAttrs, findExistingView, Map.empty, false)
    phase2.ResolveAnonymousUserView resolvedView