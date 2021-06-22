module FunWithFlags.FunDB.UserViews.Resolve

open NetJs
open NetJs.Value

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.UsedReferences
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
    { Source : SourceUserView
      Resolved : ResolvedViewExpr
      AllowBroken : bool
    }

[<NoEquality; NoComparison>]
type private UserViewError =
    { AllowBroken : bool
      Error : exn
    }

[<NoEquality; NoComparison>]
type private HalfResolvedSchema =
    { Source : SourceUserViewsSchema
      // bool : allow_broken
      UserViews : Map<UserViewName, Result<HalfResolvedView, UserViewError>>
    }

type private HalfResolvedViews = Map<SchemaName, Result<HalfResolvedSchema, UserViewsSchemaError>>

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool, flags : ExprResolutionFlags) =
    let resolveUserView (uv : SourceUserView) : HalfResolvedView =
        let parsed =
            match parse tokenizeFunQL viewExpr uv.Query with
            | Error msg -> raisef UserViewResolveException "Parse error: %s" msg
            | Ok rawExpr -> rawExpr
        let resolved =
            try
                resolveViewExpr layout flags parsed
            with
            | :? ViewResolveException as e -> raisefWithInner UserViewResolveException e "Resolve error"
        { Source = uv
          Resolved = resolved
          AllowBroken = uv.AllowBroken
        }

    let resolveUserViewsSchema (schema : SourceUserViewsSchema) : Result<HalfResolvedSchema, UserViewsSchemaError> =
        let mapUserView name uv =
            try
                checkName name
                try
                    Ok <| resolveUserView uv
                with
                | :? UserViewResolveException as e when forceAllowBroken || uv.AllowBroken ->
                    let ret =
                        { AllowBroken = uv.AllowBroken
                          Error = e :> exn
                        }
                    Error ret
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e "In user view %O" name

        let uvs = schema.UserViews |> Map.map mapUserView

        Ok { Source = schema
             UserViews = uvs
           }

    let resolveUserViews (uvs : SourceUserViews) : HalfResolvedViews =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef UserViewResolveException "Unknown schema name"
                resolveUserViewsSchema schema
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e "In schema %O" name
        uvs.Schemas |> Map.map mapSchema

    member this.ResolveUserView uv = resolveUserView uv
    member this.ResolveUserViews uvs = resolveUserViews uvs

type FindExistingView = ResolvedUserViewRef -> Result<ResolvedUserView, exn> option

type private Phase2Resolver (layout : Layout, defaultAttrs : MergedDefaultAttributes, findExistingView : FindExistingView, halfResolved : HalfResolvedViews, forceAllowBroken : bool) =
    let mutable cachedViews : Map<ResolvedUserViewRef, Result<ResolvedUserView, exn>> = Map.empty

    let findCached (ref : ResolvedUserViewRef) =
        match Map.tryFind ref cachedViews with
        | Some r -> Some r
        | None -> findExistingView ref

    let resolveUserView (homeSchema : SchemaName option) (uv : HalfResolvedView) : ResolvedUserView =
        let checkView ref =
            match findExistingView ref with
            | Some _ -> ()
            | None ->
                let mschema =
                    match Map.tryFind ref.Schema halfResolved with
                    | None -> raisef UserViewResolveException "Referenced view not found: %O" ref
                    | Some r -> r
                match mschema with
                | Ok schema ->
                    if not <| Map.containsKey ref.Name schema.UserViews then
                        raisef UserViewResolveException "Referenced view not found: %O" ref
                | Error e ->
                    raisef UserViewResolveException "User view's schema is broken: %O" ref

        let dereferenced =
            try
                dereferenceViewExpr checkView homeSchema uv.Resolved
            with
            | :? ViewDereferenceException as err -> raisefWithInner UserViewResolveException err "Dereference error"
        let compiled = compileViewExpr layout defaultAttrs dereferenced

        { Resolved = dereferenced
          Compiled = compiled
          AllowBroken = uv.AllowBroken
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : HalfResolvedSchema) : Map<UserViewName, exn> * UserViewsSchema =
        let mutable errors = Map.empty

        let mapUserView name maybeUv =
            let ref = { Schema = schemaName; Name = name }
            match maybeUv with
            | Error e ->
                cachedViews <- Map.add ref (Error e.Error) cachedViews
                if not e.AllowBroken then
                    errors <- Map.add name e.Error errors
                Error e.Error
            | Ok (uv : HalfResolvedView) ->
                match findCached ref with
                | Some (Error e) ->
                    if not uv.AllowBroken then
                        errors <- Map.add name e errors
                    Error e
                | Some (Ok cached) -> Ok cached
                | None ->
                    let r =
                        try
                            let r = resolveUserView (Some schemaName) uv
                            Ok r
                        with
                        | :? UserViewResolveException as e when uv.AllowBroken || forceAllowBroken ->
                            if not uv.AllowBroken then
                                errors <- Map.add name (e :> exn) errors
                            Error (e :> exn)
                        | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e "In user view %O" ref
                    cachedViews <- Map.add ref r cachedViews
                    r

        let userViews = schema.UserViews |> Map.map mapUserView
        let ret =
            { UserViews = userViews
              GeneratorScript = schema.Source.GeneratorScript
            } : UserViewsSchema
        (errors, ret)

    let resolveUserViews () : ErroredUserViews * UserViews =
        let mutable errors : ErroredUserViews = Map.empty

        let mapSchema name : Result<_, UserViewsSchemaError> -> Result<_, UserViewsSchemaError> = function
            | Ok schema ->
                try
                    let (schemaErrors, newSchema) = resolveUserViewsSchema name schema
                    if not <| Map.isEmpty schemaErrors then
                        errors <- Map.add name (UEUserViews schemaErrors) errors
                    Ok newSchema
                with
                | :? UserViewResolveException as e ->
                    raisefWithInner UserViewResolveException e "In schema %O" name
            | Error err ->
                errors <- Map.add name (UEGenerator err.Error) errors
                Error err

        let schemas = halfResolved |> Map.map mapSchema
        let ret = { Schemas = schemas } : UserViews
        (errors, ret)

    member this.ResolveAnonymousUserView homeSchema uv =
        assert (Map.isEmpty halfResolved)
        resolveUserView homeSchema uv
    member this.ResolveUserViews () = resolveUserViews ()

let private uvResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

// Warning: this should be executed outside of any transactions because of test runs.
let resolveUserViews (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (forceAllowBroken : bool) (userViews : SourceUserViews) : ErroredUserViews * UserViews =
    let phase1 = Phase1Resolver(layout, forceAllowBroken, uvResolutionFlags)
    let resolvedViews = phase1.ResolveUserViews userViews
    let phase2 = Phase2Resolver(layout, defaultAttrs, (fun ref -> None), resolvedViews, forceAllowBroken)
    let (errors, ret) = phase2.ResolveUserViews ()
    (errors, ret)

let resolveAnonymousUserView (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (findExistingView : FindExistingView) (homeSchema : SchemaName option) (q: string) : ResolvedUserView =
    let phase1 = Phase1Resolver(layout, false, emptyExprResolutionFlags)
    let resolvedView = phase1.ResolveUserView { Query = q; AllowBroken = false }
    let phase2 = Phase2Resolver(layout, defaultAttrs, findExistingView, Map.empty, false)
    phase2.ResolveAnonymousUserView homeSchema resolvedView