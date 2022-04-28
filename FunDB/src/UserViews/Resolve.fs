module FunWithFlags.FunDB.UserViews.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Generate
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Dereference
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Objects.Types

type UserViewResolveException (message : string, innerException : Exception) =
    inherit UserException(message, innerException)

    new (message : string) = UserViewResolveException (message, null)

let private checkName (FunQLName name) =
    if not (goodName name) then
        raisef UserViewResolveException "Invalid user view name"

let private getColumn : ColumnType -> FunQLName option = function
    | CTColumn c -> Some c
    | _ -> None

[<NoEquality; NoComparison>]
type private HalfResolvedView =
    { Resolved : ResolvedViewExpr
      AllowBroken : bool
    }

[<NoEquality; NoComparison>]
type private HalfResolvedSchema =
    { UserViews : Map<UserViewName, PossiblyBroken<HalfResolvedView>>
    }

type private HalfResolvedViews = Map<SchemaName, PossiblyBroken<HalfResolvedSchema>>

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
        { Resolved = resolved
          AllowBroken = uv.AllowBroken
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : GeneratedUserViewsSchema) : PossiblyBroken<HalfResolvedSchema> =
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
            | e -> raisefWithInner UserViewResolveException e "In user view %O" name

        let uvs = schema.UserViews |> Map.map mapUserView

        Ok { UserViews = uvs }

    let resolveUserViews (uvs : GeneratedUserViews) : HalfResolvedViews =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef UserViewResolveException "Unknown schema name"
                resolveUserViewsSchema name schema
            with
            | e -> raisefWithInner UserViewResolveException e "In schema %O" name
        uvs.Schemas |> Map.map (fun name -> Result.bind (mapSchema name))

    member this.ResolveUserView uv = resolveUserView uv
    member this.ResolveUserViews uvs = resolveUserViews uvs

type FindExistingView = ResolvedUserViewRef -> PossiblyBroken<ResolvedUserView> option

type private Phase2Resolver (layout : Layout, defaultAttrs : MergedDefaultAttributes, findExistingView : FindExistingView, halfResolved : HalfResolvedViews, forceAllowBroken : bool) =
    let mutable cachedViews : Map<ResolvedUserViewRef, PossiblyBroken<ResolvedUserView>> = Map.empty

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

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : HalfResolvedSchema) : UserViewsSchema =
        let mapUserView name (maybeUv : PossiblyBroken<HalfResolvedView>) =
            let ref = { Schema = schemaName; Name = name }
            match maybeUv with
            | Error e ->
                let ret = Error e
                cachedViews <- Map.add ref ret cachedViews
                ret
            | Ok uv ->
                match findCached ref with
                | Some (Error e) -> Error e
                | Some (Ok cached) -> Ok cached
                | None ->
                    let r =
                        try
                            let r = resolveUserView (Some schemaName) uv
                            Ok r
                        with
                        | :? UserViewResolveException as e when uv.AllowBroken || forceAllowBroken ->
                            Error { Error = e; AllowBroken = uv.AllowBroken }
                        | e -> raisefWithInner UserViewResolveException e "In user view %O" ref
                    cachedViews <- Map.add ref r cachedViews
                    r

        let userViews = schema.UserViews |> Map.map mapUserView
        { UserViews = userViews
        }

    let resolveUserViews () : UserViews =
        let mapSchema name schema =
            try
                resolveUserViewsSchema name schema
            with
            | :? UserViewResolveException as e ->
                raisefWithInner UserViewResolveException e "In schema %O" name

        let schemas = halfResolved |> Map.map (fun name -> Result.map (mapSchema name))
        { Schemas = schemas }

    member this.ResolveAnonymousUserView homeSchema uv =
        assert (Map.isEmpty halfResolved)
        resolveUserView homeSchema uv
    member this.ResolveUserViews () = resolveUserViews ()

let private uvResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

// Warning: this should be executed outside of any transactions because of test runs.
let resolveUserViews (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (forceAllowBroken : bool) (userViews : GeneratedUserViews) : UserViews =
    let phase1 = Phase1Resolver(layout, forceAllowBroken, uvResolutionFlags)
    let resolvedViews = phase1.ResolveUserViews userViews
    let phase2 = Phase2Resolver(layout, defaultAttrs, (fun ref -> None), resolvedViews, forceAllowBroken)
    phase2.ResolveUserViews ()

let resolveAnonymousUserView (layout : Layout) (isPrivileged : bool) (defaultAttrs : MergedDefaultAttributes) (findExistingView : FindExistingView) (homeSchema : SchemaName option) (q: string) : ResolvedUserView =
    let phase1 = Phase1Resolver(layout, false, { emptyExprResolutionFlags with Privileged = isPrivileged })
    let resolvedView = phase1.ResolveUserView { Query = q; AllowBroken = false }
    let phase2 = Phase2Resolver(layout, defaultAttrs, findExistingView, Map.empty, false)
    phase2.ResolveAnonymousUserView homeSchema resolvedView
