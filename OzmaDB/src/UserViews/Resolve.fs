module OzmaDB.UserViews.Resolve

open FSharpPlus
open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Parsing
open OzmaDB.UserViews.Types
open OzmaDB.UserViews.Source
open OzmaDB.UserViews.Generate
open OzmaDB.OzmaQL.AST
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.Utils
open OzmaDB.OzmaQL.Lex
open OzmaDB.OzmaQL.Parse
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.Attributes.Types
open OzmaDB.Attributes.Merge
open OzmaDB.Objects.Types

type UserViewResolveException(message: string, innerException: exn) =
    inherit UserException(message, innerException)

    new(message: string) = UserViewResolveException(message, null)

let private checkName (OzmaQLName name) =
    if not (goodName name) then
        raisef UserViewResolveException "Invalid user view name"

let private getColumn: ColumnType -> OzmaQLName option =
    function
    | CTColumn c -> Some c
    | _ -> None

type FindExistingView = ResolvedUserViewRef -> PossiblyBroken<ResolvedUserView> option

let emptyFindExistingView (ref: ResolvedUserViewRef) : PossiblyBroken<ResolvedUserView> option = None

type private Phase1Resolver
    (
        layout: Layout,
        defaultAttrs: MergedDefaultAttributes,
        forceAllowBroken: bool,
        flags: ExprResolutionFlags,
        userViews: GeneratedUserViews,
        findExistingView: FindExistingView
    ) =
    let getDefaultAttribute (fieldRef: ResolvedFieldRef) (name: AttributeName) : DefaultAttribute option =
        monad {
            let! attrs = defaultAttrs.FindField fieldRef.Entity fieldRef.Name
            let! attr = Map.tryFind name attrs
            return attr.Attribute
        }

    let hasUserView (uvRef: ResolvedUserViewRef) =
        match Map.tryFind uvRef.Schema userViews.Schemas with
        | Some(Ok schema) ->
            Map.containsKey uvRef.Name schema.UserViews
            || Option.isSome (findExistingView uvRef)
        | _ -> Option.isSome (findExistingView uvRef)

    let defaultCallbacks =
        { Layout = layout
          HomeSchema = None
          GetDefaultAttribute = getDefaultAttribute
          HasUserView = hasUserView }

    let resolveUserView (homeSchema: SchemaName option) (uv: SourceUserView) : ResolvedUserView =
        let parsed =
            match parse tokenizeOzmaQL viewExpr uv.Query with
            | Error msg -> raisef UserViewResolveException "Parse error: %s" msg
            | Ok rawExpr -> rawExpr

        let resolved =
            let callbacks =
                { defaultCallbacks with
                    HomeSchema = homeSchema }

            try
                resolveViewExpr callbacks flags parsed
            with :? QueryResolveException as e ->
                raisefWithInner UserViewResolveException e "Resolve error"

        let compiled = compileViewExpr layout defaultAttrs resolved

        { Resolved = resolved
          Compiled = compiled
          AllowBroken = uv.AllowBroken }

    let resolveUserViewsSchema
        (schemaName: SchemaName)
        (schema: GeneratedUserViewsSchema)
        : PossiblyBroken<UserViewsSchema> =
        let mapUserView name uv =
            try
                checkName name

                try
                    Ok <| resolveUserView (Some schemaName) uv
                with :? UserViewResolveException as e when forceAllowBroken || uv.AllowBroken ->
                    let ret =
                        { AllowBroken = uv.AllowBroken
                          Error = e :> exn }

                    Error ret
            with e ->
                raisefWithInner UserViewResolveException e "In user view %O" name

        let uvs = schema.UserViews |> Map.map mapUserView

        Ok { UserViews = uvs }

    let resolveUserViews () : UserViews =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef UserViewResolveException "Unknown schema name"

                resolveUserViewsSchema name schema
            with e ->
                raisefWithInner UserViewResolveException e "In schema %O" name

        { Schemas = userViews.Schemas |> Map.map (fun name -> Result.bind (mapSchema name)) }

    member this.ResolveUserView homeSchema uv = resolveUserView homeSchema uv
    member this.ResolveUserViews() = resolveUserViews ()

let private uvResolutionFlags =
    { emptyExprResolutionFlags with
        Privileged = true }

let resolveUserViews
    (layout: Layout)
    (defaultAttrs: MergedDefaultAttributes)
    (forceAllowBroken: bool)
    (userViews: GeneratedUserViews)
    : UserViews =
    let phase1 =
        Phase1Resolver(layout, defaultAttrs, forceAllowBroken, uvResolutionFlags, userViews, emptyFindExistingView)

    phase1.ResolveUserViews()

let resolveAnonymousUserView
    (layout: Layout)
    (isPrivileged: bool)
    (defaultAttrs: MergedDefaultAttributes)
    (findExistingView: FindExistingView)
    (homeSchema: SchemaName option)
    (q: string)
    : ResolvedUserView =
    let phase1 =
        Phase1Resolver(
            layout,
            defaultAttrs,
            false,
            { emptyExprResolutionFlags with
                Privileged = isPrivileged },
            emptyGeneratedUserViews,
            findExistingView
        )

    phase1.ResolveUserView homeSchema { Query = q; AllowBroken = false }
