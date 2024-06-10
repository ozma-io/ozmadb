module OzmaDB.API.UserViews

open FSharpPlus
open System.Linq
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Query
open OzmaDB.OzmaQL.Chunk
open OzmaDB.Permissions.Types
open OzmaDB.Permissions.Apply
open OzmaDB.Permissions.View
open OzmaDB.UserViews.DryRun
open OzmaDB.UserViews.Resolve
open OzmaDB.API.Types
module SQL = OzmaDB.SQL.Query

let private canExplain : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

let private userViewComments (source : UserViewSource) (role : RoleType) (arguments : ArgumentValuesMap) (chunk : SourceQueryChunk) =
    let sourceStr =
        match source with
        | UVAnonymous q -> sprintf "(%s)" q
        | UVNamed ref -> ref.ToOzmaQLString()
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let chunkStr = sprintf ", chunk %s" (JsonConvert.SerializeObject chunk)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role when role.CanRead -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [sourceStr; argumentsStr; chunkStr; roleStr]

let private removeColumnAttributes (col : UserViewColumn) =
    { col with
          AttributeTypes = Map.empty
          CellAttributeTypes = Map.empty
    }

let private applyFlags (flags : UserViewFlags) (view : PrefetchedUserView) =
    let view =
        if not flags.NoAttributes then
            view
        else
            let info =
                { view.Info with
                      AttributeTypes = Map.empty
                      RowAttributeTypes = Map.empty
                      Columns = Array.map removeColumnAttributes view.Info.Columns
                }
            // FIXME: implement!
            view
    view

type UserViewsAPI (api : IOzmaDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<UserViewsAPI>()

    let resolveSource (source : UserViewSource) (flags : UserViewFlags) : Task<Result<PrefetchedUserView, UserViewErrorInfo>> =
        task {
            try
                match source with
                | UVAnonymous query ->
                    let! anon =
                        if flags.ForceRecompile then
                            ctx.ResolveAnonymousView rctx.IsPrivileged None query
                        else
                            ctx.GetAnonymousView rctx.IsPrivileged query
                    return Ok anon
                | UVNamed ref ->
                    if flags.ForceRecompile then
                        let! uv = ctx.Transaction.System.UserViews.AsQueryable().Where(fun uv -> uv.Schema.Name = string ref.Schema && uv.Name = string ref.Name).FirstOrDefaultAsync(ctx.CancellationToken)
                        if isNull uv then
                            return Error <| UVERequest (sprintf "User view %O not found" ref)
                        else
                            let! anon = ctx.ResolveAnonymousView true (Some ref.Schema) uv.Query
                            return Ok <| applyFlags flags anon
                    else
                        match ctx.UserViews.Find ref with
                        | None -> return Error <| UVERequest (sprintf "User view %O not found" ref)
                        | Some (Error e) ->
                            logger.LogError(e.Error, "Requested user view {uv} is broken", ref)
                            let msg = sprintf "User view %O is broken: %s" ref (fullUserMessage e.Error)
                            return Error <| UVEOther msg
                        | Some (Ok cached) ->
                            return Ok <| applyFlags flags cached
            with
            | :? UserViewResolveException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to compile user view {uv}", source)
                return Error <| UVERequest (fullUserMessage ex)
            | :? UserViewDryRunException as ex when ex.IsUserException ->
                logger.LogError(ex, "Failed to dry run user view {uv}", source)
                return Error <| UVERequest (fullUserMessage ex)
        }

    member this.GetUserViewInfo (req : UserViewInfoRequest) : Task<Result<UserViewInfoResponse, UserViewErrorInfo>> =
        wrapAPIResult rctx "getUserViewInfo" req <| fun () -> task {
            let flags = Option.defaultValue emptyUserViewFlags req.Flags
            match! resolveSource req.Source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    match rctx.User.Effective.Type with
                    | RTRoot -> ()
                    | RTRole role when role.CanRead -> ()
                    | RTRole role -> ignore <| applyPermissions ctx.Layout (Option.defaultValue emptyResolvedRole role.Role) uv.UserView.Compiled.UsedDatabase
                    return Ok { Info = uv.Info
                                ConstAttributes = uv.ConstAttributes.Attributes
                                ConstColumnAttributes = uv.ConstAttributes.ColumnAttributes
                                ConstArgumentAttributes = uv.ConstAttributes.ArgumentAttributes
                              }
                with
                | :? PermissionsApplyException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied to info for user view {uv}", req.Source)
                    return Error <| UVEAccessDenied (fullUserMessage ex)
        }

    member this.GetUserViewExplain (req : UserViewExplainRequest) : Task<Result<ExplainedViewExpr, UserViewErrorInfo>> =
        wrapAPIResult rctx "getUserViewExplain" req <| fun () -> task {
            if not (canExplain rctx.User.Saved.Type) then
                logger.LogError("Explain access denied")
                return Error (UVEAccessDenied "Explain access denied")
            else
                let flags = Option.defaultValue emptyUserViewFlags req.Flags
                match! resolveSource req.Source flags with
                | Error e -> return Error e
                | Ok uv ->
                    try
                        let compiled =
                            match getReadRole rctx.User.Effective.Type with
                            | None -> uv.UserView.Compiled
                            | Some role ->
                                let appliedDb = applyPermissions ctx.Layout role uv.UserView.Compiled.UsedDatabase
                                applyRoleViewExpr ctx.Layout appliedDb uv.UserView.Compiled
                        let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
                        let resolvedChunk = resolveViewExprChunk ctx.Layout compiled chunk
                        let (extraLocalArgs, query) = queryExprChunk ctx.Layout resolvedChunk compiled.Query
                        let extraArgValues = Map.mapKeys PLocal extraLocalArgs
                        let compiled = { compiled with Query = query }
                        let maybeArgs =
                            Option.map (fun args ->
                                convertQueryArguments
                                    rctx.GlobalArguments
                                    extraArgValues
                                    args
                                    compiled.Query.Arguments
                            ) req.Args
                        let explainFlags = Option.defaultValue SQL.defaultExplainOptions req.ExplainFlags
                        let! res = explainViewExpr ctx.Transaction.Connection.Query ctx.Layout compiled maybeArgs explainFlags ctx.CancellationToken
                        return Ok res
                    with
                    | :? ChunkException as e when e.IsUserException ->
                        return Error <| UVERequest (sprintf "Error in the chunk spec: %s" (fullUserMessage e))
                    | :? ArgumentCheckException as e ->
                        return Error <| UVEExecution (UVEArgument e.Details)
                    | :? PermissionsApplyException as e when e.IsUserException ->
                        logger.LogError(e, "Access denied to user view {uv}", req.Source)
                        return Error <| UVEAccessDenied (fullUserMessage e)
                    | :? UserViewExecutionException as e when e.IsUserException ->
                        logger.LogError(e, "Failed to execute user view {uv}", req.Source)
                        return Error (UVEExecution e.Details)
        }

    member this.GetUserView (req : UserViewRequest) : Task<Result<UserViewEntriesResponse, UserViewErrorInfo>> =
        wrapAPIResult rctx "getUserView" req <| fun () -> task {
            let flags = Option.defaultValue emptyUserViewFlags req.Flags
            match! resolveSource req.Source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let compiled =
                        match getReadRole rctx.User.Effective.Type with
                        | None -> uv.UserView.Compiled
                        | Some role ->
                            let appliedDb = applyPermissions ctx.Layout role uv.UserView.Compiled.UsedDatabase
                            applyRoleViewExpr ctx.Layout appliedDb uv.UserView.Compiled
                    let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
                    let resolvedChunk = resolveViewExprChunk ctx.Layout compiled chunk
                    let (extraLocalArgs, query) = queryExprChunk ctx.Layout resolvedChunk compiled.Query
                    let extraArgValues = Map.mapKeys PLocal extraLocalArgs
                    let compiled = { compiled with Query = query }
                    let arguments =
                        convertQueryArguments
                            rctx.GlobalArguments
                            extraArgValues
                            req.Args
                            compiled.Query.Arguments

                    let getResult info (res : ExecutingViewExpr) =
                        task {
                            let! rows = res.Rows.ToArrayAsync(ctx.CancellationToken)
                            return
                                { ArgumentAttributes = Map.unionWith Map.unionUnique uv.ConstAttributes.ArgumentAttributes res.ArgumentAttributes
                                  Attributes = Map.unionUnique uv.ConstAttributes.Attributes res.Attributes
                                  ColumnAttributes = Array.map2 Map.unionUnique uv.ConstAttributes.ColumnAttributes res.ColumnAttributes
                                  Rows = rows
                                }
                        }

                    let comments = userViewComments req.Source rctx.User.Effective.Type arguments chunk
                    let! res = runViewExpr ctx.Transaction.Connection.Query ctx.Layout compiled (Some comments) arguments ctx.CancellationToken getResult
                    return Ok { Info = uv.Info
                                Result = res
                              }
                with
                | :? ChunkException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to parse chunk for user view {uv}", req.Source)
                    return Error <| UVERequest (sprintf "Error in the chunk spec: %s" (fullUserMessage e))
                | :? ArgumentCheckException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to parse arguments for user view {uv}", req.Source)
                    return Error <| UVEExecution (UVEArgument e.Details)
                | :? UserViewExecutionException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to execute user view {uv}", req.Source)
                    return Error (UVEExecution e.Details)
                | :? PermissionsApplyException as e when e.IsUserException ->
                    logger.LogError(e, "Access denied to user view {uv}", req.Source)
                    return Error <| UVEAccessDenied (fullUserMessage e)
        }

    interface IUserViewsAPI with
        member this.GetUserViewInfo req = this.GetUserViewInfo req
        member this.GetUserViewExplain req = this.GetUserViewExplain req
        member this.GetUserView req = this.GetUserView req
