module FunWithFlags.FunDB.API.UserViews

open System.Linq
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.API.Types
module SQL = FunWithFlags.FunDB.SQL.Query

let private canExplain : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

let private userViewComments (source : UserViewSource) (role : RoleType) (arguments : ArgumentValuesMap) (chunk : SourceQueryChunk) =
    let sourceStr =
        match source with
        | UVAnonymous q -> "(anonymous)"
        | UVNamed ref -> ref.ToFunQLString()
    let argumentsStr = sprintf ", arguments %s" (JsonConvert.SerializeObject arguments)
    let chunkStr = sprintf ", chunk %s" (JsonConvert.SerializeObject chunk)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role when role.CanRead -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [sourceStr; argumentsStr; chunkStr; roleStr]

let canQueryAnonymously (rctx : IRequestContext) : bool =
    let allowAnon =
        match rctx.User.Effective.Type with
        | RTRole { Role = Some role } -> role.Flattened.AllowAnonymousQueries
        | RTRoot -> true
        | _ -> false
    if allowAnon then true else
        // Still allow anonymous queries from triggers and actions.
        match rctx.Source with
        | ESAPI -> false
        | ESTrigger ref -> true
        | ESAction ref -> true

type UserViewsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<UserViewsAPI>()

    let resolveSource (source : UserViewSource) (flags : UserViewFlags) : Task<Result<PrefetchedUserView, UserViewErrorInfo>> =
        task {
            match source with
            | UVAnonymous query ->
                if not <| canQueryAnonymously rctx then
                    logger.LogError("Access denied to execute anonymous user view")
                    return Error UVEAccessDenied
                else
                    try
                        let! anon =
                            if flags.ForceRecompile then
                                ctx.ResolveAnonymousView None query
                            else
                                ctx.GetAnonymousView query
                        return Ok anon
                    with
                    | :? UserViewResolveException as err ->
                        logger.LogError(err, "Failed to resolve anonymous user view: {uv}", query)
                        return Error <| UVECompilation (exceptionString err)
            | UVNamed ref ->
                if flags.ForceRecompile then
                    let! uv = ctx.Transaction.System.UserViews.AsQueryable().Where(fun uv -> uv.Schema.Name = string ref.Schema && uv.Name = string ref.Name).FirstOrDefaultAsync(ctx.CancellationToken)
                    if isNull uv then
                        return Error UVENotFound
                    else
                        try
                            let! anon = ctx.ResolveAnonymousView (Some ref.Schema) uv.Query
                            return Ok anon
                        with
                        | :? UserViewResolveException as err ->
                            logger.LogError(err, "Failed to recompile user view {uv}", ref.ToString())
                            return Error <| UVECompilation (exceptionString err)
                else
                    match ctx.UserViews.Find ref with
                    | None -> return Error UVENotFound
                    | Some (Error e) ->
                        logger.LogError(e, "Requested user view {uv} is broken", ref.ToString())
                        return Error <| UVECompilation (exceptionString e)
                    | Some (Ok cached) -> return Ok cached
        }

    let convertViewArguments (extraArguments : ArgumentValuesMap) (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ArgumentValuesMap, string> =
        let findArgument (name, arg : CompiledArgument) =
            match Map.tryFind name extraArguments with
            | Some arg -> Ok (Some (name, arg))
            | None ->
                match name with
                | PLocal (FunQLName lname) ->
                    match Map.tryFind lname rawArgs with
                    | Some argStr ->
                        match parseValueFromJson arg.FieldType arg.Optional argStr with
                        | None -> Error <| sprintf "Cannot convert argument %O to type %O" name arg.FieldType
                        | Some FNull -> Ok None
                        | Some arg -> Ok (Some (name, arg))
                    | _ -> Ok None
                | PGlobal gname -> Ok (Some (name, Map.find gname rctx.GlobalArguments))
        compiled.Query.Arguments.Types |> Map.toSeq |> Seq.traverseResult findArgument |> Result.map (Seq.catMaybes >> Map.ofSeq)

    member this.GetUserViewInfo (source : UserViewSource) (flags : UserViewFlags) : Task<Result<UserViewInfoResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    match rctx.User.Effective.Type with
                    | RTRoot -> ()
                    | RTRole role when role.CanRead -> ()
                    | RTRole role -> checkRoleViewExpr ctx.Layout (Option.defaultValue emptyResolvedRole role.Role) uv.UserView.Compiled.UsedSchemas
                    return Ok { Info = uv.Info
                                PureAttributes = uv.PureAttributes.Attributes
                                PureColumnAttributes = uv.PureAttributes.ColumnAttributes
                              }
                with
                | :? PermissionsApplyException as err ->
                    logger.LogError(err, "Access denied to user view info")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getUserViewInfo"
                        event.Error <- "access_denied"
                        event.Details <- exceptionString err
                    )
                    return Error UVEAccessDenied
        }

    member this.GetUserViewExplain (source : UserViewSource) (maybeRawArguments : RawArguments option) (chunk : SourceQueryChunk) (flags : UserViewFlags) (explainOpts : SQL.ExplainOptions) : Task<Result<ExplainedViewExpr, UserViewErrorInfo>> =
        task {
            if not (canExplain rctx.User.Saved.Type) then
                logger.LogError("Explain access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "getUserViewExplain"
                    event.Error <- "access_denied"
                )
                return Error UVEAccessDenied
            else
                match! resolveSource source flags with
                | Error e -> return Error e
                | Ok uv ->
                    let compiled =
                        match getReadRole rctx.User.Effective.Type with
                        | None -> uv.UserView.Compiled
                        | Some role -> applyRoleViewExpr ctx.Layout role uv.UserView.Compiled.UsedSchemas uv.UserView.Compiled
                    let maybeResolvedChunk =
                        try
                            Ok <| resolveViewExprChunk ctx.Layout compiled chunk
                        with
                        | :? ChunkException as e ->
                            Error <| UVEArguments (exceptionString e)

                    match maybeResolvedChunk with
                    | Error e -> return Error e
                    | Ok resolvedChunk ->
                        let (extraLocalArgs, query) = queryExprChunk ctx.Layout resolvedChunk compiled.Query
                        let extraArgValues = Map.mapKeys PLocal extraLocalArgs
                        let compiled = { compiled with Query = query }
                        match Option.map (fun args -> convertViewArguments extraArgValues args compiled) maybeRawArguments with
                        | Some (Error msg) ->
                            return Error <| UVEArguments msg
                        | maybeRetArgs ->
                            let maybeArgs = Option.map Result.get maybeRetArgs
                            try
                                let! res = explainViewExpr ctx.Transaction.Connection.Query compiled maybeArgs explainOpts ctx.CancellationToken
                                return Ok res
                            with
                            | :? UserViewExecutionException as ex ->
                                logger.LogError(ex, "Failed to execute user view")
                                let str = exceptionString ex
                                return Error (UVEExecution str)
        }

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) (chunk : SourceQueryChunk) (flags : UserViewFlags) : Task<Result<UserViewEntriesResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let compiled =
                        match getReadRole rctx.User.Effective.Type with
                        | None -> uv.UserView.Compiled
                        | Some role -> applyRoleViewExpr ctx.Layout role uv.UserView.Compiled.UsedSchemas uv.UserView.Compiled

                    let maybeResolvedChunk =
                        try
                            Ok <| resolveViewExprChunk ctx.Layout compiled chunk
                        with
                        | :? ChunkException as e ->
                            Error <| UVEArguments (exceptionString e)

                    match maybeResolvedChunk with
                    | Error e -> return Error e
                    | Ok resolvedChunk ->
                        let (extraLocalArgs, query) = queryExprChunk ctx.Layout resolvedChunk compiled.Query
                        let extraArgValues = Map.mapKeys PLocal extraLocalArgs
                        let compiled = { compiled with Query = query }

                        match convertViewArguments extraArgValues rawArgs compiled with
                        | Error msg ->
                            return Error <| UVEArguments msg
                        | Ok arguments ->
                            let getResult info (res : ExecutedViewExpr) = Task.result (uv, { res with Rows = Array.ofSeq res.Rows })
                            let comments = userViewComments source rctx.User.Effective.Type arguments chunk
                            let! (puv, res) = runViewExpr ctx.Transaction.Connection.Query compiled (Some comments) arguments ctx.CancellationToken getResult
                            return Ok { Info = puv.Info
                                        Result = res
                                      }
                with
                | :? UserViewExecutionException as ex ->
                    logger.LogError(ex, "Failed to execute user view")
                    let str = exceptionString ex
                    return Error (UVEExecution str)
                | :? PermissionsApplyException as err ->
                    logger.LogError(err, "Access denied to user view")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getUserView"
                        event.Error <- "access_denied"
                        event.Details <- exceptionString err
                    )
                    return Error UVEAccessDenied
        }

    interface IUserViewsAPI with
        member this.GetUserViewInfo source flags = this.GetUserViewInfo source flags
        member this.GetUserViewExplain source maybeRawArguments chunk flags explainOpts = this.GetUserViewExplain source maybeRawArguments chunk flags explainOpts
        member this.GetUserView source rawArguments chunk flags = this.GetUserView source rawArguments chunk flags
