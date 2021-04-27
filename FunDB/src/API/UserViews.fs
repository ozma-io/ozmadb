module FunWithFlags.FunDB.API.UserViews

open System.Linq
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.API.Types

let private canExplain : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

type UserViewsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<UserViewsAPI>()

    let resolveSource (source : UserViewSource) (flags : UserViewFlags) : Task<Result<PrefetchedUserView, UserViewErrorInfo>> =
        task {
            match source with
            | UVAnonymous query ->
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
                    let! uv = ctx.Transaction.System.UserViews.AsQueryable().Where(fun uv -> uv.Schema.Name = string ref.schema && uv.Name = string ref.name).FirstOrDefaultAsync(ctx.CancellationToken)
                    if isNull uv then
                        return Error UVENotFound
                    else
                        try
                            let! anon = ctx.ResolveAnonymousView (Some ref.schema) uv.Query
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
                        match parseValueFromJson arg.FieldType false argStr with
                        | None -> Error <| sprintf "Cannot convert argument %O to type %O" name arg.FieldType
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
                    match rctx.User.Type with
                    | RTRoot -> ()
                    | RTRole role when role.CanRead -> ()
                    | RTRole role -> checkRoleViewExpr ctx.Layout (Option.defaultValue emptyResolvedRole role.Role) uv.UserView.Compiled
                    return Ok { Info = uv.Info
                                PureAttributes = uv.PureAttributes.Attributes
                                PureColumnAttributes = uv.PureAttributes.ColumnAttributes
                              }
                with
                | :? PermissionsViewException as err ->
                    logger.LogError(err, "Access denied to user view info")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getUserViewInfo"
                        event.Error <- "access_denied"
                        event.Details <- exceptionString err
                    )
                    return Error UVEAccessDenied
        }

    member this.GetUserViewExplain (source : UserViewSource) (chunk : SourceQueryChunk) (flags : UserViewFlags) : Task<Result<ExplainedViewExpr, UserViewErrorInfo>> =
        task {
            match! resolveSource source flags with
            | Error e -> return Error e
            | Ok uv ->
                if not (canExplain rctx.User.Type) then
                    logger.LogError("Explain access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getUserViewExplain"
                        event.Error <- "access_denied"
                    )
                    return Error UVEAccessDenied
                else
                    let compiled = uv.UserView.Compiled
                    let maybeResolvedChunk =
                        try
                            Ok <| resolveViewExprChunk compiled chunk
                        with
                        | :? ChunkException as e ->
                            rctx.WriteEvent (fun event ->
                                event.Type <- "getUserViewExplain"
                                event.Error <- "arguments"
                                event.Details <- exceptionString e
                            )
                            Error <| UVEArguments (exceptionString e)

                    match maybeResolvedChunk with
                    | Error e -> return Error e
                    | Ok resolvedChunk ->
                        let (extraLocalArgs, query) = queryExprChunk resolvedChunk compiled.Query
                        let compiled = { compiled with Query = query }
                        let! res = explainViewExpr ctx.Transaction.Connection.Query compiled ctx.CancellationToken
                        return Ok res
        }

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) (chunk : SourceQueryChunk) (flags : UserViewFlags) : Task<Result<UserViewEntriesResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let compiled =
                        match rctx.User.Type with
                        | RTRoot -> uv.UserView.Compiled
                        | RTRole role when role.CanRead -> uv.UserView.Compiled
                        | RTRole role ->
                            applyRoleViewExpr ctx.Layout (Option.defaultValue emptyResolvedRole role.Role) uv.UserView.Compiled

                    let maybeResolvedChunk =
                        try
                            Ok <| resolveViewExprChunk compiled chunk
                        with
                        | :? ChunkException as e ->
                            rctx.WriteEvent (fun event ->
                                event.Type <- "getUserView"
                                event.Error <- "arguments"
                                event.Details <- exceptionString e
                            )
                            Error <| UVEArguments (exceptionString e)

                    match maybeResolvedChunk with
                    | Error e -> return Error e
                    | Ok resolvedChunk ->
                        let (extraLocalArgs, query) = queryExprChunk resolvedChunk compiled.Query
                        let extraArgValues = Map.mapKeys PLocal extraLocalArgs
                        let compiled = { compiled with Query = query }

                        match convertViewArguments extraArgValues rawArgs compiled with
                        | Error msg ->
                            rctx.WriteEvent (fun event ->
                                event.Type <- "getUserView"
                                event.Error <- "arguments"
                                event.Details <- msg
                            )
                            return Error <| UVEArguments msg
                        | Ok arguments ->
                            let getResult info (res : ExecutedViewExpr) = Task.result (uv, { res with Rows = Array.ofSeq res.Rows })

                            let! (puv, res) = runViewExpr ctx.Transaction.Connection.Query compiled arguments ctx.CancellationToken getResult
                            return Ok { Info = puv.Info
                                        Result = res
                                      }
                with
                | :? PermissionsViewException as err ->
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
        member this.GetUserViewExplain source chunk flags = this.GetUserViewExplain source chunk flags
        member this.GetUserView source rawArguments chunk flags = this.GetUserView source rawArguments chunk flags
