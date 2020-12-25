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
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.API.Types

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
                    return Error <| UVEResolution (exceptionString err)
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
                            return Error <| UVEResolution (exceptionString err)
                else
                    match ctx.UserViews.Find ref with
                    | None -> return Error UVENotFound
                    | Some (Error e) ->
                        logger.LogError(e, "Requested user view {uv} is broken", ref.ToString())
                        return Error <| UVEResolution (exceptionString e)
                    | Some (Ok cached) -> return Ok cached
        }

    let convertViewArguments (rawArgs : RawArguments) (compiled : CompiledViewExpr) : Result<ArgumentValues, string> =
        let findArgument (name, arg : CompiledArgument) =
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

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) (flags : UserViewFlags) : Task<Result<UserViewEntriesResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source flags with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let restricted =
                        match rctx.User.Type with
                        | RTRoot -> uv.UserView.Compiled
                        | RTRole role when role.CanRead -> uv.UserView.Compiled
                        | RTRole role ->
                            applyRoleViewExpr ctx.Layout (Option.defaultValue emptyResolvedRole role.Role) uv.UserView.Compiled
                    let getResult info (res : ExecutedViewExpr) = task {
                        return (uv, { res with Rows = Array.ofSeq res.Rows })
                    }
                    match convertViewArguments rawArgs restricted with
                    | Error msg ->
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getUserView"
                            event.Error <- "arguments"
                            event.Details <- msg
                        )
                        return Error <| UVEArguments msg
                    | Ok arguments ->
                            let! (uv, res) = runViewExpr ctx.Transaction.Connection.Query restricted arguments ctx.CancellationToken getResult
                            return Ok { Info = uv.Info
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
        member this.GetUserView source rawArguments flags = this.GetUserView source rawArguments flags
