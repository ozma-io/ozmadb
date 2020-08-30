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
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.UserViews.Resolve
open FunWithFlags.FunDB.API.Types

type UserViewsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<UserViewsAPI>()

    let resolveSource (source : UserViewSource) (recompileQuery : bool) : Task<Result<PrefetchedUserView, UserViewErrorInfo>> =
        task {
            match source with
            | UVAnonymous query ->
                try
                    let! anon =
                        if not recompileQuery then
                            ctx.GetAnonymousView query
                        else
                            ctx.ResolveAnonymousView None query
                    return Ok anon
                with
                | :? UserViewResolveException as err ->
                    logger.LogError(err, "Failed to resolve anonymous user view: {uv}", query)
                    return Error <| UVEResolution (exceptionString err)
            | UVNamed ref ->
                let recompileView query = task {
                    try
                        let! anon = ctx.ResolveAnonymousView (Some ref.schema) query
                        return Ok anon
                    with
                    | :? UserViewResolveException as err ->
                        logger.LogError(err, "Failed to recompile user view {uv}", ref.ToString())
                        return Error <| UVEResolution (exceptionString err)
                }
                match ctx.UserViews.Find ref with
                | None -> return Error UVENotFound
                | Some (Error err) ->
                    if not recompileQuery then
                        logger.LogError(err.Error, "Requested user view {uv} is broken", ref.ToString())
                        return Error <| UVEResolution (exceptionString err.Error)
                    else
                        return! recompileView err.Source.Query
                | Some (Ok cached) ->
                    if not recompileQuery then
                        return Ok cached
                    else
                        let! query =
                            ctx.Transaction.System.UserViews
                                .Where(fun uv -> uv.Schema.Name = ref.schema.ToString() && uv.Name = ref.name.ToString())
                                .Select(fun uv -> uv.Query)
                                .FirstAsync()
                        return! recompileView query
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
        compiled.query.Arguments.Types |> Map.toSeq |> Seq.traverseResult findArgument |> Result.map (Seq.catMaybes >> Map.ofSeq)


    member this.GetUserViewInfo (source : UserViewSource) (recompileQuery : bool) : Task<Result<UserViewInfoResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source recompileQuery with
            | Error e -> return Error e
            | Ok uv ->
                try
                    match rctx.User.Type with
                    | RTRoot -> ()
                    | RTRole role -> checkRoleViewExpr ctx.Layout role uv.UserView.Compiled
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
                        event.Details <- sprintf "Failed to get info for %O: %s" source (exceptionString err)
                    )
                    return Error UVEAccessDenied
        }

    member this.GetUserView (source : UserViewSource) (rawArgs : RawArguments) (recompileQuery : bool) : Task<Result<UserViewEntriesResult, UserViewErrorInfo>> =
        task {
            match! resolveSource source recompileQuery with
            | Error e -> return Error e
            | Ok uv ->
                try
                    let restricted =
                        match rctx.User.Type with
                        | RTRoot -> uv.UserView.Compiled
                        | RTRole role ->
                            applyRoleViewExpr ctx.Layout role uv.UserView.Compiled
                    let getResult info (res : ExecutedViewExpr) = task {
                        return (uv, { res with Rows = Array.ofSeq res.Rows })
                    }
                    match convertViewArguments rawArgs restricted with
                    | Error msg ->
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getUserView"
                            event.Error <- "arguments"
                            event.Details <- sprintf "Invalid arguments for %O: %s" source msg
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
                        event.Details <- sprintf "Failed to execute %O: %s" source (exceptionString err)
                    )
                    return Error UVEAccessDenied
        }

    interface IUserViewsAPI with
        member this.GetUserViewInfo source recompileQuery = this.GetUserViewInfo source recompileQuery
        member this.GetUserView source rawArguments recompileQuery = this.GetUserView source rawArguments recompileQuery
