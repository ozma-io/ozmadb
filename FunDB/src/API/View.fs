module FunWithFlags.FunDB.API.View

open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Operations.Context
open FunWithFlags.FunDB.API.Utils

let private error e =
    let handler =
        match e with
        | UVEArguments _ -> RequestErrors.badRequest
        | UVEAccessDenied -> RequestErrors.forbidden
        | UVENotFound -> RequestErrors.notFound
        | UVEResolution _ -> RequestErrors.badRequest
        | UVEExecution _ -> RequestErrors.unprocessableEntity
    handler (json e)

let viewsApi : HttpHandler =
    let getRecompile (ctx : HttpContext) =
#if DEBUG
            ctx.GetQueryStringValue "__recompile" |> Result.getOption |> Option.bind tryBool |> Option.defaultValue false
#else
            false
#endif

    let selectFromView (viewRef : UserViewSource) (rctx : RequestContext) =
        queryArgs <| fun rawArgs next ctx -> task {
            match! rctx.GetUserView viewRef rawArgs (getRecompile ctx) with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Result.Error err -> return! error err next ctx
        }

    let infoView (viewRef : UserViewSource) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match! rctx.GetUserViewInfo viewRef (getRecompile ctx) with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Result.Error err -> return! error err next ctx
    }

    let viewApi (viewRef : UserViewSource) =
        choose
            [ route "/entries" >=> GET >=> withContext (selectFromView viewRef)
              route "/info" >=> GET >=> withContext (infoView viewRef)
            ]

    let anonymousView (next : HttpFunc) (ctx : HttpContext) =
        match ctx.GetQueryStringValue "__query" with
        | Ok rawView -> viewApi (UVAnonymous rawView) next ctx
        | Error _ -> RequestErrors.BAD_REQUEST "Query not specified" next ctx
    let namedView (schemaName, uvName) = viewApi <| UVNamed { schema = FunQLName schemaName; name = FunQLName uvName }

    choose
        [ subRoute "/views/anonymous" anonymousView
          subRoutef "/views/by_name/%s/%s" namedView
        ]