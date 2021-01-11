module FunWithFlags.FunDB.HTTP.Views

open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.HTTP.Utils

let private uvError e =
    let handler =
        match e with
        | UVEArguments _ -> RequestErrors.badRequest
        | UVEAccessDenied -> RequestErrors.forbidden
        | UVENotFound -> RequestErrors.notFound
        | UVEResolution _ -> RequestErrors.badRequest
        | UVEExecution _ -> RequestErrors.unprocessableEntity
    handler (json e)

let private getFlags (ctx : HttpContext) : UserViewFlags =
    { ForceRecompile =
#if DEBUG
        ctx.Request.Query.ContainsKey("__force_recompile")
#else
        false
#endif
    }

let viewsApi : HttpHandler =
    let selectFromView (viewRef : UserViewSource) (api : IFunDBAPI) =
        queryArgs <| fun rawArgs next ctx ->
            task {
                let flags = getFlags ctx
                match! api.UserViews.GetUserView viewRef rawArgs flags with
                | Ok res -> return! Successful.ok (json res) next ctx
                | Result.Error err -> return! uvError err next ctx
            }

    let infoView (viewRef : UserViewSource) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let flags = getFlags ctx
            match! api.UserViews.GetUserViewInfo viewRef flags with
                | Ok res -> return! Successful.ok (json res) next ctx
                | Result.Error err -> return! uvError err next ctx
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