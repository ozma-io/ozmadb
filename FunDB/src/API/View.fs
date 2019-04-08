module FunWithFlags.FunDB.API.View

open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.Operations.Context
open FunWithFlags.FunDB.API.Utils

[<NoComparison>]
type ViewEntriesGetResponse =
    { info : UserViewInfo
      result : ExecutedViewExpr
    }

[<NoComparison>]
type ViewInfoGetResponse =
    { info : UserViewInfo
      pureAttributes : ExecutedAttributeMap
      pureColumnAttributes : ExecutedAttributeMap array
    }

let viewsApi (settings : APISettings) : HttpHandler =
    let guarded = withContext settings

    let returnError = function
        | UVEArguments msg -> sprintf "Invalid arguments: %s" msg |> text |> RequestErrors.badRequest
        | UVEAccessDenied -> text "Forbidden" |> RequestErrors.forbidden
        | UVENotFound -> text "Not found" |> RequestErrors.notFound
        | UVEResolve msg -> text msg |> RequestErrors.badRequest
        | UVEExecute msg -> text msg |> RequestErrors.badRequest

    let selectFromView (viewRef : UserViewSource) (rctx : RequestContext) =
        queryArgs <| fun rawArgs next ctx -> task {
            match! rctx.GetUserView viewRef rawArgs with
            | Ok (cached, res) ->
                let ret =
                    { info = cached.info
                      result = res
                    }
                return! Successful.ok (json ret) next ctx
            | Result.Error err -> return! returnError err next ctx
        }

    let infoView (viewRef : UserViewSource) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match! rctx.GetUserViewInfo viewRef with
            | Ok cached ->
                let res =
                    { info = cached.info
                      pureAttributes = cached.pureAttributes.attributes
                      pureColumnAttributes = cached.pureAttributes.columnAttributes
                    }
                return! Successful.ok (json res) next ctx
            | Result.Error err -> return! returnError err next ctx
    }

    let viewApi (viewRef : UserViewSource) =
        choose
            [ route "/entries" >=> GET >=> guarded (selectFromView viewRef)
              route "/info" >=> GET >=> guarded (infoView viewRef)
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