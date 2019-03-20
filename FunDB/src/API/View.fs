module FunWithFlags.FunDB.API.View

open Microsoft.AspNetCore.Http
open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.ContextCache
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.FunQL.Query

[<NoComparison>]
type ViewEntriesGetResponse =
    { info : MergedViewInfo
      result : ExecutedViewExpr
    }

type ViewInfoGetResponse =
    { info : MergedViewInfo
      pureAttributes : ExecutedAttributeMap
      pureColumnAttributes : ExecutedAttributeMap array
    }

let viewsApi (cacheStore : ContextCacheStore) : HttpHandler =
    let guarded = withContext cacheStore

    let returnError = function
        | UVEArguments msg -> RequestErrors.BAD_REQUEST <| sprintf "Invalid arguments: %s" msg
        | UVEAccessDenied -> RequestErrors.FORBIDDEN "Forbidden"
        | UVENotFound -> RequestErrors.NOT_FOUND "Not found"
        | UVEParse msg -> RequestErrors.BAD_REQUEST <| sprintf "Parse error: %s" msg
        | UVEResolve msg -> RequestErrors.BAD_REQUEST <| sprintf "Resolution error: %s" msg
        | UVEExecute msg -> RequestErrors.BAD_REQUEST <| sprintf "Execution error: %s" msg
        | UVEFixup msg -> RequestErrors.BAD_REQUEST <| sprintf "Condition compilation error: %s" msg

    let selectFromView (viewRef : UserViewRef) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) =
        let rawArgs = queryArgs ctx
        match rctx.GetUserView viewRef rawArgs with
        | Ok (cached, res) -> json { info = cached.info
                                     result = res
                                   } next ctx
        | Result.Error err -> returnError err next ctx

    let infoView (viewRef : UserViewRef) (rctx : RequestContext) =
        match rctx.GetUserViewInfo viewRef with
            | Ok cached ->
                let res =
                    { info = cached.info
                      pureAttributes = cached.pureAttributes.attributes
                      pureColumnAttributes = cached.pureAttributes.columnAttributes
                    }
                json res
            | Result.Error err -> returnError err

    let viewApi (viewRef : UserViewRef) =
        choose
            [ route "/entries" >=> GET >=> guarded (selectFromView viewRef)
              route "/info" >=> GET >=> guarded (infoView viewRef)
            ]

    let anonymousView (next : HttpFunc) (ctx : HttpContext) =
        match ctx.GetQueryStringValue "__query" with
        | Ok rawView -> viewApi (UVAnonymous rawView) next ctx
        | Error _ -> RequestErrors.BAD_REQUEST "Query not specified" next ctx
    let namedView name = viewApi <| UVNamed name

    choose
        [ subRoute "/views/anonymous" anonymousView
          subRoutef "/views/by_name/%s" namedView
        ]