module FunWithFlags.FunDB.HTTP.Views

open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.HTTP.Utils

let private uvError e =
    let handler =
        match e with
        | UVEArguments _ -> RequestErrors.badRequest
        | UVEAccessDenied -> RequestErrors.forbidden
        | UVENotFound -> RequestErrors.notFound
        | UVECompilation _ -> RequestErrors.badRequest
        | UVEExecution _ -> RequestErrors.unprocessableEntity
    handler (json e)

let private flagIfDebug (flag : bool) : bool =
#if DEBUG
    flag
#else
    false
#endif

type UserViewRequest =
    { Args: RawArguments
      ForceRecompile : bool
      Offset : int option
      Limit : int option
      Where : SourceChunkWhere option
    }

type UserViewExplainRequest =
    { ForceRecompile : bool
      Offset : int option
      Limit : int option
      Where : SourceChunkWhere option
    }

type AnonymousUserViewRequest =
    { Query : string
    }

type UserViewInfoRequest =
    { ForceRecompile : bool
    }

let viewsApi : HttpHandler =
    let getSelectFromView (viewRef : UserViewSource) (api : IFunDBAPI) =
        queryArgs <| fun rawArgs next ctx ->
            task {
                let flags =
                    { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                    } : UserViewFlags
                let chunk =
                    { Offset = intRequestArg "__offset" ctx
                      Limit = intRequestArg "__limit" ctx
                      Where = None
                    } : SourceQueryChunk
                match! api.UserViews.GetUserView viewRef rawArgs chunk flags with
                | Ok res -> return! Successful.ok (json res) next ctx
                | Error err -> return! uvError err next ctx
            }

    let doPostSelectFromView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewRequest) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let flags =
                { ForceRecompile = flagIfDebug req.ForceRecompile
                } : UserViewFlags
            let chunk =
                { Offset = req.Offset
                  Limit = req.Limit
                  Where = req.Where
                } : SourceQueryChunk
            match! api.UserViews.GetUserView viewRef req.Args chunk flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let postSelectFromView (viewRef : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostSelectFromView viewRef api)
        | Some req -> bindJsonToken req (doPostSelectFromView viewRef api)

    let getInfoView (viewRef : UserViewSource) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let flags =
                { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                } : UserViewFlags
            match! api.UserViews.GetUserViewInfo viewRef flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let doPostInfoView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewInfoRequest) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let flags =
                { ForceRecompile = flagIfDebug req.ForceRecompile
                } : UserViewFlags
            match! api.UserViews.GetUserViewInfo viewRef flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let postInfoView (viewRef : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostInfoView viewRef api)
        | Some req -> bindJsonToken req (doPostSelectFromView viewRef api)

    let getExplainView (viewRef : UserViewSource) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let flags =
                { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                } : UserViewFlags
            let chunk =
                { Offset = intRequestArg "__offset" ctx
                  Limit = intRequestArg "__limit" ctx
                  Where = None
                } : SourceQueryChunk
            match! api.UserViews.GetUserViewExplain viewRef chunk flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let doPostExplainView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewExplainRequest) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let flags =
                { ForceRecompile = flagIfDebug req.ForceRecompile
                } : UserViewFlags
            let chunk =
                { Offset = req.Offset
                  Limit = req.Limit
                  Where = req.Where
                } : SourceQueryChunk
            match! api.UserViews.GetUserViewExplain viewRef chunk flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let postExplainView (viewRef : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostExplainView viewRef api)
        | Some req -> bindJsonToken req (doPostExplainView viewRef api)

    let viewApi (viewRef : UserViewSource) (maybeReq : JToken option) =
        choose
            [ route "/entries" >=> GET >=> withContext (getSelectFromView viewRef)
              route "/entries" >=> POST >=> withContext (postSelectFromView viewRef maybeReq)
              route "/info" >=> GET >=> withContext (getInfoView viewRef)
              route "/info" >=> POST >=> withContext (postInfoView viewRef maybeReq)
              route "/explain" >=> GET >=> withContext (getExplainView viewRef)
              route "/explain" >=> POST >=> withContext (postExplainView viewRef maybeReq)
            ]

    let postAnonymousView (rawData : JToken) =
        bindJsonToken rawData <| fun anon -> viewApi (UVAnonymous anon.Query) (Some rawData)

    let anonymousView (next : HttpFunc) (ctx : HttpContext) =
        match ctx.Request.Method with
        | "GET" ->
            match ctx.GetQueryStringValue "__query" with
            | Ok rawView -> viewApi (UVAnonymous rawView) None next ctx
            | Error _ -> RequestErrors.BAD_REQUEST "Query not specified" next ctx
        | "POST" -> safeBindJson postAnonymousView next ctx
        | _ -> RequestErrors.METHOD_NOT_ALLOWED "Method not allowed" next ctx

    let namedView (schemaName, uvName) = viewApi (UVNamed { schema = FunQLName schemaName; name = FunQLName uvName }) None

    choose
        [ subRoute "/views/anonymous" anonymousView
          subRoutef "/views/by_name/%s/%s" namedView
        ]