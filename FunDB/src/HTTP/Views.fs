module FunWithFlags.FunDB.HTTP.Views

open Newtonsoft.Json.Linq
open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.HTTP.Utils
module SQL = FunWithFlags.FunDB.SQL.Query

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
      PretendRole : ResolvedEntityRef option
      PretendUser : UserName option
    }

type UserViewExplainRequest =
    { Args: RawArguments option
      ForceRecompile : bool
      Offset : int option
      Limit : int option
      Where : SourceChunkWhere option
      PretendRole : ResolvedEntityRef option
      PretendUser : UserName option
      Analyze : bool option
      Verbose : bool option
      Costs : bool option
    }

type AnonymousUserViewRequest =
    { Query : string
    }

type UserViewInfoRequest =
    { ForceRecompile : bool
    }

let viewsApi : HttpHandler =
    let returnView (viewRef : UserViewSource) (api : IFunDBAPI) (rawArgs : RawArguments) (chunk : SourceQueryChunk) (flags : UserViewFlags) next ctx =
        task {
            match! api.UserViews.GetUserView viewRef rawArgs chunk flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

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
                return! returnView viewRef api rawArgs chunk flags next ctx
            }

    let doPostSelectFromView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewRequest) (next : HttpFunc) (ctx : HttpContext) =
        let flags =
            { ForceRecompile = flagIfDebug req.ForceRecompile
            } : UserViewFlags
        let chunk =
            { Offset = req.Offset
              Limit = req.Limit
              Where = req.Where
            } : SourceQueryChunk
        setPretends api req.PretendUser req.PretendRole (fun () -> returnView viewRef api req.Args chunk flags next ctx)

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

    let returnExplainView (viewRef : UserViewSource) (api : IFunDBAPI) (maybeArgs : RawArguments option) (chunk : SourceQueryChunk) (flags : UserViewFlags) (explainOpts : SQL.ExplainOptions) next ctx =
        task {
            match! api.UserViews.GetUserViewExplain viewRef maybeArgs chunk flags explainOpts with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let getExplainView (viewRef : UserViewSource) (api : IFunDBAPI) =
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
                let explainOpts =
                    { Analyze = tryBoolRequestArg "__analyze" ctx
                      Costs = tryBoolRequestArg "__costs" ctx
                      Verbose = tryBoolRequestArg "__verbose" ctx
                    } : SQL.ExplainOptions
                let maybeArgs =
                    if Map.isEmpty rawArgs then None else Some rawArgs
                return! returnExplainView viewRef api maybeArgs chunk flags explainOpts next ctx
            }

    let doPostExplainView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewExplainRequest) (next : HttpFunc) (ctx : HttpContext) =
        let flags =
            { ForceRecompile = flagIfDebug req.ForceRecompile
            } : UserViewFlags
        let chunk =
            { Offset = req.Offset
              Limit = req.Limit
              Where = req.Where
            } : SourceQueryChunk
        let explainOpts =
            { Analyze = req.Analyze
              Costs = req.Costs
              Verbose = req.Verbose
            } : SQL.ExplainOptions
        setPretends api req.PretendUser req.PretendRole (fun () -> returnExplainView viewRef api req.Args chunk flags explainOpts next ctx)

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

    let namedView (schemaName, uvName) = viewApi (UVNamed { Schema = FunQLName schemaName; Name = FunQLName uvName }) None

    choose
        [ subRoute "/views/anonymous" anonymousView
          subRoutef "/views/by_name/%s/%s" namedView
        ]