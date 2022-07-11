module FunWithFlags.FunDB.HTTP.Views

open Newtonsoft.Json.Linq
open Microsoft.AspNetCore.Http
open Giraffe
open FSharp.Control.Tasks.Affine
open Giraffe.EndpointRouting

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Arguments
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

type UserViewRequest =
    { Args: RawArguments
      ForceRecompile : bool
      NoAttributes : bool
      NoTracking : bool
      NoPuns : bool
      Chunk : SourceQueryChunk option
      PretendRole : ResolvedEntityRef option
      PretendUser : UserName option
    }

type UserViewExplainRequest =
    { Args: RawArguments option
      ForceRecompile : bool
      NoAttributes : bool
      NoTracking : bool
      NoPuns : bool
      Chunk : SourceQueryChunk option
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

let viewsApi : Endpoint list =
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
                      NoAttributes = boolRequestArg "__no_attributes" ctx
                      NoTracking = boolRequestArg "__no_tracking" ctx
                      NoPuns = boolRequestArg "__no_puns" ctx
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
              NoAttributes = req.NoAttributes
              NoTracking = req.NoTracking
              NoPuns = req.NoPuns
            } : UserViewFlags
        let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
        setPretends api req.PretendUser req.PretendRole (fun () -> returnView viewRef api req.Args chunk flags next ctx)

    let postSelectFromView (viewRef : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostSelectFromView viewRef api)
        | Some req -> bindJsonToken req (doPostSelectFromView viewRef api)

    let getInfoView (viewRef : UserViewSource) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let flags =
                { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                  NoAttributes = false
                  NoTracking = false
                  NoPuns = false
                } : UserViewFlags
            match! api.UserViews.GetUserViewInfo viewRef flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! uvError err next ctx
        }

    let doPostInfoView (viewRef : UserViewSource) (api : IFunDBAPI) (req : UserViewInfoRequest) (next : HttpFunc) (ctx : HttpContext) =
        task {
            let flags =
                { ForceRecompile = flagIfDebug req.ForceRecompile
                  NoAttributes = false
                  NoTracking = false
                  NoPuns = false
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
                      NoAttributes = boolRequestArg "__no_attributes" ctx
                      NoTracking = boolRequestArg "__no_tracking" ctx
                      NoPuns = boolRequestArg "__no_puns" ctx
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
              NoAttributes = req.NoAttributes
              NoTracking = req.NoTracking
              NoPuns = req.NoPuns
            } : UserViewFlags
        let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
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

    let withPostAnonymousView nextHandler =
        safeBindJson <| fun rawData ->
            bindJsonToken rawData <| fun anon ->
                withContextRead (nextHandler (UVAnonymous anon.Query) (Some rawData))

    let withGetAnonymousView nextHandler (next : HttpFunc) (ctx : HttpContext) =
        match ctx.GetQueryStringValue "__query" with
        | Ok rawView -> withContextRead (nextHandler (UVAnonymous rawView)) next ctx
        | Error _ -> RequestErrors.BAD_REQUEST "Query not specified" next ctx

    let withNamedView next (schemaName, uvName) =
        let ref = UVNamed { Schema = FunQLName schemaName; Name = FunQLName uvName }
        withContextRead (next ref)

    let withPostNamedView next =
        withNamedView (fun ref -> next ref None)

    let viewsApi =
        [ GET [ route "/anonymous/entries" <| withGetAnonymousView getSelectFromView
                route "/anonymous/info" <| withGetAnonymousView getInfoView
                route "/anonymous/explain" <| withGetAnonymousView getExplainView
              ]
          POST [ route "/anonymous/entries" <| withPostAnonymousView postSelectFromView
                 route "/anonymous/info" <| withPostAnonymousView postInfoView
                 route "/anonymous/explain" <| withPostAnonymousView postExplainView
               ]

          GET [ routef "/by_name/%s/%s/entries" <| withNamedView getSelectFromView
                routef "/by_name/%s/%s/info" <| withNamedView getInfoView
                routef "/by_name/%s/%s/explain" <| withNamedView getExplainView
              ]
          POST [ routef "/by_name/%s/%s/entries" <| withPostNamedView postSelectFromView
                 routef "/by_name/%s/%s/info" <| withPostNamedView postInfoView
                 routef "/by_name/%s/%s/explain" <| withPostNamedView postExplainView
               ]
        ]

    [ subRoute "/views" viewsApi
    ]