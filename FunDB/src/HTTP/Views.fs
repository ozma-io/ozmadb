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

type UserViewHTTPRequest =
    { Args: RawArguments
      ForceRecompile : bool
      NoAttributes : bool
      NoTracking : bool
      NoPuns : bool
      Chunk : SourceQueryChunk option
      PretendRole : ResolvedEntityRef option
      PretendUser : UserName option
    }

type UserViewExplainHTTPRequest =
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

type AnonymousUserViewHTTPRequest =
    { Query : string
    }

type UserViewInfoHTTPRequest =
    { ForceRecompile : bool
    }

let viewsApi : Endpoint list =
    let returnView (source : UserViewSource) (api : IFunDBAPI) (rawArgs : RawArguments) (chunk : SourceQueryChunk) (flags : UserViewFlags) =
        let req =
            { Source = source
              Args = rawArgs
              Chunk = Some chunk
              Flags = Some flags
            }
        handleRequest (api.UserViews.GetUserView req)

    let getSelectFromView (source : UserViewSource) (api : IFunDBAPI) =
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
                return! returnView source api rawArgs chunk flags next ctx
            }

    let doPostSelectFromView (source : UserViewSource) (api : IFunDBAPI) (req : UserViewHTTPRequest) =
        let flags =
            { ForceRecompile = flagIfDebug req.ForceRecompile
              NoAttributes = req.NoAttributes
              NoTracking = req.NoTracking
              NoPuns = req.NoPuns
            } : UserViewFlags
        let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
        setPretendUser api req.PretendUser
            >=> setPretendRole api req.PretendRole
            >=> returnView source api req.Args chunk flags

    let postSelectFromView (source : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostSelectFromView source api)
        | Some req -> bindJsonToken req (doPostSelectFromView source api)

    let getInfoView (source : UserViewSource) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let flags =
            { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
              NoAttributes = false
              NoTracking = false
              NoPuns = false
            } : UserViewFlags
        let req =
            { Source = source
              Flags = Some flags
            }
        handleRequest (api.UserViews.GetUserViewInfo req) next ctx

    let doPostInfoView (source : UserViewSource) (api : IFunDBAPI) (req : UserViewInfoHTTPRequest) =
            let flags =
                { ForceRecompile = flagIfDebug req.ForceRecompile
                  NoAttributes = false
                  NoTracking = false
                  NoPuns = false
                } : UserViewFlags
            let req =
                { Source = source
                  Flags = Some flags
                }
            handleRequest (api.UserViews.GetUserViewInfo req)

    let postInfoView (source : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostInfoView source api)
        | Some req -> bindJsonToken req (doPostSelectFromView source api)

    let returnExplainView (source : UserViewSource) (api : IFunDBAPI) (maybeArgs : RawArguments option) (chunk : SourceQueryChunk) (flags : UserViewFlags) (explainOpts : SQL.ExplainOptions) =
        let req =
            { Source = source
              Args = maybeArgs
              Flags = Some flags
              Chunk = Some chunk
              ExplainFlags = Some explainOpts
            } : UserViewExplainRequest
        handleRequest (api.UserViews.GetUserViewExplain req)

    let getExplainView (source : UserViewSource) (api : IFunDBAPI) =
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
                return! returnExplainView source api maybeArgs chunk flags explainOpts next ctx
            }

    let doPostExplainView (source : UserViewSource) (api : IFunDBAPI) (req : UserViewExplainHTTPRequest) =
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
        setPretendUser api req.PretendUser
            >=> setPretendRole api req.PretendRole
            >=> returnExplainView source api req.Args chunk flags explainOpts

    let postExplainView (source : UserViewSource) (maybeReq : JToken option) (api : IFunDBAPI) =
        match maybeReq with
        | None -> safeBindJson (doPostExplainView source api)
        | Some req -> bindJsonToken req (doPostExplainView source api)

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