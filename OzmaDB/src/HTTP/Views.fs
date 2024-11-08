module OzmaDB.HTTP.Views

open System
open Newtonsoft.Json.Linq
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http
open Giraffe
open Giraffe.EndpointRouting

open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Chunk
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Permissions.Types
open OzmaDB.API.Types
open OzmaDB.HTTP.Utils

module SQL = OzmaDB.SQL.Query

// TODO: make it private after we fix JSON serialization for the private F# types.
type UserViewHTTPRequest =
    { Args: RawArguments
      ForceRecompile: bool
      NoAttributes: bool
      NoTracking: bool
      NoPuns: bool
      Chunk: SourceQueryChunk option
      PretendRole: ResolvedEntityRef option
      PretendUser: UserName option }

// TODO: make it private after we fix JSON serialization for the private F# types.
type UserViewExplainHTTPRequest =
    { Args: RawArguments option
      ForceRecompile: bool
      NoAttributes: bool
      NoTracking: bool
      NoPuns: bool
      Chunk: SourceQueryChunk option
      PretendRole: ResolvedEntityRef option
      PretendUser: UserName option
      Analyze: bool option
      Verbose: bool option
      Costs: bool option }

// TODO: make it private after we fix JSON serialization for the private F# types.
type AnonymousUserViewHTTPRequest = { Query: string }

// TODO: make it private after we fix JSON serialization for the private F# types.
type UserViewInfoHTTPRequest = { ForceRecompile: bool }

let viewsApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let returnView (api: IOzmaDBAPI) (req: UserViewRequest) =
        task {
            let! ret = api.UserViews.GetUserView req
            return jobReply ret
        }

    let getSelectFromView (source: UserViewSource) =
        queryArgs
        <| fun rawArgs next ctx ->
            task {
                let flags =
                    { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                      NoAttributes = boolRequestArg "__no_attributes" ctx
                      NoTracking = boolRequestArg "__no_tracking" ctx
                      NoPuns = boolRequestArg "__no_puns" ctx }
                    : UserViewFlags

                let chunk =
                    { emptySourceQueryChunk with
                        Offset = intRequestArg "__offset" ctx
                        Limit = intRequestArg "__limit" ctx }
                    : SourceQueryChunk

                let req =
                    { Source = source
                      Args = rawArgs
                      Chunk = Some chunk
                      Flags = Some flags }
                    : UserViewRequest

                let job api = returnView api req
                return! utils.PerformReadJob job next ctx
            }

    let doPostSelectFromView (source: UserViewSource) (httpReq: UserViewHTTPRequest) =
        let flags =
            { ForceRecompile = flagIfDebug httpReq.ForceRecompile
              NoAttributes = httpReq.NoAttributes
              NoTracking = httpReq.NoTracking
              NoPuns = httpReq.NoPuns }
            : UserViewFlags

        let req =
            { Source = source
              Args = httpReq.Args
              Chunk = httpReq.Chunk
              Flags = Some flags }

        let job api =
            setPretendUser api httpReq.PretendUser (fun () ->
                setPretendRole api httpReq.PretendRole (fun () -> returnView api req))

        utils.PerformReadJob job

    let postSelectFromView (source: UserViewSource) (maybeReq: JToken option) =
        match maybeReq with
        | None -> safeBindJson (doPostSelectFromView source)
        | Some req -> bindJsonToken req (doPostSelectFromView source)

    let returnViewInfo (api: IOzmaDBAPI) (req: UserViewInfoRequest) =
        task {
            let! ret = api.UserViews.GetUserViewInfo req
            return jobReply ret
        }

    let getViewInfo (source: UserViewSource) (next: HttpFunc) (ctx: HttpContext) : HttpFuncResult =
        let flags =
            { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
              NoAttributes = false
              NoTracking = false
              NoPuns = false }
            : UserViewFlags

        let req = { Source = source; Flags = Some flags }
        let job (api: IOzmaDBAPI) = returnViewInfo api req
        utils.PerformReadJob job next ctx

    let doPostViewInfo (source: UserViewSource) (req: UserViewInfoHTTPRequest) =
        let flags =
            { ForceRecompile = flagIfDebug req.ForceRecompile
              NoAttributes = false
              NoTracking = false
              NoPuns = false }
            : UserViewFlags

        let req = { Source = source; Flags = Some flags }
        let job (api: IOzmaDBAPI) = returnViewInfo api req
        utils.PerformReadJob job

    let postViewInfo (source: UserViewSource) (maybeReq: JToken option) =
        match maybeReq with
        | None -> safeBindJson (doPostViewInfo source)
        | Some req -> bindJsonToken req (doPostSelectFromView source)

    let returnViewExplain (api: IOzmaDBAPI) (req: UserViewExplainRequest) =
        task {
            let! ret = api.UserViews.GetUserViewExplain req
            return jobReply ret
        }

    let getViewExplain (source: UserViewSource) =
        queryArgs
        <| fun rawArgs next ctx ->
            let flags =
                { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                  NoAttributes = boolRequestArg "__no_attributes" ctx
                  NoTracking = boolRequestArg "__no_tracking" ctx
                  NoPuns = boolRequestArg "__no_puns" ctx }
                : UserViewFlags

            let chunk =
                { emptySourceQueryChunk with
                    Offset = intRequestArg "__offset" ctx
                    Limit = intRequestArg "__limit" ctx }

            let explainOpts =
                { Analyze = tryBoolRequestArg "__analyze" ctx
                  Costs = tryBoolRequestArg "__costs" ctx
                  Verbose = tryBoolRequestArg "__verbose" ctx }
                : SQL.ExplainOptions

            let maybeArgs = if Map.isEmpty rawArgs then None else Some rawArgs

            let req =
                { Source = source
                  Args = maybeArgs
                  Flags = Some flags
                  Chunk = Some chunk
                  ExplainFlags = Some explainOpts }
                : UserViewExplainRequest

            let job (api: IOzmaDBAPI) = returnViewExplain api req
            utils.PerformReadJob job next ctx

    let doPostViewExplain (source: UserViewSource) (httpReq: UserViewExplainHTTPRequest) =
        let flags =
            { ForceRecompile = flagIfDebug httpReq.ForceRecompile
              NoAttributes = httpReq.NoAttributes
              NoTracking = httpReq.NoTracking
              NoPuns = httpReq.NoPuns }
            : UserViewFlags

        let chunk = Option.defaultValue emptySourceQueryChunk httpReq.Chunk

        let explainOpts =
            { Analyze = httpReq.Analyze
              Costs = httpReq.Costs
              Verbose = httpReq.Verbose }
            : SQL.ExplainOptions

        let req =
            { Source = source
              Args = httpReq.Args
              Flags = Some flags
              Chunk = Some chunk
              ExplainFlags = Some explainOpts }
            : UserViewExplainRequest

        let job (api: IOzmaDBAPI) =
            setPretendUser api httpReq.PretendUser (fun () ->
                setPretendRole api httpReq.PretendRole (fun () -> returnViewExplain api req))

        utils.PerformReadJob job

    let postViewExplain (source: UserViewSource) (maybeReq: JToken option) =
        match maybeReq with
        | None -> safeBindJson (doPostViewExplain source)
        | Some req -> bindJsonToken req (doPostViewExplain source)

    let withPostAnonymousView nextHandler =
        safeBindJson
        <| fun rawData ->
            bindJsonToken rawData
            <| fun anon -> nextHandler (UVAnonymous anon.Query) (Some rawData)

    let withGetAnonymousView nextHandler (next: HttpFunc) (ctx: HttpContext) =
        match ctx.GetQueryStringValue "__query" with
        | Ok rawView -> nextHandler (UVAnonymous rawView) next ctx
        | Error _ -> RequestErrors.BAD_REQUEST "Query not specified" next ctx

    let withNamedView next (schemaName, uvName) =
        let ref =
            UVNamed
                { Schema = OzmaQLName schemaName
                  Name = OzmaQLName uvName }

        next ref

    let withPostNamedView nextHandler =
        withNamedView (fun ref -> nextHandler ref None)

    let viewsApi =
        [ GET
              [ route "/anonymous/entries" <| withGetAnonymousView getSelectFromView
                route "/anonymous/info" <| withGetAnonymousView getViewInfo
                route "/anonymous/explain" <| withGetAnonymousView getViewExplain ]
          POST
              [ route "/anonymous/entries" <| withPostAnonymousView postSelectFromView
                route "/anonymous/info" <| withPostAnonymousView postViewInfo
                route "/anonymous/explain" <| withPostAnonymousView postViewExplain ]

          GET
              [ routef "/by_name/%s/%s/entries" <| withNamedView getSelectFromView
                routef "/by_name/%s/%s/info" <| withNamedView getViewInfo
                routef "/by_name/%s/%s/explain" <| withNamedView getViewExplain ]
          POST
              [ routef "/by_name/%s/%s/entries" <| withPostNamedView postSelectFromView
                routef "/by_name/%s/%s/info" <| withPostNamedView postViewInfo
                routef "/by_name/%s/%s/explain" <| withPostNamedView postViewExplain ] ]

    [ subRoute "/views" viewsApi ]
