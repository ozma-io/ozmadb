module OzmaDB.HTTP.Domains

open System
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open OzmaDB.HTTP.Utils
open OzmaDB.Permissions.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Chunk
open OzmaDB.API.Types

module SQL = OzmaDB.SQL.Query

// TODO: make it private after we fix JSON serialization for the private F# types.
type DomainHTTPRequest =
    { Chunk: SourceQueryChunk option
      PretendUser: UserName option
      PretendRole: ResolvedEntityRef option
      RowId: int option
      ForceRecompile: bool }

// TODO: make it private after we fix JSON serialization for the private F# types.
type DomainExplainHTTPRequest =
    { Chunk: SourceQueryChunk option
      PretendUser: UserName option
      PretendRole: ResolvedEntityRef option
      RowId: int option
      ForceRecompile: bool
      Analyze: bool option
      Verbose: bool option
      Costs: bool option }

let domainsApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let returnValues (api: IOzmaDBAPI) (req: GetDomainValuesRequest) =
        task {
            let! ret = api.Domains.GetDomainValues req
            return jobReply ret
        }

    let getDomainValues (ref: ResolvedFieldRef) next ctx =
        let rowId = intRequestArg "id" ctx

        let chunk =
            { Offset = intRequestArg "__offset" ctx
              Limit = intRequestArg "__limit" ctx
              Where = None
              Search = None }
            : SourceQueryChunk

        let flags =
            { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx }: DomainFlags

        let req =
            { Field = ref
              Id = Option.map RRKPrimary rowId
              Chunk = Some chunk
              Flags = Some flags }
            : GetDomainValuesRequest

        let job api = returnValues api req
        utils.PerformReadJob job next ctx

    let postGetDomainValues (ref: ResolvedFieldRef) =
        safeBindJson
        <| fun (httpReq: DomainHTTPRequest) ->
            let flags = { ForceRecompile = flagIfDebug <| httpReq.ForceRecompile }: DomainFlags

            let req =
                { Field = ref
                  Id = Option.map RRKPrimary httpReq.RowId
                  Chunk = httpReq.Chunk
                  Flags = Some flags }
                : GetDomainValuesRequest

            let job api =
                setPretendRole api httpReq.PretendRole (fun () ->
                    setPretendUser api httpReq.PretendUser (fun () -> returnValues api req))

            utils.PerformReadJob job

    let returnExplain (api: IOzmaDBAPI) (req: GetDomainExplainRequest) =
        task {
            let! ret = api.Domains.GetDomainExplain req
            return jobReply ret
        }

    let getDomainExplain (ref: ResolvedFieldRef) next ctx =
        task {
            let rowId = intRequestArg "id" ctx

            let chunk =
                { emptySourceQueryChunk with
                    Offset = intRequestArg "__offset" ctx
                    Limit = intRequestArg "__limit" ctx }

            let flags =
                { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx }: DomainFlags

            let explainOpts =
                { Analyze = tryBoolRequestArg "__analyze" ctx
                  Costs = tryBoolRequestArg "__costs" ctx
                  Verbose = tryBoolRequestArg "__verbose" ctx }
                : SQL.ExplainOptions

            let req =
                { Field = ref
                  Id = Option.map RRKPrimary rowId
                  Chunk = Some chunk
                  Flags = Some flags
                  ExplainFlags = Some explainOpts }
                : GetDomainExplainRequest

            let job api = returnExplain api req
            return! utils.PerformReadJob job next ctx
        }

    let postGetDomainExplain (ref: ResolvedFieldRef) =
        safeBindJson
        <| fun (httpReq: DomainExplainHTTPRequest) ->
            let chunk = Option.defaultValue emptySourceQueryChunk httpReq.Chunk
            let flags = { ForceRecompile = flagIfDebug <| httpReq.ForceRecompile }: DomainFlags

            let explainOpts =
                { Analyze = httpReq.Analyze
                  Costs = httpReq.Costs
                  Verbose = httpReq.Verbose }
                : SQL.ExplainOptions

            let req =
                { Field = ref
                  Id = Option.map RRKPrimary httpReq.RowId
                  Chunk = Some chunk
                  Flags = Some flags
                  ExplainFlags = Some explainOpts }
                : GetDomainExplainRequest

            let job api =
                setPretendUser api httpReq.PretendUser (fun () ->
                    setPretendRole api httpReq.PretendRole (fun () -> returnExplain api req))

            utils.PerformReadJob job

    let getRef next (schema, entity, name) =
        let ref =
            { Entity =
                { Schema = OzmaQLName schema
                  Name = OzmaQLName entity }
              Name = OzmaQLName name }

        next ref

    let domainApi =
        [ GET
              [ routef "/%s/%s/%s/entries" <| getRef getDomainValues
                routef "/%s/%s/%s/explain" <| getRef getDomainExplain ]
          POST
              [ routef "/%s/%s/%s/entries" <| getRef postGetDomainValues
                routef "/%s/%s/%s/explain" <| getRef postGetDomainExplain ] ]

    [ subRoute "/domains" domainApi ]
