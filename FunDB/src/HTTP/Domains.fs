module FunWithFlags.FunDB.HTTP.Domains

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe
open Giraffe.EndpointRouting

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.API.Types
module SQL = FunWithFlags.FunDB.SQL.Query

type DomainHTTPRequest =
    { Chunk : SourceQueryChunk option
      PretendUser : UserName option
      PretendRole : ResolvedEntityRef option
      RowId : int option
      ForceRecompile : bool
    }

type DomainExplainHTTPRequest =
    { Chunk : SourceQueryChunk option
      PretendUser : UserName option
      PretendRole : ResolvedEntityRef option
      RowId : int option
      ForceRecompile : bool
      Analyze : bool option
      Verbose : bool option
      Costs : bool option
    }

let domainsApi : Endpoint list =
    let returnValues (api : IFunDBAPI) (ref : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) (flags : DomainFlags) =
        let req = { Field = ref; Id = Option.map RRKPrimary rowId; Chunk = Some chunk; Flags = Some flags } : GetDomainValuesRequest
        handleRequest (api.Domains.GetDomainValues req)

    let getDomainValues (ref : ResolvedFieldRef) (api : IFunDBAPI) next ctx =
        let rowId = intRequestArg "id" ctx
        let chunk =
            { Offset = intRequestArg "__offset" ctx
              Limit = intRequestArg "__limit" ctx
              Where = None
            } : SourceQueryChunk
        let flags =
            { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
            } : DomainFlags
        returnValues api ref rowId chunk flags next ctx

    let postGetDomainValues (ref : ResolvedFieldRef) (api : IFunDBAPI) =
        safeBindJson <| fun (req : DomainHTTPRequest) (next : HttpFunc) (ctx : HttpContext) ->
            let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
            let flags =
                { ForceRecompile = flagIfDebug <| req.ForceRecompile
                } : DomainFlags
            (setPretendRole api req.PretendRole >=> setPretendUser api req.PretendUser >=> returnValues api ref req.RowId chunk flags) next ctx

    let returnExplain (api : IFunDBAPI) (ref : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) (flags : DomainFlags) (explainOpts : SQL.ExplainOptions) =
        let req = { Field = ref; Id = Option.map RRKPrimary rowId; Chunk = Some chunk; Flags = Some flags; ExplainFlags = Some explainOpts } : GetDomainExplainRequest
        handleRequest (api.Domains.GetDomainExplain req)

    let getDomainExplain (ref : ResolvedFieldRef) (api : IFunDBAPI) =
        queryArgs <| fun rawArgs next ctx ->
            task {
                let rowId = intRequestArg "id" ctx
                let chunk =
                    { Offset = intRequestArg "__offset" ctx
                      Limit = intRequestArg "__limit" ctx
                      Where = None
                    } : SourceQueryChunk
                let flags =
                    { ForceRecompile = flagIfDebug <| boolRequestArg "__force_recompile" ctx
                    } : DomainFlags
                let explainOpts =
                    { Analyze = tryBoolRequestArg "__analyze" ctx
                      Costs = tryBoolRequestArg "__costs" ctx
                      Verbose = tryBoolRequestArg "__verbose" ctx
                    } : SQL.ExplainOptions
                return! returnExplain api ref rowId chunk flags explainOpts next ctx
            }

    let postGetDomainExplain (ref : ResolvedFieldRef) (api : IFunDBAPI) =
        safeBindJson <| fun (req : DomainExplainHTTPRequest) (next : HttpFunc) (ctx : HttpContext) ->
            let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
            let flags =
                { ForceRecompile = flagIfDebug <| req.ForceRecompile
                } : DomainFlags
            let explainOpts =
                { Analyze = req.Analyze
                  Costs = req.Costs
                  Verbose = req.Verbose
                } : SQL.ExplainOptions
            let handler =
                setPretendUser api req.PretendUser >=>
                    setPretendRole api req.PretendRole >=>
                    returnExplain api ref req.RowId chunk flags explainOpts
            handler next ctx

    let withDomainRead next (schema, entity, name) =
        let ref = { Entity = { Schema = FunQLName schema; Name = FunQLName entity }; Name = FunQLName name }
        withContextRead (next ref)

    let domainApi =
        [ GET
            [ routef "/%s/%s/%s/entries" <| withDomainRead getDomainValues
              routef "/%s/%s/%s/explain" <| withDomainRead getDomainExplain
            ]
          POST
            [ routef "/%s/%s/%s/entries" <| withDomainRead postGetDomainValues
              routef "/%s/%s/%s/explain" <| withDomainRead postGetDomainExplain
            ]
        ]

    [ subRoute "/domains" domainApi
    ]
