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

let private domainError e =
    let handler =
        match e with
        | DEArguments _ -> RequestErrors.badRequest
        | DENotFound -> RequestErrors.notFound
        | DEAccessDenied -> RequestErrors.forbidden
        | DEExecution _ -> RequestErrors.unprocessableEntity
    handler (json e)

type DomainRequest =
    { Chunk : SourceQueryChunk option
      PretendUser : UserName option
      PretendRole : ResolvedEntityRef option
      RowId : int option
      ForceRecompile : bool
    }

type DomainExplainRequest =
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
    let returnValues (api : IFunDBAPI) (ref : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) (flags : DomainFlags) next ctx =
        task {
            match! api.Domains.GetDomainValues ref rowId chunk flags with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! domainError err next ctx
        }

    let getDomainValues (ref : ResolvedFieldRef) (api : IFunDBAPI) next ctx =
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
            return! returnValues api ref rowId chunk flags next ctx
        }

    let postGetDomainValues (ref : ResolvedFieldRef) (api : IFunDBAPI) =
        safeBindJson <| fun (req : DomainRequest) (next : HttpFunc) (ctx : HttpContext) ->
            let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
            let flags =
                { ForceRecompile = flagIfDebug <| req.ForceRecompile
                } : DomainFlags
            setPretends api req.PretendUser req.PretendRole (fun () -> returnValues api ref req.RowId chunk flags next ctx)

    let returnExplain (api : IFunDBAPI) (ref : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) (flags : DomainFlags) (explainOpts : SQL.ExplainOptions) next ctx =
        task {
            match! api.Domains.GetDomainExplain ref rowId chunk flags explainOpts with
            | Ok res -> return! Successful.ok (json res) next ctx
            | Error err -> return! domainError err next ctx
        }

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
        safeBindJson <| fun (req : DomainExplainRequest) (next : HttpFunc) (ctx : HttpContext) ->
            let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
            let flags =
                { ForceRecompile = flagIfDebug <| req.ForceRecompile
                } : DomainFlags
            let explainOpts =
                { Analyze = req.Analyze
                  Costs = req.Costs
                  Verbose = req.Verbose
                } : SQL.ExplainOptions
            setPretends api req.PretendUser req.PretendRole (fun () -> returnExplain api ref req.RowId chunk flags explainOpts next ctx)

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
