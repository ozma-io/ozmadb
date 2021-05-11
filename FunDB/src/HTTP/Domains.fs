module FunWithFlags.FunDB.HTTP.Domains

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.API.Types

let private domainError e =
    let handler =
        match e with
        | DEArguments _ -> RequestErrors.badRequest
        | DENotFound -> RequestErrors.notFound
        | DEAccessDenied -> RequestErrors.forbidden
        | DEExecution _ -> RequestErrors.unprocessableEntity
    handler (json e)

type DomainRequest =
    { Limit : int option
      Offset : int option
      Where : SourceChunkWhere option
      PretendRole : ResolvedEntityRef option
      RowId : int option
    }

let domainsApi : HttpHandler =
    let returnValues (api : IFunDBAPI) (ref : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) next ctx =
        task {
            match! api.Domains.GetDomainValues ref rowId chunk with
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
            return! returnValues api ref rowId chunk next ctx
        }

    let postGetDomainValues (ref : ResolvedFieldRef) (api : IFunDBAPI) =
        safeBindJson <| fun (req : DomainRequest) (next : HttpFunc) (ctx : HttpContext) ->
            task {
                let chunk =
                    { Limit = req.Limit
                      Offset = req.Offset
                      Where = req.Where
                    } : SourceQueryChunk
                match req.PretendRole with
                | None -> return! returnValues api ref req.RowId chunk next ctx
                | Some roleType -> return! api.Request.PretendRole roleType (fun () -> returnValues api ref req.RowId chunk next ctx)
            }

    let domainApi (schema, entity, name) =
        let ref = { Entity = { Schema = FunQLName schema; Name = FunQLName entity }; Name = FunQLName name }
        choose
            [ GET >=> withContext (getDomainValues ref)
              POST >=> withContext (postGetDomainValues ref)
            ]

    choose
        [ subRoutef "/domains/%s/%s/%s" domainApi
        ]
