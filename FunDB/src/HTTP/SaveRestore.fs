module FunWithFlags.FunDB.HTTP.SaveRestore

open System
open System.IO
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe
open Giraffe.EndpointRouting

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.HTTP.LongRunning
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.SaveRestore
open FunWithFlags.FunDB.Operations.SaveRestore

// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type RestoreSchemasHTTPFlags =
    { DropOthers : bool
      ForceAllowBroken : bool
    }

let saveRestoreApi (serviceProvider : IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let saveZipSchemas (schemas : SaveSchemas) : HttpHandler =
        let job (api : IFunDBAPI) =
            task {
                match! api.SaveRestore.SaveSchemas { Schemas = schemas } with
                | Ok dump ->
                    let header =
                        { ResponseCode = 200
                          Headers =
                            dict <| seq {
                                ("Content-Type", "application/zip")
                            }
                        }
                    let writer stream cancellationToken =
                        schemasToZipFile dump.Schemas stream
                    return (header, JOSync writer)
                | Error err -> return jobError err
            }
        // This query is quite expensive, so apply write rate limit.
        utils.PerformWriteJob job

    let saveJsonSchemas (schemas : SaveSchemas) : HttpHandler =
        let job (api : IFunDBAPI) = 
            task {
                let! ret = api.SaveRestore.SaveSchemas { Schemas = schemas }
                return jobReply ret
            }
        utils.PerformWriteJob job

    let saveSchemaNegotiationRules =
        dict [
            "*/*"             , (fun (o : obj) -> saveZipSchemas (o :?> SaveSchemas))
            "application/json", (fun (o : obj) -> saveJsonSchemas (o :?> SaveSchemas))
            "application/zip" , (fun (o : obj) -> saveZipSchemas (o :?> SaveSchemas))
        ]

    let saveSchemas (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let schemas =
                match ctx.Request.Query.TryGetValue "schema" with
                | (true, schemas) -> Seq.map FunQLName schemas |> Array.ofSeq |> SSNames
                | (false, _) ->
                    if boolRequestArg "skip_preloaded" ctx then
                        SSNonPreloaded
                    else
                        SSAll
            return! negotiateWith saveSchemaNegotiationRules (requestError RIUnacceptable) schemas next ctx
        }

    let restoreJsonSchemas (flags : RestoreSchemasHTTPFlags) =
        safeBindJson <| fun dump ->
            let job (api : IFunDBAPI) =
                task {
                    let req =
                        { Schemas = dump
                          Flags = Some { DropOthers = flags.DropOthers }
                        }
                    let! ret = api.SaveRestore.RestoreSchemas req
                    return! jobReplyWithCommit api ret
                }
            utils.PerformWriteJob job

    let restoreZipSchemas (flags : RestoreSchemasHTTPFlags) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            // Used because the ZipArchive API is synchronous.
            use stream = new MemoryStream()
            do! ctx.Request.Body.CopyToAsync(stream)
            let job (api : IFunDBAPI) =
                task {
                    ignore <| stream.Seek(0L, SeekOrigin.Begin)
                    match trySchemasFromZipFile stream with
                    | Error e -> return jobError e
                    | Ok dump ->
                        let req =
                            { Schemas = dump
                              Flags = Some { DropOthers = flags.DropOthers }
                            }
                        let! ret = api.SaveRestore.RestoreSchemas req
                        return! jobReplyWithCommit api ret
                }
            return! utils.PerformWriteJob job next ctx
        }

    let restoreSchemas (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let flags =
                { DropOthers = boolRequestArg "drop_others" ctx
                  ForceAllowBroken = boolRequestArg "force_allow_broken" ctx
                }
            match ctx.TryGetRequestHeader "Content-Type" with
            | Some "application/json" -> return! restoreJsonSchemas flags next ctx
            | Some "application/zip" -> return! restoreZipSchemas flags next ctx
            | _ -> return! requestError RIUnsupportedMediaType next ctx
        }

    let massSaveRestoreApi =
        [ GET [route "" <| saveSchemas]
          PUT [route "" <| restoreSchemas]
        ]

    [ subRoute "/layouts" massSaveRestoreApi
    ]