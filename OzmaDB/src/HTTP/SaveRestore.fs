module OzmaDB.HTTP.SaveRestore

open System
open System.IO
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http
open Giraffe
open Giraffe.EndpointRouting

open OzmaDB.OzmaUtils
open OzmaDB.HTTP.LongRunning
open OzmaDB.HTTP.Utils
open OzmaDB.OzmaQL.AST
open OzmaDB.API.Types
open OzmaDB.API.SaveRestore
open OzmaDB.Operations.SaveRestore

// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type RestoreSchemasHTTPFlags =
    { DropOthers: bool
      ForceAllowBroken: bool }

let saveRestoreApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let saveJsonSchemas (req: SaveSchemasRequest) : HttpHandler =
        let job (api: IOzmaDBAPI) =
            task {
                let! ret = api.SaveRestore.SaveSchemas req
                return jobReply ret
            }

        utils.PerformWriteJob job

    let saveZipSchemas (req: SaveSchemasRequest) : HttpHandler =
        let job (api: IOzmaDBAPI) =
            task {
                match! api.SaveRestore.SaveSchemas req with
                | Ok dump ->
                    let header =
                        { ResponseCode = 200
                          Headers = dict <| seq { ("Content-Type", "application/zip") } }

                    let writer stream cancellationToken = schemasToZipFile dump.Schemas stream
                    return (header, JOSync writer)
                | Error err -> return jobError err
            }
        // This query is quite expensive, so apply write rate limit.
        utils.PerformWriteJob job

    let saveSchemaNegotiationRules =
        dict
            [ "*/*", (fun (o: obj) -> saveZipSchemas (o :?> SaveSchemasRequest))
              "application/json", (fun (o: obj) -> saveJsonSchemas (o :?> SaveSchemasRequest))
              "application/zip", (fun (o: obj) -> saveZipSchemas (o :?> SaveSchemasRequest)) ]

    let saveSchemas (next: HttpFunc) (ctx: HttpContext) : HttpFuncResult =
        task {
            let skipPreloaded = boolRequestArg "skip_preloaded" ctx

            let settings =
                { OnlyCustomEntities = if not skipPreloaded then None else Some OCPreloaded }: SaveSchemaSettings

            let req: SaveSchemasRequest =
                match ctx.Request.Query.TryGetValue "schema" with
                | (true, schemas) ->
                    Seq.map (fun strName -> (OzmaQLName strName, settings)) schemas
                    |> Map.ofSeq
                    |> SRSpecified
                | (false, _) ->
                    // `skip_preloaded` now actually controls `OnlyCustomEntities`.
                    SRAll settings

            return! negotiateWith saveSchemaNegotiationRules (requestError RIUnacceptable) req next ctx
        }

    let restoreJsonSchemas (flags: RestoreSchemasHTTPFlags) =
        safeBindJson
        <| fun dump ->
            let job (api: IOzmaDBAPI) =
                task {
                    if flags.ForceAllowBroken then
                        api.Request.Context.SetForceAllowBroken()

                    let req =
                        { Schemas = dump
                          Flags = Some { DropOthers = flags.DropOthers } }

                    let! ret = api.SaveRestore.RestoreSchemas req
                    return! jobReplyWithCommit api ret
                }

            utils.PerformWriteJob job

    let restoreZipSchemas (flags: RestoreSchemasHTTPFlags) (next: HttpFunc) (ctx: HttpContext) : HttpFuncResult =
        task {
            // Used because the ZipArchive API is synchronous.
            use stream = new MemoryStream()
            do! ctx.Request.Body.CopyToAsync(stream)

            let job (api: IOzmaDBAPI) =
                task {
                    if flags.ForceAllowBroken then
                        api.Request.Context.SetForceAllowBroken()

                    ignore <| stream.Seek(0L, SeekOrigin.Begin)

                    match trySchemasFromZipFile stream with
                    | Error e -> return jobError e
                    | Ok dump ->
                        let req =
                            { Schemas = dump
                              Flags = Some { DropOthers = flags.DropOthers } }

                        let! ret = api.SaveRestore.RestoreSchemas req
                        return! jobReplyWithCommit api ret
                }

            return! utils.PerformWriteJob job next ctx
        }

    let restoreSchemas (next: HttpFunc) (ctx: HttpContext) : HttpFuncResult =
        task {
            let flags =
                { DropOthers = boolRequestArg "drop_others" ctx
                  ForceAllowBroken = boolRequestArg "force_allow_broken" ctx }

            match ctx.TryGetRequestHeader "Content-Type" with
            | Some "application/json" -> return! restoreJsonSchemas flags next ctx
            | Some "application/zip" -> return! restoreZipSchemas flags next ctx
            | _ -> return! requestError RIUnsupportedMediaType next ctx
        }

    let massSaveRestoreApi =
        [ GET [ route "" <| saveSchemas ]; PUT [ route "" <| restoreSchemas ] ]

    [ subRoute "/layouts" massSaveRestoreApi ]
