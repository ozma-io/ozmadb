module FunWithFlags.FunDB.HTTP.SaveRestore

open System.IO
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe
open Giraffe.EndpointRouting

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types

[<NoEquality; NoComparison>]
type RestoreSchemasHTTPFlags =
    { DropOthers : bool
      ForceAllowBroken : bool
    }

let saveRestoreApi : Endpoint list =
    let saveZipSchemas (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let (schemas, api) = arg :?> (SaveSchemas * IFunDBAPI)
            match! api.SaveRestore.SaveZipSchemas { Schemas = schemas } with
            | Ok dumpStream ->
                ctx.SetHttpHeader("Content-Type", "application/zip")
                return! ctx.WriteStreamAsync(false, dumpStream, None, None)
            | Error err -> return! requestError err next ctx
        }

    let saveJsonSchemas (arg: obj) : HttpHandler =
        let (schemas, api) = arg :?> (SaveSchemas * IFunDBAPI)
        handleRequest (api.SaveRestore.SaveSchemas { Schemas = schemas })

    let saveSchemaNegotiationRules =
        dict [
            "*/*"             , saveZipSchemas
            "application/json", saveJsonSchemas
            "application/zip" , saveZipSchemas
        ]

    let saveSchemas (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let schemas =
                match ctx.Request.Query.TryGetValue "schema" with
                | (true, schemas) -> Seq.map FunQLName schemas |> Array.ofSeq |> SSNames
                | (false, _) ->
                    if boolRequestArg "skip_preloaded" ctx then
                        SSNonPreloaded
                    else
                        SSAll
            let negotiateConfig = ctx.GetService<INegotiationConfig>()
            return! negotiateWith saveSchemaNegotiationRules negotiateConfig.UnacceptableHandler (schemas, api) next ctx
        }

    let getRestoreFlags (nextOp : RestoreSchemasHTTPFlags -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let flags =
            { DropOthers = boolRequestArg "drop_others" ctx
              ForceAllowBroken = boolRequestArg "force_allow_broken" ctx
            }
        nextOp flags next ctx

    let restoreJsonSchemas (api : IFunDBAPI) (flags : RestoreSchemasHTTPFlags) =
        safeBindJson <| fun dump ->
            let req =
                { Schemas = dump
                  Flags = Some { DropOthers = flags.DropOthers }
                }
            handleRequestWithCommit api (api.SaveRestore.RestoreSchemas req)

    let restoreZipSchemas (api : IFunDBAPI) (flags : RestoreSchemasHTTPFlags) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            // Used because the ZipArchive API is synchronous.
            use stream = new MemoryStream()
            do! ctx.Request.Body.CopyToAsync(stream)
            ignore <| stream.Seek(0L, SeekOrigin.Begin)
            let req = { Flags = Some { DropOthers = flags.DropOthers } }
            return! handleRequestWithCommit api (api.SaveRestore.RestoreZipSchemas ctx.Request.Body req) next ctx
        }

    let restoreSchemas (api : IFunDBAPI) (flags : RestoreSchemasHTTPFlags) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            if flags.ForceAllowBroken then
                api.Request.Context.SetForceAllowBroken ()
            match ctx.TryGetRequestHeader "Content-Type" with
            | Some "application/json" -> return! restoreJsonSchemas api flags next ctx
            | Some "application/zip" -> return! restoreZipSchemas api flags next ctx
            | _ -> return! RequestErrors.unsupportedMediaType (json (RIRequest "Unsupported media type")) next ctx
        }

    let massSaveRestoreApi =
        [ // This query is quite expensive, so apply write rate limit.
          GET [route "" <| withContextWrite saveSchemas]
          PUT [route "" <| withContextWrite (fun api -> getRestoreFlags (restoreSchemas api))]
        ]

    [ subRoute "/layouts" massSaveRestoreApi
    ]