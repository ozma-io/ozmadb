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

let private saveError e =
    let handler =
        match e with
        | RSENotFound -> RequestErrors.notFound
        | RSEAccessDenied -> RequestErrors.forbidden
    handler (json e)

let private restoreError e =
    let handler =
        match e with
        | RREAccessDenied -> RequestErrors.forbidden
        | RREPreloaded -> RequestErrors.unprocessableEntity
        | RREInvalidFormat _ -> RequestErrors.unprocessableEntity
        | RREConsistency _ -> RequestErrors.badRequest
    handler (json e)

[<NoEquality; NoComparison>]
type private RestoreFlags =
    { DropOthers : bool
      ForceAllowBroken : bool
    }

let saveRestoreApi : Endpoint list =
    let saveZipSchemas (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let (schemas, api) = arg :?> (SaveSchemas * IFunDBAPI)
            match! api.SaveRestore.SaveZipSchemas schemas with
            | Ok dumpStream ->
                ctx.SetHttpHeader("Content-Type", "application/zip")
                return! ctx.WriteStreamAsync(false, dumpStream, None, None)
            | Error err -> return! saveError err next ctx
        }

    let saveJsonSchemas (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let (schemas, api) = arg :?> (SaveSchemas * IFunDBAPI)
            match! api.SaveRestore.SaveSchemas schemas with
            | Ok dump -> return! Successful.ok (json dump) next ctx
            | Error err -> return! saveError err next ctx
        }

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

    let getRestoreFlags (nextOp : RestoreFlags -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        let flags =
            { DropOthers = boolRequestArg "drop_others" ctx
              ForceAllowBroken = boolRequestArg "force_allow_broken" ctx
            }
        nextOp flags next ctx

    let restoreJsonSchemas (api : IFunDBAPI) (flags : RestoreFlags) =
        safeBindJson <| fun dump (next : HttpFunc) (ctx : HttpContext) ->
            task {
                match! api.SaveRestore.RestoreSchemas dump flags.DropOthers with
                | Ok () -> return! commitAndOk api next ctx
                | Error err -> return! restoreError err next ctx
            }

    let restoreZipSchemas (api : IFunDBAPI) (flags : RestoreFlags) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            use stream = new MemoryStream()
            do! ctx.Request.Body.CopyToAsync(stream)
            ignore <| stream.Seek(0L, SeekOrigin.Begin)
            match! api.SaveRestore.RestoreZipSchemas stream flags.DropOthers with
            | Ok () -> return! commitAndOk api next ctx
            | Error err -> return! restoreError err next ctx
        }

    let restoreSchemas (api : IFunDBAPI) (flags : RestoreFlags) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
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