module FunWithFlags.FunDB.HTTP.SaveRestore

open System.IO
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.Affine
open Giraffe

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.HTTP.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.SaveRestore
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

let saveRestoreApi : HttpHandler =
    let saveZipSchemas (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (names, api) = arg :?> (SchemaName seq * IFunDBAPI)
        match! api.SaveRestore.SaveZipSchemas names with
        | Ok dumpStream ->
            ctx.SetHttpHeader "Content-Type" "application/zip"
            return! ctx.WriteStreamAsync false dumpStream None None
        | Error err -> return! saveError err next ctx
    }

    let saveJsonSchemas (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (names, api) = arg :?> (SchemaName seq * IFunDBAPI)
        match! api.SaveRestore.SaveSchemas names with
        | Ok dump -> return! Successful.ok (json dump) next ctx
        | Error err -> return! saveError err next ctx
    }

    let saveSchemaNegotiationRules =
        dict [
            "*/*"             , saveZipSchemas
            "application/json", saveJsonSchemas
            "application/zip" , saveZipSchemas
        ]

    let saveSchemas (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let names =
            match ctx.Request.Query.TryGetValue "schema" with
            | (false, _) -> Map.keys api.Request.Context.Layout.schemas
            | (true, schemas) -> Seq.map FunQLName schemas
        let negotiateConfig = ctx.GetService<INegotiationConfig>()
        return! negotiateWith saveSchemaNegotiationRules negotiateConfig.UnacceptableHandler (names, api) next ctx
    }

    let getDropOthers (nextOp : bool -> HttpHandler) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        match ctx.GetQueryStringValue "drop_others" with
        | Ok v ->
            match Parsing.tryBool v with
            | Some r -> nextOp r next ctx
            | None -> RequestErrors.BAD_REQUEST "Invalid value for drop_others" next ctx
        | Error _ -> nextOp false next ctx

    let restoreJsonSchemas (api : IFunDBAPI) (dropOthers : bool) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let! dump = ctx.BindJsonAsync<Map<SchemaName, SchemaDump>>()
        match! api.SaveRestore.RestoreSchemas dump dropOthers with
        | Ok () -> return! commitAndOk api next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreZipSchemas (api : IFunDBAPI) (dropOthers : bool) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        use stream = new MemoryStream()
        do! ctx.Request.Body.CopyToAsync(stream)
        ignore <| stream.Seek(0L, SeekOrigin.Begin)
        match! api.SaveRestore.RestoreZipSchemas stream dropOthers with
        | Ok () -> return! commitAndOk api next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreSchemas (api : IFunDBAPI) (dropOthers : bool) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match ctx.TryGetRequestHeader "Content-Type" with
        | Some "application/json" -> return! restoreJsonSchemas api dropOthers next ctx
        | Some "application/zip" -> return! restoreZipSchemas api dropOthers next ctx
        | _ -> return! RequestErrors.unsupportedMediaType (json (RERequest "Unsupported media type")) next ctx
    }

    let massSaveRestoreApi =
        choose
            [ GET >=> withContext saveSchemas
              PUT >=> withContext (fun api -> getDropOthers (restoreSchemas api))
            ]

    choose
        [ subRoute "/layouts" massSaveRestoreApi
        ]