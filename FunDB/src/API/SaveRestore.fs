module FunWithFlags.FunDB.API.SaveRestore

open System.IO
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.SaveRestore
open FunWithFlags.FunDB.Operations.Context

let private saveError e = 
    let handler =
        match e with
        | SENotFound -> RequestErrors.notFound
        | SEAccessDenied -> RequestErrors.forbidden
    handler (json e)

let private restoreError e = 
    let handler =
        match e with
        | REAccessDenied -> RequestErrors.forbidden
        | REPreloaded -> RequestErrors.unprocessableEntity
        | REInvalidFormat _ -> RequestErrors.unprocessableEntity
    handler (json e)

let saveRestoreApi : HttpHandler =
    let saveZipSchema (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (schemaName, rctx) = arg :?> (SchemaName * RequestContext)
        match! rctx.SaveZipSchema schemaName with
        | Ok dumpStream -> return! ctx.WriteStreamAsync false dumpStream None None
        | Error err -> return! saveError err next ctx
    }

    let saveJsonSchema (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (schemaName, rctx) = arg :?> (SchemaName * RequestContext)
        match! rctx.SaveSchema schemaName with
        | Ok dump -> return! Successful.ok (json dump) next ctx
        | Error err -> return! saveError err next ctx
    }

    let saveSchemaNegotiationRules =
        dict [
            "*/*"             , saveZipSchema
            "application/json", saveJsonSchema
            "application/zip" , saveZipSchema
        ]

    let saveSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let negotiateConfig = ctx.GetService<INegotiationConfig>()
        return! negotiateWith saveSchemaNegotiationRules negotiateConfig.UnacceptableHandler (schemaName, rctx) next ctx
    }

    let restoreJsonSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let! dump = ctx.BindJsonAsync<SchemaDump>()
        match! rctx.RestoreSchema schemaName dump with
        | Ok () -> return! commitAndOk rctx next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreZipSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        use stream = new MemoryStream()
        do! ctx.Request.Body.CopyToAsync(stream)
        ignore <| stream.Seek(0L, SeekOrigin.Begin)
        match! rctx.RestoreZipSchema schemaName stream with
        | Ok () -> return! commitAndOk rctx next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match ctx.TryGetRequestHeader "Content-Type" with
        | Some "application/zip" -> return! restoreZipSchema schemaName rctx next ctx
        | Some "application/json" -> return! restoreJsonSchema schemaName rctx next ctx
        | _ -> return! RequestErrors.unsupportedMediaType (errorJson "Unsupported media type") next ctx
    }

    let saveRestoreApi (schema : string) =
        let schemaName = FunQLName schema
        choose
            [ GET >=> withContext (saveSchema schemaName)
              PUT >=> withContext (restoreSchema schemaName)
            ]

    choose
        [ subRoutef "/layouts/%s" saveRestoreApi
        ]