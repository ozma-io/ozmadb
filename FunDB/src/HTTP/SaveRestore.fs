module FunWithFlags.FunDB.HTTP.SaveRestore

open System.IO
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

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
    handler (json e)

let saveRestoreApi : HttpHandler =
    let saveZipSchema (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (schemaName, api) = arg :?> (SchemaName * IFunDBAPI)
        match! api.SaveRestore.SaveZipSchema schemaName with
        | Ok dumpStream -> return! ctx.WriteStreamAsync false dumpStream None None
        | Error err -> return! saveError err next ctx
    }

    let saveJsonSchema (arg: obj) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let (schemaName, api) = arg :?> (SchemaName * IFunDBAPI)
        match! api.SaveRestore.SaveSchema schemaName with
        | Ok dump -> return! Successful.ok (json dump) next ctx
        | Error err -> return! saveError err next ctx
    }

    let saveSchemaNegotiationRules =
        dict [
            "*/*"             , saveZipSchema
            "application/json", saveJsonSchema
            "application/zip" , saveZipSchema
        ]

    let saveSchema (schemaName : SchemaName) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let negotiateConfig = ctx.GetService<INegotiationConfig>()
        return! negotiateWith saveSchemaNegotiationRules negotiateConfig.UnacceptableHandler (schemaName, api) next ctx
    }

    let restoreJsonSchema (schemaName : SchemaName) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let! dump = ctx.BindJsonAsync<SchemaDump>()
        match! api.SaveRestore.RestoreSchema schemaName dump with
        | Ok () -> return! commitAndOk api next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreZipSchema (schemaName : SchemaName) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        use stream = new MemoryStream()
        do! ctx.Request.Body.CopyToAsync(stream)
        ignore <| stream.Seek(0L, SeekOrigin.Begin)
        match! api.SaveRestore.RestoreZipSchema schemaName stream with
        | Ok () -> return! commitAndOk api next ctx
        | Error err -> return! restoreError err next ctx
    }

    let restoreSchema (schemaName : SchemaName) (api : IFunDBAPI) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match ctx.TryGetRequestHeader "Content-Type" with
        | Some "application/zip" -> return! restoreZipSchema schemaName api next ctx
        | Some "application/json" -> return! restoreJsonSchema schemaName api next ctx
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