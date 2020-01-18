module FunWithFlags.FunDB.API.SaveRestore

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.SaveRestore
open FunWithFlags.FunDB.Operations.Context

let saveRestoreApi : HttpHandler =
    let returnSaveError = function
        | SENotFound -> text "Not found" |> RequestErrors.notFound
        | SEAccessDenied -> text "Forbidden" |> RequestErrors.forbidden

    let returnRestoreError = function
        | REAccessDenied -> text "Forbidden" |> RequestErrors.forbidden
        | REPreloaded -> text "Cannot restore preloaded schema" |> RequestErrors.badRequest

    let saveSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match! rctx.SaveSchema schemaName with
        | Ok dump -> return! Successful.ok (json dump) next ctx
        | Error err -> return! returnSaveError err next ctx
    }

    let restoreSchema (schemaName : SchemaName) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        let! dump = ctx.BindModelAsync<SchemaDump>()
        match! rctx.RestoreSchema schemaName dump with
        | Ok () -> return! commitAndOk rctx next ctx
        | Error err -> return! returnRestoreError err next ctx
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