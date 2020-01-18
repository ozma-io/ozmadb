module FunWithFlags.FunDB.API.Entity

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.Context

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseName("insert")>] TInsertEntity of entity : ResolvedEntityRef * entries : RawArguments
    | [<CaseName("update")>] TUpdateEntity of entity : ResolvedEntityRef * id : int * entries : RawArguments
    | [<CaseName("delete")>] TDeleteEntity of entity : ResolvedEntityRef * id : int

[<SerializeAsObject("type")>]
type TransactionResult =
    | [<CaseName("insert")>] TRInsertEntity of id : int
    | [<CaseName("update")>] TRUpdateEntity
    | [<CaseName("delete")>] TRDeleteEntity

[<NoEquality; NoComparison>]
type Transaction =
    { operations: TransactionOp[]
    }

let entitiesApi : HttpHandler =
    let returnError = function
        | EEArguments msg -> sprintf "Invalid arguments: %s" msg |> text |> RequestErrors.badRequest
        | EEAccessDenied -> text "Forbidden" |> RequestErrors.forbidden
        | EENotFound -> text "Not found" |> RequestErrors.notFound
        | EEExecute msg -> text msg |> RequestErrors.badRequest

    let getEntityInfo (entityRef : ResolvedEntityRef) (rctx : RequestContext) : HttpHandler =
        fun next ctx -> task {
            match! rctx.GetEntityInfo entityRef with
            | Ok info ->
                return! Successful.ok (json info) next ctx
            | Result.Error err -> return! returnError err next ctx
        }

    let insertEntity (entityRef : ResolvedEntityRef) (rctx : RequestContext) : HttpHandler =
        formArgs <| fun rawArgs next ctx -> task {
            match! rctx.InsertEntity entityRef rawArgs with
            | Ok newId ->
                let ret = TRInsertEntity newId
                return! commitAndReturn (json ret) rctx next ctx
            | Result.Error err -> return! returnError err next ctx
        }

    let updateEntity (entityRef : ResolvedEntityRef) (id : int) (rctx : RequestContext) : HttpHandler =
        formArgs <| fun rawArgs next ctx -> task {
                match! rctx.UpdateEntity entityRef id rawArgs with
                | Ok () ->
                    return! commitAndReturn (json TRUpdateEntity) rctx next ctx
                | Result.Error err -> return! returnError err next ctx
        }

    let deleteEntity (entityRef : ResolvedEntityRef) (id : int) (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult = task {
        match! rctx.DeleteEntity entityRef id with
        | Ok () -> return! commitAndReturn (json TRDeleteEntity) rctx next ctx
        | Result.Error err -> return! returnError err next ctx
    }

    let recordApi (entityRef : ResolvedEntityRef) (id : int) =
        choose
            [ PUT >=> withContext (updateEntity entityRef id)
              DELETE >=> withContext (deleteEntity entityRef id)
            ]

    let rootEntityApi (ref : ResolvedEntityRef) =
        choose
            [ GET >=> withContext (getEntityInfo ref)
              POST >=> withContext (insertEntity ref)
            ]

    let entityApi (schema : string, name : string) =
        let entityRef =
            { schema = FunQLName schema
              name = FunQLName name
            }
        choose
            [ route "" >=> (rootEntityApi entityRef)
              routef "/%i" (recordApi entityRef)
            ]

    let performTransaction (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let! transaction = ctx.BindModelAsync<Transaction>()
            let handleOne op =
                task {
                    let! res =
                        match op with
                        | TInsertEntity (ref, entries) ->
                            Task.map (Result.map TRInsertEntity) <| rctx.InsertEntity ref entries
                        | TUpdateEntity (ref, id, entries) ->
                            Task.map (Result.map (fun _ -> TRUpdateEntity)) <| rctx.UpdateEntity ref id entries
                        | TDeleteEntity (ref, id) ->
                            Task.map (Result.map (fun _ -> TRDeleteEntity)) <| rctx.DeleteEntity ref id
                    return Result.mapError (fun err -> (op, err)) res
                }
            let mutable failed = false
            match! transaction.operations |> Seq.traverseResultTaskSync handleOne with
            | Ok ret -> return! commitAndReturn (json ret) rctx next ctx
            | Error (op, err) -> return! returnError err next ctx
        }

    let transactionApi =
        POST >=> withContext performTransaction

    choose
        [ subRoutef "/entity/%s/%s" entityApi
          subRoute "/transaction" transactionApi
        ]