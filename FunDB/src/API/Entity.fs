module FunWithFlags.FunDB.API.Entity

open Newtonsoft.Json
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Serialization.Utils
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
type TransactionOpResult =
    | [<CaseName("insert")>] TRInsertEntity of id : int
    | [<CaseName("update")>] TRUpdateEntity
    | [<CaseName("delete")>] TRDeleteEntity

[<NoEquality; NoComparison>]
type Transaction =
    { operations : TransactionOp[]
    }

[<NoEquality; NoComparison>]
type TransactionResult =
    { results : TransactionOpResult[]
    }

type TransactionErrorType =
    | [<CaseName("transaction")>]
      ErrorTransaction

type TransactionSubErrorType =
    | [<CaseName("generic")>]
      TEArguments
    | [<CaseName("access_denied")>]
      TEAccessDenied
    | [<CaseName("not_found")>]
      TENotFound
    | [<CaseName("execution")>]
      TEExecution

type TransactionError =
    { [<JsonProperty("type")>]
      errorType : TransactionErrorType
      subtype : TransactionSubErrorType
      message : string
      operation : int
    }

let entitiesApi : HttpHandler =
    let errorString = function
        | EEArguments msg -> (TEArguments, sprintf "Invalid arguments: %s" msg)
        | EEAccessDenied -> (TEAccessDenied, "Insufficient privileges to perform operation")
        | EENotFound -> (TENotFound, "Entity not found")
        | EEExecution msg -> (TEExecution, msg)

    let returnError e = 
        let (typ, msg) = errorString e
        let resp = errorJson msg
        match typ with 
        | TEArguments -> RequestErrors.badRequest resp
        | TEAccessDenied -> RequestErrors.forbidden resp
        | TENotFound -> RequestErrors.notFound resp
        | TEExecution -> RequestErrors.unprocessableEntity resp

    let getEntityInfo (entityRef : ResolvedEntityRef) (rctx : RequestContext) : HttpHandler =
        fun next ctx -> task {
            match! rctx.GetEntityInfo entityRef with
            | Ok info ->
                return! Successful.ok (json info) next ctx
            | Result.Error err -> return! returnError err next ctx
        }

    let entityApi (schema : string, name : string) =
        let entityRef =
            { schema = FunQLName schema
              name = FunQLName name
            }
        choose
            [ GET >=> withContext (getEntityInfo entityRef)
            ]

    let performTransaction (rctx : RequestContext) (next : HttpFunc) (ctx : HttpContext) : HttpFuncResult =
        task {
            let! transaction = ctx.BindModelAsync<Transaction>()
            let handleOne (i, op) =
                task {
                    let! res =
                        match op with
                        | TInsertEntity (ref, entries) ->
                            Task.map (Result.map TRInsertEntity) <| rctx.InsertEntity ref entries
                        | TUpdateEntity (ref, id, entries) ->
                            Task.map (Result.map (fun _ -> TRUpdateEntity)) <| rctx.UpdateEntity ref id entries
                        | TDeleteEntity (ref, id) ->
                            Task.map (Result.map (fun _ -> TRDeleteEntity)) <| rctx.DeleteEntity ref id
                    return Result.mapError (fun err -> (i, err)) res
                }
            match! transaction.operations |> Seq.indexed |> Seq.traverseResultTaskSync handleOne with
            | Ok results ->
                let ret = { results = Array.ofSeq results }
                return! commitAndReturn (json ret) rctx next ctx
            | Error (i, err) ->
                let (typ, msg) = errorString err
                let ret =
                    { errorType = ErrorTransaction
                      subtype = typ
                      operation = i
                      message = msg
                    }
                return! RequestErrors.badRequest (json ret) next ctx
        }

    let transactionApi =
        POST >=> withContext performTransaction

    choose
        [ subRoutef "/entity/%s/%s" entityApi
          subRoute "/transaction" transactionApi
        ]