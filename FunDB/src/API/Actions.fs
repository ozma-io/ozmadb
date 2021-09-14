module FunWithFlags.FunDB.API.Actions

open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.Actions.Run
open FunWithFlags.FunDB.API.Types

type ActionsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<ActionsAPI>()

    member this.RunAction (ref : ActionRef) (args : JObject) : Task<Result<ActionResult, ActionErrorInfo>> =
        task {
            match ctx.FindAction(ref) with
            | Some (Ok action) ->
                try
                    return! rctx.RunWithSource (ESAction ref) <| fun () ->
                        task {
                            let! res = action.Run(args, ctx.CancellationToken)
                            return Ok { Result = res }
                        }
                with
                    | :? ActionRunException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Exception in action {}", ref)
                        let str = exceptionString ex
                        rctx.WriteEvent (fun event ->
                            event.Type <- "runAction"
                            event.Error <- "exception"
                            event.Details <- str
                        )
                        return Error (AEException str)
            | Some (Error e) ->
                logger.LogError(e, "Requested action {action} is broken", ref.ToString())
                return Error <| AECompilation (exceptionString e)
            | None -> return Error AENotFound
        }

    interface IActionsAPI with
        member this.RunAction ref args = this.RunAction ref args