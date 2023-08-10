module FunWithFlags.FunDB.API.Actions

open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Actions.Run
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.Exception

type ActionsAPI (api : IFunDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<ActionsAPI>()

    member this.RunAction (req : RunActionRequest) : Task<Result<ActionResponse, ActionErrorInfo>> =
        wrapAPIResult rctx "runAction" req <| task {
            match ctx.FindAction(req.Action) with
            | None ->
                let msg = sprintf "Action %O not found" req.Action
                return Error <| AERequest msg
            | Some (Ok action) ->
                try
                    return! rctx.RunWithSource (ESAction req.Action) <| fun () ->
                        task {
                            let args = Option.defaultWith (fun () -> JObject ()) req.Args
                            let! res = action.Run(args, ctx.CancellationToken)
                            return Ok { Result = res }
                        }
                with
                    | :? ActionRunException as e when e.IsUserException ->
                        logger.LogError(e, "Exception in action {action}", ref)
                        return Error (AEException (fullUserMessage e, e.UserData))
            | Some (Error e) ->
                logger.LogError(e, "Requested action {action} is broken", ref)
                let msg = sprintf "Requested action %O is broken: %s" ref (fullUserMessage e)
                return Error <| AEOther msg
        }

    interface IActionsAPI with
        member this.RunAction req = this.RunAction req