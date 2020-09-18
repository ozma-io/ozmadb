module FunWithFlags.FunDB.Actions.Run

open Newtonsoft.Json.Linq
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs
open NetJs.Value
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types

type ActionRunException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ActionRunException (message, null)

type ActionScript (runtime : IJSRuntime, name : string, scriptSource : string) =
    let func =
        try
            runtime.CreateDefaultFunction name scriptSource
        with
        | :? NetJsException as e ->
            raisefWithInner ActionRunException e "Couldn't initialize"

    member this.Run (args : JObject, cancellationToken : CancellationToken) : Task<JObject option> =
        task {
            let argsValue =
                use writer = new V8JsonWriter(runtime.Context)
                args.WriteTo(writer)
                writer.Result
            try
                return! runtime.EventLoopScope <| fun eventLoop ->
                    task {
                        let maybeResult = func.Call(cancellationToken, null, [|argsValue|])
                        do! eventLoop.Run ()
                        let maybeResult = maybeResult.GetValueOrPromiseResult ()
                        match maybeResult.Data with
                        | :? Undefined -> return None
                        | :? Object ->
                            use reader = new V8JsonReader(maybeResult)
                            let result = JToken.ReadFrom(reader) :?> JObject
                            return Some result
                        | _ -> return raisef ActionRunException "Invalid return value"
                    }
            with
            | :? JSException as e ->
                return raisefWithInner ActionRunException e "Unhandled exception:\n%s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner ActionRunException e "Failed to run"
        }

    member this.Runtime = runtime

type ActionScriptsSchema =
    { Actions : Map<ActionName, ActionScript>
    }

type ActionScripts =
    { Schemas : Map<ActionName, ActionScriptsSchema>
    } with
        member this.FindAction (ref : ActionRef) : ActionScript option =
             Map.tryFind ref.schema this.Schemas
                |> Option.bind (fun schema -> Map.tryFind ref.name schema.Actions)

let private actionName (actionRef : ActionRef) =
    sprintf "%O/actions/%O.mjs" actionRef.schema actionRef.name

let private prepareActionScriptsSchema (runtime : IJSRuntime) (schemaName : SchemaName) (actions : ActionsSchema) : ActionScriptsSchema =
    let getOne name = function
    | Error e -> None
    | Ok action -> Some (ActionScript(runtime, actionName { schema = schemaName; name = name }, action.Function))

    { Actions = Map.mapMaybe getOne actions.Actions
    }

let prepareActionScripts (runtime : IJSRuntime) (actions : ResolvedActions) : ActionScripts =
    { Schemas = Map.map (prepareActionScriptsSchema runtime) actions.Schemas
    }

type private TestActionEvaluator (runtime : IJSRuntime, forceAllowBroken : bool) =
    let testAction (actionRef : ActionRef) (action : ResolvedAction) : unit =
        ignore <| ActionScript(runtime, actionName actionRef, action.Function)

    let testActionsSchema (schemaName : SchemaName) (entityActions : ActionsSchema) : ErroredActionsSchema * ActionsSchema =
        let mutable errors = Map.empty

        let mapAction name (action : ResolvedAction) =
            try
                try
                    let ref = { schema = schemaName; name = name }
                    testAction ref action
                    Ok action
                with
                | :? ActionRunException as e when action.AllowBroken || forceAllowBroken ->
                    errors <- Map.add name (e :> exn) errors
                    Error (e :> exn)
            with
            | :? ActionRunException as e -> raisefWithInner ActionRunException e.InnerException "Error in action %O: %s" name e.Message

        let ret =
            { Actions = entityActions.Actions |> Map.map (fun name -> Result.bind (mapAction name))
            } : ActionsSchema
        (errors, ret)

    let testActions (actions : ResolvedActions) : ErroredActions * ResolvedActions =
        let mutable errors = Map.empty

        let mapDatabase name schema =
            try
                let (dbErrors, newSchema) = testActionsSchema name schema
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newSchema
            with
            | :? ActionRunException as e -> raisefWithInner ActionRunException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { Schemas = actions.Schemas |> Map.map mapDatabase
            } : ResolvedActions
        (errors, ret)

    member this.TestActions actions = testActions actions

let testEvalActions (runtime : IJSRuntime) (forceAllowBroken : bool) (actions : ResolvedActions) : ErroredActions * ResolvedActions =
    let eval = TestActionEvaluator(runtime, forceAllowBroken)
    eval.TestActions actions
