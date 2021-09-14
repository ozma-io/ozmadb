module FunWithFlags.FunDB.Actions.Run

open Newtonsoft.Json.Linq
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs.Value
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Actions.Types

type ActionRunException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ActionRunException (message, innerException, isUserException innerException)

    new (message : string) = ActionRunException (message, null, true)

type ActionScript (runtime : IJSRuntime, name : string, scriptSource : string) =
    let func =
        try
            runtime.CreateDefaultFunction { Path = name; Source = scriptSource }
        with
        | :? JavaScriptRuntimeException as e ->
            raisefWithInner ActionRunException e "Couldn't initialize"

    member this.Run (args : JObject, cancellationToken : CancellationToken) : Task<JObject option> =
        task {
            let argsValue =
                use writer = new V8JsonWriter(runtime.Context)
                args.WriteTo(writer)
                writer.Result
            try
                let! result =  runFunctionInRuntime runtime func cancellationToken [|argsValue|]
                match result.Data with
                | :? Undefined -> return None
                | :? Object ->
                    use reader = new V8JsonReader(result)
                    let result = JToken.ReadFrom(reader) :?> JObject
                    return Some result
                | _ -> return raisef ActionRunException "Invalid return value"
            with
            | :? JavaScriptRuntimeException as e -> return raisefWithInner ActionRunException e "Failed to run action"
        }

    member this.Runtime = runtime

type ActionScriptsSchema =
    { Actions : Map<ActionName, ActionScript>
    }

type ActionScripts =
    { Schemas : Map<ActionName, ActionScriptsSchema>
    } with
        member this.FindAction (ref : ActionRef) : ActionScript option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun schema -> Map.tryFind ref.Name schema.Actions)

let private actionName (actionRef : ActionRef) =
    sprintf "actions/%O/%O.mjs" actionRef.Schema actionRef.Name

let private prepareActionScriptsSchema (runtime : IJSRuntime) (schemaName : SchemaName) (actions : ActionsSchema) : ActionScriptsSchema =
    let getOne name = function
    | Error e -> None
    | Ok action -> Some (ActionScript(runtime, actionName { Schema = schemaName; Name = name }, action.Function))

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
                    let ref = { Schema = schemaName; Name = name }
                    testAction ref action
                    Ok action
                with
                | :? ActionRunException as e when action.AllowBroken || forceAllowBroken ->
                    if not action.AllowBroken then
                        errors <- Map.add name (e :> exn) errors
                    Error (e :> exn)
            with
            | e -> raisefWithInner ActionRunException e "In action %O" name

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
            | e -> raisefWithInner ActionRunException e "In schema %O" name

        let ret =
            { Schemas = actions.Schemas |> Map.map mapDatabase
            } : ResolvedActions
        (errors, ret)

    member this.TestActions actions = testActions actions

let testEvalActions (runtime : IJSRuntime) (forceAllowBroken : bool) (actions : ResolvedActions) : ErroredActions * ResolvedActions =
    let eval = TestActionEvaluator(runtime, forceAllowBroken)
    eval.TestActions actions
