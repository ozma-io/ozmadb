module FunWithFlags.FunDB.Actions.Run

open FSharpPlus
open Newtonsoft.Json.Linq
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs.Value
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Objects.Types
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

type PreparedActionsSchema =
    { Actions : Map<ActionName, PossiblyBroken<ActionScript>>
    }

type PreparedActions =
    { Schemas : Map<ActionName, PreparedActionsSchema>
    } with
        member this.FindAction (ref : ActionRef) : PossiblyBroken<ActionScript> option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun schema -> Map.tryFind ref.Name schema.Actions)

let private actionName (actionRef : ActionRef) =
    sprintf "actions/%O/%O.mjs" actionRef.Schema actionRef.Name

type private PreparedActionsBuilder (runtime : IJSRuntime, forceAllowBroken : bool) =
    let prepareActionsSchema (schemaName : SchemaName) (actions : ActionsSchema) : PreparedActionsSchema =
        let prepareOne name (action : ResolvedAction) =
            try
                let script = ActionScript(runtime, actionName { Schema = schemaName; Name = name }, action.Function)
                Ok script
            with
            | :? ActionRunException as e when action.AllowBroken || forceAllowBroken ->
                Error { Error = e; AllowBroken = action.AllowBroken }

        { Actions = Map.map (fun name -> Result.bind (prepareOne name)) actions.Actions
        }

    let prepareActions (actions : ResolvedActions) : PreparedActions =
        { Schemas = Map.map prepareActionsSchema actions.Schemas
        }

    member this.PrepareActions actions = prepareActions actions

let prepareActions (runtime : IJSRuntime) (forceAllowBroken : bool) (actions : ResolvedActions) : PreparedActions =
    let eval = PreparedActionsBuilder(runtime, forceAllowBroken)
    eval.PrepareActions actions

let private resolvedPreparedActionsSchema (resolved : ActionsSchema) (prepared : PreparedActionsSchema) : ActionsSchema =
    let getOne name = Result.map2 (fun a b -> b) prepared.Actions.[name]
    let actions = Map.map getOne resolved.Actions
    { Actions = actions }

let resolvedPreparedActions (resolved : ResolvedActions) (prepared : PreparedActions) : ResolvedActions =
    let getOne name resolvedSchema = resolvedPreparedActionsSchema resolvedSchema prepared.Schemas.[name]
    let schemas = Map.map getOne resolved.Schemas
    { Schemas = schemas }