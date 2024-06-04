module OzmaDB.Actions.Run

open FSharpPlus
open Newtonsoft.Json.Linq
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Microsoft.ClearScript
open Newtonsoft.Json

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Objects.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.JavaScript.Json
open OzmaDB.JavaScript.Runtime
open OzmaDB.Actions.Types

type ActionRunException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ActionRunException (message, innerException, isUserException innerException)

    new (message : string) = ActionRunException (message, null, true)

type ActionScript (engine : JSEngine, name : string, scriptSource : string) =
    let func =
        try
            engine.CreateDefaultFunction <| moduleFile name scriptSource
        with
        | :? JavaScriptRuntimeException as e ->
            raisefWithInner ActionRunException e "Couldn't initialize action"

    member this.Run (args : JObject, cancellationToken : CancellationToken) : Task<JObject option> =
        task {
            let argsValue =
                use writer = new V8JsonWriter(engine.Json)
                args.WriteTo(writer)
                Option.get writer.Result
            try
                let! result =  engine.RunAsyncJSFunction(func, [|argsValue|], cancellationToken)
                match result with
                | :? Undefined -> return None
                | _ ->
                    use reader = new V8JsonReader(result)
                    try
                        let result = JToken.ReadFrom(reader) :?> JObject
                        return Some result
                    with
                    | :? JsonReaderException as e -> return raisefWithInner ActionRunException e ""
            with
            | :? JavaScriptRuntimeException as e -> return raisefWithInner ActionRunException e ""
        }

    member this.Runtime = engine

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

type private PreparedActionsBuilder (engine : JSEngine, forceAllowBroken : bool) =
    let prepareActionsSchema (schemaName : SchemaName) (actions : ActionsSchema) : PreparedActionsSchema =
        let prepareOne name (action : ResolvedAction) =
            try
                let script = ActionScript(engine, actionName { Schema = schemaName; Name = name }, action.Function)
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

let prepareActions (engine : JSEngine) (forceAllowBroken : bool) (actions : ResolvedActions) : PreparedActions =
    let eval = PreparedActionsBuilder(engine, forceAllowBroken)
    eval.PrepareActions actions

let private resolvedPreparedActionsSchema (resolved : ActionsSchema) (prepared : PreparedActionsSchema) : ActionsSchema =
    let getOne name = Result.map2 (fun a b -> b) prepared.Actions.[name]
    let actions = Map.map getOne resolved.Actions
    { Actions = actions }

let resolvedPreparedActions (resolved : ResolvedActions) (prepared : PreparedActions) : ResolvedActions =
    let getOne name resolvedSchema = resolvedPreparedActionsSchema resolvedSchema prepared.Schemas.[name]
    let schemas = Map.map getOne resolved.Schemas
    { Schemas = schemas }