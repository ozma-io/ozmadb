module FunWithFlags.FunDB.Triggers.Run

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs
open NetJs.Json
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Objects.Types

type TriggerRunException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        TriggerRunException (message, innerException, isUserException innerException)

    new (message : string) = TriggerRunException (message, null, true)

[<SerializeAsObject("type")>]
type SerializedTriggerSource =
    | [<CaseKey("insert")>] TSInsert of Id : int option
    | [<CaseKey("update")>] TSUpdate of Id : int
    | [<CaseKey("delete")>] TSDelete of Id : int option

type SerializedTriggerEvent =
    { Entity : ResolvedEntityRef
      Time : TriggerTime
      Source : SerializedTriggerSource
    }

type ArgsTriggerResult =
    | ATTouched of RawArguments
    | ATUntouched
    | ATCancelled

type TriggerScript (runtime : IJSRuntime, name : string, scriptSource : string) =
    let func =
        try
            runtime.CreateDefaultFunction <| moduleFile name scriptSource
        with
        | :? JavaScriptRuntimeException as e ->
            raisefUserWithInner TriggerRunException e ""

    let runArgsTrigger (entity : ResolvedEntityRef) (source : SerializedTriggerSource) (args : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        task {
            let event =
                { Entity = entity
                  Time = TTBefore
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(runtime.Context, event)
            let oldArgs = V8JsonWriter.Serialize(runtime.Context, args)
            let! newArgs =
                task {
                    try
                        return! runAsyncFunctionInRuntime runtime func cancellationToken [|eventValue; oldArgs|]
                    with
                    | :? JavaScriptRuntimeException as e ->
                        return raisefWithInner TriggerRunException e ""
                }
            match newArgs.Data with
            | :? bool as ret -> return (if ret then ATUntouched else ATCancelled)
            | _ ->
                let ret =
                    try
                        V8JsonReader.Deserialize(newArgs)
                    with
                    | :? JsonReaderException as e -> raisefUserWithInner TriggerRunException e "Failed to parse return value"
                if isRefNull ret then
                    raisef TriggerRunException "Return value must not be null"
                return ATTouched ret
        }

    let runAfterTrigger (entity : ResolvedEntityRef) (source : SerializedTriggerSource) (args : LocalArgumentsMap option) (cancellationToken : CancellationToken) : Task =
        unitTask {
            let event =
                { Entity = entity
                  Time = TTAfter
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(runtime.Context, event)
            let functionArgs =
                match args with
                | Some oldArgsObj ->
                    let oldArgs = V8JsonWriter.Serialize(runtime.Context, oldArgsObj)
                    [|eventValue; oldArgs|]
                | None -> [|eventValue|]
            try
                let! _ = runAsyncFunctionInRuntime runtime func cancellationToken functionArgs
                return ()
            with
            | :? JavaScriptRuntimeException as e ->
                return raisefWithInner TriggerRunException e ""
        }

    member this.RunInsertTriggerBefore (entity : ResolvedEntityRef) (args : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        runArgsTrigger entity (TSInsert None) args cancellationToken

    member this.RunUpdateTriggerBefore (entity : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        runArgsTrigger entity (TSUpdate id) args cancellationToken

    member this.RunDeleteTriggerBefore (entity : ResolvedEntityRef) (id : int) (cancellationToken : CancellationToken) : Task<bool> =
        task {
            let event =
                { Entity = entity
                  Time = TTBefore
                  Source = TSDelete (Some id)
                }
            let eventValue = V8JsonWriter.Serialize(runtime.Context, event)
            let! maybeContinue =
                task {
                    try
                        return! runAsyncFunctionInRuntime runtime func cancellationToken [|eventValue|]
                    with
                    | :? JavaScriptRuntimeException as e ->
                        return raisefWithInner TriggerRunException e ""
                }
            try
                return maybeContinue.GetBoolean ()
            with
            | :? NetJsException as e ->
                return raisefUserWithInner TriggerRunException e "Invalid return value for trigger"
        }

    member this.RunInsertTriggerAfter (entity : ResolvedEntityRef) (newId : int) (args : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task =
        runAfterTrigger entity (TSInsert (Some newId)) (Some args) cancellationToken

    member this.RunUpdateTriggerAfter (entity : ResolvedEntityRef) (id : int) (args : LocalArgumentsMap) (cancellationToken : CancellationToken) : Task=
        runAfterTrigger entity (TSUpdate id) (Some args) cancellationToken

    member this.RunDeleteTriggerAfter (entity : ResolvedEntityRef) (cancellationToken : CancellationToken) : Task =
        runAfterTrigger entity (TSDelete None) None cancellationToken

    member this.Runtime = runtime

type PreparedTrigger =
    { Resolved : ResolvedTrigger
      Script : TriggerScript
    }

type PreparedTriggersEntity =
    { Triggers : Map<TriggerName, PossiblyBroken<PreparedTrigger>>
    }

type PreparedTriggersSchema =
    { Entities : Map<TriggerName, PreparedTriggersEntity>
    }

type PreparedTriggersDatabase =
    { Schemas : Map<SchemaName, PreparedTriggersSchema>
    }

type PreparedTriggers =
    { Schemas : Map<SchemaName, PreparedTriggersDatabase>
    } with
        member this.FindTrigger (ref : TriggerRef) : PreparedTrigger option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun db -> Map.tryFind ref.Entity.Schema db.Schemas)
                |> Option.bind (fun schema -> Map.tryFind ref.Entity.Name schema.Entities)
                |> Option.bind (fun entity -> Map.tryFind ref.Name entity.Triggers)
                |> Option.bind Result.getOption

let private triggerName (triggerRef : TriggerRef) =
    sprintf "triggers/%O/%O/%O/%O.mjs" triggerRef.Schema triggerRef.Entity.Schema triggerRef.Entity.Name triggerRef.Name

type private PreparedTriggersBuilder (runtime : IJSRuntime, forceAllowBroken : bool) =
    let prepareTriggersEntity (schemaName : SchemaName) (triggerEntity : ResolvedEntityRef) (triggers : TriggersEntity) : PreparedTriggersEntity =
        let prepareOne name (trigger : ResolvedTrigger) =
            let triggerRef = { Schema = schemaName; Entity = triggerEntity; Name = name }
            try
                let script = TriggerScript(runtime, triggerName triggerRef, trigger.Procedure)
                Ok { Resolved = trigger; Script = script }
            with
            | :? TriggerRunException as e when trigger.AllowBroken || forceAllowBroken ->
                Error { Error = e; AllowBroken = trigger.AllowBroken }
            | e -> raisefWithInner TriggerRunException e "In trigger %O" triggerRef

        { Triggers = triggers.Triggers |> Map.map (fun name -> Result.bind (prepareOne name))
        }

    let prepareTriggersSchema (schemaName : SchemaName) (triggerSchema : SchemaName) (triggers : TriggersSchema) : PreparedTriggersSchema =
        let getOne name = prepareTriggersEntity schemaName { Schema = triggerSchema; Name = name }
        { Entities = Map.map getOne triggers.Entities
        }

    let prepareTriggersDatabase (schemaName : SchemaName) (triggers : TriggersDatabase) : PreparedTriggersDatabase =
        { Schemas = Map.map (prepareTriggersSchema schemaName) triggers.Schemas
        }

    let prepareTriggers (triggers : ResolvedTriggers) : PreparedTriggers =
        { Schemas = Map.map prepareTriggersDatabase triggers.Schemas
        }

    member this.PrepareTriggers triggers = prepareTriggers triggers

let prepareTriggers (runtime : IJSRuntime) (forceAllowBroken : bool) (triggers : ResolvedTriggers) =
    let builder = PreparedTriggersBuilder(runtime, forceAllowBroken)
    builder.PrepareTriggers triggers
