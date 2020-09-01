module FunWithFlags.FunDB.API.Triggers

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs
open NetJs.Value
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.JavaScript

type TriggerRunException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = TriggerRunException (message, null)

[<SerializeAsObject("type")>]
type SerializedTriggerSource =
    | [<CaseName("insert")>] TSInsert of NewId : int option
    | [<CaseName("update")>] TSUpdate of Id : int
    | [<CaseName("delete")>] TSDelete of Id : int option

type SerializedTriggerEvent =
    { Entity : ResolvedEntityRef
      Time : TriggerTime
      Source : SerializedTriggerSource
    }

type TriggerScript (template : APITemplate, name : string, scriptSource : string) =
    let func =
        try
            CachedFunction.FromScript template name scriptSource
        with
        | :? NetJsException as e ->
            raisefWithInner TriggerRunException e "Couldn't initialize trigger"

    let runArgsTrigger (entity : ResolvedEntityRef) (source : SerializedTriggerSource) (args : EntityArguments) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        task {
            let event =
                { Entity = entity
                  Time = TTBefore
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(func.Context, event)
            let oldArgs = V8JsonWriter.Serialize(func.Context, args)
            try
                let newArgs = func.Function.Call(cancellationToken, null, [|eventValue; oldArgs|])
                do! template.Isolate.EventLoop.Run ()
                let newArgs = newArgs.GetValueOrPromiseResult ()
                return
                    match newArgs.Data with
                    | :? bool as ret -> if ret then ATUntouched else ATCancelled
                    | _ -> ATTouched <| V8JsonReader.Deserialize(newArgs)
            with
            | :? JSException as e ->
                return raisefWithInner TriggerRunException e "Unhandled exception in trigger:\n%s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner TriggerRunException e "Failed to run trigger"
        }

    let runAfterTrigger (entity : ResolvedEntityRef)(source : SerializedTriggerSource) (args : EntityArguments option) (cancellationToken : CancellationToken) : Task =
        unitTask {
            let event =
                { Entity = entity
                  Time = TTAfter
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(func.Context, event)
            try
                let functionArgs =
                    match args with
                    | Some oldArgsObj ->
                        let oldArgs = V8JsonWriter.Serialize(func.Context, oldArgsObj)
                        [|eventValue; oldArgs|]
                    | None -> [|eventValue|]
                let maybePromise = func.Function.Call(cancellationToken, null, functionArgs)
                do! template.Isolate.EventLoop.Run ()
                ignore <| maybePromise.GetValueOrPromiseResult ()
            with
            | :? JSException as e ->
                return raisefWithInner TriggerRunException e "Unhandled exception in trigger:\n%s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner TriggerRunException e "Failed to run trigger"
        }

    member this.RunInsertTriggerBefore (entity : ResolvedEntityRef) (args : EntityArguments) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        runArgsTrigger entity (TSInsert None) args cancellationToken

    member this.RunUpdateTriggerBefore (entity : ResolvedEntityRef) (id : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task<ArgsTriggerResult> =
        runArgsTrigger entity (TSUpdate id) args cancellationToken

    member this.RunDeleteTriggerBefore (entity : ResolvedEntityRef) (id : int) (cancellationToken : CancellationToken) : Task<bool> =
        task {
            let event =
                { Entity = entity
                  Time = TTBefore
                  Source = TSDelete (Some id)
                }
            let eventValue = V8JsonWriter.Serialize(func.Context, event)
            try
                let maybeContinue = func.Function.Call(cancellationToken, null, [|eventValue|])
                do! template.Isolate.EventLoop.Run ()
                let maybeContinue = maybeContinue.GetValueOrPromiseResult ()
                return maybeContinue.GetBoolean ()
            with
            | :? JSException as e ->
                return raisefWithInner TriggerRunException e "Unhandled exception in trigger:\n%s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner TriggerRunException e "Failed to run trigger"
        }
    member this.RunInsertTriggerAfter (entity : ResolvedEntityRef) (newId : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task =
        runAfterTrigger entity (TSInsert (Some newId)) (Some args) cancellationToken

    member this.RunUpdateTriggerAfter (entity : ResolvedEntityRef) (id : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task=
        runAfterTrigger entity (TSUpdate id) (Some args) cancellationToken

    member this.RunDeleteTriggerAfter (entity : ResolvedEntityRef) (cancellationToken : CancellationToken) : Task =
        runAfterTrigger entity (TSDelete None) None cancellationToken

    member this.Context = func.Context

    interface ITriggerScript with
        member this.RunInsertTriggerBefore entity args cancellationToken = this.RunInsertTriggerBefore entity args cancellationToken
        member this.RunUpdateTriggerBefore entity id args cancellationToken = this.RunUpdateTriggerBefore entity id args cancellationToken
        member this.RunDeleteTriggerBefore entity id cancellationToken = this.RunDeleteTriggerBefore entity id cancellationToken

        member this.RunInsertTriggerAfter entity newId args cancellationToken = this.RunInsertTriggerAfter entity newId args cancellationToken
        member this.RunUpdateTriggerAfter entity id args cancellationToken = this.RunUpdateTriggerAfter entity id args cancellationToken
        member this.RunDeleteTriggerAfter entity cancellationToken = this.RunDeleteTriggerAfter entity cancellationToken


type TriggerScriptsEntity =
    { Triggers : Map<TriggerName, ITriggerScript>
    }

type TriggerScriptsSchema =
    { Entities : Map<TriggerName, TriggerScriptsEntity>
    }

type TriggerScriptsDatabase =
    { Schemas : Map<SchemaName, TriggerScriptsSchema>
    }

type TriggerScripts =
    { Schemas : Map<SchemaName, TriggerScriptsDatabase>
    } with
        member this.FindTrigger (ref : TriggerRef) : ITriggerScript option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun db -> Map.tryFind ref.Entity.schema db.Schemas)
                |> Option.bind (fun schema -> Map.tryFind ref.Entity.name schema.Entities)
                |> Option.bind (fun entity -> Map.tryFind ref.Name entity.Triggers)

let private triggerName (triggerRef : TriggerRef) =
    sprintf "%O/triggers/%O/%O/%O.mjs" triggerRef.Schema triggerRef.Entity.schema triggerRef.Entity.name triggerRef.Name

let private prepareTriggerScriptsEntity (template : APITemplate) (schemaName : SchemaName) (triggerEntity : ResolvedEntityRef) (triggers : TriggersEntity) : TriggerScriptsEntity =
    let getOne name = function
    | Error e -> None
    | Ok trigger -> Some (TriggerScript(template, triggerName { Schema = schemaName; Entity = triggerEntity; Name = name }, trigger.Procedure) :> ITriggerScript)

    { Triggers = Map.mapMaybe getOne triggers.Triggers
    }

let private prepareTriggerScriptsSchema (template : APITemplate) (schemaName : SchemaName) (triggerSchema : SchemaName) (triggers : TriggersSchema) : TriggerScriptsSchema =
    let getOne name = prepareTriggerScriptsEntity template schemaName { schema = triggerSchema; name = name }
    { Entities = Map.map getOne triggers.Entities
    }

let private prepareTriggerScriptsDatabase (template : APITemplate) (schemaName : SchemaName) (triggers : TriggersDatabase) : TriggerScriptsDatabase =
    { Schemas = Map.map (prepareTriggerScriptsSchema template schemaName) triggers.Schemas
    }

let prepareTriggerScripts (template : APITemplate) (triggers : ResolvedTriggers) : TriggerScripts =
    { Schemas = Map.map (prepareTriggerScriptsDatabase template) triggers.Schemas
    }

type private TestTriggerEvaluator (forceAllowBroken : bool) =
    let isolate = Isolate.NewWithHeapSize (1024UL * 1024UL, 16UL * 1024UL * 1024UL)
    let apiTemplate = APITemplate(isolate)

    let testTrigger (triggerRef : TriggerRef) (trigger : ResolvedTrigger) : unit =
        ignore <| TriggerScript(apiTemplate, triggerName triggerRef, trigger.Procedure)

    let testTriggersEntity (schemaName : SchemaName) (triggerEntity : ResolvedEntityRef) (sourceTriggers : SourceTriggersEntity) (entityTriggers : TriggersEntity) : ErroredTriggersEntity * TriggersEntity =
        let mutable errors = Map.empty

        let mapTrigger name (trigger : ResolvedTrigger) =
            try
                try
                    let ref = { Schema = schemaName; Entity = triggerEntity; Name = name }
                    testTrigger ref trigger
                    Ok trigger
                with
                | :? TriggerRunException as e when trigger.AllowBroken || forceAllowBroken ->
                    errors <- Map.add name (e :> exn) errors
                    Error { Source = Map.find name sourceTriggers.Triggers; Error = e }
            with
            | :? TriggerRunException as e -> raisefWithInner TriggerRunException e.InnerException "Error in trigger %O: %s" name e.Message

        let ret =
            { Triggers = entityTriggers.Triggers |> Map.map (fun name -> Result.bind (mapTrigger name))
            } : TriggersEntity
        (errors, ret)

    let testTriggersSchema (schemaName : SchemaName) (triggerSchema : SchemaName) (sourceTriggers : SourceTriggersSchema) (schemaTriggers : TriggersSchema) : ErroredTriggersSchema * TriggersSchema =
        let mutable errors = Map.empty

        let mapEntity name entityTriggers =
            try
                let ref = { schema = triggerSchema; name = name }
                let (entityErrors, newEntity) = testTriggersEntity schemaName ref (Map.find name sourceTriggers.Entities) entityTriggers
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? TriggerRunException as e -> raisefWithInner TriggerRunException e.InnerException "Error in triggers entity %O: %s" name e.Message

        let ret =
            { Entities = schemaTriggers.Entities |> Map.map mapEntity
            } : TriggersSchema
        (errors, ret)

    let testTriggersDatabase (schemaName : SchemaName) (sourceTriggers : SourceTriggersDatabase) (db : TriggersDatabase) : ErroredTriggersDatabase * TriggersDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaTriggers =
            try
                let (schemaErrors, newSchema) = testTriggersSchema schemaName name (Map.find name sourceTriggers.Schemas) schemaTriggers
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                newSchema
            with
            | :? TriggerRunException as e -> raisefWithInner TriggerRunException e.InnerException "Error in triggers schema %O: %s" name e.Message

        let ret =
            { Schemas = db.Schemas |> Map.map mapSchema
            } : TriggersDatabase
        (errors, ret)

    let testTriggers (sourceTriggers : SourceTriggers) (triggers : ResolvedTriggers) : ErroredTriggers * ResolvedTriggers =
        let mutable errors = Map.empty

        let mapDatabase name db =
            try
                let (dbErrors, newDb) = testTriggersDatabase name (Map.find name sourceTriggers.Schemas) db
                if not <| Map.isEmpty dbErrors then
                    errors <- Map.add name dbErrors errors
                newDb
            with
            | :? TriggerRunException as e -> raisefWithInner TriggerRunException e.InnerException "Error in schema %O: %s" name e.Message

        let ret =
            { Schemas = triggers.Schemas |> Map.map mapDatabase
            } : ResolvedTriggers
        (errors, ret)

    member this.TestTriggers sourceTriggers triggers = testTriggers sourceTriggers triggers

let testEvalTriggers (forceAllowBroken : bool) (source : SourceTriggers) (triggers : ResolvedTriggers) : ErroredTriggers * ResolvedTriggers =
    let eval = TestTriggerEvaluator (forceAllowBroken)
    eval.TestTriggers source triggers
