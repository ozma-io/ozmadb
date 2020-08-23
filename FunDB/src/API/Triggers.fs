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
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Operations.Entity
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.API.JavaScript

type TriggerRunException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = TriggerRunException (message, null)

let private triggerFunction = "handleEvent"

[<SerializeAsObject("type")>]
type SerializedTriggerSource =
    | [<CaseName("insert")>] TSInsert of NewId : int option * Args : EntityArguments
    | [<CaseName("update")>] TSUpdate of Id : int * Args : EntityArguments
    | [<CaseName("delete")>] TSDelete of Id : int option

type SerializedTriggerEvent =
    { Entity : ResolvedEntityRef
      Time : TriggerTime
      Source : SerializedTriggerSource
    }

type TriggerScript (template : APITemplate, scriptSource : string) =
    let func =
        try
            CachedFunction.FromScript(template.Isolate, template.Template, scriptSource, triggerFunction)
        with
        | :? NetJsException as e ->
            raisefWithInner TriggerRunException e "Couldn't initialize trigger"

    let runArgsTrigger (entity : ResolvedEntityRef) (source : SerializedTriggerSource) (cancellationToken : CancellationToken) : Task<RawArguments option> =
        task {
            let event =
                { Entity = entity
                  Time = TTBefore
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(func.Context, event)
            try
                let newArgs = func.Function.Call(cancellationToken, null, [|eventValue|])
                do! template.Isolate.EventLoop.Run ()
                let newArgs = newArgs.GetValueOrPromiseResult ()
                if newArgs.ValueType = ValueType.Undefined then
                    return None
                else
                    return V8JsonReader.Deserialize(newArgs)
            with
            | :? JSException as e ->
                return raisefWithInner TriggerRunException e "Unhandled exception in trigger: %s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner TriggerRunException e "Failed to run trigger"
        }

    let runVoidTrigger (entity : ResolvedEntityRef) (time : TriggerTime) (source : SerializedTriggerSource) (cancellationToken : CancellationToken) : Task =
        unitTask {
            let event =
                { Entity = entity
                  Time = time
                  Source = source
                }
            let eventValue = V8JsonWriter.Serialize(func.Context, event)
            try
                let maybePromise = func.Function.Call(cancellationToken, null, [|eventValue|])
                do! template.Isolate.EventLoop.Run ()
                ignore <| maybePromise.GetValueOrPromiseResult ()
            with
            | :? JSException as e ->
                return raisefWithInner TriggerRunException e "Unhandled exception in trigger: %s" (e.JSStackTrace.ToPrettyString())
            | :? NetJsException as e ->
                return raisefWithInner TriggerRunException e "Failed to run trigger"
        }

    member this.RunInsertTriggerBefore (entity : ResolvedEntityRef) (args : EntityArguments) (cancellationToken : CancellationToken) : Task<RawArguments option> =
        runArgsTrigger entity (TSInsert (None, args)) cancellationToken

    member this.RunUpdateTriggerBefore (entity : ResolvedEntityRef) (id : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task<RawArguments option> =
        runArgsTrigger entity (TSUpdate (id, args)) cancellationToken

    member this.RunDeleteTriggerBefore (entity : ResolvedEntityRef) (id : int) (cancellationToken : CancellationToken) : Task =
        runVoidTrigger entity TTBefore (TSDelete (Some id)) cancellationToken

    member this.RunInsertTriggerAfter (entity : ResolvedEntityRef) (newId : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task =
        runVoidTrigger entity TTAfter (TSInsert (Some newId, args)) cancellationToken

    member this.RunUpdateTriggerAfter (entity : ResolvedEntityRef) (id : int) (args : EntityArguments) (cancellationToken : CancellationToken) : Task=
        runVoidTrigger entity TTAfter (TSUpdate (id, args)) cancellationToken

    member this.RunDeleteTriggerAfter (entity : ResolvedEntityRef) (cancellationToken : CancellationToken) : Task =
        runVoidTrigger entity TTAfter (TSDelete None) cancellationToken

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

let private prepareTriggerScriptsEntity (template : APITemplate) (triggers : TriggersEntity) : TriggerScriptsEntity =
    let getOne name = function
    | Error e -> None
    | Ok trigger -> Some (TriggerScript(template, trigger.Procedure) :> ITriggerScript)

    { Triggers = Map.mapMaybe getOne triggers.Triggers
    }

let private prepareTriggerScriptsSchema (template : APITemplate) (triggers : TriggersSchema) : TriggerScriptsSchema =
    { Entities = Map.map (fun name -> prepareTriggerScriptsEntity template) triggers.Entities
    }

let private prepareTriggerScriptsDatabase (template : APITemplate) (triggers : TriggersDatabase) : TriggerScriptsDatabase =
    { Schemas = Map.map (fun name -> prepareTriggerScriptsSchema template) triggers.Schemas
    }

let prepareTriggerScripts (template : APITemplate) (triggers : ResolvedTriggers) : TriggerScripts =
    { Schemas = Map.map (fun name -> prepareTriggerScriptsDatabase template) triggers.Schemas
    }

type private TestTriggerEvaluator (forceAllowBroken : bool) =
    let isolate = Isolate.NewWithHeapSize (1024UL * 1024UL, 16UL * 1024UL * 1024UL)
    let apiTemplate = APITemplate(isolate)

    let testTrigger (trigger : ResolvedTrigger) : unit =
        ignore <| TriggerScript(apiTemplate, trigger.Procedure)

    let testTriggersEntity (sourceTriggers : SourceTriggersEntity) (entityTriggers : TriggersEntity) : ErroredTriggersEntity * TriggersEntity =
        let mutable errors = Map.empty

        let mapTrigger name (trigger : ResolvedTrigger) =
            try
                try
                    testTrigger trigger
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

    let testTriggersSchema (sourceTriggers : SourceTriggersSchema) (schemaTriggers : TriggersSchema) : ErroredTriggersSchema * TriggersSchema =
        let mutable errors = Map.empty

        let mapEntity name entityTriggers =
            try
                let (entityErrors, newEntity) = testTriggersEntity (Map.find name sourceTriggers.Entities) entityTriggers
                if not <| Map.isEmpty entityErrors then
                    errors <- Map.add name entityErrors errors
                newEntity
            with
            | :? TriggerRunException as e -> raisefWithInner TriggerRunException e.InnerException "Error in triggers entity %O: %s" name e.Message

        let ret =
            { Entities = schemaTriggers.Entities |> Map.map mapEntity
            } : TriggersSchema
        (errors, ret)

    let testTriggersDatabase (sourceTriggers : SourceTriggersDatabase) (db : TriggersDatabase) : ErroredTriggersDatabase * TriggersDatabase =
        let mutable errors = Map.empty

        let mapSchema name schemaTriggers =
            try
                let (schemaErrors, newSchema) = testTriggersSchema (Map.find name sourceTriggers.Schemas) schemaTriggers
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
                let (dbErrors, newDb) = testTriggersDatabase (Map.find name sourceTriggers.Schemas) db
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
