module FunWithFlags.FunDB.API.JavaScript

open Printf
open NetJs
open NetJs.Json
open NetJs.Template
open System
open System.Threading.Tasks
open System.Runtime.Serialization
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.JavaScript.Runtime

[<SerializeAsObject("error")>]
type APICallErrorInfo =
    | [<CaseName("call")>] ACECall of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | ACECall msg -> msg

        interface IAPIError with
            member this.Message = this.Message

type JavaScriptAPIException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        JavaScriptAPIException (message, innerException, isUserException innerException)

    new (message : string) = JavaScriptAPIException (message, null, true)

type JavaScriptUserException (message : string, userData: JToken) =
    inherit UserException(message, null, true, Some userData)

type private APIHandle (api : IFunDBAPI) =
    let logger = api.Request.Context.LoggerFactory.CreateLogger<APIHandle>()
    let mutable lock = None

    member this.API = api
    member this.Logger = logger
    member this.Lock =
        match lock with
        | None ->
            let newLock = Task.AsyncSemaphore(1)
            lock <- Some newLock
            newLock
        | Some l -> l

    member inline this.StackLock (f : unit -> Task<'a>) : Task<'a> =
        task {
            let oldLock = lock
            lock <- None
            try
                return! f ()
            finally
                lock <- oldLock
        }

let inline private wrapApiCall (handle : APIHandle) (wrap : (unit -> Task<'a>) -> Task<'b>) (f : unit -> Task<'a>) : Task<'b> =
    task {
        let mutable lockTaken = false
        do! handle.Lock.WaitAsync()
        lockTaken <- true
        try
            return! wrap <| fun () ->
                task {
                    handle.Lock.Release()
                    lockTaken <- false
                    let! r = f ()
                    do! handle.Lock.WaitAsync()
                    lockTaken <- true
                    return r
                }
        finally
            if lockTaken then
                handle.Lock.Release()
    }

let inline private runVoidApiCall (handle : APIHandle) (context : Context)  (f : unit -> Task) : Func<Task<Value.Value>> =
    let run () =
        Task.lock handle.Lock <| fun () ->
            task {
                do! handle.StackLock <| fun () ->
                    task {
                        do! f ()
                        return ()
                    }
                return Value.Undefined.New(context.Isolate)
            }
    Func<_>(run)

let inline private runResultApiCall<'a, 'e when 'e :> IAPIError> (handle : APIHandle) (context : Context) (f : unit -> Task<Result<'a, 'e>>) : Func<Task<Value.Value>> =
    let run () =
        Task.unmaskableLock handle.Lock <| fun unmask ->
            task {
                let! res = handle.StackLock f
                unmask ()
                match res with
                | Ok r -> return V8JsonWriter.Serialize(context, r)
                | Error e -> return raise <| JavaScriptAPIException(e.Message)
            }
    Func<_>(run)

let inline private runVoidResultApiCall<'e when 'e :> IAPIError> (handle : APIHandle) (context : Context) (f : unit -> Task<Result<unit, 'e>>) : Func<Task<Value.Value>> =
    let run () =
        Task.unmaskableLock handle.Lock <| fun unmask ->
            task {
                let! res = handle.StackLock f
                unmask ()
                match res with
                | Ok r -> return Value.Undefined.New(context.Isolate)
                | Error e -> return raise <| JavaScriptAPIException(e.Message)
            }
    Func<_>(run)

type APITemplate (isolate : Isolate) =
    let mutable currentHandle = None : APIHandle option
    let mutable errorConstructor = None : Value.Function option
    let mutable runtime = Unchecked.defaultof<IJSRuntime>

    let throwError (context : Context) (e : 'a when 'a :> IAPIError) : 'b =
        let body = V8JsonWriter.Serialize(context, e)
        let constructor = Option.get errorConstructor
        let exc = constructor.NewInstance(body)
        raise <| JSException(e.Message, exc.Value)

    let throwCallError (context : Context) : StringFormat<'a, 'b> -> 'a =
        let thenRaise str =
            throwError context (ACECall str)
        kprintf thenRaise

    let jsDeserialize (context : Context) (v : Value.Value) : 'a =
        let ret =
            try
                V8JsonReader.Deserialize<'a>(v)
            with
            | :? JsonException as e -> throwCallError context "Failed to parse value: %s" e.Message
        if isRefNull ret then
            throwCallError context "Value must not be null"
        ret

    let jsInt (context : Context) (v : Value.Value) =
        match v.Data with
        | :? double as d -> int d
        | _ -> throwCallError context "Unexpected value type: %O, expected number" v.ValueType

    let jsString (context : Context) (v : Value.Value) =
        match v.Data with
        | :? Value.String as s -> s.Get()
        | _ -> throwCallError context "Unexpected value type: %O, expected string" v.ValueType

    let template =
        let template = ObjectTemplate.New(isolate)

        template.Set("renderFunQLName", FunctionTemplate.New(template.Isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let ret = args.[0].GetString().Get() |> renderFunQLName
            Value.String.New(template.Isolate, ret).Value
        ))

        template.Set("renderFunQLValue", FunctionTemplate.New(template.Isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            use reader = new V8JsonReader(args.[0])
            let source =
                try
                    JToken.Load(reader)
                with
                | :? JsonReaderException as e -> throwCallError context "Failed to parse value: %s" e.Message
            let ret =
                try
                    renderFunQLJson source
                with
                | Failure msg -> throwCallError context "%s" msg
            Value.String.New(template.Isolate, ret).Value
        ))

        let fundbTemplate = ObjectTemplate.New(isolate)
        template.Set("FunDB", fundbTemplate)

        fundbTemplate.Set("getUserView", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 3 then
                throwCallError context "Number of arguments must be between 1 and 3"
            let source = jsDeserialize context args.[0] : UserViewSource
            let uvArgs =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[1] : RawArguments
                else
                    Map.empty
            let chunk =
                if args.Length >= 3 && args.[2].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[2] : SourceQueryChunk
                else
                    emptyQueryChunk
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.UserViews.GetUserView source uvArgs chunk emptyUserViewFlags
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("getUserViewInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let source = jsDeserialize context args.[0] : UserViewSource
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.UserViews.GetUserViewInfo source emptyUserViewFlags
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("getEntityInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Entities.GetEntityInfo ref
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("insertEntities", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let rowsArgs = jsDeserialize context args.[1] : RawArguments[]
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Entities.InsertEntities ref rowsArgs
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("updateEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 3 then
                throwCallError context "Number of arguments must be 3"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsDeserialize context args.[1] : RawRowKey
            let rawArgs = jsDeserialize context args.[2] : RawArguments
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Entities.UpdateEntity ref id rawArgs
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("deleteEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsDeserialize context args.[1] : RawRowKey
            let handle = Option.get currentHandle
            let run = runVoidResultApiCall handle context <| fun () -> handle.API.Entities.DeleteEntity ref id
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("getRelatedEntities", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsDeserialize context args.[1] : RawRowKey
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Entities.GetRelatedEntities ref id
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("recursiveDeleteEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsDeserialize context args.[1] : RawRowKey
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Entities.RecursiveDeleteEntity ref id
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("runCommand", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 2 then
                throwCallError context "Number of arguments must be between 1 and 2"
            let cmd = jsString context args.[0]
            let cmdArgs =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[1] : RawArguments
                else
                    Map.empty
            let handle = Option.get currentHandle
            let run = runVoidResultApiCall handle context <| fun () -> handle.API.Entities.RunCommand cmd cmdArgs
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("deferConstraints", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let func = args.[0].GetFunction()
            let handle = Option.get currentHandle
            let run () =
                task {
                    let! res =  wrapApiCall handle handle.API.Entities.DeferConstraints (fun () -> func.CallAsync(null))
                    match res with
                    | Ok r -> return r
                    | Error e -> return raise <| JavaScriptRuntimeException(e.Message)
                }
            runtime.EventLoop.NewPromise(context, Func<_>(run)).Value
        ))

        fundbTemplate.Set("pretendRole", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let asRole =
                match args.[0].Data with
                | :? Value.Object -> Some <| (jsDeserialize context args.[0] : ResolvedRoleRef)
                | :? Value.String as str when str.Get() = "root" -> None
                | _ -> throwCallError context "Invalid argument `role`"
            let func = args.[1].GetFunction()
            let handle = Option.get currentHandle
            let wrapper =
                match asRole with
                | None -> handle.API.Request.PretendRoot
                | Some ref -> handle.API.Request.PretendRole ref
            let run () = wrapApiCall handle wrapper (fun () -> func.CallAsync(null))
            runtime.EventLoop.NewPromise(context, Func<_>(run)).Value
        ))

        fundbTemplate.Set("getDomainValues", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 3 then
                throwCallError context "Number of arguments must be between 1 and 3"
            let ref = jsDeserialize context args.[0] : ResolvedFieldRef
            let rowId =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    Some <| int (args.[1].GetNumber())
                else
                    None
            let chunk =
                if args.Length >= 3 && args.[2].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[2] : SourceQueryChunk
                else
                    emptyQueryChunk
            let handle = Option.get currentHandle
            let run = runResultApiCall handle context <| fun () -> handle.API.Domains.GetDomainValues ref rowId chunk emptyDomainFlags
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("writeEvent", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let details = jsString context args.[0]
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {source} wrote event from JavaScript: {details}", handle.API.Request.Source, details)
            handle.API.Request.WriteEvent (fun event ->
                event.Type <- "jsEvent"
                event.Details <- details
            )
            Value.Undefined.New(isolate)
        ))

        fundbTemplate.Set("writeEventSync", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let details = jsString context args.[0]
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {source} wrote sync event from JavaScript: {details}", handle.API.Request.Source, details)
            let run = runVoidApiCall handle context <| fun () ->
                handle.API.Request.WriteEventSync (fun event ->
                    event.Type <- "jsEvent"
                    event.Details <- details
                )
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("cancelWith", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 2 then
                throwCallError context "Number of arguments must be between 1 and 2"
            let userData = jsDeserialize context args.[0] :> JToken
            let message =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    args.[1].GetString().Get()
                else
                    "Operation has been cancelled"
            raise <| JavaScriptUserException(message, userData)
        ))

        template

    let preludeScriptSource = "
class FunDBError extends Error {
  constructor(body) {
    super(body.message);
    this.body = body;
  }
};
global.FunDBError = FunDBError;

global.formatDate = (date) => date.toISOString().split('T')[0];

global.renderDate = global.formatDate;

global.FunDB.insertEntity = async (entityRef, rowArgs) => {
    try {
        const retIds = await FunDB.insertEntities(entityRef, [rowArgs]);
        return retIds[0];
    } catch (e) {
        if (e.body.error === 'transaction') {
            throw new FunDBError(e.body.details);
        } else {
            throw e;
        }
    }
};
    "
    let preludeScript = UnboundScript.Compile(Value.String.New(isolate, preludeScriptSource.Trim()), ScriptOrigin("prelude.js"))

    member this.Isolate = isolate

    member this.SetAPI (api : IFunDBAPI) =
        assert (Option.isNone currentHandle)
        currentHandle <- Some <| APIHandle(api)

    member this.ResetAPI api =
        currentHandle <- None

    interface IJavaScriptTemplate with
        member this.ObjectTemplate = template
        member this.FinishInitialization newRuntime context =
            runtime <- newRuntime
            let p = preludeScript.Bind(context)
            ignore <| p.Run()
            errorConstructor <- Some <| context.Global.Get("FunDBError").GetFunction()
