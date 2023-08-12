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
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.JavaScript.Runtime

[<SerializeAsObject("error")>]
type APICallErrorInfo =
    | [<CaseKey("call")>] ACECall of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | ACECall msg -> msg

        static member private LookupKey = prepareLookupCaseKey<APICallErrorInfo>
        member this.Error =
            APICallErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = false

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.Message
            member this.HTTPResponseCode = 500
            member this.Error = this.Error

type JavaScriptAPIException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        JavaScriptAPIException (message, innerException, isUserException innerException)

    new (message : string) = JavaScriptAPIException (message, null, true)

// We don't declare these public because we need to fix JSON serialization for these first.
// See https://stackoverflow.com/questions/54169707/f-internal-visibility-changes-record-constructor-behavior
type WriteEventRequest =
    { Details : JToken
    }

type CancelWithRequest =
    { [<DataMember(EmitDefaultValue = false)>]
      UserData : JToken option
      [<DataMember(EmitDefaultValue = false)>]
      Message : string option
    }

type private APIHandle (api : IFunDBAPI, errorConstructor : Value.Function) =
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

    member this.ErrorConstructor = errorConstructor

let inline private throwErrorWithInner (handle : APIHandle) (context : Context) (e : 'a when 'a :> IErrorDetails) (innerException : exn) : 'b =
    let body = V8JsonWriter.Serialize(context, e)
    let exc = handle.ErrorConstructor.NewInstance(body)
    raise <| JSException(e.Message, exc.Value, innerException)

let inline private throwError (handle : APIHandle) (context : Context) (e : 'a when 'a :> IErrorDetails) : 'b =
    throwErrorWithInner handle context e null

let inline private wrapApiCall<'Result, 'Error when 'Error :> IErrorDetails> (handle : APIHandle) (context : Context) (wrap : (unit -> Task<'Result>) -> Task<Result<'Result, 'Error>>) (f : unit -> Task<'Result>) : Task<'Result> =
    task {
        let mutable lockTaken = false
        do! handle.Lock.WaitAsync()
        lockTaken <- true
        try
            let! res = wrap <| fun () ->
                task {
                    handle.Lock.Release()
                    lockTaken <- false
                    let! r = f ()
                    do! handle.Lock.WaitAsync()
                    lockTaken <- true
                    return r
                }
            match res with
            | Ok r -> return r
            | Error e -> return throwError handle context e
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

let inline private runResultApiCall<'a, 'e when 'e :> IErrorDetails> (handle : APIHandle) (context : Context) (f : unit -> Task<Result<'a, 'e>>) : Func<Task<Value.Value>> =
    let run () =
        Task.unmaskableLock handle.Lock <| fun unmask ->
            task {
                let! res = handle.StackLock f
                unmask ()
                match res with
                | Ok r -> return V8JsonWriter.Serialize(context, r)
                | Error e -> return throwError handle context e
            }
    Func<_>(run)

let inline private runVoidResultApiCall<'e when 'e :> IErrorDetails> (handle : APIHandle) (context : Context) (f : unit -> Task<Result<unit, 'e>>) : Func<Task<Value.Value>> =
    let run () =
        Task.unmaskableLock handle.Lock <| fun unmask ->
            task {
                let! res = handle.StackLock f
                unmask ()
                match res with
                | Ok r -> return Value.Undefined.New(context.Isolate)
                | Error e -> return throwError handle context e
            }
    Func<_>(run)

type APITemplate (isolate : Isolate) =
    let mutable currentHandle = None : APIHandle option
    let mutable errorConstructor = None : Value.Function option
    let mutable runtime = Unchecked.defaultof<IJSRuntime>

    let throwCallErrorWithInner (context : Context) (innerException : exn) : StringFormat<'a, 'b> -> 'a =
        let thenRaise str =
            let handle = Option.get currentHandle
            throwErrorWithInner handle context (ACECall str) innerException
        kprintf thenRaise

    let throwCallError (context : Context) = throwCallErrorWithInner context null

    let jsDeserialize (context : Context) (v : Value.Value) : 'a =
        let ret =
            try
                V8JsonReader.Deserialize<'a>(v)
            with
            | :? JsonException as e ->
                throwCallErrorWithInner context e "Failed to parse value: %s" e.Message
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

        let fundbTemplate = ObjectTemplate.New(isolate)
        template.Set("internal", fundbTemplate)

        fundbTemplate.Set("formatFunQLName", FunctionTemplate.New(template.Isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let ret = args.[0].GetString().Get() |> renderFunQLName
            Value.String.New(template.Isolate, ret).Value
        ))

        fundbTemplate.Set("formatFunQLValue", FunctionTemplate.New(template.Isolate, fun args ->
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

        let simpleApiCallTemplate (f : APIHandle -> 'Request -> Task<Result<'Response, 'Error>>) =
            FunctionTemplate.New(isolate, fun args ->
                let context = isolate.CurrentContext
                if args.Length <> 1 then
                    throwCallError context "Number of arguments must be 1"
                let req = jsDeserialize context args.[0] : 'Request
                let handle = Option.get currentHandle
                let run = runResultApiCall handle context <| fun () -> f handle req
                runtime.EventLoop.NewPromise(context, run).Value
            )

        fundbTemplate.Set("getUserView", simpleApiCallTemplate (fun handle -> handle.API.UserViews.GetUserView))
        fundbTemplate.Set("getUserViewInfo", simpleApiCallTemplate (fun handle -> handle.API.UserViews.GetUserViewInfo))

        fundbTemplate.Set("getEntityInfo", simpleApiCallTemplate (fun handle -> handle.API.Entities.GetEntityInfo))
        fundbTemplate.Set("insertEntries", simpleApiCallTemplate (fun handle -> handle.API.Entities.InsertEntries))
        fundbTemplate.Set("updateEntry", simpleApiCallTemplate (fun handle -> handle.API.Entities.UpdateEntry))
        fundbTemplate.Set("deleteEntry", simpleApiCallTemplate (fun handle -> handle.API.Entities.DeleteEntry))
        fundbTemplate.Set("getRelatedEntries", simpleApiCallTemplate (fun handle -> handle.API.Entities.GetRelatedEntries))
        fundbTemplate.Set("recursiveDeleteEntry", simpleApiCallTemplate (fun handle -> handle.API.Entities.RecursiveDeleteEntry))
        fundbTemplate.Set("runCommand", simpleApiCallTemplate (fun handle -> handle.API.Entities.RunCommand))

        fundbTemplate.Set("deferConstraints", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let func = args.[0].GetFunction()
            let handle = Option.get currentHandle
            let run () = wrapApiCall handle context handle.API.Entities.DeferConstraints (fun () -> func.CallAsync(null))
            runtime.EventLoop.NewPromise(context, Func<_>(run)).Value
        ))

        fundbTemplate.Set("pretendRole", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let req = jsDeserialize context args.[0] : PretendRoleRequest
            let func = args.[1].GetFunction()
            let handle = Option.get currentHandle
            let run () = wrapApiCall handle context (handle.API.Request.PretendRole req) (fun () -> func.CallAsync(null))
            runtime.EventLoop.NewPromise(context, Func<_>(run)).Value
        ))

        fundbTemplate.Set("getDomainValues", simpleApiCallTemplate (fun handle -> handle.API.Domains.GetDomainValues))

        fundbTemplate.Set("writeEvent", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let req = jsDeserialize context args.[0] : WriteEventRequest
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {source} wrote event from JavaScript: {details}", handle.API.Request.Source, req.Details)
            handle.API.Request.WriteEvent (fun event ->
                event.Type <- "writeEvent"
                event.Request <- JsonConvert.SerializeObject req
            )
            Value.Undefined.New(isolate)
        ))

        fundbTemplate.Set("writeEventSync", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let req = jsDeserialize context args.[0] : WriteEventRequest
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {source} wrote sync event from JavaScript: {details}", handle.API.Request.Source, req.Details)
            let run = runVoidApiCall handle context <| fun () ->
                handle.API.Request.WriteEventSync (fun event ->
                    event.Type <- "writeEvent"
                    event.Request <- JsonConvert.SerializeObject req
                )
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        template

    let preludeScriptSource = "
const internal = global.internal;
delete global.internal;

const findInnerByKey = (object, key) => {
    for (value of Object.values(object)) {
        if (typeof value === 'object' && value) {
            const currValue = object[key];
            if (currValue !== undefined) {
                return currValue;
            }
            const nestedValue = findInnerByKey(value, key);
            if (nestedValue !== undefined) {
                return nestedValue;
            }
        }
    }
};

class FunDBError extends Error {
  constructor(body) {
    super(body.message);
    Object.assign(this, body);
    // Find `userData` and bring it to the top-level.
    if (!('userData' in body)) {
        const userData = findInnerByKey(body, 'userData');
        if (userData !== undefined) {
            this.userData = userData;
        }
    }
  }
};

global.FunDBError = FunDBError;

global.formatDate = (date) => date.toISOString().split('T')[0];
global.formatFunQLName = internal.formatFunQLName;
global.formatFunQLValue = internal.formatFunQLValue;

// DEPRECATED
global.renderDate = global.formatDate;
global.renderFunQLName = global.formatFunQLName;
global.renderFunQLValue = global.formatFunQLValue;

const normalizeSource = (source) => {
    if (source.ref) {
        return { ...source, ...source.ref };
    } else {
        return source;
    }
};

class FunDB1 {
    getUserView(source, args, chunk) {
        return internal.getUserView({ source: normalizeSource(source), args, chunk });
    };

    getUserViewInfo(source) {
        return internal.getUserViewInfo({ source: normalizeSource(source) });
    };

    getEntityInfo(entity) {
        return internal.getEntityInfo({ entity });
    };

    // DEPRECATED
    insertEntity(entity, fields) {
        return this.insertEntry(entity, fields);
    }

    async insertEntry(entity, fields) {
        try {
            const retIds = await this.insertEntries(entity, [fields]);
            return retIds[0];
        } catch (e) {
            if (e.error === 'transaction') {
                throw new FunDBError(e.inner);
            } else {
                throw e;
            }
        }
    };

    // DEPRECATED
    insertEntities(entity, entries) {
        return this.insertEntries(entity, entries);
    }

    insertEntries(entity, entries) {
        return internal.insertEntries({ entity, entries });
    };

    // DEPRECATED
    updateEntity(entity, id, fields) {
        return this.updateEntry(entity, id, fields);
    };

    updateEntry(entity, id, fields) {
        return internal.updateEntry({ entity, id, fields });
    };

    // DEPRECATED
    deleteEntity(entity, id) {
        return this.deleteEntry(entity, id);
    };

    deleteEntry(entity, id) {
        return internal.deleteEntry({ entity, id });
    };

    // DEPRECATED
    getRelatedEntities(entity, id) {
        return this.getRelatedEntries(entity, id);
    };

    getRelatedEntries(entity, id) {
        return internal.getRelatedEntries({ entity, id });
    };

    // DEPRECATED
    recursiveDeleteEntity(entity, id) {
        return this.recursiveDeleteEntry(entity, id);
    };

    recursiveDeleteEntry(entity, id) {
        return internal.recursiveDeleteEntry({ entity, id });
    };

    runCommand(command, args) {
        return internal.runCommand({ command, args });
    };

    deferConstraints(func) {
        return internal.deferConstraints(func);
    };

    pretendRole(asRole, func) {
        return internal.pretendRole(asRole, func);
    };

    getDomainValues(entity, id, chunk) {
        return internal.getDomainValues({ entity, id, chunk });
    };

    writeEvent(details) {
        return internal.writeEvent({ details });
    };

    writeEventSync(details) {
        return internal.writeEventSync({ details });
    };

    cancelWith(userData, message) {
        throw new FunDBError({ message, userData });
    };
};

global.FunDB = new FunDB1();
    "
    let preludeScript = UnboundScript.Compile(Value.String.New(isolate, preludeScriptSource.Trim()), ScriptOrigin("prelude.js"))

    member this.Isolate = isolate

    member this.SetAPI (api : IFunDBAPI) =
        assert (Option.isNone currentHandle)
        currentHandle <- Some <| APIHandle(api, Option.get errorConstructor)

    member this.ResetAPI api =
        currentHandle <- None

    interface IJavaScriptTemplate with
        member this.ObjectTemplate = template
        member this.FinishInitialization newRuntime context =
            runtime <- newRuntime
            let p = preludeScript.Bind(context)
            ignore <| p.Run()
            errorConstructor <- Some <| context.Global.Get("FunDBError").GetFunction()
