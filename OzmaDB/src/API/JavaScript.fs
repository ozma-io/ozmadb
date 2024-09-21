module OzmaDB.API.JavaScript

open Printf
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Microsoft.Extensions.Logging
open Microsoft.ClearScript
open Microsoft.ClearScript.JavaScript
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.Affine
open Nito.AsyncEx

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.Exception
open OzmaDB.OzmaQL.Utils
open OzmaDB.API.Types
open OzmaDB.JavaScript.Json
open OzmaDB.JavaScript.Runtime

[<SerializeAsObject("error")>]
type APICallErrorInfo =
    | [<CaseKey("call")>] ACECall of Details: string

    [<DataMember>]
    member this.Message =
        match this with
        | ACECall msg -> msg

    member this.ShouldLog = false

    static member private LookupKey = prepareLookupCaseKey<APICallErrorInfo>
    member this.Error = APICallErrorInfo.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = Map.empty

    interface IErrorDetails with
        member this.Message = this.Message
        member this.LogMessage = this.Message
        member this.HTTPResponseCode = 500
        member this.Error = this.Error

// We don't declare these private because JSON serialization then breaks.
// See https://stackoverflow.com/questions/54169707/f-internal-visibility-changes-record-constructor-behavior
type WriteEventRequest = { Details: JToken }

type private APIHandle(api: IOzmaDBAPI) =
    let logger = api.Request.Context.LoggerFactory.CreateLogger<APIHandle>()
    let mutable lock = AsyncLocal<AsyncLock>()
    do lock.Value <- AsyncLock()

    member this.API = api
    member this.Logger = logger

    member this.Lock() =
        lock.Value.LockAsync(api.Request.Context.CancellationToken)

    member this.UncancellableLock() = lock.Value.LockAsync()


    // We need to support stacking the locks because API functions may call back into JavaScript.
    member inline this.StackLock(f: unit -> Task<'a>) : Task<'a> =
        task {
            use! guard = this.Lock()
            lock.Value <- AsyncLock()
            return! f ()
        }

let private preludeSource =
    """
    const apiProxy = globalThis.apiProxy;
    delete globalThis.apiProxy;
    const unwrapHostResult = globalThis.unwrapHostResult;
    delete globalThis.unwrapHostResult;

    const wrapHostFunction = (func) => (...args) => {
        const ret = func(...args);
        return unwrapHostResult(ret);
    };

    const internal = {};
    for (const key of Object.getOwnPropertyNames(apiProxy)) {
        const value = apiProxy[key];
        if (typeof value === 'function') {
            internal[key] = wrapHostFunction(value);
        }
    }

    const findInnerByKey = (object, key) => {
        for (const value of Object.values(object)) {
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

    class OzmaDBError extends Error {
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

    globalThis.OzmaDBError = OzmaDBError;
    // DEPRECATED
    globalThis.FunDBError = OzmaDBError;

    globalThis.formatDate = (date) => date.toISOString().split('T')[0];
    globalThis.formatOzmaQLName = (arg) => internal.FormatOzmaQLName(arg);
    globalThis.formatOzmaQLValue = (arg) => internal.FormatOzmaQLValue(arg);

    // DEPRECATED
    globalThis.renderDate = globalThis.formatDate;
    globalThis.renderFunQLName = globalThis.formatOzmaQLName;
    globalThis.renderFunQLValue = globalThis.formatOzmaQLValue;
    globalThis.formatFunQLName = globalThis.formatOzmaQLName;
    globalThis.formatFunQLValue = globalThis.formatOzmaQLValue;

    const normalizeSource = (source) => {
        if (source.ref) {
            return { ...source, ...source.ref };
        } else {
            return source;
        }
    };

    class OzmaDBCurrent {
        constructor() {
            Object.freeze(this);
        }

        getUserView(source, args, chunk) {
            return internal.GetUserView({ source: normalizeSource(source), args, chunk });
        };

        getUserViewInfo(source) {
            return internal.GetUserViewInfo({ source: normalizeSource(source) });
        };

        getEntityInfo(entity) {
            return internal.GetEntityInfo({ entity });
        };

        async insertEntry(entity, fields) {
            try {
                const ret = await this.insertEntries(entity, [fields]);
                return ret.entries[0];
            } catch (e) {
                if (e.error === 'transaction') {
                    // We want to keep the stack trace, so we mutate the exception.
                    const inner = e.inner;
                    delete e.operation;
                    delete e.inner;
                    Object.assign(e, inner);
                }
                throw e;
            }
        };

        // DEPRECATED
        async insertEntities(entity, entries) {
            const ret = await this.insertEntries(entity, entries);
            return ret.entries.map(entry => entry.id);
        }

        insertEntries(entity, entries) {
            return internal.InsertEntries({ entity, entries });
        };

        // DEPRECATED
        async updateEntity(entity, id, fields) {
            const ret = await this.updateEntry(entity, id, fields);
            return ret.id;
        };

        updateEntry(entity, id, fields) {
            return internal.UpdateEntry({ entity, id, fields });
        };

        // DEPRECATED
        deleteEntity(entity, id) {
            return this.deleteEntry(entity, id);
        };

        deleteEntry(entity, id) {
            return internal.DeleteEntry({ entity, id });
        };

        // DEPRECATED
        getRelatedEntities(entity, id) {
            return this.getRelatedEntries(entity, id);
        };

        getRelatedEntries(entity, id) {
            return internal.GetRelatedEntries({ entity, id });
        };

        // DEPRECATED
        recursiveDeleteEntity(entity, id) {
            return this.recursiveDeleteEntry(entity, id);
        };

        recursiveDeleteEntry(entity, id) {
            return internal.RecursiveDeleteEntry({ entity, id });
        };

        runCommand(command, args) {
            return internal.RunCommand({ command, args });
        };

        deferConstraints(func) {
            return internal.DeferConstraints(func);
        };

        pretendRole(asRole, func) {
            return internal.PretendRole({ asRole }, func);
        };

        getDomainValues(entity, id, chunk) {
            return internal.GetDomainValues({ entity, id, chunk });
        };

        writeEvent(details) {
            return internal.WriteEvent({ details });
        };

        writeEventSync(details) {
            return internal.WriteEventSync({ details });
        };

        cancelWith(userData, message) {
            throw new OzmaDBError({ message, userData });
        };
    };

    class OzmaDB1 extends OzmaDBCurrent {
        // DEPRECATED
        async insertEntity(entity, fields) {
            const ret = await this.insertEntry(entity, fields);
            return ret.id;
        }

        // DEPRECATED
        async insertEntities(entity, entries) {
            const ret = await this.insertEntries(entity, entries);
            return ret.entries.map(entry => entry.id);
        }

        // DEPRECATED
        async updateEntity(entity, id, fields) {
            const ret = await this.updateEntry(entity, id, fields);
            return ret.id;
        };

        // DEPRECATED
        deleteEntity(entity, id) {
            return this.deleteEntry(entity, id);
        };

        // DEPRECATED
        getRelatedEntities(entity, id) {
            return this.getRelatedEntries(entity, id);
        };

        // DEPRECATED
        recursiveDeleteEntity(entity, id) {
            return this.recursiveDeleteEntry(entity, id);
        };
    };

    globalThis.OzmaDB = new OzmaDB1();
    // DEPRECATED
    globalThis.FunDB = globalThis.OzmaDB;
"""

let private preludeDoc =
    let info = DocumentInfo("ozmadb_prelude.js", Category = ModuleCategory.Standard)
    RuntimeLocal(fun runtime -> runtime.Runtime.Compile(info, preludeSource))

[<DefaultScriptUsage(ScriptAccess.None)>]
type APIProxy(engine: JSEngine) as this =
    let mutable currentHandle = None: APIHandle option

    do
        engine.Engine.AddHostObject("apiProxy", this)
        ignore <| engine.Engine.Evaluate(preludeDoc.GetValue(engine.Runtime))

    let errorConstructor = engine.Engine.Global.["OzmaDBError"] :?> IJavaScriptObject

    member inline private this.ThrowErrorWithInner (e: #IErrorDetails) (innerException: exn) : 'b =
        let body = engine.Json.Serialize(e)
        let exc = errorConstructor.Invoke(true, body) :?> IJavaScriptObject
        raise <| JSException(e.Message, exc, innerException)

    member inline private this.ThrowError(e: #IErrorDetails) : 'b = this.ThrowErrorWithInner e null

    member private this.FormatErrorWithInner (innerException: exn) format =
        let thenRaise str =
            this.ThrowErrorWithInner (ACECall str) innerException

        ksprintf thenRaise format

    member private this.FormatError format = this.FormatErrorWithInner null format

    member private this.Deserialize(v: obj) : 'a =
        let ret =
            try
                V8JsonReader.Deserialize<'a>(v)
            with :? JsonException as e ->
                this.FormatErrorWithInner e "Failed to parse value: %s" e.Message

        if isRefNull ret then
            this.FormatError "Value must not be null"

        ret

    member private this.GetHandle() =
        match currentHandle with
        | Some handle -> handle
        | None -> this.FormatError "This API call cannot be used in this context"

    member inline private this.WrapApiCall<'Result, 'Error when 'Error :> IErrorDetails>
        ([<InlineIfLambda>] wrap: APIHandle -> (unit -> Task<'Result>) -> Task<Result<'Result, 'Error>>)
        ([<InlineIfLambda>] f: APIHandle -> Task<'Result>)
        : Task<'Result> =
        task {
            let handle = this.GetHandle()

            let! ret =
                task {
                    let! firstLock = handle.Lock()
                    let mutable lock = firstLock

                    try
                        return!
                            wrap handle
                            <| fun () ->
                                lock.Dispose()

                                let inline lockBack () =
                                    unitTask {
                                        let! newLock = handle.UncancellableLock()
                                        lock <- newLock
                                    }

                                Task.deferAsync lockBack (fun () -> f handle)
                    finally
                        lock.Dispose()
                }

            match ret with
            | Ok r -> return r
            | Error e -> return this.ThrowError e
        }

    member inline private this.RunVoidApiCall([<InlineIfLambda>] f: APIHandle -> Task) : Task =
        unitTask {
            let handle = this.GetHandle()

            do!
                handle.StackLock
                <| fun () ->
                    task {
                        do! f handle
                        return ()
                    }
        }

    member inline private this.RunResultApiCall<'a, 'e when 'e :> IErrorDetails>
        ([<InlineIfLambda>] f: APIHandle -> Task<Result<'a, 'e>>)
        : Task<obj> =
        task {
            let handle = this.GetHandle()
            let! res = handle.StackLock <| fun () -> f handle

            match res with
            | Ok r -> return engine.Json.Serialize(r)
            | Error e -> return this.ThrowError e
        }

    member inline private this.RunVoidResultApiCall<'e when 'e :> IErrorDetails>
        ([<InlineIfLambda>] f: APIHandle -> Task<Result<unit, 'e>>)
        : Task =
        unitTask {
            let handle = this.GetHandle()
            let! res = handle.StackLock <| fun () -> f handle

            match res with
            | Ok r -> ()
            | Error e -> this.ThrowError e
        }

    member inline private this.SimpleApiCall
        (arg: obj)
        ([<InlineIfLambda>] f: APIHandle -> 'Request -> Task<Result<'Response, 'Error>>)
        =
        engine.WrapAsyncHostFunction
        <| fun () ->
            let req = this.Deserialize arg: 'Request
            this.RunResultApiCall <| fun handle -> f handle req

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.FormatOzmaQLName(name: string) =
        engine.WrapHostFunction <| fun () -> renderOzmaQLName name

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.FormatOzmaQLValue(arg: obj) =
        engine.WrapHostFunction
        <| fun () ->
            use reader = new V8JsonReader(arg)

            let source =
                try
                    JToken.Load(reader)
                with :? JsonReaderException as e ->
                    this.FormatError "Failed to parse value: %s" e.Message

            try
                renderOzmaQLJson source
            with Failure msg ->
                this.FormatError "%s" msg

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetUserView arg =
        this.SimpleApiCall arg (fun handle -> handle.API.UserViews.GetUserView)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetUserViewInfo arg =
        this.SimpleApiCall arg (fun handle -> handle.API.UserViews.GetUserViewInfo)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetEntityInfo arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.GetEntityInfo)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.InsertEntries arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.InsertEntries)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.UpdateEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.UpdateEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GeleteEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.DeleteEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetRelatedEntries arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.GetRelatedEntries)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.RecursiveDeleteEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.RecursiveDeleteEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.RunCommand arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.RunCommand)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.DeferConstraints(f: IJavaScriptObject) =
        engine.WrapAsyncHostFunction
        <| fun () ->
            this.WrapApiCall(fun handle -> handle.API.Entities.DeferConstraints)
            <| fun handle -> engine.RunAsyncJSFunction(f, [||])

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.PretendRole (req: obj) (f: IJavaScriptObject) =
        engine.WrapAsyncHostFunction
        <| fun () ->
            let req = this.Deserialize req: PretendRoleRequest

            this.WrapApiCall(fun handle -> handle.API.Request.PretendRole req)
            <| fun handle -> engine.RunAsyncJSFunction(f, [||])

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetDomainValues arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Domains.GetDomainValues)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.WriteEvent(arg: obj) =
        engine.WrapHostFunction
        <| fun () ->
            let req = this.Deserialize arg: WriteEventRequest
            let handle = this.GetHandle()

            handle.Logger.LogInformation(
                "Source {source} wrote event from JavaScript: {details}",
                handle.API.Request.Source,
                req.Details.ToString()
            )

            handle.API.Request.WriteEvent(fun event ->
                event.Type <- "writeEvent"
                event.Request <- JsonConvert.SerializeObject req)

            Undefined.Value

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.WriteEventSync(arg: obj) =
        let req = this.Deserialize arg: WriteEventRequest

        this.RunVoidApiCall
        <| fun handle ->
            handle.Logger.LogInformation(
                "Source {source} wrote sync event from JavaScript: {details}",
                handle.API.Request.Source,
                req.Details.ToString()
            )

            handle.API.Request.WriteEventSync(fun event ->
                event.Type <- "writeEvent"
                event.Request <- JsonConvert.SerializeObject req)

    member this.SetAPI(api: IOzmaDBAPI) =
        assert (Option.isNone currentHandle)
        currentHandle <- Some <| APIHandle(api)

    member this.ResetAPI api = currentHandle <- None

    member this.Engine = engine
