module OzmaDB.JavaScript.Runtime

open System
open System.Threading
open System.Threading.Tasks
open System.Runtime.CompilerServices
open System.Runtime.ExceptionServices
open System.Collections.Generic
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Microsoft.ClearScript
open Microsoft.ClearScript.JavaScript
open Microsoft.ClearScript.V8
open Serilog

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.JavaScript.Json

type JavaScriptRuntimeException
    (
        message: string,
        innerException: exn,
        isUserException: bool,
        userData: JToken option,
        messageContainsInnerError: bool
    ) =
    inherit UserException(message, innerException, isUserException, userData)

    new(message: string, innerException: exn, isUserException: bool) =
        JavaScriptRuntimeException(message, innerException, isUserException, userExceptionData innerException, false)

    new(message: string, innerException: exn) =
        JavaScriptRuntimeException(
            message,
            innerException,
            isUserException innerException,
            userExceptionData innerException,
            false
        )

    new(message: string) = JavaScriptRuntimeException(message, null, true, None, false)

    interface ICustomFormatException with
        member this.MessageContainsInnerError = messageContainsInnerError

type Path = POSIXPath.Path
type Source = string

type ModuleFile =
    { Path: Path
      Source: Source
      AllowBroken: bool }

let moduleFile (path: Path) (source: Source) =
    { Path = path
      Source = source
      AllowBroken = false }

type private VirtualFSModule =
    { Path: Path
      DirPath: Path
      Module: V8Script }

    interface IDisposable with
        member this.Dispose() = this.Module.Dispose()

let private pathAliases (path: Path) =
    seq {
        path
        POSIXPath.trimExtension path
    }

type JSEnvironment =
    { Files: ModuleFile seq
      SearchPath: Path seq }

let private convertPath (p: Path) =
    let normalized = POSIXPath.normalize p

    if POSIXPath.isAbsolute normalized then
        failwithf "Absolute path is not expected: %s" p

    normalized

type private JSDocumentLoader(env: JSEnvironment) =
    inherit DocumentLoader()

    let searchPath =
        env.SearchPath |> Seq.map convertPath |> Seq.distinct |> Seq.toArray

    let makeModuleDocuments (modul: ModuleFile) =
        let info = DocumentInfo(modul.Path, Category = ModuleCategory.Standard)
        let document = StringDocument(info, modul.Source)
        Seq.map (fun npath -> (npath, document)) (pathAliases modul.Path)

    let documentsMap = env.Files |> Seq.collect makeModuleDocuments |> Map.ofSeqUnique


    // ClearScript doesn't allow to get an evaluated module and just returns `undefined`.
    // So to import a module, we need to inject it into the loader, and remove later.
    // Ugh.
    let currentModule = new ThreadLocal<StringDocument>()

    member this.WithCurrentModuleOnce(doc: StringDocument, f: unit -> 'a) : 'a =
        let oldCurrentModule = currentModule.Value

        try
            currentModule.Value <- doc
            f ()
        finally
            currentModule.Value <- oldCurrentModule

    override this.LoadDocument
        (
            settings: DocumentSettings,
            sourceInfo: Nullable<DocumentInfo>,
            specifier: string,
            category: DocumentCategory,
            contextCallback: DocumentContextCallback
        ) =
        let inline moduleNotFound () =
            raisef JavaScriptRuntimeException "Module not found: %s" specifier

        if specifier = "__current__" && not (isNull currentModule.Value) then
            let m = currentModule.Value
            // Remove it so that the module itself can't use this import.
            currentModule.Value <- null
            m
        else if POSIXPath.isAbsolute specifier then
            // We don't support absolute paths.
            moduleNotFound ()
        else
            // First check the normal relative path.
            let relPath =
                if sourceInfo.HasValue then
                    POSIXPath.combine sourceInfo.Value.Name specifier
                else
                    specifier

            let fullPath = POSIXPath.normalize relPath

            match Map.tryFind fullPath documentsMap with
            | Some doc -> doc
            | None ->
                // If the first component is "." or "..", we skip using the search path.
                let firstComponent = (POSIXPath.splitComponents specifier).[0]

                if firstComponent = "." || firstComponent = ".." then
                    moduleNotFound ()

                let trySearch (search: Path) =
                    let fullPath = POSIXPath.normalize (POSIXPath.combine search specifier)
                    Map.tryFind fullPath documentsMap

                match Seq.tryPick trySearch searchPath with
                | Some doc -> doc
                | None -> moduleNotFound ()

    override this.LoadDocumentAsync
        (
            settings: DocumentSettings,
            sourceInfo: Nullable<DocumentInfo>,
            specifier: string,
            category: DocumentCategory,
            contextCallback: DocumentContextCallback
        ) =
        Task.FromResult
        <| this.LoadDocument(settings, sourceInfo, specifier, category, contextCallback)

[<NoComparison>]
type JSRuntimeLimits =
    { MaxHeapSize: int option
      MaxStackSize: int option }

let noJSRuntimeLimits =
    { MaxHeapSize = None
      MaxStackSize = None }

type JSRuntime(limits: JSRuntimeLimits) =
    let constraints =
        new V8RuntimeConstraints(
            MaxArrayBufferAllocation = 0UL,
            HeapExpansionMultiplier = 2,
            // We expect `HeapExpansionMultiplier` to keep us safe from crashes.
            MaxOldSpaceSize = 2 * (Option.defaultValue 768 limits.MaxHeapSize)
        )

    let runtime = new V8Runtime(constraints)

    do
        match limits.MaxHeapSize with
        | Some maxHeapSize -> runtime.MaxHeapSize <- UIntPtr.CreateChecked maxHeapSize
        | None -> ()

        match limits.MaxStackSize with
        | Some maxStackSize -> runtime.MaxStackUsage <- UIntPtr.CreateChecked maxStackSize
        | None -> ()

    do runtime.EnableInterruptPropagation <- true

    let memoryLimitCancellationTokenSource = new CancellationTokenSource()

    member this.Runtime = runtime

    member this.Limits = limits

    member this.SetMetMemoryLimit() =
        memoryLimitCancellationTokenSource.Cancel()

    member this.MemoryLimitCancellationToken = memoryLimitCancellationTokenSource.Token

type RuntimeLocal<'a when 'a: not struct>(create: JSRuntime -> 'a) =
    let table = ConditionalWeakTable<JSRuntime, 'a>()

    member this.HasValue(runtime: JSRuntime) =
        match table.TryGetValue(runtime) with
        | (ret, _) -> ret

    member this.GetValue(runtime: JSRuntime) =
        table.GetValue(runtime, ConditionalWeakTable.CreateValueCallback<JSRuntime, 'a> create)

let private preludeSource =
    """
    const unwrappedEngine = globalThis.unwrappedEngine;
    delete globalThis.unwrappedEngine;
    const enrichStackTrace = unwrappedEngine.EnrichStackTrace;

    // Can't delete `gc`; unset it this way instead.
    globalThis.gc = undefined;

    const Promise = globalThis.Promise;
    const OldError = globalThis.Error;
    const stringMatch = String.prototype.match;
    const errorToString = OldError.prototype.toString;
    const arrayJoin = Array.prototype.join;
    const oldCaptureStackTrace = OldError.captureStackTrace;

    // Needed because ClearScript doesn't allow to throw unchanged JavaScript exceptions
    // from a host function.
    // If we make a `throwException` function which throws from JS instead, the
    // exception will get altered while passing through the host function which
    // called it, and we lose the EDI assosiaced with it.
    // Instead we pass the host return values through `unwrapHostResult`, which
    // may rethrow.
    class WrappedHostException {
        constructor(e) {
            this.e = e;
        }
    }
    globalThis.WrappedHostException = WrappedHostException;

    const unwrapHostResult = (ret) => {
        if (ret instanceof WrappedHostException) {
            throw ret.e;
        } else {
            return ret;
        }
    };
    globalThis.unwrapHostResult = unwrapHostResult;

    const namedFunction = (name, f) => {
        Object.defineProperty(f, "name", { value: name });
        return f;
    };

    const wrapHostFunction = (func) => namedFunction(func.name, function(...args) {
        const ret = func(...args);
        return unwrapHostResult(ret);
    });
    globalThis.wrapHostFunction = wrapHostFunction;

    const wrapHostObject = (obj) => {
        const newObj = {};
        for (const key of Object.getOwnPropertyNames(obj)) {
            const value = obj[key];
            if (typeof value === 'function') {
                newObj[key] = wrapHostFunction(value);
            }
        }
        return newObj;
    };
    globalThis.wrapHostObject = wrapHostObject;

    // Enable this if you need to debug something.
    // globalThis.engine = wrapHostObject(unwrappedEngine);

    const enrichedExceptionsMap = new WeakMap();
    globalThis.enrichedExceptionsMap = enrichedExceptionsMap;

    const throwException = (exc) => {
        throw exc;
    };
    globalThis.throwException = throwException;

    // Unfortunately, V8 doesn't have any hook to call on any error,
    // and doesn't have any way to patch it completely.
    // Even with all this, all the errors emitted by the engine itself
    // are not updated.
    const updateError = (error) => {
        error.stack = enrichStackTrace(error.stack);
        enrichedExceptionsMap.set(error, true);
    };

    const captureStackTrace = (targetObject, ...args) => {
        oldCaptureStackTrace(targetObject, ...args);
        updateError(targetObject);
    };

    const patchErrorClass = (oldError) => {
        const newError = namedFunction(oldError.name, function(...args) {
            const ret = oldError(...args);
            updateError(ret);
            return ret;
        });
        newError.captureStackTrace = captureStackTrace;
        for (const prop of Object.getOwnPropertyNames(oldError)) {
            if (prop === "prototype" || !(prop in newError)) {
                newError[prop] = oldError[prop];
            }
        }
        newError.prototype.constructor = newError;
        return newError;
    };

    const Error = patchErrorClass(OldError);
    globalThis.Error = Error;

    // Patch all the other error types.
    const errorRegex = /^[A-Z][a-z0-9]*Error$/;
    for (const globalName of Object.getOwnPropertyNames(globalThis)) {
        if (globalName.match(errorRegex)) {
            const oldError = globalThis[globalName];
            if (oldError.__proto__ !== OldError) {
                throw new OldError("Unexpected error type parent: " + globalName);
            }
            const newError = patchErrorClass(oldError);
            newError.__proto__ = Error;
            globalThis[globalName] = newError;
        }
    }

    OldError.prepareStackTrace = (error, trace) => {
        const errorString = errorToString.call(error);
        if (trace.length === 0) {
            return errorString;
        }
        trace = trace.filter(frame => {
            const fileName = frame.getFileName();
            // Hide the internals.
            if (fileName === null) {
                return false;
            }
            if (fileName === "V8ScriptEngine [internal]") {
                return false;
            }
            const functionName = frame.getFunctionName();
            // Hide the wrapped Error.
            if (fileName === "prelude.js" && (functionName === "Error" || (typeof functionName === "string" && stringMatch.call(functionName, errorRegex)) || functionName === "getRawStackTrace")) {
                return false;
            }
            return true;
        });
        return `${errorString}\nat ${arrayJoin.call(trace, ',\nat ')}`;
    };

    // There is an issue when using ClearScript's `GetStackTrace()`:
    // It automatically skips the first two frames, and because of our `prepareStackTrace`
    // function, we end up removing more than we should.
    // Instead, implement our own `getRawStackTrace` which works in the same way.
    const getRawStackTrace = () => {
        try {
            throw new Error('[stack trace]');
        }
        catch (e) {
            return e.stack;
        }
    };
    globalThis.getRawStackTrace = getRawStackTrace;
"""

let private preludeDoc =
    let info = DocumentInfo("prelude.js", Category = ModuleCategory.Standard)
    RuntimeLocal(fun runtime -> runtime.Runtime.Compile(info, preludeSource))

let inline private (|IsInterrupt|_|) (e: exn) =
    match e with
    | :? OperationCanceledException -> Some e
    | :? ScriptEngineException as e when e.IsFatal -> Some e
    | _ -> None

let private getJSExceptionUserData (obj: IJavaScriptObject) : JToken option =
    let userData = obj.["userData"]

    match userData with
    | :? Undefined
    | null -> None
    | _ ->
        use reader = new V8JsonReader(userData)

        try
            Some <| JToken.Load(reader)
        with :? JsonReaderException as e ->
            raisefWithInner JavaScriptRuntimeException e "Failed to parse user data"

[<DefaultScriptUsage(ScriptAccess.None)>]
type JSEngine(runtime: JSRuntime, env: JSEnvironment) as this =
    let engine =
        runtime.Runtime.CreateScriptEngine(
            V8ScriptEngineFlags.DisableGlobalMembers
            ||| V8ScriptEngineFlags.EnableDateTimeConversion
            ||| V8ScriptEngineFlags.HideHostExceptions
            ||| V8ScriptEngineFlags.EnableStringifyEnhancements
        )

    let loader = JSDocumentLoader(env)

    do
        engine.EnableRuntimeInterruptPropagation <- true
        engine.DocumentSettings.Loader <- loader
        engine.AddHostObject("unwrappedEngine", HostItemFlags.PrivateAccess, this)
        // engine.AddHostType("console", typeof<Console>)
        ignore <| engine.Evaluate(preludeDoc.GetValue(runtime))

    let jsonEngine = V8JsonEngine(engine)

    // Used to store and later restore the original host exceptions in case they
    // go through JS, because ClearScript doesn't allow to set internal object fields.
    let weakMapConstructor = engine.Global.["WeakMap"] :?> IJavaScriptObject
    let errorConstructor = engine.Global.["Error"] :?> IJavaScriptObject
    let promiseConstructor = engine.Global.["Promise"] :?> IJavaScriptObject

    let throwException = engine.Global.["throwException"] :?> IJavaScriptObject

    let wrappedHostExceptionConstructor =
        engine.Global.["WrappedHostException"] :?> IJavaScriptObject

    let enrichedExceptionsMap =
        engine.Global.["enrichedExceptionsMap"] :?> IJavaScriptObject

    let getRawStackTrace = engine.Global.["getRawStackTrace"] :?> IJavaScriptObject

    do
        ignore <| engine.Global.DeleteProperty("throwException")
        ignore <| engine.Global.DeleteProperty("WrappedHostException")
        ignore <| engine.Global.DeleteProperty("enrichedExceptionsMap")
        ignore <| engine.Global.DeleteProperty("getRawStackTrace")

    let jsExceptionsMap = AsyncLocal<IJavaScriptObject>()
    let interruptEDI = AsyncLocal<ExceptionDispatchInfo ref>()
    let exceptionsMap = AsyncLocal<ConditionalWeakTable<exn, obj>>()
    let asyncStackTrace = AsyncLocal<string>()

    let createError (message: obj) =
        errorConstructor.Invoke(true, message) :?> IJavaScriptObject

    let getStackTrace () =
        let rawTrace = getRawStackTrace.InvokeAsFunction() :?> string
        // Remove the first line.
        rawTrace.Substring(rawTrace.IndexOf('\n') + 1)

    let wrapEvaluationException (innerException: exn) (jsExc: IJavaScriptObject option) =
        let (userData, errorMessage) =
            match jsExc with
            | Some jsExc ->
                let userData = getJSExceptionUserData jsExc

                let errorMessage =
                    match jsExc.["stack"] with
                    | :? string as stack ->
                        // Find out if we need to enrich the stack.
                        match enrichedExceptionsMap.InvokeMethod("get", jsExc) with
                        | :? bool as b when b ->
                            // Nope, the enrichment had already happened.
                            Some stack
                        | _ ->
                            // Yes, we need to enrich the stack.
                            let stack = this.EnrichStackTrace stack
                            // Pass the enriched stack back.
                            jsExc.["stack"] <- stack
                            ignore <| enrichedExceptionsMap.InvokeMethod("set", jsExc, true)
                            Some stack
                    | smth ->
                        // Weird.
                        None

                (userData, errorMessage)
            | None -> (None, None)

        match errorMessage with
        | Some msg -> JavaScriptRuntimeException(msg, innerException, true, userData, true)
        | None -> JavaScriptRuntimeException("", innerException, true, userData, false)

    [<TailCall>]
    let rec findPreviousJSException (e: exn) =
        let excMap = exceptionsMap.Value

        match excMap.TryGetValue(e) with
        | (true, obj) ->
            ignore <| excMap.Remove(e)
            Some obj
        | (false, _) ->
            match e.InnerException with
            | null -> None
            | ie -> findPreviousJSException ie

    let hostExceptionToJSException (e: exn) =
        match findPreviousJSException e with
        | Some obj -> obj
        | None ->
            match e with
            | :? JSException as e -> e.Value
            | :? ScriptEngineException as e when not e.IsFatal ->
                match e.ScriptExceptionAsObject with
                | :? IJavaScriptObject as obj -> obj
                | null -> createError (fullUserMessage e)
                | exc -> createError exc
            | e -> createError (fullUserMessage e)

    let hostEDIToJSException (edi: ExceptionDispatchInfo) =
        try
            let error = hostExceptionToJSException edi.SourceException
            let jsExcMap = jsExceptionsMap.Value

            ignore <| jsExcMap.InvokeMethod("set", error, edi)

            error
        with
        | IsInterrupt e -> this.RethrowInterrupt e
        | e ->
            Log.Error(e, "Unexpected exception while saving the original exception.")
            reraise ()

    let tryRethrowPreviousHostExceptionFromJS (jsException: obj) =
        try
            let jsExcMap = jsExceptionsMap.Value
            let excMap = exceptionsMap.Value

            match jsExcMap.InvokeMethod("get", jsException) with
            | :? ExceptionDispatchInfo as edi ->
                ignore <| jsExcMap.InvokeMethod("delete", jsException)
                excMap.AddOrUpdate(edi.SourceException, jsException)
                edi.Throw()
            | _ -> ()
        with :? ScriptEngineException as e when e.IsFatal ->
            runtime.SetMetMemoryLimit()
            reraise ()

    let tryRethrowPreviousHostException (e: exn) =
        match e with
        | :? ScriptEngineException as e when e.IsFatal -> runtime.SetMetMemoryLimit()
        | _ -> ()

        match interruptEDI.Value.Value with
        | null -> ()
        | edi -> edi.Throw()

        match e with
        | :? ScriptEngineException as e when not e.IsFatal && not <| isNull e.ScriptExceptionAsObject ->
            tryRethrowPreviousHostExceptionFromJS e.ScriptExceptionAsObject
            exceptionsMap.Value.AddOrUpdate(e, e.ScriptExceptionAsObject)
        | _ -> ()

    member inline private this.RethrowInterrupt<'a>(e: exn) : 'a =
        let edi = lazy (ExceptionDispatchInfo.Capture(e))
        let intEDI = interruptEDI.Value

        while true do
            match intEDI.Value with
            | null ->
                if isNull <| Interlocked.CompareExchange(&intEDI.contents, null, edi.Value) then
                    engine.Interrupt()
                    edi.Value.Throw()
            | oldE -> oldE.Throw()

        failwith "Impossible"

    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.EnrichStackTrace(stack: string) =
        let currTrace = asyncStackTrace.Value

        if obj.ReferenceEquals(currTrace, null) then
            stack
        else
            seq {
                yield stack
                yield currTrace
            }
            |> String.concat ",\n"

    // These are used to test if the stack traces get enriched correctly.
    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.Yield() =
        this.WrapAsyncHostFunction(fun () ->
            task {
                do! Task.Yield()
                return null
            })

    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.YieldAndRun(f: IJavaScriptObject) =
        this.WrapAsyncHostFunction(fun () ->
            task {
                do! Task.Yield()
                let! _ = this.RunAsyncJSFunction(f, [||])
                return null
            })

    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.ThrowHostException(message: string) =
        this.WrapHostFunction(fun () -> raise <| Exception(message))

    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.ThrowJSException(message: string) =
        this.WrapHostFunction(fun () -> raise <| JSException(message, this))

    member inline private this.EvaluateJS
        (cancellationToken: CancellationToken)
        ([<InlineIfLambda>] f: unit -> obj)
        : obj =
        let inline run () =
            // This is not only called recursively when evaluating sync JS,
            // but also to continue an async computation, when there is no nested JS
            // evaluation happening and so, no handle is registered yet.
            // To simplify this, simply always register one.
            cancellationToken.ThrowIfCancellationRequested()
            use handle = cancellationToken.UnsafeRegister((fun _ -> engine.Interrupt()), null)

            try
                try
                    f ()
                with e ->
                    tryRethrowPreviousHostException e
                    reraise ()
            with
            | :? ScriptEngineException as e ->
                let jsExc =
                    match e.ScriptExceptionAsObject with
                    | :? IJavaScriptObject as obj when not e.IsFatal -> Some obj
                    | _ -> None

                raise <| wrapEvaluationException e jsExc
            | :? JSException as e -> raise <| wrapEvaluationException e (Some e.Value)

        match exceptionsMap.Value with
        | null ->
            try
                exceptionsMap.Value <- ConditionalWeakTable()
                jsExceptionsMap.Value <- weakMapConstructor.Invoke(true) :?> IJavaScriptObject
                interruptEDI.Value <- ref null

                run ()
            finally
                exceptionsMap.Value <- null
                jsExceptionsMap.Value <- null
                interruptEDI.Value <- Unchecked.defaultof<_>
        | _ -> run ()

    member val JSRequestID = AsyncLocal<int>()

    member inline private this.EvaluateAsyncJS
        (cancellationToken: CancellationToken)
        ([<InlineIfLambda>] f: unit -> obj)
        ([<InlineIfLambda>] whenPromiseFinished: unit -> unit)
        : Task<obj> =
        task {
            let inline run () : Task<obj> =
                task {
                    cancellationToken.ThrowIfCancellationRequested()
                    use handle = cancellationToken.UnsafeRegister((fun _ -> engine.Interrupt()), null)

                    let randId = this.JSRequestID.Value
                    Log.Debug("Evaluating async JS {id}", randId)

                    try
                        let! maybeResult =
                            task {
                                try
                                    let result = f ()
                                    Log.Debug("Got first async result {id}", randId)

                                    match result with
                                    | :? IJavaScriptObject as promise when promise.Kind = JavaScriptObjectKind.Promise ->
                                        let resultSource = TaskCompletionSource<Result<obj, obj>>()

                                        use handle =
                                            runtime.MemoryLimitCancellationToken.UnsafeRegister(
                                                (fun _ ->
                                                    resultSource.SetResult(
                                                        Error(JavaScriptRuntimeException("Memory limit exceeded"))
                                                    )),
                                                null
                                            )

                                        Log.Debug("Invoking then for {id}", randId)

                                        ignore
                                        <| promise.InvokeMethod(
                                            "then",
                                            // Not converting these to Actions result in no methods invoked D:
                                            Action<obj>(fun result ->
                                                Log.Debug("Resolved async JS {id}", randId)
                                                resultSource.SetResult(Ok result)
                                                // We want to have a callback synchronous to the async host JS functions calling `resolve`.
                                                // This is needed for the `SchedulerJSEngine` to be able to detect that the resulting promise
                                                // has been fulfilled. We cannot just check if `EvaluateAsyncJS`'s task is finished, because
                                                // there is post-processing and cancellation tokens involved, so it may not be finished
                                                // synchronously after the host functions.
                                                whenPromiseFinished ()),
                                            Action<obj>(fun reason ->
                                                Log.Debug("Rejected async JS {id}", randId)
                                                resultSource.SetResult(Error reason)
                                                whenPromiseFinished ())
                                        )

                                        return! resultSource.Task.WaitAsync(cancellationToken)
                                    | _ -> return (Ok result)
                                with e ->
                                    tryRethrowPreviousHostException e
                                    return reraise' e
                            }

                        Log.Debug("Returning async JS {id}", randId)

                        match maybeResult with
                        | (Ok result) -> return result
                        | (Error(:? exn as e)) ->
                            // Happens only when the memory limit is met.
                            return raise e
                        | (Error reason) ->
                            tryRethrowPreviousHostExceptionFromJS reason
                            ignore <| throwException.InvokeAsFunction(reason)
                            return failwith "Impossible"
                    with
                    | :? ScriptEngineException as e ->
                        let jsExc =
                            match e.ScriptExceptionAsObject with
                            | :? IJavaScriptObject as obj when not e.IsFatal -> Some obj
                            | _ -> None

                        return raise <| wrapEvaluationException e jsExc
                    | :? JSException as e -> return raise <| wrapEvaluationException e (Some e.Value)
                }

            match exceptionsMap.Value with
            | null ->
                exceptionsMap.Value <- ConditionalWeakTable()
                jsExceptionsMap.Value <- weakMapConstructor.Invoke(true) :?> IJavaScriptObject
                interruptEDI.Value <- ref null
                return! run ()
            | _ -> return! run ()
        }

    // As far as I understand, ClearScript doesn't have any way to compile modules and then
    // return them when requested by imports the way pure V8 has.
    // Instead, they cache Module instances by the source code digests, and there's
    // no way to call `Instantiate()`.
    // We evaluate them immediately instead.
    abstract member CreateModule: ModuleFile * CancellationToken -> IJavaScriptObject

    default this.CreateModule(file: ModuleFile, cancellationToken: CancellationToken) : IJavaScriptObject =
        let info = DocumentInfo(file.Path, Category = ModuleCategory.Standard)
        let doc = StringDocument(info, file.Source)

        let ret =
            this.EvaluateJS cancellationToken
            <| fun () ->
                let glob = engine.Script :?> IDictionary<string, obj>

                let snippetInfo = DocumentInfo("import.js", Category = ModuleCategory.Standard)

                // First evaluation; needed because we don't want to expose our internals,
                // and the evaluations are cached.
                ignore
                <| loader.WithCurrentModuleOnce(doc, (fun () -> engine.Evaluate(snippetInfo, "import '__current__'")))
                // Careful to not remove something that the user set up.
                let oldCurrent =
                    match glob.TryGetValue("current") with
                    | (true, current) -> Some current
                    | (false, _) -> None

                try
                    // We expect no re-evaluation to occur here.
                    ignore
                    <| loader.WithCurrentModuleOnce(
                        doc,
                        fun () ->
                            engine.Evaluate(
                                snippetInfo,
                                """
                        import * as current from '__current__';
                        globalThis.current = current;
                    """
                            )
                    )

                    glob.["current"]
                finally
                    match oldCurrent with
                    | Some current -> glob.["current"] <- current
                    | None -> ignore <| glob.Remove("current")

        ret :?> IJavaScriptObject

    abstract member CreateDefaultFunction: ModuleFile * CancellationToken -> IJavaScriptObject

    default this.CreateDefaultFunction(file: ModuleFile, cancellationToken: CancellationToken) : IJavaScriptObject =
        let jsModule = this.CreateModule(file, cancellationToken)

        match jsModule.GetProperty("default") with
        | :? IJavaScriptObject as obj when obj.Kind = JavaScriptObjectKind.Function -> obj
        | _ -> raisef JavaScriptRuntimeException "Invalid default function"

    member this.Engine = engine
    member this.Runtime = runtime
    member this.Json = jsonEngine

    abstract member CreateError: string -> IJavaScriptObject
    default this.CreateError message = createError message

    abstract member WrapHostFunction: (unit -> 'a) -> obj

    default this.WrapHostFunction(f: unit -> 'a) =
        try
            f () :> obj
        with
        | IsInterrupt e -> this.RethrowInterrupt e
        | e ->
            let error = hostEDIToJSException <| ExceptionDispatchInfo.Capture(e)
            wrappedHostExceptionConstructor.Invoke(true, error)

    abstract member StartAsyncHostTask: (unit -> Task) -> unit
    default this.StartAsyncHostTask(f: unit -> Task) = f () |> ignore

    abstract member WrapAsyncHostFunction: (unit -> Task<'a>) -> obj

    override this.WrapAsyncHostFunction(f: unit -> Task<'a>) =
        let stack = getStackTrace ()
        let id = this.JSRequestID.Value

        promiseConstructor.Invoke(
            true,
            Action<_, _>(fun (resolve: IJavaScriptObject) (reject: IJavaScriptObject) ->
                this.StartAsyncHostTask
                <| fun () ->
                    task {
                        try
                            let! result =
                                task {
                                    try
                                        asyncStackTrace.Value <- stack
                                        let! ret = f ()
                                        return Ok ret
                                    with
                                    | IsInterrupt e -> return this.RethrowInterrupt e
                                    | e -> return Error(ExceptionDispatchInfo.Capture(e))
                                }
                            // We don't expect any JavaScript exceptions but the fatal ones here.
                            try
                                Log.Debug("Continuing async JS function {id}", id)

                                match result with
                                | Ok r -> ignore <| resolve.InvokeAsFunction(r)
                                | Error e ->
                                    let jsE = hostEDIToJSException e
                                    ignore <| reject.InvokeAsFunction(jsE)

                                Log.Debug("Finished continuing async JS function {id}", id)
                            with :? ScriptEngineException as e when e.IsFatal ->
                                runtime.SetMetMemoryLimit()
                                reraise' e
                        with e ->
                            Serilog.Log.Error(e, "Unexpected exception in async host function")
                    })
        )

    abstract member RunJSFunction: IJavaScriptObject * obj[] * CancellationToken -> obj

    default this.RunJSFunction(func: IJavaScriptObject, args: obj[], cancellationToken: CancellationToken) : obj =
        if func.Engine <> engine then
            failwith "Function belongs to another engine"

        this.EvaluateJS cancellationToken <| fun () -> func.InvokeAsFunction(args)

    member this.RunJSFunction(func: IJavaScriptObject, args: obj[]) : obj =
        this.RunJSFunction(func, args, CancellationToken.None)

    abstract member RunAsyncJSFunction: IJavaScriptObject * obj[] * (unit -> unit) * CancellationToken -> Task<obj>

    default this.RunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], whenPromiseFinished: (unit -> unit), cancellationToken: CancellationToken) : Task<
                                                                                                                                obj
                                                                                                                             >
        =
        if func.Engine <> engine then
            failwith "Function belongs to another engine"

        this.EvaluateAsyncJS cancellationToken (fun () -> func.InvokeAsFunction(args)) whenPromiseFinished

    member this.RunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], cancellationToken: CancellationToken)
        : Task<obj> =
        this.RunAsyncJSFunction(func, args, (fun () -> ()), cancellationToken)

    member this.RunAsyncJSFunction(func: IJavaScriptObject, args: obj[]) : Task<obj> =
        this.RunAsyncJSFunction(func, args, CancellationToken.None)

// ClearScript doesn't allow you to set the `ScriptException` externally,
// so we need our own type of an exception for that.
and JSException(message: string, value: IJavaScriptObject, innerException: exn) =
    inherit Exception(message, innerException)

    new(message: string, value: IJavaScriptObject) = JSException(message, value, null)

    new(message: string, innerException: exn, engine: JSEngine) =
        let error = engine.CreateError message
        JSException(message, error, innerException)

    new(message: string, engine: JSEngine) =
        let error = engine.CreateError message
        JSException(message, error, null)

    member this.Value = value

[<AbstractClass>]
type SchedulerJSEngine<'s when 's :> Task.ICustomTaskScheduler>(runtime: JSRuntime, env: JSEnvironment) =
    inherit JSEngine(runtime, env)

    let scheduler = AsyncLocal<'s>()

    member this.Scheduler: 's option =
        let currScheduler = scheduler.Value
        if isRefNull currScheduler then None else Some currScheduler

    override this.StartAsyncHostTask(runTask: unit -> Task) : unit =
        let wrappedRunTask () =
            let currScheduler = scheduler.Value

            Log.Debug("Starting async host task {id}", this.JSRequestID.Value)

            if isRefNull currScheduler then
                Log.Debug("No scheduler for async host task {id}", this.JSRequestID.Value)
                runTask ()
            else
                currScheduler.Post(runTask)

        base.StartAsyncHostTask(wrappedRunTask)

    abstract CreateScheduler: unit -> 's

    // F# is dumb here.
    member private this.BaseRunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], whenPromiseFinished: (unit -> unit), cancellationToken: CancellationToken) =
        base.RunAsyncJSFunction(func, args, whenPromiseFinished, cancellationToken)

    override this.RunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], whenPromiseFinished: (unit -> unit), cancellationToken: CancellationToken) : Task<
                                                                                                                                obj
                                                                                                                             >
        =
        task {
            let mutable promiseFinished = false

            let randId = Random.Shared.Next()
            Log.Debug("Starting async JS function {id}, previous id: {prevId}", randId, this.JSRequestID.Value)
            this.JSRequestID.Value <- randId

            let newWhenPromiseFinished () =
                promiseFinished <- true
                Log.Debug("Set that the promise finished {id}", randId)
                whenPromiseFinished ()

            let currScheduler = this.CreateScheduler()
            scheduler.Value <- currScheduler

            let retTask =
                this.BaseRunAsyncJSFunction(func, args, newWhenPromiseFinished, cancellationToken)

            Log.Debug("Waiting for all pending tasks in {id}", randId)
            do! currScheduler.WaitAll(cancellationToken)
            this.Runtime.MemoryLimitCancellationToken.ThrowIfCancellationRequested()

            Log.Debug("Is promise finished {id}: {finished}", randId, promiseFinished)

            if not promiseFinished then
                raisef JavaScriptRuntimeException "The called function haven't resolved to a response"

            return! retTask
        }
