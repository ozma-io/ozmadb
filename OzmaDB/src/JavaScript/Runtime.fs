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

        if specifier = "__current__" && Option.isSome this.CurrentModule then
            let m = Option.get this.CurrentModule
            // Reset it after a single usage.
            this.CurrentModule <- None
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

    // ClearScript doesn't allow to get an evaluated module and just returns `undefined`.
    // So to import a module, we need to inject it into the loader, and remove later.
    // Ugh.
    member val CurrentModule: StringDocument option = None with get, set

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

    let mutable metMemoryLimit = false

    member this.Runtime = runtime

    member this.Limits = limits

    member this.SetMetMemoryLimit() = metMemoryLimit <- true

    member this.MetMemoryLimit = metMemoryLimit

type RuntimeLocal<'a when 'a: not struct>(create: JSRuntime -> 'a) =
    let table = ConditionalWeakTable<JSRuntime, 'a>()

    member this.HasValue(runtime: JSRuntime) =
        match table.TryGetValue(runtime) with
        | (ret, _) -> ret

    member this.GetValue(runtime: JSRuntime) =
        table.GetValue(runtime, ConditionalWeakTable.CreateValueCallback<JSRuntime, 'a> create)

let private preludeSource =
    """
    const engine = globalThis.engine;
    delete globalThis.engine;
    // Can't delete `gc`; unset it this way instead.
    globalThis.gc = undefined;

    const Promise = globalThis.Promise;
    const OldError = globalThis.Error;
    const errorToString = OldError.prototype.toString;
    const arrayJoin = Array.prototype.join;

    // Needed because ClearScript doesn't allow to throw plain JavaScript exceptions
    // from a host function.
    // Instead we pass the host return values through `unwrapHostResult`, which
    // may rethrow.
    class WrappedHostException {
        constructor(e) {
            this.e = e;
        }
    }
    globalThis.WrappedHostException = WrappedHostException;

    // We need control over where the continuations get called because
    // we maintain a set of currently progressing tasks.
    // But ClearScript automatically converts tasks to promises and back for us
    // (maybe disable that? but we need to wrap the host return values anyway...).
    // So if we just construct a Promise with the constructor and immediately return it,
    // ClearScript converts the Promise to the Task, and then back ~ _ ~
    // This garbles our exceptions and wastes cycles, so we need to call the constructor
    // outside of the host function.
    class WrappedHostPromise {
        constructor(f) {
            this.f = f;
        }
    }
    globalThis.WrappedHostPromise = WrappedHostPromise;

    globalThis.unwrapHostResult = (ret) => {
        if (ret instanceof WrappedHostException) {
            throw ret.e;
        } else if (ret instanceof WrappedHostPromise) {
            return new Promise(ret.f);
        } else {
            return ret;
        }
    };

    const namedFunction = (name, f) => {
        Object.defineProperty(f, "name", { value: name });
        return f;
    };

    const enrichedExceptionsMap = new WeakMap();
    globalThis.enrichedExceptionsMap = enrichedExceptionsMap;

    // Unfortunately, V8 doesn't have any hook to call on any error,
    // and doesn't have any way to patch it completely.
    // Even with all this, all the errors emitted by the engine itself
    // are not updated.
    const updateError = (error) => {
        error.stack = engine.EnrichStackTrace(error.stack);
        enrichedExceptionsMap.set(error, true);
    };

    const oldCaptureStackTrace = globalThis.Error.captureStackTrace;
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

    const prepareStackTraceFlags = {
        inHostGetStackTrace: false,
    };
    globalThis.prepareStackTraceFlags = prepareStackTraceFlags;
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
            // Okay, this is crazy: if we filter this frame, `engine.GetStackTrace()` breaks.
            // So we set this flag before we call it and reset it back afterwards.
            if (!prepareStackTraceFlags.inHostGetStackTrace && fileName === "V8ScriptEngine [internal]") {
                return false;
            }
            const functionName = frame.getFunctionName();
            // Hide the wrapped Error.
            if (fileName === "prelude.js" && (functionName === "Error" || functionName.match(errorRegex))) {
                return false;
            }
            return true;
        });
        return `${errorString}\nat ${arrayJoin.call(trace, ',\nat ')}`;
    };
"""

let private preludeDoc =
    let info = DocumentInfo("prelude.js", Category = ModuleCategory.Standard)
    RuntimeLocal(fun runtime -> runtime.Runtime.Compile(info, preludeSource))

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
            ||| V8ScriptEngineFlags.EnableTaskPromiseConversion
            ||| V8ScriptEngineFlags.EnableValueTaskPromiseConversion
            ||| V8ScriptEngineFlags.HideHostExceptions
            ||| V8ScriptEngineFlags.EnableStringifyEnhancements
        )

    let loader = JSDocumentLoader(env)

    do
        engine.EnableRuntimeInterruptPropagation <- true
        engine.DocumentSettings.Loader <- loader
        engine.AddHostObject("engine", HostItemFlags.PrivateAccess, this)
        // engine.AddHostType("console", typeof<Console>)
        ignore <| engine.Evaluate(preludeDoc.GetValue(runtime))

    let jsonEngine = V8JsonEngine(engine)

    // Used to store and later restore the original host exceptions in case they
    // go through JS, because ClearScript doesn't allow to set internal object fields.
    let weakMapConstructor = engine.Global.["WeakMap"] :?> IJavaScriptObject
    let errorConstructor = engine.Global.["Error"] :?> IJavaScriptObject

    let wrappedHostExceptionConstructor =
        engine.Global.["WrappedHostException"] :?> IJavaScriptObject

    let wrappedHostPromiseConstructor =
        engine.Global.["WrappedHostPromise"] :?> IJavaScriptObject

    let enrichedExceptionsMap =
        engine.Global.["enrichedExceptionsMap"] :?> IJavaScriptObject

    let prepareStackTraceFlags =
        engine.Global.["prepareStackTraceFlags"] :?> IJavaScriptObject

    do
        ignore <| engine.Global.DeleteProperty("WrappedHostException")
        ignore <| engine.Global.DeleteProperty("WrappedHostPromise")
        ignore <| engine.Global.DeleteProperty("enrichedExceptionsMap")
        ignore <| engine.Global.DeleteProperty("prepareStackTraceFlags")

    let mutable currentJSExceptionsMap: IJavaScriptObject option = None
    let mutable interruptEDI: ExceptionDispatchInfo option = None
    let mutable currentCancellationToken: CancellationToken option = None
    let exceptionsMap = ConditionalWeakTable<exn, IJavaScriptObject>()

    let asyncStackTrace = AsyncLocal<string>()
    let pendingTasks = AsyncLocal<Collections.Generic.HashSet<Task>>()

    let createError (message: string) =
        errorConstructor.Invoke(true, message) :?> IJavaScriptObject

    let wrapJSException (innerException: exn) (jsExc: IJavaScriptObject option) =
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

    let rec findPreviousExceptionJSObject (e: exn) =
        match exceptionsMap.TryGetValue(e) with
        | (true, obj) ->
            ignore <| exceptionsMap.Remove(e)
            Some obj
        | (false, _) ->
            match e.InnerException with
            | null -> None
            | ie -> findPreviousExceptionJSObject ie

    let getHostExceptionJSObject (e: exn) =
        match findPreviousExceptionJSObject e with
        | Some obj -> obj
        | None ->
            match e with
            | :? JSException as e -> e.Value
            | :? ScriptEngineException as e when not <| isNull e.ScriptExceptionAsObject ->
                e.ScriptExceptionAsObject :?> IJavaScriptObject
            | e -> createError (fullUserMessage e)

    let handleHostExceptionInJS (hostException: exn) =
        let interrupt (e: exn) =
            match interruptEDI with
            | Some oldE -> oldE.Throw()
            | None ->
                let edi = ExceptionDispatchInfo.Capture(e)
                interruptEDI <- Some edi
                engine.Interrupt()
                edi.Throw()

            failwith "Impossible"

        try
            let error =
                match hostException with
                | :? OperationCanceledException as e -> interrupt e
                | :? ScriptEngineException as e when e.IsFatal -> interrupt e
                | e -> getHostExceptionJSObject e

            let jsExceptionsMap = Option.get currentJSExceptionsMap

            ignore
            <| jsExceptionsMap.InvokeMethod("set", error, ExceptionDispatchInfo.Capture(hostException))

            error
        with
        | :? OperationCanceledException as e -> interrupt e
        | :? ScriptEngineException as e when e.IsFatal -> interrupt e
        | e ->
            Log.Error(e, "Unexpected exception while saving the original exception.")
            reraise ()

    let tryRestoreJSException (jsException: exn) =
        match jsException with
        | :? ScriptEngineException as e when e.IsFatal -> runtime.SetMetMemoryLimit()
        | _ -> ()

        match interruptEDI with
        | Some edi -> edi.Throw()
        | None -> ()

        match jsException with
        | :? ScriptEngineException as e when not e.IsFatal && not <| isNull e.ScriptExceptionAsObject ->
            let jsExc = e.ScriptExceptionAsObject :?> IJavaScriptObject

            let maybeEdi =
                try
                    let jsExceptionsMap = Option.get currentJSExceptionsMap

                    match jsExceptionsMap.InvokeMethod("get", jsExc) with
                    | :? ExceptionDispatchInfo as edi ->
                        ignore <| jsExceptionsMap.InvokeMethod("delete", jsExc)
                        Some edi
                    | _ -> None
                with :? ScriptEngineException as e when e.IsFatal ->
                    runtime.SetMetMemoryLimit()
                    reraise ()

            match maybeEdi with
            | Some edi ->
                exceptionsMap.AddOrUpdate(edi.SourceException, jsExc)
                edi.Throw()
            | None -> exceptionsMap.AddOrUpdate(jsException, jsExc)
        | _ -> ()

    [<ScriptUsage(ScriptAccess.Full)>]
    member private this.EnrichStackTrace(stack: string) =
        if obj.ReferenceEquals(asyncStackTrace.Value, null) then
            stack
        else
            seq {
                yield stack
                yield asyncStackTrace.Value
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

    member inline private this.EvaluateJS<'a>
        (cancellationToken: CancellationToken)
        ([<InlineIfLambda>] f: unit -> 'a)
        : 'a =
        let inline run () =
            try
                try
                    f ()
                with e ->
                    tryRestoreJSException e
                    reraise ()
            with
            | :? ScriptEngineException as e ->
                let jsExc =
                    if e.IsFatal || isNull e.ScriptExceptionAsObject then
                        None
                    else
                        Some(e.ScriptExceptionAsObject :?> IJavaScriptObject)

                raise <| wrapJSException e jsExc
            | :? JSException as e -> raise <| wrapJSException e (Some e.Value)

        match currentCancellationToken with
        | None ->
            currentCancellationToken <- Some cancellationToken
            currentJSExceptionsMap <- Some(weakMapConstructor.Invoke(true) :?> IJavaScriptObject)

            try
                use handle = cancellationToken.UnsafeRegister((fun _ -> engine.Interrupt()), null)
                run ()
            finally
                currentCancellationToken <- None
                currentJSExceptionsMap <- None
                interruptEDI <- None
                exceptionsMap.Clear()
        | Some otherCancellationToken ->
            if
                cancellationToken <> CancellationToken.None
                && cancellationToken <> otherCancellationToken
            then
                failwith "Nested cancellation tokens are not supported"

            run ()

    member inline private this.EvaluateAsyncJS<'a>
        (cancellationToken: CancellationToken)
        ([<InlineIfLambda>] f: unit -> Task<'a>)
        : Task<'a> =
        task {
            let inline run () =
                task {
                    // We collect all the tasks that are spawned during the execution of this function,
                    // and make sure they all finish.
                    // This way we avoid leaking tasks that can even cross to the next evaluation.
                    let newPendingTasks = Collections.Generic.HashSet()
                    pendingTasks.Value <- newPendingTasks

                    use _ =
                        Task.toDisposable
                        <| fun () ->
                            task {
                                while newPendingTasks.Count > 0 do
                                    let task = newPendingTasks |> Seq.first |> Option.get

                                    try
                                        do! task
                                    with _ ->
                                        ()

                                    ignore <| newPendingTasks.Remove(task)
                            }

                    try
                        try
                            return! f ()
                        with e ->
                            tryRestoreJSException e
                            return reraise' e
                    with
                    | :? ScriptEngineException as e ->
                        let jsExc =
                            if e.IsFatal || isNull e.ScriptExceptionAsObject then
                                None
                            else
                                Some(e.ScriptExceptionAsObject :?> IJavaScriptObject)

                        return raise <| wrapJSException e jsExc
                    | :? JSException as e -> return raise <| wrapJSException e (Some e.Value)
                }

            match currentCancellationToken with
            | None ->
                currentCancellationToken <- Some cancellationToken
                currentJSExceptionsMap <- Some(weakMapConstructor.Invoke(true) :?> IJavaScriptObject)

                try
                    // We have observed a bug in ClearScript when a task for a cancelled JavaScript
                    // evaluation is never cancelled. Explicitly subscribing to the cancellation
                    // token like that seems to solve the issue.
                    let cancelledTask = TaskCompletionSource()

                    use handle =
                        cancellationToken.UnsafeRegister(
                            (fun _ ->
                                engine.Interrupt()
                                cancelledTask.SetCanceled()),
                            null
                        )

                    let runTask = run ()
                    let! successful = Task.WhenAny(runTask :> Task, cancelledTask.Task)

                    if successful = cancelledTask.Task then
                        cancellationToken.ThrowIfCancellationRequested()
                        return failwith "Impossible"
                    else
                        return! runTask
                finally
                    currentCancellationToken <- None
                    currentJSExceptionsMap <- None
                    interruptEDI <- None
                    exceptionsMap.Clear()
            | Some otherCancellationToken ->
                if
                    cancellationToken <> CancellationToken.None
                    && cancellationToken <> otherCancellationToken
                then
                    failwith "Nested cancellation tokens are not supported"

                return! run ()
        }

    // As far as I understand, ClearScript doesn't have any way to compile modules and then
    // return them when requested by imports the way pure V8 has.
    // Instead, they cache Module instances by the source code digests, and there's
    // no way to call `Instantiate()`.
    // We evaluate them immediately instead.
    member this.CreateModule(file: ModuleFile) : IJavaScriptObject =
        let info = DocumentInfo(file.Path, Category = ModuleCategory.Standard)
        let doc = StringDocument(info, file.Source)

        this.EvaluateJS CancellationToken.None
        <| fun () ->
            let glob = engine.Script :?> IDictionary<string, obj>
            // First evaluation; needed because we don't want to expose our internals,
            // and the evaluations are cached.
            let snippetInfo = DocumentInfo("import.js", Category = ModuleCategory.Standard)
            loader.CurrentModule <- Some doc

            try
                ignore <| engine.Evaluate(snippetInfo, "import '__current__'")
                // Careful to not remove something that the user set up.
                let oldCurrent =
                    match glob.TryGetValue("current") with
                    | (true, current) -> Some current
                    | (false, _) -> None

                try
                    // Rearm the current module.
                    loader.CurrentModule <- Some doc
                    // We expect no re-evaluation to occur here.
                    ignore
                    <| engine.Evaluate(
                        snippetInfo,
                        """
                        import * as current from '__current__';
                        globalThis.current = current;
                    """
                    )

                    glob.["current"] :?> IJavaScriptObject
                finally
                    match oldCurrent with
                    | Some current -> glob.["current"] <- current
                    | None -> ignore <| glob.Remove("current")
            finally
                loader.CurrentModule <- None

    member this.CreateDefaultFunction(file: ModuleFile) : IJavaScriptObject =
        let jsModule = this.CreateModule file

        match jsModule.GetProperty("default") with
        | :? IJavaScriptObject as obj when obj.Kind = JavaScriptObjectKind.Function -> obj
        | _ -> raisef JavaScriptRuntimeException "Invalid default function"

    member this.Engine = engine
    member this.Runtime = runtime
    member this.Json = jsonEngine

    member this.CreateError message = createError message

    member this.WrapHostFunction(f: unit -> 'a) =
        try
            f () :> obj
        with e ->
            let error = handleHostExceptionInJS e
            wrappedHostExceptionConstructor.Invoke(true, error)

    member this.WrapAsyncHostFunction(f: unit -> Task<'a>) =
        prepareStackTraceFlags.SetProperty("inHostGetStackTrace", true)

        let stack =
            try
                engine.GetStackTrace()
            finally
                prepareStackTraceFlags.SetProperty("inHostGetStackTrace", false)

        wrappedHostPromiseConstructor.Invoke(
            true,
            Action<_, _>(fun (resolve: IJavaScriptObject) (reject: IJavaScriptObject) ->
                let myPendingTasks = pendingTasks.Value

                if isNull myPendingTasks then
                    raisef JavaScriptRuntimeException "Async functions are not allowed here"

                let mutable runTask: Task = null

                runTask <-
                    task {
                        try
                            let! ret =
                                task {
                                    asyncStackTrace.Value <- stack

                                    try
                                        let! ret = f ()
                                        return Ok ret
                                    with e ->
                                        let jsExc = handleHostExceptionInJS e
                                        return Error jsExc
                                }

                            ignore
                            <| match ret with
                               | Ok r -> resolve.InvokeAsFunction(r)
                               | Error e -> reject.InvokeAsFunction(e)
                        finally
                            // Might be `null` is the task is finished immediately.
                            if not <| isNull runTask then
                                ignore <| myPendingTasks.Remove(runTask)
                    }

                if not <| runTask.IsCompleted then
                    ignore <| myPendingTasks.Add(runTask))
        )

    member this.RunJSFunction(func: IJavaScriptObject, args: obj[], cancellationToken: CancellationToken) : obj =
        if func.Engine <> engine then
            failwith "Function belongs to another engine"

        this.EvaluateJS cancellationToken <| fun () -> func.InvokeAsFunction(args)

    member this.RunJSFunction(func: IJavaScriptObject, args: obj[]) : obj =
        this.RunJSFunction(func, args, CancellationToken.None)

    member this.RunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], cancellationToken: CancellationToken)
        : Task<obj> =
        if func.Engine <> engine then
            failwith "Function belongs to another engine"

        this.EvaluateAsyncJS cancellationToken
        <| fun () ->
            match func.InvokeAsFunction(args) with
            | :? Task<obj> as t -> t
            | ret -> Task.result (ret)

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
