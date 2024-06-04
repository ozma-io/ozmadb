module OzmaDB.JavaScript.Runtime

open System
open System.Threading
open System.Threading.Tasks
open System.Runtime.CompilerServices
open System.Runtime.ExceptionServices
open System.Collections.Generic
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Microsoft.ClearScript
open Microsoft.ClearScript.JavaScript
open Microsoft.ClearScript.V8
open Serilog

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.JavaScript.Json

let private longJSMessage (shortMessage : string) (innerException : Exception) =
    let stackTrace =
        match innerException with
        | :? ScriptEngineException as e -> Some e.ScriptException
        | _ -> None

    match stackTrace with
    | None -> shortMessage
    | Some trace ->
        let innerMessage = fullUserMessage innerException
        let fullMessage =
            if innerMessage = "" then
                shortMessage
            else if shortMessage = "" then
                innerMessage
            else
                sprintf "%s: %s" shortMessage innerMessage
        sprintf "%s\nJavaScript stack trace:\n%s" fullMessage ""

type JavaScriptRuntimeException (message : string, innerException : Exception, isUserException : bool, userData: JToken option) =
    inherit UserException(longJSMessage message innerException, innerException, isUserException, userData)

    new (message : string, innerException : Exception, isUserException : bool) =
        JavaScriptRuntimeException (message, innerException, isUserException, userExceptionData innerException)

    new (message : string, innerException : Exception) =
        JavaScriptRuntimeException (message, innerException, isUserException innerException, userExceptionData innerException)

    new (message : string) = JavaScriptRuntimeException (message, null, true, None)

    interface ICustomFormatException with
        member this.MessageContainsInnerError = message <> this.Message
        member this.ShortMessage = message

type Path = POSIXPath.Path
type Source = string

type ModuleFile =
    { Path : Path
      Source : Source
      AllowBroken : bool
    }

let moduleFile (path : Path) (source : Source) =
    { Path = path
      Source = source
      AllowBroken = false
    }

type private VirtualFSModule =
    { Path : Path
      DirPath : Path
      Module : V8Script
    } with
    interface IDisposable with
        member this.Dispose() = this.Module.Dispose()

let private pathAliases (path : Path) =
    seq {
        path
        POSIXPath.trimExtension path
    }

type JSEnvironment =
    { Files : ModuleFile seq
      SearchPath : Path seq
    }

let private convertPath (p : Path) =
    let normalized = POSIXPath.normalize p
    if POSIXPath.isAbsolute normalized then
        failwithf "Absolute path is not expected: %s" p
    normalized

let getJSExceptionUserData (e : ScriptEngineException) : JToken option =
    match e.ScriptException with
    | :? IJavaScriptObject as obj ->
        let userData = obj.["userData"]
        match userData with
        | :? Undefined
        | null -> None
        | _ ->
            use reader = new V8JsonReader(userData)
            try
                Some <| JToken.Load(reader)
            with
            | :? JsonReaderException as e -> raisefWithInner JavaScriptRuntimeException e "Failed to parse user data"
    | _ -> None

type private JSDocumentLoader (env : JSEnvironment) =
    inherit DocumentLoader()

    let searchPath = env.SearchPath |> Seq.map convertPath |> Seq.distinct |> Seq.toArray

    let makeModuleDocuments (modul : ModuleFile) =
        let info = DocumentInfo(modul.Path, Category = ModuleCategory.Standard)
        let document = StringDocument(info, modul.Source)
        Seq.map (fun npath -> (npath, document)) (pathAliases modul.Path)

    let documentsMap = env.Files |> Seq.collect makeModuleDocuments |> Map.ofSeqUnique

    override this.LoadDocument (settings : DocumentSettings, sourceInfo : Nullable<DocumentInfo>, specifier : string, category : DocumentCategory, contextCallback : DocumentContextCallback) =
        let inline moduleNotFound () = raisef JavaScriptRuntimeException "Module not found: %s" specifier

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

                let trySearch (search : Path) =
                    let fullPath = POSIXPath.normalize (POSIXPath.combine search specifier)
                    Map.tryFind fullPath documentsMap
                match Seq.tryPick trySearch searchPath with
                | Some doc -> doc
                | None -> moduleNotFound ()

    override this.LoadDocumentAsync (settings : DocumentSettings, sourceInfo : Nullable<DocumentInfo>, specifier : string, category : DocumentCategory, contextCallback : DocumentContextCallback) =
        Task.FromResult <| this.LoadDocument(settings, sourceInfo, specifier, category, contextCallback)

    // ClearScript doesn't allow to get an evaluated module and just returns `undefined`.
    // So to import a module, we need to inject it into the loader, and remove later.
    // Ugh.
    member val CurrentModule : StringDocument option = None with get, set

type JSRuntime (constraints : V8RuntimeConstraints) =
    let runtime = new V8Runtime(constraints)
    do
        runtime.EnableInterruptPropagation <- true

    let mutable metMemoryLimit = false

    member this.Runtime = runtime

    member this.SetMetMemoryLimit () =
        metMemoryLimit <- true

    member this.MetMemoryLimit = metMemoryLimit

type RuntimeLocal<'a when 'a : not struct> (create : JSRuntime -> 'a) =
    let table = ConditionalWeakTable<JSRuntime, 'a> ()

    member this.HasValue (runtime : JSRuntime) =
        match table.TryGetValue(runtime) with
        | (ret, _) -> ret

    member this.GetValue (runtime : JSRuntime) =
        table.GetValue(runtime, ConditionalWeakTable.CreateValueCallback<JSRuntime,'a> create)

let private preludeSource = """
    // Needed because ClearScript doesn't allow to throw plain JavaScript exceptions
    // from a host function.
    class WrappedHostException {
        constructor(e) {
            this.e = e;
        }
    }
    globalThis.WrappedHostException = WrappedHostException;

    globalThis.unwrapHostResult = (ret) => {
        // A host function wrapper below wraps host exceptions
        // into WrappedHostException and returns them. This way
        // we keep the 1:1 mapping between host and JS exceptions.
        if (ret instanceof WrappedHostException) {
            throw ret.e;
        } else {
            return ret;
        }
    };

    // Remove what shouldn't be accessed. `gc` can't be removed, so redefine it instead.
    globalThis.gc = undefined;
    delete globalThis.console;
"""

let private preludeDoc =
    let info = DocumentInfo("prelude.js", Category=ModuleCategory.Standard)
    RuntimeLocal(fun runtime -> runtime.Runtime.Compile(info, preludeSource))

type JSEngine (runtime : JSRuntime, env : JSEnvironment) =
    let engine = runtime.Runtime.CreateScriptEngine(
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
        ignore <| engine.Evaluate(preludeDoc.GetValue(runtime))

    let jsonEngine = V8JsonEngine(engine)

    // Used to store and later restore the original host exceptions in case they
    // go through JS, because ClearScript doesn't allow to set internal object fields.
    let weakMapConstructor = engine.Global.["WeakMap"] :?> IJavaScriptObject
    let exceptionsMap = weakMapConstructor.Invoke(true) :?> IJavaScriptObject
    let errorConstructor = engine.Global.["Error"] :?> IJavaScriptObject
    let wrappedHostExceptionConstructor = engine.Global.["WrappedHostException"] :?> IJavaScriptObject

    do
        ignore <| engine.Global.DeleteProperty("WrappedHostException")

    let mutable interruptEDI : ExceptionDispatchInfo option = None
    let mutable topLevelCancellationToken : CancellationToken option = None

    let createError (message : string) =
        errorConstructor.Invoke(true, message) :?> IJavaScriptObject

    member this.InternalHandleHostExceptionInJS (hostException : exn) =

        let interrupt (e : exn) =
            match interruptEDI with
            | Some oldE ->
                // A call to engine unwrapped into `runFunctionInRuntime` happened.
                Log.Error(oldE.SourceException, "The JavaScript engine got interrupted before, but the host didn't wrap the call properly.")
            | None -> ()
            engine.Interrupt()
            let edi = ExceptionDispatchInfo.Capture(e)
            interruptEDI <- Some edi
            edi.Throw()
            failwith "Impossible"
        try
            let error =
                match hostException with
                | :? OperationCanceledException as e ->
                    interrupt e
                | :? JSException as e ->
                    e.Value
                | :? ScriptEngineException as e when e.IsFatal ->
                    interrupt e
                | :? ScriptEngineException as e when not <| isNull e.ScriptExceptionAsObject ->
                    e.ScriptExceptionAsObject :?> IJavaScriptObject
                | e ->
                    createError (fullUserMessage e)
            ignore <| exceptionsMap.InvokeMethod("set", error, ExceptionDispatchInfo.Capture(hostException))
            wrappedHostExceptionConstructor.Invoke(true, error) :?> IJavaScriptObject
        with
        | :? OperationCanceledException as e ->
            interrupt e
        | :? ScriptEngineException as e when e.IsFatal ->
            interrupt e
        | e ->
            Log.Error(e, "Unexpected exception while saving the original exception.")
            reraise ()

    member this.InternalTryRestoreJSException (jsException : exn) =
        match jsException with
        | :? ScriptEngineException as e when e.IsFatal ->
            runtime.SetMetMemoryLimit()
        | _ -> ()
        match interruptEDI with
        | Some edi ->
            interruptEDI <- None
            edi.Throw()
        | None -> ()
        match jsException with
        | :? ScriptEngineException as e when not <| isNull e.ScriptExceptionAsObject ->
            let maybeEdi =
                try
                    match exceptionsMap.InvokeMethod("get", e.ScriptExceptionAsObject) with
                    | :? ExceptionDispatchInfo as edi -> Some edi
                    | _ -> None
                with
                | :? ScriptEngineException as e when e.IsFatal ->
                    runtime.SetMetMemoryLimit()
                    reraise ()
            match maybeEdi with
            | Some edi -> edi.Throw()
            | None -> ()
        | _ -> ()


    member inline private this.EvaluateJS<'a> (cancellationToken : CancellationToken) ([<InlineIfLambda>] f : unit -> 'a) : 'a =
        try
            try
                match topLevelCancellationToken with
                | None ->
                    topLevelCancellationToken <- Some cancellationToken
                    try
                        use handle = cancellationToken.UnsafeRegister((fun _ -> engine.Interrupt()), null)
                        f ()
                    finally
                        topLevelCancellationToken <- None
                | Some otherCancellationToken ->
                    if cancellationToken <> CancellationToken.None && cancellationToken <> otherCancellationToken then
                        failwith "Nested cancellation tokens are not supported"
                    f ()
            with
            | e ->
                this.InternalTryRestoreJSException e
                reraise ()
        with
        | :? ScriptEngineException as e ->
            let userData = getJSExceptionUserData e
            raise <| JavaScriptRuntimeException("", e, true, userData)

    member inline private this.EvaluateAsyncJS<'a> (cancellationToken : CancellationToken) ([<InlineIfLambda>] f : unit -> Task<'a>) : Task<'a> =
        task {
            try
                try

                    match topLevelCancellationToken with
                    | None ->
                        topLevelCancellationToken <- Some cancellationToken
                        try
                            use handle = cancellationToken.UnsafeRegister((fun _ -> engine.Interrupt()), null)
                            return! f ()
                        finally
                            topLevelCancellationToken <- None
                    | Some otherCancellationToken ->
                        if cancellationToken <> CancellationToken.None && cancellationToken <> otherCancellationToken then
                            failwith "Nested cancellation tokens are not supported"
                        return! f ()
                with
                | e ->
                    this.InternalTryRestoreJSException e
                    return reraise' e
            with
            | :? ScriptEngineException as e ->
                let userData = getJSExceptionUserData e
                return raise <| JavaScriptRuntimeException("", e, true, userData)
        }

    // As far as I understand, ClearScript doesn't have any way to compile modules and then
    // return them when requested by imports the way pure V8 has.
    // Instead, they cache Module instances by the source code digests, and there's
    // no way to call `Instantiate()`.
    // We evaluate them immediately instead.
    member this.CreateModule (file : ModuleFile) : IJavaScriptObject =
        let info = DocumentInfo(file.Path, Category=ModuleCategory.Standard)
        let doc = StringDocument(info, file.Source)
        this.EvaluateJS CancellationToken.None <| fun () ->
            let glob = engine.Script :?> IDictionary<string, obj>
            // First evaluation; needed because we don't want to expose our internals,
            // and the evaluations are cached.
            let snippetInfo = DocumentInfo("import.js", Category=ModuleCategory.Standard)
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
                    ignore <| engine.Evaluate(snippetInfo, """
                        import * as current from '__current__';
                        globalThis.current = current;
                    """)
                    glob.["current"] :?> IJavaScriptObject
                finally
                    match oldCurrent with
                    | Some current -> glob.["current"] <- current
                    | None -> ignore <| glob.Remove("current")
            finally
                loader.CurrentModule <- None

    member this.CreateDefaultFunction (file : ModuleFile) : IJavaScriptObject =
        let jsModule = this.CreateModule file
        match jsModule.GetProperty("default") with
        | :? IJavaScriptObject as obj when obj.Kind = JavaScriptObjectKind.Function -> obj
        | _ -> raisef JavaScriptRuntimeException "Invalid default function"

    member this.Engine = engine
    member this.Runtime = runtime
    member this.Json = jsonEngine

    member this.CreateError message = createError message

    member inline this.WrapHostFunction ([<InlineIfLambda>] f : unit -> 'a) =
        try
            f () :> obj
        with
        | e ->
            this.InternalHandleHostExceptionInJS e :> obj

    member inline this.WrapAsyncHostFunction ([<InlineIfLambda>] f : unit -> Task<'a>) =
        task {
            try
                let! ret = f ()
                return ret :> obj
            with
            | e ->
                return this.InternalHandleHostExceptionInJS e :> obj
        }

    member this.RunJSFunction (func : IJavaScriptObject, args : obj[], cancellationToken : CancellationToken) =
        if func.Engine <> engine then
            failwith "Function belongs to another engine"
        this.EvaluateJS cancellationToken <| fun () -> func.InvokeAsFunction(args)

    member this.RunJSFunction (func : IJavaScriptObject, args : obj[]) =
        this.RunJSFunction(func, args, CancellationToken.None)

    member this.RunAsyncJSFunction (func : IJavaScriptObject, args : obj[], cancellationToken : CancellationToken) =
        this.EvaluateAsyncJS cancellationToken <| fun () ->
            match func.InvokeAsFunction(args) with
            | :? Task<obj> as t -> t
            | ret -> Task.result(ret)

    member this.RunAsyncJSFunction (func : IJavaScriptObject, args : obj[]) =
        this.RunAsyncJSFunction(func, args, CancellationToken.None)

// ClearScript doesn't allow you to set the `ScriptException` externally,
// so we need our own type of an exception for that.
and JSException (message : string, value : IJavaScriptObject, innerException : exn) =
    inherit Exception(message, innerException)

    new (message : string, value : IJavaScriptObject) =
        JSException(message, value, null)

    new (message : string, innerException : exn, engine : JSEngine) =
        let error = engine.CreateError message
        JSException(message, error, innerException)

    new (message : string, engine : JSEngine) =
        let error = engine.CreateError message
        JSException(message, error, null)

    member this.Value = value