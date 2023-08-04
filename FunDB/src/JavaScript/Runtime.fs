module FunWithFlags.FunDB.JavaScript.Runtime

open System
open System.Threading
open System.Threading.Tasks
open System.Runtime.CompilerServices
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open NetJs
open NetJs.Value
open NetJs.Template
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception

let private longJSMessage (shortMessage : string) (innerException : Exception) =
    let stackTrace =
        match innerException with
        | :? JSException as e -> e.JSStackTrace
        | :? CallbackException as e -> e.JSStackTrace
        | _ -> null

    match stackTrace with
    | null -> shortMessage
    | trace ->
        let innerMessage = fullUserMessage innerException
        let fullMessage =
            if innerMessage = "" then
                shortMessage
            else if shortMessage = "" then
                innerMessage
            else
                sprintf "%s: %s" shortMessage innerMessage
        sprintf "%s\nJavaScript stack trace:\n%s" fullMessage (trace.ToPrettyString())

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

type IsolateLocal<'a when 'a : not struct> (create : Isolate -> 'a) =
    let table = ConditionalWeakTable<Isolate, 'a> ()

    member this.HasValue (isolate : Isolate) =
        match table.TryGetValue(isolate) with
        | (ret, _) -> ret

    member this.GetValue (isolate : Isolate) =
        table.GetValue(isolate, ConditionalWeakTable.CreateValueCallback<Isolate,'a> create)

type Path = POSIXPath.Path
type Source = string

type ModuleFile =
    { Path : Path
      Source : Source
    }

type private VirtualFSModule =
    { Path : Path
      DirPath : Path
      Module : Module
    }

let private pathAliases (path : Path) =
    seq {
        path
        POSIXPath.trimExtension path
    }

type IJavaScriptTemplate =
    abstract member ObjectTemplate : ObjectTemplate
    abstract member FinishInitialization : IJSRuntime -> Context -> unit

and IJSRuntime =
    abstract member CreateModule : ModuleFile -> Module
    abstract member CreateDefaultFunction : ModuleFile -> Function
    abstract member Context : Context
    abstract member Isolate : Isolate
    abstract member EventLoop : EventLoop
    abstract member EventLoopScope : (EventLoop -> Task<'a>) -> Task<'a>

type JSEnvironment =
    { Files : ModuleFile seq
      SearchPath : Path seq
    }

let private convertPath (p : Path) =
    let normalized = POSIXPath.normalize p
    if POSIXPath.isAbsolute normalized then
        failwithf "Absolute path is not expected: %s" p
    normalized

let getJSExceptionUserData (e : JSException) : JToken option =
    match e.Value.Data with
    | :? Value.Object as obj ->
        let userData = obj.Get("userData")
        match userData.Data with
        | :? Value.Undefined
        | :? Value.Null -> None
        | _ ->
            use reader = new V8JsonReader(userData)
            try
                Some <| JToken.Load(reader)
            with
            | :? JsonReaderException as e -> raisefWithInner JavaScriptRuntimeException e "Failed to parse user data"
    | _ -> None

type JSRuntime<'a when 'a :> IJavaScriptTemplate> (isolate : Isolate, templateConstructor : Isolate -> 'a, env : JSEnvironment) as this =
    let mutable currentEventLoop = None : EventLoop option
    let template = templateConstructor isolate

    let context = Context.New(isolate, template.ObjectTemplate)
    do
        context.Global.Set("global", context.Global.Value)
        template.FinishInitialization this context

    let makeModule (file : ModuleFile) =
        let normalized = convertPath file.Path
        let modul =
            try
                Module.Compile(String.New(context.Isolate, file.Source), ScriptOrigin(normalized, IsModule = true))
            with
            | :? JSException as e -> raisefUserWithInner JavaScriptRuntimeException e "Uncaught error during compilation"
        { Path = normalized
          Module = modul
          DirPath = POSIXPath.dirName normalized
        }

    let modules =
        env.Files |> Seq.map makeModule |> Seq.cache
    let modulePathsMap =
        modules |> Seq.collect (fun modul -> Seq.map (fun npath -> (npath, modul)) (pathAliases modul.Path)) |> Map.ofSeqUnique
    let moduleIdsMap =
        modules |> Seq.map (fun modul -> (modul.Module.IdentityHash, modul)) |> Map.ofSeq

    let searchPath = env.SearchPath |> Seq.map convertPath |> Seq.distinct |> Seq.toArray

    let resolveModule currentModule path =
        let inline moduleNotFound () = raise <| JSException.NewFromString(context, sprintf "Module not found: %s" path)

        if POSIXPath.isAbsolute path then
            moduleNotFound ()
        let fullPath = POSIXPath.normalize (POSIXPath.combine currentModule.DirPath path)
        match Map.tryFind fullPath modulePathsMap with
        | Some modul -> modul.Module
        | None ->
            let firstComponent = (POSIXPath.splitComponents path).[0]
            if firstComponent = "." || firstComponent = ".." then
                moduleNotFound ()
            let trySearch (search : Path) =
                let fullPath = POSIXPath.normalize (POSIXPath.combine search path)
                Map.tryFind fullPath modulePathsMap
            match Seq.tryPick trySearch searchPath with
            | Some modul -> modul.Module
            | None -> moduleNotFound ()

    do
        let resolveOne path id =
            let currentModule = Map.find id moduleIdsMap
            resolveModule currentModule path
        for modul in modules do
            modul.Module.Instantiate(context, Func<_, _, _> resolveOne)

    member this.CreateModule (file : ModuleFile) =
        let jsModule = makeModule file
        let resolveOne path id =
            let currentModule =
                if id = jsModule.Module.IdentityHash then
                    jsModule
                else
                    Map.find id moduleIdsMap
            resolveModule currentModule path
        try
            jsModule.Module.Instantiate(context, Func<_, _, _> resolveOne)
        with
        | :? JSException as e -> raisefUserWithInner JavaScriptRuntimeException e "Uncaught error during instantiation"
        jsModule.Module

    member this.CreateDefaultFunction (file : ModuleFile) =
        let jsModule =
            try
                let jsModule = this.CreateModule file
                ignore <| jsModule.Evaluate()
                jsModule
            with
            | :? JSException as e -> raisefUserWithInner JavaScriptRuntimeException e "Uncaught error during evaluation"
        try
            jsModule.Namespace.Get("default").GetFunction()
        with
        | :? NetJsException as e -> raisefUserWithInner JavaScriptRuntimeException e "Couldn't get default module"

    member this.Context = context
    member this.Isolate = isolate
    member this.EventLoop = Option.get currentEventLoop
    member this.API = template

    member this.EventLoopScope (f : EventLoop -> Task<'r>) =
        task {
            if Option.isSome currentEventLoop then
                // Yield to ensure all uses of current event loop are finished. Otherwise, tasks meant for outer
                // loop can escape to the inner loop due to eager evaluation of tasks.
                //
                // Is there any better way to ensure this?
                do! Task.Yield ()
            let oldEventLoop = currentEventLoop
            let loop = EventLoop ()
            currentEventLoop <- Some loop
            try
                return! f loop
            finally
                currentEventLoop <- oldEventLoop
        }

    interface IJSRuntime with
        member this.CreateModule file = this.CreateModule file
        member this.CreateDefaultFunction file = this.CreateDefaultFunction file
        member this.Context = context
        member this.Isolate = isolate
        member this.EventLoop = this.EventLoop
        member this.EventLoopScope f = this.EventLoopScope f

let inline runFunctionInRuntime (runtime : IJSRuntime) (func : Function) (cancellationToken : CancellationToken) (args : Value.Value[]) =
    try
        func.Call(cancellationToken, null, args)
    with
    | :? CallbackException as e ->
        raise <| JavaScriptRuntimeException("", e, isUserException e.InnerException, userExceptionData e.InnerException)
    | :? JSException as e ->
        raise <| JavaScriptRuntimeException("", e, true, getJSExceptionUserData e)
    | :? NetJsException as e ->
        raisefUserWithInner JavaScriptRuntimeException e ""

let inline runAsyncFunctionInRuntime (runtime : IJSRuntime) (func : Function) (cancellationToken : CancellationToken) (args : Value.Value[]) =
    task {
        try
            return! runtime.EventLoopScope <| fun eventLoop ->
                task {
                    let maybeResult = func.Call(cancellationToken, null, args)
                    eventLoop.Run(cancellationToken)
                    return maybeResult.GetValueOrPromiseResult ()
                }
        with
        | :? CallbackException as e ->
            return raise <| JavaScriptRuntimeException("", e, isUserException e.InnerException, userExceptionData e.InnerException)
        | :? JSException as e ->
            return raise <| JavaScriptRuntimeException("", e, true, getJSExceptionUserData e)
        | :? NetJsException as e ->
            return raisefUserWithInner JavaScriptRuntimeException e ""
    }