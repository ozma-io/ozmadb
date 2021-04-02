module FunWithFlags.FunDB.JavaScript.Runtime

open System
open System.Threading.Tasks
open System.Runtime.CompilerServices
open FSharp.Control.Tasks.Affine
open NetJs
open NetJs.Value
open NetJs.Template

open FunWithFlags.FunUtils

type JavaScriptRuntimeException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = JavaScriptRuntimeException (message, null)

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

type JSRuntime<'a when 'a :> IJavaScriptTemplate> (isolate : Isolate, templateConstructor : Isolate -> 'a, env : JSEnvironment) as this =
    let mutable currentEventLoop = None : EventLoop option
    let template = templateConstructor isolate

    let context = Context.New(isolate, template.ObjectTemplate)
    do
        context.Global.Set("global", context.Global.Value)
        template.FinishInitialization this context

    let makeModule (file : ModuleFile) =
        let normalized = convertPath file.Path
        let modul = Module.Compile(String.New(context.Isolate, file.Source), ScriptOrigin(normalized, IsModule = true))
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
        if POSIXPath.isAbsolute path then
            raise <| JSException.NewFromString(context, "Module not found")
        let fullPath = POSIXPath.normalize (POSIXPath.combine currentModule.DirPath path)
        match Map.tryFind fullPath modulePathsMap with
        | Some modul -> modul.Module
        | None ->
            let firstComponent = (POSIXPath.splitComponents path).[0]
            if firstComponent = "." || firstComponent = ".." then
                raise <| JSException.NewFromString(context, "Module not found")
            let trySearch (search : Path) =
                let fullPath = POSIXPath.normalize (POSIXPath.combine search path)
                Map.tryFind fullPath modulePathsMap
            match Seq.tryPick trySearch searchPath with
            | Some modul -> modul.Module
            | None -> raise <| JSException.NewFromString(context, "Module not found")

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
        jsModule.Module.Instantiate(context, Func<_, _, _> resolveOne)
        jsModule.Module

    member this.CreateDefaultFunction (file : ModuleFile) =
        let jsModule = this.CreateModule file
        ignore <| jsModule.Evaluate()
        jsModule.Namespace.Get("default").GetFunction()

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

let stackTraceString (e : JSException) =
    match JSException.GetStackTrace e.Value with
    | null -> "(no stack trace)"
    | trace -> trace.ToPrettyString()