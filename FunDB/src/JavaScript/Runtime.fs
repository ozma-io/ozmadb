module FunWithFlags.FunDB.JavaScript.Runtime

open System
open System.Runtime.CompilerServices
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

type IJavaScriptTemplate =
    abstract member ObjectTemplate : ObjectTemplate
    abstract member FinishInitialization : Context -> unit

type Path = string
type Source = string

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

type JSRuntime (template : IJavaScriptTemplate, moduleSources : (Path * Source) seq) =
    let isolate = template.ObjectTemplate.Isolate

    let context = Context.New(isolate, template.ObjectTemplate)
    do
        context.Global.Set("global", context.Global.Value)
        template.FinishInitialization context

    let makeModule (path : Path) src =
        let modul = Module.Compile(String.New(context.Isolate, src), ScriptOrigin(path, IsModule = true))
        { Path = path
          Module = modul
          DirPath = POSIXPath.dirName path
        }

    let modules =
        moduleSources |> Map.ofSeqUnique |> Map.toSeq |> Seq.map (uncurry makeModule) |> Seq.cache
    let modulePathsMap =
        modules |> Seq.collect (fun modul -> Seq.map (fun npath -> (npath, modul)) (pathAliases modul.Path)) |> Map.ofSeq
    let moduleIdsMap =
        modules |> Seq.map (fun modul -> (modul.Module.IdentityHash, modul)) |> Map.ofSeq

    let resolveModule path id =
        let currentModule = Map.find id moduleIdsMap
        let fullPath = POSIXPath.normalizePath (POSIXPath.combinePath currentModule.DirPath path)
        match Map.tryFind fullPath modulePathsMap with
        | None -> raise <| JSException.NewFromString(context, "Module not found")
        | Some modul -> modul.Module

    do
        for modul in modules do
            modul.Module.Instantiate(context, Func<_, _, _> resolveModule)

    member this.CreateModule (path : string) (script : string) =
        let jsModule = Module.Compile(String.New(context.Isolate, script), ScriptOrigin(path, IsModule = true))
        jsModule.Instantiate(context, Func<_, _, _> resolveModule)
        jsModule

    member this.CreateDefaultFunction (path : string) (script : string) =
        let jsModule = this.CreateModule path script
        ignore <| jsModule.Evaluate()
        jsModule.Namespace.Get("default").GetFunction()

    member this.Context = context
    member this.Isolate = isolate