module FunWithFlags.FunDB.JavaScript.Runtime

open System.Runtime.CompilerServices
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open NetJs
open NetJs.Value
open NetJs.Template

open FunWithFlags.FunUtils

type IsolateLocal<'a when 'a : not struct> (create : Isolate -> 'a) =
    let table = ConditionalWeakTable<Isolate, 'a> ()

    member this.HasValue (isolate : Isolate) =
        match table.TryGetValue(isolate) with
        | (ret, _) -> ret

    member this.GetValue (isolate : Isolate) =
        table.GetValue(isolate, fun isolate -> create isolate)

type IJavaScriptTemplate =
    abstract member ObjectTemplate : ObjectTemplate
    abstract member FinishInitialization : Context -> unit

let createFunction (context : Context) (name : string) (script : string) =
    context.Global.Set("global", context.Global.Value)
    let func =
        let jsModule = Module.Compile(String.New(context.Isolate, script), ScriptOrigin(name, IsModule = true))
        jsModule.Instantiate(context, (fun name id -> raise <| JSException.NewFromString(context, "No imports allowed")))
        ignore <| jsModule.Evaluate()
        jsModule.Namespace.Get("default").GetFunction()
    func

type CachedFunction private (func : Function) =
    static member FromScript (api : IJavaScriptTemplate) (name : string) (script : string) =
        let context = Context.New(api.ObjectTemplate.Isolate, api.ObjectTemplate)
        context.Global.Set("global", context.Global.Value)
        api.FinishInitialization context
        let func =
            let jsModule = Module.Compile(String.New(context.Isolate, script), ScriptOrigin(name, IsModule = true))
            jsModule.Instantiate(context, (fun name id -> raise <| JSException.NewFromString(context, "No imports allowed")))
            ignore <| jsModule.Evaluate()
            jsModule.Namespace.Get("default").GetFunction()
        CachedFunction func

    member this.Context = func.CreationContext
    member this.Function = func