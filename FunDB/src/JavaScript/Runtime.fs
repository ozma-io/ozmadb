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

type CachedFunction private (func : Function) =
    static member FromScript (isolate : Isolate, template : ObjectTemplate, script : string, functionName : string) =
        let context = Context.New(isolate, template)
        do
            let script = Script.Compile(context, String.New(isolate, script))
            ignore <| script.Run()
        let func = context.Global.Get(functionName).GetFunction()
        CachedFunction func

    member this.Context = func.CreationContext
    member this.Function = func