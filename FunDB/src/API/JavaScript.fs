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
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.JavaScript.Runtime

[<SerializeAsObject("error")>]
type APICallErrorInfo =
    | [<CaseName("call")>] ACECall of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | ACECall msg -> msg

        interface IAPIError with
            member this.Message = this.Message

type private APIHandle =
    { API : IFunDBAPI
      Logger : ILogger
    }

let private runApiCall<'a, 'e when 'e :> IAPIError> (context : Context) (f : unit -> Task<Result<'a, 'e>>) : Func<Task<Value.Value>> =
    let run () =
        task {
            let! res = f ()
            match res with
            | Ok r -> return V8JsonWriter.Serialize(context, r)
            | Error e -> return raise <| JavaScriptRuntimeException(e.Message)
        }
    Func<_>(run)

type APITemplate (isolate : Isolate) =
    let mutable currentHandle = None : APIHandle option
    let mutable errorConstructor = None : Value.Function option
    let mutable runtime = Unchecked.defaultof<IJSRuntime>

    let throwError (context : Context) (e : 'a when 'a :> IAPIError) : 'b =
        let body = V8JsonWriter.Serialize(context, e)
        let constructor = Option.get errorConstructor
        let exc = constructor.NewInstance(body)
        raise <| JSException(e.Message, exc.Value)

    let throwCallError (context : Context) : StringFormat<'a, 'b> -> 'a =
        let thenRaise str =
            throwError context (ACECall str)
        kprintf thenRaise

    let jsDeserialize (context : Context) (v : Value.Value) : 'a =
        let ret =
            try
                V8JsonReader.Deserialize<'a>(v)
            with
            | :? JsonReaderException as e -> throwCallError context "Failed to parse value: %s" e.Message
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

        template.Set("renderFunQLName", FunctionTemplate.New(template.Isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let ret = args.[0].GetString().Get() |> renderFunQLName
            Value.String.New(template.Isolate, ret).Value
        ))

        template.Set("renderFunQLValue", FunctionTemplate.New(template.Isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            use reader = new V8JsonReader(args.[0])
            let source =
                try
                    JToken.Load(reader)
                with
                | :? JsonReaderException as e -> throwCallError context "Failed to parse value: %s" e.Message
            let ret = renderFunQLJson source
            Value.String.New(template.Isolate, ret).Value
        ))

        let fundbTemplate = ObjectTemplate.New(isolate)
        template.Set("FunDB", fundbTemplate)

        fundbTemplate.Set("getUserView", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 2 then
                throwCallError context "Number of arguments must be between 1 and 2"
            let source = jsDeserialize context args.[0] : UserViewSource
            let uvArgs =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[1] : RawArguments
                else
                    Map.empty
            let chunk =
                if args.Length >= 3 && args.[2].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[2] : SourceQueryChunk
                else
                    emptyQueryChunk
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.UserViews.GetUserView source uvArgs chunk emptyUserViewFlags
            runtime.EventLoop.NewPromise(context, run).Value
        ))
        fundbTemplate.Set("getUserViewInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let source = jsDeserialize context args.[0] : UserViewSource
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.UserViews.GetUserViewInfo source emptyUserViewFlags
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("getEntityInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.Entities.GetEntityInfo ref
            runtime.EventLoop.NewPromise(context, run).Value
        ))
        fundbTemplate.Set("insertEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let rawArgs = jsDeserialize context args.[1] : RawArguments
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.Entities.InsertEntity ref rawArgs
            runtime.EventLoop.NewPromise(context, run).Value
        ))
        fundbTemplate.Set("updateEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 3 then
                throwCallError context "Number of arguments must be 3"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsInt context args.[1]
            let rawArgs = jsDeserialize context args.[2] : RawArguments
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.Entities.UpdateEntity ref id rawArgs
            runtime.EventLoop.NewPromise(context, run).Value
        ))
        fundbTemplate.Set("deleteEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                throwCallError context "Number of arguments must be 2"
            let ref = jsDeserialize context args.[0] : ResolvedEntityRef
            let id = jsInt context args.[1]
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.Entities.DeleteEntity ref id
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("deferConstraints", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let func = args.[0].GetFunction()
            let handle = Option.get currentHandle
            let run () =
                task {
                    let! res = handle.API.Entities.DeferConstraints <| fun () -> func.CallAsync(null)
                    match res with
                    | Ok r -> return r
                    | Error e -> return raise <| JavaScriptRuntimeException(e.Message)
                }
            runtime.EventLoop.NewPromise(context, Func<_>(run)).Value
        ))

        fundbTemplate.Set("getDomainValues", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 3 then
                throwCallError context "Number of arguments must be between 1 and 3"
            let ref = jsDeserialize context args.[0] : ResolvedFieldRef
            let rowId =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    Some <| int (args.[1].GetNumber())
                else
                    None
            let chunk =
                if args.Length >= 3 && args.[2].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize context args.[2] : SourceQueryChunk
                else
                    emptyQueryChunk
            let handle = Option.get currentHandle
            let run = runApiCall context <| fun () -> handle.API.Domains.GetDomainValues ref rowId chunk
            runtime.EventLoop.NewPromise(context, run).Value
        ))

        fundbTemplate.Set("writeEvent", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let details = jsString context args.[0]
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {} wrote event from JavaScript: {}", string handle.API.Request.Source, details)
            handle.API.Request.WriteEvent (fun event ->
                event.Type <- "jsEvent"
                event.Details <- details
            )
            Value.Undefined.New(isolate)
        ))

        fundbTemplate.Set("writeEventSync", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                throwCallError context "Number of arguments must be 1"
            let details = jsString context args.[0]
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {} wrote sync event from JavaScript: {}", string handle.API.Request.Source, details)
            handle.API.Request.WriteEventSync (fun event ->
                event.Type <- "jsEvent"
                event.Details <- details
            )
            Value.Undefined.New(isolate)
        ))

        template

    let preludeScriptSource = "
class FunDBError extends Error {
  constructor(body) {
    super(body.message);
    this.body = body;
  }
}
global.FunDBError = FunDBError;
    "
    let preludeScript = UnboundScript.Compile(Value.String.New(isolate, preludeScriptSource.Trim()), ScriptOrigin("prelude.js"))

    member this.Isolate = isolate

    member this.SetAPI (api : IFunDBAPI) =
        assert (Option.isNone currentHandle)
        currentHandle <- Some
            { API = api
              Logger = api.Request.Context.LoggerFactory.CreateLogger<APITemplate>()
            }

    member this.ResetAPI api =
        currentHandle <- None

    interface IJavaScriptTemplate with
        member this.ObjectTemplate = template
        member this.FinishInitialization newRuntime context =
            runtime <- newRuntime
            let p = preludeScript.Bind(context)
            ignore <| p.Run()
            errorConstructor <- Some <| context.Global.Get("FunDBError").GetFunction()
