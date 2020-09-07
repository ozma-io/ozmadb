module FunWithFlags.FunDB.API.JavaScript

open NetJs
open NetJs.Json
open NetJs.Template
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.JavaScript.Runtime

type private APIHandle =
    { API : IFunDBAPI
      Logger : ILogger
    }

let inline jsDeserialize< ^a when ^a : not struct > (v : Value.Value) =
    let ret = V8JsonReader.Deserialize< ^a >(v)
    if isRefNull ret then
        failwith "Value must not be null"
    ret

type APITemplate (isolate : Isolate) =
    let mutable currentHandle = None : APIHandle option
    let mutable errorConstructor = None : Value.Function option

    let returnResult context = function
    | Ok r ->
        V8JsonWriter.Serialize(context, r)
    | Error e ->
        let body = V8JsonWriter.Serialize(context, e)
        let constructor = Option.get errorConstructor
        let exc = constructor.NewInstance(body)
        raise <| JSException("Exception while making API call", exc.Value)

    let template =
        let template = ObjectTemplate.New(isolate)

        template.Set("renderFunQLName", FunctionTemplate.New(template.Isolate, fun args ->
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            let ret = args.[0].GetString().Get() |> renderFunQLName
            Value.String.New(template.Isolate, ret).Value
        ))

        template.Set("renderFunQLValue", FunctionTemplate.New(template.Isolate, fun args ->
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            use reader = new V8JsonReader(args.[0])
            let source = JToken.Load(reader)
            let ret = renderFunQLJson source
            Value.String.New(template.Isolate, ret).Value
        ))

        let fundbTemplate = ObjectTemplate.New(isolate)
        template.Set("FunDB", fundbTemplate)

        fundbTemplate.Set("getUserView", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 2 then
                failwith "Number of arguments must be between 1 and 2"
            let source = jsDeserialize<UserViewSource>(args.[0])
            let args =
                if args.Length >= 2 && args.[1].ValueType <> Value.ValueType.Undefined then
                    jsDeserialize<RawArguments>(args.[1])
                else
                    Map.empty
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.UserViews.GetUserView source args false
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("getUserViewInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            let source = jsDeserialize<UserViewSource>(args.[0])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.UserViews.GetUserViewInfo source false
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("getEntityInfo", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            let ref = jsDeserialize<ResolvedEntityRef>(args.[0])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.GetEntityInfo ref
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("insertEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                failwith "Number of arguments must be 2"
            let ref = jsDeserialize<ResolvedEntityRef>(args.[0])
            let rawArgs = jsDeserialize<RawArguments>(args.[1])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.InsertEntity ref rawArgs
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("updateEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 3 then
                failwith "Number of arguments must be 3"
            let ref = jsDeserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let rawArgs = jsDeserialize<RawArguments>(args.[2])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.UpdateEntity ref id rawArgs
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("deleteEntity", FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                failwith "Number of arguments must be 2"
            let ref = jsDeserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.DeleteEntity ref id
                return returnResult context ret
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("writeEvent", FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            let details = args.[0].GetString().Get()
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {} wrote event from JavaScript: {}", string handle.API.Request.Source, details)
            handle.API.Request.WriteEvent (fun event ->
                event.Type <- "jsEvent"
                event.Details <- details
            )
            Value.Undefined.New(isolate)
        ))

        fundbTemplate.Set("writeEventSync", FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                failwith "Number of arguments must be 1"
            let details = args.[0].GetString().Get()
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
        member this.FinishInitialization context =
            let p = preludeScript.Bind(context)
            ignore <| p.Run()
            errorConstructor <- Some <| context.Global.Get("FunDBError").GetFunction()
