module FunWithFlags.FunDB.API.JavaScript

open NetJs
open NetJs.Json
open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types
open FunWithFlags.FunDB.JavaScript.Runtime

type private APIHandle =
    { API : IFunDBAPI
      Logger : ILogger
    }

type APITemplate (isolate : Isolate) =
    let mutable currentHandle = None : APIHandle option
    let mutable errorConstructor = None : Value.Function option

    let returnResult = function
    | Ok r ->
        let context = isolate.CurrentContext
        V8JsonWriter.Serialize(context, r)
    | Error e ->
        let context = isolate.CurrentContext
        let body = V8JsonWriter.Serialize(context, e)
        let constructor = Option.get errorConstructor
        let exc = constructor.NewInstance(body)
        raise <| JSException("Exception while making API call", exc.Value)

    let template =
        let template = Template.ObjectTemplate.New(isolate)

        template.Set("renderSqlName", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let ret = args.[0].GetString().Get() |> renderSqlName
            Value.String.New(isolate, ret).Value
        ))

        let fundbTemplate = Template.ObjectTemplate.New(isolate)
        template.Set("FunDB", fundbTemplate)

        fundbTemplate.Set("getUserView", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length < 1 || args.Length > 2 then
                invalidArg "args" "Number of arguments must be between 1 and 2"
            let source = V8JsonReader.Deserialize<UserViewSource>(args.[0])
            let args =
                if args.Length >= 2 then
                    V8JsonReader.Deserialize<RawArguments>(args.[1])
                else
                    Map.empty
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.UserViews.GetUserView source args false
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("getUserViewInfo", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let source = V8JsonReader.Deserialize<UserViewSource>(args.[0])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.UserViews.GetUserViewInfo source false
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("getEntityInfo", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.GetEntityInfo ref
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("insertEntity", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                invalidArg "args" "Number of arguments must be 2"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let rawArgs = V8JsonReader.Deserialize<RawArguments>(args.[1])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.InsertEntity ref rawArgs
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("updateEntity", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 3 then
                invalidArg "args" "Number of arguments must be 3"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let rawArgs = V8JsonReader.Deserialize<RawArguments>(args.[2])
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.UpdateEntity ref id rawArgs
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("deleteEntity", Template.FunctionTemplate.New(isolate, fun args ->
            let context = isolate.CurrentContext
            if args.Length <> 2 then
                invalidArg "args" "Number of arguments must be 2"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let handle = Option.get currentHandle
            isolate.EventLoop.NewPromise(context, fun () -> task {
                let! ret = handle.API.Entities.DeleteEntity ref id
                return returnResult ret
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("writeEvent", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let details = args.[0].GetString().Get()
            let handle = Option.get currentHandle
            handle.Logger.LogInformation("Source {} wrote event from JavaScript: {}", string handle.API.Request.Source, details)
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
