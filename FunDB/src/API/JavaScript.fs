module FunWithFlags.FunDB.API.JavaScript

open NetJs
open NetJs.Json
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types

type APITemplate (isolate : Isolate) =
    let mutable currentAPI = None : IFunDBAPI option

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
            if args.Length < 1 || args.Length > 2 then
                invalidArg "args" "Number of arguments must be between 1 and 2"
            let source = V8JsonReader.Deserialize<UserViewSource>(args.[0])
            let args =
                if args.Length >= 2 then
                    V8JsonReader.Deserialize<RawArguments>(args.[1])
                else
                    Map.empty
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.UserViews.GetUserView source args false
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("getUserViewInfo", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let source = V8JsonReader.Deserialize<UserViewSource>(args.[0])
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.UserViews.GetUserViewInfo source false
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("getEntityInfo", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.Entities.GetEntityInfo ref
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("insertEntity", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 2 then
                invalidArg "args" "Number of arguments must be 2"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let rawArgs = V8JsonReader.Deserialize<RawArguments>(args.[1])
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.Entities.InsertEntity ref rawArgs
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("updateEntity", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 3 then
                invalidArg "args" "Number of arguments must be 3"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let rawArgs = V8JsonReader.Deserialize<RawArguments>(args.[2])
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.Entities.UpdateEntity ref id rawArgs
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))
        fundbTemplate.Set("deleteEntity", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 2 then
                invalidArg "args" "Number of arguments must be 2"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let id = int (args.[1].Data :?> double)
            let api = Option.get currentAPI
            isolate.EventLoop.NewPromise(isolate.CurrentContext, fun () -> task {
                let! ret = api.Entities.DeleteEntity ref id
                return V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
            }, isolate.CurrentCancellationToken).Value
        ))

        fundbTemplate.Set("writeEvent", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let details = args.[0].GetString().Get()
            let api = Option.get currentAPI
            api.Request.WriteEventSync (fun event ->
                event.Type <- "triggerEvent"
                event.Details <- details
            )
            Value.Undefined.New(isolate)
        ))

        template

    member this.Isolate = isolate
    member this.Template = template

    member this.SetAPI api =
        assert (Option.isNone currentAPI)
        currentAPI <- Some api

    member this.ResetAPI api =
        currentAPI <- None