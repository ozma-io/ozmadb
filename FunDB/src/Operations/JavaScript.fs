module FunWithFlags.FunDB.Operations.JavaScript

open NetJs
open NetJs.Json

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Operations.Context

type TriggerTemplate (isolate : Isolate) =
    let mutable currentContext = None : RequestContext option

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
            let ctx = Option.get currentContext
            let ret = Task.awaitSync <| ctx.GetUserView source args false
            V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
        ))
        fundbTemplate.Set("getUserViewInfo", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let source = V8JsonReader.Deserialize<UserViewSource>(args.[0])
            let ctx = Option.get currentContext
            let ret = Task.awaitSync <| ctx.GetUserViewInfo source false
            V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
        ))

        fundbTemplate.Set("getEntityInfo", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let ref = V8JsonReader.Deserialize<ResolvedEntityRef>(args.[0])
            let ctx = Option.get currentContext
            let ret = Task.awaitSync <| ctx.GetEntityInfo ref
            V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
        ))
        fundbTemplate.Set("runTransaction", Template.FunctionTemplate.New(isolate, fun args ->
            if args.Length <> 1 then
                invalidArg "args" "Number of arguments must be 1"
            let transaction = V8JsonReader.Deserialize<Transaction>(args.[0])
            let ctx = Option.get currentContext
            let ret = Task.awaitSync <| ctx.RunTransaction transaction
            V8JsonWriter.Serialize(isolate.CurrentContext, Result.result (fun x -> x :> obj) (fun x -> x :> obj) ret)
        ))

        template

    member this.Isolate = isolate
    member this.Template = template

    member this.RunWithContext ctx action =
        assert (Option.isNone currentContext)
        currentContext <- Some ctx
        try
            action ()
        finally
            currentContext <- None
