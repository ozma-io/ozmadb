module FunWithFlags.FunDB.JavaScript.Common

open Newtonsoft.Json.Linq
open NetJs
open NetJs.Template
open NetJs.Json

open FunWithFlags.FunDB.FunQL.Utils

let addCommonFunQLAPI (template : ObjectTemplate) =
    template.Set("renderFunQLName", FunctionTemplate.New(template.Isolate, fun args ->
        if args.Length <> 1 then
            invalidArg "args" "Number of arguments must be 1"
        let ret = args.[0].GetString().Get() |> renderFunQLName
        Value.String.New(template.Isolate, ret).Value
    ))

    template.Set("renderFunQLValue", FunctionTemplate.New(template.Isolate, fun args ->
        if args.Length <> 1 then
            invalidArg "args" "Number of arguments must be 1"
        let source = V8JsonReader.Deserialize<JToken>(args.[0])
        let ret = renderFunQLJson source
        Value.String.New(template.Isolate, ret).Value
    ))