module FunWithFlags.FunDB.UserViews.Generate

open System
open Newtonsoft.Json
open NetJs
open NetJs.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Utils

type UserViewGenerateException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewGenerateException (message, null)

let userViewsFunction = "GetUserViews"

let private convertUserView (KeyValue (k, v : Value.Value)) =
    let query = v.GetString().Get()
    let uv =
        { query = query
          allowBroken = false
        }
    (FunQLName k, uv)

type UserViewGeneratorTemplate (isolate : Isolate) =
    let template =
        let jsRenderSqlName = Template.FunctionTemplate.New(isolate, fun args ->
            let ret = args.[0].GetString().Get() |> renderSqlName
            Value.String.New(isolate, ret).Value
        )
        let template = Template.ObjectTemplate.New(isolate)
        template.Set("renderSqlName", jsRenderSqlName)
        template
    
    member this.Isolate = isolate
    member this.Template = template

type UserViewGenerator (template : UserViewGeneratorTemplate, jsProgram : string) =
    let context = Context.New(template.Isolate, template.Template)
    do
        try
            let script = Script.Compile(context, Value.String.New(template.Isolate, jsProgram))
            ignore <| script.Run()
        with
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't initialize user view generator"
    let runnable =
        match context.Global.Get(userViewsFunction).Data with
        | :? Value.Function as f -> f
        | v -> raisef UserViewGenerateException "%s is not a function, buf %O" userViewsFunction v

    let generateUserViews (layout : Layout) : SourceUserViewsSchema =
        use jsWriter = new V8JsonWriter(context)
        let serializer = JsonSerializer()
        serializer.Serialize(jsWriter, layout)
        try
            let newViews = runnable.Call(Nullable(), null, [|jsWriter.Result|])
            let userViews = newViews.GetObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
            { userViews = userViews
            }
        with
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't generate user views"

    member this.Generate layout = generateUserViews layout