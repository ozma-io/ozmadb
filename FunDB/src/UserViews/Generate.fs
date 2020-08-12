module FunWithFlags.FunDB.UserViews.Generate

open System.Threading
open NetJs
open NetJs.Json

open FunWithFlags.FunUtils.Utils
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
        { Query = query
          AllowBroken = false
        }
    (FunQLName k, uv)

type UserViewGeneratorTemplate (isolate : Isolate) =
    let template =
        let template = Template.ObjectTemplate.New(isolate)

        template.Set("renderSqlName", Template.FunctionTemplate.New(isolate, fun args ->
            let ret = args.[0].GetString().Get() |> renderSqlName
            Value.String.New(isolate, ret).Value
        ))

        template

    member this.Isolate = isolate
    member this.Template = template

type private UserViewGenerator (template : UserViewGeneratorTemplate, scriptSource : string) =
    let context = Context.New(template.Isolate, template.Template)
    do
        try
            let script = Script.Compile(context, Value.String.New(template.Isolate, scriptSource))
            ignore <| script.Run()
        with
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't initialize user view generator"
    let runnable =
        match context.Global.Get(userViewsFunction).Data with
        | :? Value.Function as f -> f
        | v -> raisef UserViewGenerateException "%s is not a function, buf %O" userViewsFunction v

    let generateUserViews (layout : Value.Value) (cancellationToken : CancellationToken) : SourceUserViewsSchema =
        try
            let newViews = runnable.Call(cancellationToken, null, [|layout|])
            let userViews = newViews.GetObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
            { GeneratorScript = Some scriptSource
              UserViews = userViews
            }
        with
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't generate user views"

    member this.Generate layout cancellationToken = generateUserViews layout cancellationToken

type private UserViewsGenerator (template : UserViewGeneratorTemplate, layout : Layout, cancellationToken : CancellationToken) =
    let mutable jsLayout : Value.Value option = None

    let getLayout () =
        match jsLayout with
        | Some l -> l
        | None ->
            let context = Context.New(template.Isolate, template.Template)
            let layout = V8JsonWriter.Serialize(context, layout)
            jsLayout <- Some layout
            layout

    let generateUserViewsSchema (schema : SourceUserViewsSchema) : SourceUserViewsSchema =
        match schema.GeneratorScript with
        | None -> schema
        | Some script ->
            let gen = UserViewGenerator(template, script)
            gen.Generate (getLayout ()) cancellationToken

    let generateUserViews (views : SourceUserViews) : SourceUserViews =
        { Schemas = Map.map (fun name -> generateUserViewsSchema) views.Schemas
        }

    member this.GenerateUserViews views = generateUserViews views

let generateUserViews (template : UserViewGeneratorTemplate) (layout : Layout) (uvs : SourceUserViews) (cancellationToken : CancellationToken) : SourceUserViews =
    let gen = UserViewsGenerator(template, layout, cancellationToken)
    gen.GenerateUserViews uvs