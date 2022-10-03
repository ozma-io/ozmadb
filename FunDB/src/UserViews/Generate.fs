module FunWithFlags.FunDB.UserViews.Generate

open System.Threading
open NetJs
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Objects.Types

type UserViewGenerateException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        UserViewGenerateException (message, innerException, isUserException innerException)

    new (message : string) = UserViewGenerateException (message, null, true)

[<NoEquality; NoComparison>]
type GeneratedUserViewsSchema =
    { UserViews : Map<SchemaName, SourceUserView>
    }

[<NoEquality; NoComparison>]
type GeneratedUserViews =
    { Schemas : Map<SchemaName, PossiblyBroken<GeneratedUserViewsSchema>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.Name schema.UserViews
            | _ -> None

let emptyGeneratedUserViews : GeneratedUserViews =
    { Schemas = Map.empty
    }

let private convertUserView (KeyValue (k, v : Value.Value)) =
    let query = v.GetString().Get()
    let uv =
        { Query = query
          AllowBroken = false
        }
    (FunQLName k, uv)

type private UserViewsGeneratorScript (runtime : IJSRuntime, name : string, scriptSource : string) =
    let func =
        try
            runtime.CreateDefaultFunction { Path = name; Source = scriptSource }
        with
        | :? NetJsException as e ->
            raisefUserWithInner UserViewGenerateException e "Couldn't initialize user view generator"

    let generateUserViews (layout : Value.Value) (cancellationToken : CancellationToken) : Map<UserViewName, SourceUserView> =
        try
            let newViews = runFunctionInRuntime runtime func cancellationToken [|layout|]
            newViews.GetObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
        with
        | :? JavaScriptRuntimeException as e ->
            raisefUserWithInner UserViewGenerateException e ""

    member this.Generate layout cancellationToken = generateUserViews layout cancellationToken
    member this.Runtime = runtime

let private generatorName (schemaName : SchemaName) =
    sprintf "user_views_generators/%O.mjs" schemaName

type private UserViewsGenerator (runtime : IJSRuntime, layout : Layout, triggers : MergedTriggers, cancellationToken : CancellationToken, forceAllowBroken : bool) =
    let serializedLayout = serializeLayout triggers layout

    let generateUserViewsSchema (name : SchemaName) (script : SourceUserViewsGeneratorScript) : PossiblyBroken<GeneratedUserViewsSchema> =
        try
            let gen = UserViewsGeneratorScript(runtime, generatorName name, script.Script)
            let layout = V8JsonWriter.Serialize(runtime.Context, serializedLayout)
            let uvs = gen.Generate layout cancellationToken
            Ok { UserViews = uvs }
        with
        | :? NetJsException as e when forceAllowBroken || script.AllowBroken ->
            Error { Error = e; AllowBroken = script.AllowBroken }

    let generateUserViews (views : SourceUserViews) : GeneratedUserViews =
        let generateOne name (view : SourceUserViewsSchema) =
            match view.GeneratorScript with
            | None -> Ok { UserViews = view.UserViews }
            | Some script -> generateUserViewsSchema name script
        let schemas = Map.map generateOne views.Schemas
        { Schemas = schemas }

    member this.GenerateUserViews views = generateUserViews views

let generateUserViews
        (runtime : IJSRuntime)
        (layout : Layout)
        (triggers : MergedTriggers)
        (forceAllowBroken : bool)
        (userViews : SourceUserViews)
        (cancellationToken : CancellationToken) : GeneratedUserViews =
    let state = UserViewsGenerator(runtime, layout, triggers, cancellationToken, forceAllowBroken)
    state.GenerateUserViews userViews

let passthruGeneratedUserViews (source : SourceUserViews) : GeneratedUserViews =
    let schemas = source.Schemas |> Map.map (fun name schema -> Ok { UserViews = schema.UserViews })
    { Schemas = schemas }

let generatedUserViewsSource (source : SourceUserViews) (generated : GeneratedUserViews) : SourceUserViews =
    let getOne name (sourceSchema : SourceUserViewsSchema) =
        let userViews =
            match generated.Schemas.[name] with
            | Ok generatedSchema -> generatedSchema.UserViews
            | Error e -> Map.empty
        { UserViews = userViews; GeneratorScript = sourceSchema.GeneratorScript }

    let schemas = Map.map getOne source.Schemas
    { Schemas = schemas }
