module FunWithFlags.FunDB.UserViews.Generate

open System.Threading
open NetJs
open NetJs.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.JavaScript.Runtime

type UserViewGenerateException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewGenerateException (message, null)

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
            runtime.CreateDefaultFunction name scriptSource
        with
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't initialize user view generator"

    let generateUserViews (layout : Value.Value) (cancellationToken : CancellationToken) : Map<UserViewName, SourceUserView> =
        try
            let newViews = func.Call(cancellationToken, null, [|layout|])
            newViews.GetObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
        with
        | :? JSException as e ->
            raisefWithInner UserViewGenerateException e "Unhandled exception in user view generator: %s" (e.JSStackTrace.ToPrettyString())
        | :? NetJsException as e ->
            raisefWithInner UserViewGenerateException e "Couldn't generate user views"

    member this.Generate layout cancellationToken = generateUserViews layout cancellationToken
    member this.Runtime = runtime

type private UserViewGenerator =
    { Generator : UserViewsGeneratorScript
      Source : SourceUserViewsGeneratorScript
      SourceSchema : SourceUserViewsSchema
    }

type private PreparedUserViewSchema =
    | PUVGenerator of UserViewGenerator
    | PUVError of SourceUserViewsGeneratorScript * UserViewsSchemaError
    | PUVStatic of SourceUserViewsSchema

type private UserViewsGenerators = Map<SchemaName, PreparedUserViewSchema>

type private UserViewsGeneratorState (layout : Layout, cancellationToken : CancellationToken, forceAllowBroken : bool) =
    let generateUserViewsSchema (gen : UserViewGenerator) : Result<SourceUserViewsSchema, UserViewsSchemaError> =
        let layout = V8JsonWriter.Serialize(gen.Generator.Runtime.Context, serializeLayout layout)
        try
            let uvs = gen.Generator.Generate layout cancellationToken
            Ok { UserViews = uvs
                 GeneratorScript = Some gen.Source
               }
        with
        | :? NetJsException as e when forceAllowBroken || gen.Source.AllowBroken ->
            Error { Source = gen.SourceSchema; Error = e :> exn }

    let generateUserViews (gens : UserViewsGenerators) : ErroredUserViews * SourceUserViews =
        let mutable errors : ErroredUserViews = Map.empty
        let generateOne name = function
            | PUVGenerator gen ->
                match generateUserViewsSchema gen with
                | Ok ret -> ret
                | Error err ->
                    errors <- Map.add name (UEGenerator err.Error) errors
                    { UserViews = Map.empty; GeneratorScript = err.Source.GeneratorScript }
            | PUVStatic ret -> ret
            | PUVError (script, err) ->
                if not script.AllowBroken then
                    errors <- Map.add name (UEGenerator err.Error) errors
                { UserViews = Map.empty; GeneratorScript = err.Source.GeneratorScript }
        let schemas = Map.map generateOne gens
        let ret = { Schemas = schemas } : SourceUserViews
        (errors, ret)

    member this.GenerateUserViews gens = generateUserViews gens

type UserViewsGenerator (runtime : IJSRuntime, userViews : SourceUserViews, createForceAllowBroken : bool) =
    let prepareGenerator (name : SchemaName) (schema : SourceUserViewsSchema) =
        match schema.GeneratorScript with
        | None -> PUVStatic schema
        | Some script ->
            try
                let gen = UserViewsGeneratorScript (runtime, sprintf "%O/user_views_generator.mjs" name, script.Script)
                let ret =
                    { Generator = gen
                      Source = script
                      SourceSchema = schema
                    }
                PUVGenerator ret
            with
            | :? NetJsException as e when createForceAllowBroken || script.AllowBroken ->
                PUVError (script, { Source = schema; Error = e :> exn })
    let gens : UserViewsGenerators = Map.map prepareGenerator userViews.Schemas

    member this.GenerateUserViews (layout : Layout) (cancellationToken : CancellationToken) (forceAllowBroken : bool) : ErroredUserViews * SourceUserViews =
        let state = UserViewsGeneratorState(layout, cancellationToken, forceAllowBroken)
        state.GenerateUserViews gens