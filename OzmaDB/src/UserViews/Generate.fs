module OzmaDB.UserViews.Generate

open System.Threading
open System.Collections.Generic

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.UserViews.Source
open OzmaDB.UserViews.Types
open OzmaDB.Layout.Types
open OzmaDB.Layout.Info
open OzmaDB.Triggers.Merge
open OzmaDB.JavaScript.Json
open OzmaDB.JavaScript.Runtime
open OzmaDB.Objects.Types

type UserViewGenerateException(message: string, innerException: exn, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: exn) =
        UserViewGenerateException(message, innerException, isUserException innerException)

    new(message: string) = UserViewGenerateException(message, null, true)

[<NoEquality; NoComparison>]
type GeneratedUserViewsSchema =
    { UserViews: Map<SchemaName, SourceUserView> }

[<NoEquality; NoComparison>]
type GeneratedUserViews =
    { Schemas: Map<SchemaName, PossiblyBroken<GeneratedUserViewsSchema>> }

    member this.Find(ref: ResolvedUserViewRef) =
        match Map.tryFind ref.Schema this.Schemas with
        | Some(Ok schema) -> Map.tryFind ref.Name schema.UserViews
        | _ -> None

let emptyGeneratedUserViews: GeneratedUserViews = { Schemas = Map.empty }

type private UserViewsGeneratorScript
    (engine: AbstractJSEngine, path: string, scriptSource: string, cancellationToken: CancellationToken) =
    let func =
        try
            engine.CreateDefaultFunction(
                { Path = path
                  Source = scriptSource
                  AllowBroken = false },
                cancellationToken
            )
        with :? JavaScriptRuntimeException as e ->
            raisefUserWithInner UserViewGenerateException e "Couldn't initialize user view generator"

    let generateUserViews (layout: obj) (cancellationToken: CancellationToken) : Map<UserViewName, SourceUserView> =
        let convertUserView (KeyValue(uvName, res: obj)) =
            match res with
            | :? string as query ->
                let uv = { Query = query; AllowBroken = false }
                (OzmaQLName uvName, uv)
            | _ -> raisef UserViewGenerateException "User view %s must be a string" uvName

        try
            match engine.RunJSFunction(func, [| layout |], cancellationToken) with
            | :? IDictionary<string, obj> as newViews -> newViews |> Seq.map convertUserView |> Map.ofSeq
            | _ -> raisef UserViewGenerateException "User view generator must return an object"
        with :? JavaScriptRuntimeException as e ->
            raisefUserWithInner UserViewGenerateException e ""

    member this.Generate layout cancellationToken =
        generateUserViews layout cancellationToken

let private generatorName (schemaName: SchemaName) =
    sprintf "user_views_generators/%O.mjs" schemaName

type private UserViewsGenerator
    (
        engine: AbstractJSEngine,
        layout: Layout,
        triggers: MergedTriggers,
        cancellationToken: CancellationToken,
        forceAllowBroken: bool
    ) =
    let serializedLayout = serializeLayout triggers layout

    let generateUserViewsSchema
        (name: SchemaName)
        (script: SourceUserViewsGeneratorScript)
        : PossiblyBroken<GeneratedUserViewsSchema> =
        let layout = engine.Json.Serialize(serializedLayout)

        let gen =
            UserViewsGeneratorScript(engine, generatorName name, script.Script, cancellationToken)

        try
            let uvs = gen.Generate layout cancellationToken
            Ok { UserViews = uvs }
        with
        | :? UserViewGenerateException as e when forceAllowBroken || script.AllowBroken ->
            Error
                { Error = e
                  AllowBroken = script.AllowBroken }
        | :? UserViewGenerateException as e -> raisefWithInner UserViewGenerateException e "In user view schema %O" name

    let generateUserViews (views: SourceUserViews) : GeneratedUserViews =
        let generateOne name (view: SourceUserViewsSchema) =
            match view.GeneratorScript with
            | None -> Ok { UserViews = view.UserViews }
            | Some script -> generateUserViewsSchema name script

        let schemas = Map.map generateOne views.Schemas
        { Schemas = schemas }

    member this.GenerateUserViews views = generateUserViews views

let generateUserViews
    (engine: AbstractJSEngine)
    (layout: Layout)
    (triggers: MergedTriggers)
    (forceAllowBroken: bool)
    (userViews: SourceUserViews)
    (cancellationToken: CancellationToken)
    : GeneratedUserViews =
    let state =
        UserViewsGenerator(engine, layout, triggers, cancellationToken, forceAllowBroken)

    state.GenerateUserViews userViews

let passthruGeneratedUserViews (source: SourceUserViews) : GeneratedUserViews =
    let schemas =
        source.Schemas
        |> Map.map (fun name schema -> Ok { UserViews = schema.UserViews })

    { Schemas = schemas }

let generatedUserViewsSource (source: SourceUserViews) (generated: GeneratedUserViews) : SourceUserViews =
    let getOne name (sourceSchema: SourceUserViewsSchema) =
        let userViews =
            match generated.Schemas.[name] with
            | Ok generatedSchema -> generatedSchema.UserViews
            | Error e -> Map.empty

        { UserViews = userViews
          GeneratorScript = sourceSchema.GeneratorScript }

    let schemas = Map.map getOne source.Schemas
    { Schemas = schemas }
