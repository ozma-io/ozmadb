module OzmaDB.Modules.Load

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Parsing
open OzmaDB.OzmaQL.AST
open OzmaDB.Objects.Types
open OzmaDB.Modules.Types
open OzmaDB.JavaScript.Runtime

let moduleDirectory = "lib"

let private moduleName (ref: ModuleRef) = sprintf "lib/%O/%s" ref.Schema ref.Path

let moduleFiles (modules: ResolvedModules) : ModuleFile seq =
    seq {
        for KeyValue(schemaName, moduleSchema) in modules.Schemas do
            for KeyValue(path, maybeModule) in moduleSchema.Modules do
                match maybeModule with
                | Error e -> ()
                | Ok modul ->
                    let ref = { Schema = schemaName; Path = path }

                    yield
                        { Path = moduleName ref
                          Source = modul.Source
                          AllowBroken = modul.AllowBroken }
    }

let private resolvedPreparedModulesSchema
    (runtime: JSEngine)
    (forceAllowBroken: bool)
    (schemaName: SchemaName)
    (resolved: ModulesSchema)
    : ModulesSchema =
    let getOne path maybeModule =
        match maybeModule with
        | Error e -> Error e
        | Ok(modul: ResolvedModule) ->
            let ref = { Schema = schemaName; Path = path }

            let moduleFile =
                { Path = moduleName ref
                  Source = modul.Source
                  AllowBroken = modul.AllowBroken }

            try
                ignore <| runtime.CreateModule moduleFile
                Ok modul
            with
            | :? JavaScriptRuntimeException as e when modul.AllowBroken || forceAllowBroken ->
                Error
                    { AllowBroken = modul.AllowBroken
                      Error = e }
            | :? JavaScriptRuntimeException as e ->
                raisefWithInner JavaScriptRuntimeException e "In module %O/%s" schemaName path

    let modules = Map.map getOne resolved.Modules
    { Modules = modules }

let resolvedLoadedModules (resolved: ResolvedModules) (runtime: JSEngine) (forceAllowBroken: bool) : ResolvedModules =
    let getOne name schema =
        resolvedPreparedModulesSchema runtime forceAllowBroken name schema

    let schemas = Map.map getOne resolved.Schemas
    { Schemas = schemas }
