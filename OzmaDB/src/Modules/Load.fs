module OzmaDB.Modules.Load

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Parsing
open OzmaDB.OzmaQL.AST
open OzmaDB.Objects.Types
open OzmaDB.Modules.Types
open OzmaDB.JavaScript.Runtime

let moduleDirectory = "lib"

let private moduleName (ref : ModuleRef) =
    sprintf "lib/%O/%s" ref.Schema ref.Path

let moduleFiles (modules : ResolvedModules) : ModuleFile seq =
    seq {
        for KeyValue(schemaName, moduleSchema) in modules.Schemas do
            for KeyValue(path, maybeModule) in moduleSchema.Modules do
                match maybeModule with
                | Error e -> ()
                | Ok modul -> 
                    let ref = {
                        Schema = schemaName
                        Path = path
                    }
                    yield {
                        Path = moduleName ref
                        Source = modul.Source
                        AllowBroken = modul.AllowBroken
                    }
    }

let private extractBrokenModulesInfo (runtime : IJSRuntime) =
    let extractOne (path, info) =
        match path with
        | CIRegex @"lib/^([^/]+)/(.*)$" [rawSchemaName; rawPath] ->
            Some <| Map.singleton (OzmaQLName rawSchemaName) (Map.singleton rawPath info)
        | _ -> None
    runtime.BrokenModules
    |> Map.toSeq
    |> Seq.mapMaybe extractOne
    |> Seq.fold (Map.unionWithKey (fun k -> Map.unionUnique)) Map.empty

let private resolvedPreparedModulesSchema (resolved : ModulesSchema) (failed : Map<Path, BrokenInfo>) : ModulesSchema =
    let getOne path maybeModule =
        match maybeModule with
        | Error e -> Error e
        | Ok modul ->
            match Map.tryFind path failed with
            | Some error -> Error error
            | None -> Ok modul
    let Modules = Map.map getOne resolved.Modules
    { Modules = Modules }

let resolvedLoadedModules (resolved : ResolvedModules) (runtime : IJSRuntime) : ResolvedModules =
    let failed = extractBrokenModulesInfo runtime
    let getOne name resolvedSchema = resolvedPreparedModulesSchema resolvedSchema (Map.findWithDefault name Map.empty failed)
    let schemas = Map.map getOne resolved.Schemas
    { Schemas = schemas }