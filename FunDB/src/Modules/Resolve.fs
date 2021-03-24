module FunWithFlags.FunDB.Modules.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Modules.Source
open FunWithFlags.FunDB.Modules.Types
open FunWithFlags.FunDB.JavaScript.Runtime

type ResolveModulesException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveModulesException (message, null)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolveModulesException "Invalid module name"

type private Phase1Resolver (layout : Layout) =
    let resolveModulesSchema (schema : SourceModulesSchema) : ModulesSchema =
        let mapModule path (modul : SourceModule) =
            try
                let normPath = POSIXPath.normalize path
                if path <> normPath then
                    raisef ResolveModulesException "Path should be normalized to %s" normPath
                if POSIXPath.goesBack path then
                    raisef ResolveModulesException "Path shouldn't contain .."
                if POSIXPath.isAbsolute path then
                    raisef ResolveModulesException "Path shouldn't be absolute"
                if POSIXPath.extension path <> Some "mjs" then
                    raisef ResolveModulesException "File should have .mjs extension"
                { Source = modul.Source
                }
            with
            | :? ResolveModulesException as e -> raisefWithInner ResolveModulesException e.InnerException "Error in module %O: %s" path e.Message

        let modules = Map.map mapModule schema.Modules

        { Modules = modules
        }

    member this.ResolveModules (source : SourceModules) : ResolvedModules =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveModulesException "Unknown schema name"
                resolveModulesSchema schema
            with
            | :? ResolveModulesException as e -> raisefWithInner ResolveModulesException e.InnerException "Error in modules schema %O: %s" name e.Message

        { Schemas = Map.map (fun name -> resolveModulesSchema) source.Schemas
        }

let resolveModules (layout : Layout) (source : SourceModules) : ResolvedModules =
    let phase1 = Phase1Resolver (layout)
    phase1.ResolveModules source