module FunWithFlags.FunDB.Modules.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Modules.Source
open FunWithFlags.FunDB.JavaScript.Runtime
module SQL = FunWithFlags.FunDB.SQL.AST

type ModuleRef =
    { Schema : SchemaName
      Path : Path
    }

[<NoEquality; NoComparison>]
type ResolvedModule =
    { IsModule : bool
      Source : string
    }

[<NoEquality; NoComparison>]
type ModulesSchema =
    { Modules : Map<ModulePath, ResolvedModule>
    }

[<NoEquality; NoComparison>]
type ResolvedModules =
    { Schemas : Map<SchemaName, ModulesSchema>
    }

let moduleDirectory = "lib"

let private moduleName (ref : ModuleRef) =
    sprintf "%s/%O/%s" moduleDirectory ref.Schema ref.Path

let moduleFiles (modules : ResolvedModules) : ModuleFile seq =
    seq {
        for KeyValue(schemaName, moduleSchema) in modules.Schemas do
            for KeyValue(path, modul) in moduleSchema.Modules do
                let ref = {
                    Schema = schemaName
                    Path = path
                }
                yield {
                    Path = moduleName ref
                    Source = modul.Source
                    IsModule = modul.IsModule
                }
    }
