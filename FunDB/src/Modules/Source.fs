module FunWithFlags.FunDB.Modules.Source

open FunWithFlags.FunDB.FunQL.AST

type SourceModule =
    { Source : string
      AllowBroken : bool
    }

type ModulePath = string

type SourceModulesSchema =
    { Modules : Map<ModulePath, SourceModule>
    }

let emptySourceModulesSchema : SourceModulesSchema =
    { Modules = Map.empty }

type SourceModules =
    { Schemas : Map<SchemaName, SourceModulesSchema>
    }

let emptySourceModules : SourceModules =
    { Schemas = Map.empty }
