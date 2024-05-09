module OzmaDB.Modules.Source

open OzmaDB.OzmaQL.AST

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
