module FunWithFlags.FunDB.Modules.Types

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Objects.Types
open FunWithFlags.FunDB.Modules.Source
open FunWithFlags.FunDB.JavaScript.Runtime
module SQL = FunWithFlags.FunDB.SQL.AST

type ModuleRef =
    { Schema : SchemaName
      Path : Path
    }

[<NoEquality; NoComparison>]
type ResolvedModule =
    { Source : string
      AllowBroken : bool
    }

[<NoEquality; NoComparison>]
type ModulesSchema =
    { Modules : Map<ModulePath, PossiblyBroken<ResolvedModule>>
    }

[<NoEquality; NoComparison>]
type ResolvedModules =
    { Schemas : Map<SchemaName, ModulesSchema>
    }
