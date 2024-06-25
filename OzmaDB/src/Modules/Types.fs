module OzmaDB.Modules.Types

open OzmaDB.OzmaQL.AST
open OzmaDB.Objects.Types
open OzmaDB.Modules.Source
open OzmaDB.JavaScript.Runtime

module SQL = OzmaDB.SQL.AST

type ModuleRef = { Schema: SchemaName; Path: Path }

[<NoEquality; NoComparison>]
type ResolvedModule = { Source: string; AllowBroken: bool }

[<NoEquality; NoComparison>]
type ModulesSchema =
    { Modules: Map<ModulePath, PossiblyBroken<ResolvedModule>> }

[<NoEquality; NoComparison>]
type ResolvedModules =
    { Schemas: Map<SchemaName, ModulesSchema> }
