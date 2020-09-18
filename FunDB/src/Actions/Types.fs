module FunWithFlags.FunDB.Actions.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Actions.Source
module SQL = FunWithFlags.FunDB.SQL.AST

type ActionRef = ResolvedEntityRef

[<NoEquality; NoComparison>]
type ResolvedAction =
    { AllowBroken : bool
      Function : string
    }

[<NoEquality; NoComparison>]
type ActionsSchema =
    { Actions : Map<ActionName, Result<ResolvedAction, exn>>
    }

[<NoEquality; NoComparison>]
type ResolvedActions =
    { Schemas : Map<SchemaName, ActionsSchema>
    }

type ErroredActionsSchema = Map<ActionName, exn>
type ErroredActions = Map<SchemaName, ErroredActionsSchema>

let unionErroredActions (a : ErroredActions) (b : ErroredActions) : ErroredActions =
    Map.unionWith (fun name -> Map.unionUnique) a b