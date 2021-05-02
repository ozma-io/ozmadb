module FunWithFlags.FunDB.Actions.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
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
    } with
        member this.FindAction (ref : ActionRef) : Result<ResolvedAction, exn> option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun schema -> Map.tryFind ref.Name schema.Actions)

type ErroredActionsSchema = Map<ActionName, exn>
type ErroredActions = Map<SchemaName, ErroredActionsSchema>

let unionErroredActions (a : ErroredActions) (b : ErroredActions) : ErroredActions =
    Map.unionWith (fun name -> Map.unionUnique) a b