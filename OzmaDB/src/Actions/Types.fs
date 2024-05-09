module OzmaDB.Actions.Types

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.Objects.Types
module SQL = OzmaDB.SQL.AST

type ActionRef = ResolvedEntityRef

[<NoEquality; NoComparison>]
type ResolvedAction =
    { AllowBroken : bool
      Function : string
    }

[<NoEquality; NoComparison>]
type ActionsSchema =
    { Actions : Map<ActionName, PossiblyBroken<ResolvedAction>>
    }

[<NoEquality; NoComparison>]
type ResolvedActions =
    { Schemas : Map<SchemaName, ActionsSchema>
    } with
        member this.FindAction (ref : ActionRef) : PossiblyBroken<ResolvedAction> option =
             Map.tryFind ref.Schema this.Schemas
                |> Option.bind (fun schema -> Map.tryFind ref.Name schema.Actions)
