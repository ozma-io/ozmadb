module FunWithFlags.FunDB.Actions.Source

open FunWithFlags.FunDB.FunQL.AST

type SourceAction =
    { AllowBroken : bool
      Function : string
    }

type SourceActionsSchema =
    { Actions : Map<ActionName, SourceAction>
    }

let emptySourceActionsSchema : SourceActionsSchema =
    { Actions = Map.empty }

type SourceActions =
    { Schemas : Map<SchemaName, SourceActionsSchema>
    }

let emptySourceActions : SourceActions =
    { Schemas = Map.empty }
