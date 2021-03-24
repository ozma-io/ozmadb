module FunWithFlags.FunDB.Actions.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types

type ResolveActionsException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveActionsException (message, null)

let private checkName (FunQLName name) : unit =
    if not <| goodName name then
        raisef ResolveActionsException "Invalid action name"

type private Phase1Resolver (layout : Layout) =
    let resolveActionsSchema (schema : SourceActionsSchema) : ActionsSchema =
        let mapAction name (action : SourceAction) =
            try
                checkName name
                let ret =
                    { AllowBroken = action.AllowBroken
                      Function = action.Function
                    }
                Ok ret
            with
            | :? ResolveActionsException as e -> raisefWithInner ResolveActionsException e.InnerException "Error in action %O: %s" name e.Message

        { Actions = Map.map mapAction schema.Actions
        }

    member this.ResolveActions (source : SourceActions) : ResolvedActions =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveActionsException "Unknown schema name"
                resolveActionsSchema schema
            with
            | :? ResolveActionsException as e -> raisefWithInner ResolveActionsException e.InnerException "Error in actions schema %O: %s" name e.Message

        { Schemas = Map.map (fun name -> resolveActionsSchema) source.Schemas
        }

let resolveActions (layout : Layout) (forceAllowBroken : bool) (source : SourceActions) : ErroredActions * ResolvedActions =
    let phase1 = Phase1Resolver (layout)
    let ret = phase1.ResolveActions source
    (Map.empty, ret)
