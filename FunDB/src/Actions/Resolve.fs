module FunWithFlags.FunDB.Actions.Resolve

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.Actions.Types

type ResolveActionsException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ResolveActionsException (message, innerException, isUserException innerException)

    new (message : string) = ResolveActionsException (message, null, true)

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
            | e -> raisefWithInner ResolveActionsException e "In action %O" name

        { Actions = Map.map mapAction schema.Actions
        }

    member this.ResolveActions (source : SourceActions) : ResolvedActions =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.Schemas then
                    raisef ResolveActionsException "Unknown schema name"
                resolveActionsSchema schema
            with
            | e -> raisefWithInner ResolveActionsException e "In actions schema %O" name

        { Schemas = Map.map (fun name -> resolveActionsSchema) source.Schemas
        }

let resolveActions (layout : Layout) (forceAllowBroken : bool) (source : SourceActions) : ResolvedActions =
    let phase1 = Phase1Resolver (layout)
    phase1.ResolveActions source