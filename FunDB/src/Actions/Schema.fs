module FunWithFlags.FunDB.Actions.Schema

open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Actions.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

let private makeSourceAttributeField (action : Action) : ActionName * SourceAction =
    let ret =
        { AllowBroken = action.AllowBroken
          Function = action.Function
        }
    (FunQLName action.Name, ret)

let private makeSourceActionsSchema (schema : Schema) : SourceActionsSchema =
    let actions =
        schema.Actions
        |> Seq.map makeSourceAttributeField
        |> Map.ofSeq
    { Actions = actions }

let buildSchemaActions (db : SystemContext) (cancellationToken : CancellationToken) : Task<SourceActions> =
    task {
        let currentSchemas = db.GetActionsObjects ()
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceActionsSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }