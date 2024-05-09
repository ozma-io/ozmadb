module OzmaDB.Actions.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils
open OzmaDB.Actions.Source
open OzmaDB.OzmaQL.AST
open OzmaDBSchema.System

let private makeSourceAttributeField (action : Action) : ActionName * SourceAction =
    let ret =
        { AllowBroken = action.AllowBroken
          Function = action.Function
        }
    (OzmaQLName action.Name, ret)

let private makeSourceActionsSchema (schema : Schema) : SourceActionsSchema =
    let actions =
        schema.Actions
        |> Seq.map makeSourceAttributeField
        |> Map.ofSeq
    { Actions = actions }

let buildSchemaActions (db : SystemContext) (filter : Expression<Func<Schema, bool>> option) (cancellationToken : CancellationToken) : Task<SourceActions> =
    task {
        let currentSchemas = db.GetActionsObjects()
        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (OzmaQLName schema.Name, makeSourceActionsSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }