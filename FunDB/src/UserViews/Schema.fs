module FunWithFlags.FunDB.UserViews.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDBSchema.Schema

let private makeSourceUserView (uv : UserView) : SourceUserView =
    { allowBroken = uv.AllowBroken
      query = uv.Query
    }

let private makeSourceSchema (schema : Schema) : SourceUserViewsSchema =
    { userViews = schema.UserViews |> Seq.map (fun uv -> (FunQLName uv.Name, makeSourceUserView uv)) |> Map.ofSeqUnique
    }

let buildSchemaUserViews (db : SystemContext) : Task<SourceUserViews> =
    task {
        let currentSchemas = getUserViewsObjects db.Schemas
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }