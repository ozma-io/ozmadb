module OzmaDB.UserViews.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.UserViews.Source
open OzmaDBSchema.System

let private makeSourceUserView (uv : UserView) : SourceUserView =
    { AllowBroken = uv.AllowBroken
      Query = uv.Query
    }

let private makeSourceSchema (schema : Schema) : SourceUserViewsSchema =
    { UserViews = schema.UserViews |> Seq.map (fun uv -> (OzmaQLName uv.Name, makeSourceUserView uv)) |> Map.ofSeqUnique
      GeneratorScript =
        match schema.UserViewGenerator with
        | null -> None
        | gen -> Some { Script = gen.Script; AllowBroken = gen.AllowBroken }
    }

let buildSchemaUserViews (db : SystemContext) (filter : Expression<Func<Schema, bool>> option) (cancellationToken : CancellationToken) : Task<SourceUserViews> =
    task {
        let currentSchemas = db.GetUserViewsObjects()
        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (OzmaQLName schema.Name, makeSourceSchema schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }