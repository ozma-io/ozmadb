module OzmaDB.Modules.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore

open OzmaDB.OzmaUtils
open OzmaDB.Modules.Source
open OzmaDB.OzmaQL.AST
open OzmaDBSchema.System

let private makeSourceAttributeField (modul: Module) : ModulePath * SourceModule =
    let ret =
        { Source = modul.Source
          AllowBroken = modul.AllowBroken }

    (modul.Path, ret)

let private makeSourceModulesSchema (schema: Schema) : SourceModulesSchema =
    let modules = schema.Modules |> Seq.map makeSourceAttributeField |> Map.ofSeq
    { Modules = modules }

let buildSchemaModules
    (db: SystemContext)
    (filter: Expression<Func<Schema, bool>> option)
    (cancellationToken: CancellationToken)
    : Task<SourceModules> =
    task {
        let currentSchemas = db.GetModulesObjects()

        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)

        let! schemas = currentSchemas.ToListAsync(cancellationToken)

        let sourceSchemas =
            schemas
            |> Seq.map (fun schema -> (OzmaQLName schema.Name, makeSourceModulesSchema schema))
            |> Map.ofSeqUnique

        return ({ Schemas = sourceSchemas }: SourceModules)
    }
