module FunWithFlags.FunDB.Attributes.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

let private makeSourceAttributeField (attrs : FieldAttributes) : SourceAttributesField =
    { AllowBroken = attrs.AllowBroken
      Priority = attrs.Priority
      Attributes = attrs.Attributes
    }

let private makeSourceAttributesDatabase (schema : Schema) : SourceAttributesDatabase =
    let makeFields (entityName, attrs : FieldAttributes seq) =
        let fields =
            attrs
            |> Seq.map (fun attrs -> (FunQLName attrs.FieldName, makeSourceAttributeField attrs))
            |> Map.ofSeq
        (entityName, { Fields = fields })
    let makeEntities (schemaName, attrs : FieldAttributes seq) =
        let entities =
            attrs
            |> Seq.groupBy (fun attrs -> FunQLName attrs.FieldEntity.Name)
            |> Seq.map makeFields
            |> Map.ofSeq
        let schema : SourceAttributesSchema =
            { Entities = entities }
        (schemaName, schema)
    let schemas =
        schema.FieldsAttributes
        |> Seq.groupBy (fun attrs -> FunQLName attrs.FieldEntity.Schema.Name)
        |> Seq.map makeEntities
        |> Map.ofSeq
    { Schemas = schemas }

let buildSchemaAttributes (db : SystemContext) (filter : Expression<Func<Schema, bool>> option) (cancellationToken : CancellationToken) : Task<SourceDefaultAttributes> =
    task {
        let currentSchemas = db.GetAttributesObjects()
        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceAttributesDatabase schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }