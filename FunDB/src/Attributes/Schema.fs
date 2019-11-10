module FunWithFlags.FunDB.Attributes.Schema

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.Schema

let private makeSourceAttributeField (attrs : FieldAttributes) : SourceAttributesField =
    { allowBroken = attrs.AllowBroken
      priority = attrs.Priority
      attributes = attrs.Attributes
    }

let private makeSourceAttributesDatabase (schema : Schema) : SourceAttributesDatabase =
    let makeFields (entityName, attrs : FieldAttributes seq) =
        let fields =
            attrs
            |> Seq.map (fun attrs -> (FunQLName attrs.FieldName, makeSourceAttributeField attrs))
            |> Map.ofSeq
        (entityName, { fields = fields })
    let makeEntities (schemaName, attrs : FieldAttributes seq) =
        let entities =
            attrs
            |> Seq.groupBy (fun attrs -> FunQLName attrs.FieldEntity.Name)
            |> Seq.map makeFields
            |> Map.ofSeq
        (schemaName, { entities = entities })
    let schemas =
        schema.FieldsAttributes
        |> Seq.groupBy (fun attrs -> FunQLName attrs.FieldEntity.Schema.Name)
        |> Seq.map makeEntities
        |> Map.ofSeq
    { schemas = schemas }

let buildSchemaAttributes (db : SystemContext) : Task<SourceDefaultAttributes> =
    task {
        let currentSchemas = getAttributesObjects db.Schemas
        let! schemas = currentSchemas.ToListAsync()
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceAttributesDatabase schema)) |> Map.ofSeqUnique

        return
            { schemas = sourceSchemas
            }
    }