module FunWithFlags.FunDB.Triggers.Schema

open System
open System.Linq
open System.Linq.Expressions
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.System

let timeCasesMap =
    enumCases<TriggerTime>
    |> Seq.map (fun (case, value) -> (Option.get (caseKey case.Info), value))
    |> dict

let private makeSourceAttributeField (trig : Trigger) : SourceTrigger =
    { AllowBroken = trig.AllowBroken
      Priority = trig.Priority
      Time = timeCasesMap.[trig.Time]
      OnInsert = trig.OnInsert
      OnUpdateFields = Array.map FunQLName trig.OnUpdateFields
      OnDelete = trig.OnDelete
      Procedure = trig.Procedure
    }

let private makeSourceTriggersDatabase (schema : Schema) : SourceTriggersDatabase =
    let makeTriggers (entityName, attrs : Trigger seq) =
        let fields =
            attrs
            |> Seq.map (fun attrs -> (FunQLName attrs.Name, makeSourceAttributeField attrs))
            |> Map.ofSeq
        (entityName, { Triggers = fields })
    let makeEntities (schemaName, attrs : Trigger seq) =
        let entities =
            attrs
            |> Seq.groupBy (fun attrs -> FunQLName attrs.TriggerEntity.Name)
            |> Seq.map makeTriggers
            |> Map.ofSeq
        let schema = { Entities = entities } : SourceTriggersSchema
        (schemaName, schema)
    let schemas =
        schema.Triggers
        |> Seq.groupBy (fun attrs -> FunQLName attrs.TriggerEntity.Schema.Name)
        |> Seq.map makeEntities
        |> Map.ofSeq
    { Schemas = schemas }

let buildSchemaTriggers (db : SystemContext) (filter : Expression<Func<Schema, bool>> option) (cancellationToken : CancellationToken) : Task<SourceTriggers> =
    task {
        let currentSchemas = db.GetTriggersObjects()
        let currentSchemas =
            match filter with
            | None -> currentSchemas
            | Some expr -> currentSchemas.Where(expr)
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceTriggersDatabase schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }