module FunWithFlags.FunDB.Triggers.Schema

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

let private timeCasesMap = unionNames (unionCases typeof<TriggerTime>) |> Map.mapWithKeys (fun name case -> (Option.get name, FSharpValue.MakeUnion(case.Info, [||]) :?> TriggerTime))

let private makeSourceAttributeField (trig : Trigger) : SourceTrigger =
    { AllowBroken = trig.AllowBroken
      Priority = trig.Priority
      Time = Map.find trig.Time timeCasesMap
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
        (schemaName, { Entities = entities })
    let schemas =
        schema.Triggers
        |> Seq.groupBy (fun attrs -> FunQLName attrs.TriggerEntity.Schema.Name)
        |> Seq.map makeEntities
        |> Map.ofSeq
    { Schemas = schemas }

let buildSchemaTriggers (db : SystemContext) (cancellationToken : CancellationToken) : Task<SourceTriggers> =
    task {
        let currentSchemas = db.GetTriggersObjects ()
        let! schemas = currentSchemas.ToListAsync(cancellationToken)
        let sourceSchemas = schemas |> Seq.map (fun schema -> (FunQLName schema.Name, makeSourceTriggersDatabase schema)) |> Map.ofSeqUnique

        return
            { Schemas = sourceSchemas
            }
    }