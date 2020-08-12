module FunWithFlags.FunDB.Triggers.Merge

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Layout.Types

[<NoEquality; NoComparison>]
type MergedTrigger =
    { SchemaName : SchemaName
      Name : TriggerName
      Priority : int
      Inherited : bool
      Procedure : string
    }

type TriggerUpdateField =
    | MUFAll
    | MUFField of FieldName

[<NoEquality; NoComparison>]
type MergedTriggersTime =
    { OnInsert : MergedTrigger[]
      OnUpdateFields : Map<TriggerUpdateField, MergedTrigger[]>
      OnDelete : MergedTrigger[]
    }

[<NoEquality; NoComparison>]
type MergedTriggersEntity =
    { Before : MergedTriggersTime
      After : MergedTriggersTime
    }

[<NoEquality; NoComparison>]
type MergedTriggersSchema =
    { Entities : Map<EntityName, MergedTriggersEntity>
    }

[<NoEquality; NoComparison>]
type MergedTriggers =
    { Schemas : Map<SchemaName, MergedTriggersSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.Entities

let emptyMergedTriggerTime =
    { OnInsert = [||]
      OnUpdateFields = Map.empty
      OnDelete = [||]
    }

let emptyMergedTriggersEntity =
    { Before = emptyMergedTriggerTime
      After = emptyMergedTriggerTime
    }

let emptyMergedTriggers =
    { Schemas = Map.empty
    } : MergedTriggers

let private mergeSortedTriggers (a : MergedTrigger[]) (b : MergedTrigger[]) : MergedTrigger[] =
    Seq.mergeSortedBy (fun trig -> (trig.Priority, trig.Inherited, trig.SchemaName, trig.Name)) a b |> Array.ofSeq

let private mergeTriggersTime (a : MergedTriggersTime) (b : MergedTriggersTime) : MergedTriggersTime =
    { OnInsert = mergeSortedTriggers a.OnInsert b.OnInsert
      OnUpdateFields = Map.unionWith (fun name -> mergeSortedTriggers) a.OnUpdateFields b.OnUpdateFields
      OnDelete = mergeSortedTriggers a.OnDelete b.OnDelete
    }

let private mergeTriggersEntity (a : MergedTriggersEntity) (b : MergedTriggersEntity) : MergedTriggersEntity =
    { Before = mergeTriggersTime a.Before b.Before
      After = mergeTriggersTime a.After b.After
    }

let private mergeTriggersSchema (a : MergedTriggersSchema) (b : MergedTriggersSchema) : MergedTriggersSchema =
    { Entities = Map.unionWith (fun name -> mergeTriggersEntity) a.Entities b.Entities
    }

let private mergeTriggersPair (a : MergedTriggers) (b : MergedTriggers) : MergedTriggers =
    { Schemas = Map.unionWith (fun name -> mergeTriggersSchema) a.Schemas b.Schemas
    }

let private makeOneMergedTriggerEntity (schemaName : SchemaName) (name : TriggerName) : Result<ResolvedTrigger, TriggerError> -> MergedTriggersEntity option = function
    | Ok trigger when trigger.OnInsert || trigger.OnUpdateFields <> TUFSet Set.empty || trigger.OnDelete ->
        let merged =
            { SchemaName = schemaName
              Name = name
              Priority = trigger.Priority
              Inherited = false
              Procedure = trigger.Procedure
            }
        let time =
            { OnInsert = if trigger.OnInsert then [|merged|] else [||]
              OnUpdateFields =
                  match trigger.OnUpdateFields with
                  | TUFAll -> Map.singleton MUFAll [|merged|]
                  | TUFSet fields -> fields |> Seq.map (fun field -> (MUFField field, [|merged|])) |> Map.ofSeq
              OnDelete = if trigger.OnDelete then [|merged|] else [||]
            }
        let entity =
            match trigger.Time with
            | TTBefore -> { emptyMergedTriggersEntity with Before = time }
            | TTAfter -> { emptyMergedTriggersEntity with After = time }
        Some entity
    | _ -> None

let private markTriggerInherited (trigger : MergedTrigger) : MergedTrigger =
    { trigger with Inherited = true }

let private markTimeInherited (time : MergedTriggersTime) : MergedTriggersTime =
    { OnInsert = Array.map markTriggerInherited time.OnInsert
      OnUpdateFields = Map.map (fun key -> Array.map markTriggerInherited) time.OnUpdateFields
      OnDelete = Array.map markTriggerInherited time.OnDelete
    }

let private markEntityInherited (entityTriggers : MergedTriggersEntity) : MergedTriggersEntity =
    { Before = markTimeInherited entityTriggers.Before
      After = markTimeInherited entityTriggers.After
    }

type private TriggersMerger (layout : Layout) =
    let emitMergedTriggersEntity (schemaName : SchemaName) (triggerName : TriggerName) (triggerEntityRef : ResolvedEntityRef) (triggerEntity : ResolvedEntity) (trigger : Result<ResolvedTrigger,TriggerError>) =
        seq {
            match makeOneMergedTriggerEntity schemaName triggerName trigger with
            | None -> ()
            | Some merged ->
                yield (triggerEntityRef, merged)
                if not (Map.isEmpty triggerEntity.children) then
                    let inheritedMerged = markEntityInherited merged
                    for KeyValue (childRef, childInfo) in triggerEntity.children do
                        yield (childRef, inheritedMerged)
        }

    let emitTriggers (triggers : ResolvedTriggers) =
        seq {
            for KeyValue(schemaName, triggersDb) in triggers.Schemas do
                for KeyValue(triggerSchemaName, schema) in triggersDb.Schemas do
                    for KeyValue(triggerEntityName, entity) in schema.Entities do
                        let triggerRef = { schema = triggerSchemaName; name = triggerEntityName }
                        let triggerEntity = layout.FindEntity triggerRef |> Option.get
                        for KeyValue(triggerName, trigger) in entity.Triggers do
                            yield! emitMergedTriggersEntity schemaName triggerName triggerRef triggerEntity trigger
        }

    let mergeTriggers (triggers : ResolvedTriggers) : MergedTriggers =
        let schemas =
            emitTriggers triggers
            |> Map.ofSeqWith (fun ref attrs1 attrs2 -> mergeTriggersEntity attrs1 attrs2)
            |> Map.toSeq
            |> Seq.map (fun (ref, attrs) -> (ref.schema, { Entities = Map.singleton ref.name attrs }))
            |> Map.ofSeqWith (fun name -> mergeTriggersSchema)
        { Schemas = schemas }

    member this.MergeTriggers attrs = mergeTriggers attrs

let mergeTriggers (layout: Layout) (attrs : ResolvedTriggers) : MergedTriggers =
    let merger = TriggersMerger (layout)
    merger.MergeTriggers attrs