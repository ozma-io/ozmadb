module FunWithFlags.FunDB.Triggers.Merge

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Layout.Types

[<NoEquality; NoComparison>]
type MergedTrigger =
    { Schema : SchemaName
      Name : TriggerName
      Priority : int
      Inherited : ResolvedEntityRef option
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
            match Map.tryFind entity.Schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.Name schema.Entities

let private findTriggersTime (entity : ResolvedEntityRef) (time : TriggerTime) (triggers : MergedTriggers) : MergedTriggersTime option =
    Map.tryFind entity.Schema triggers.Schemas
        |> Option.bind (fun schema -> Map.tryFind entity.Name schema.Entities)
        |> Option.map (fun entity -> match time with | TTBefore -> entity.Before | TTAfter -> entity.After)

let findMergedTriggersInsert (entity : ResolvedEntityRef) (time : TriggerTime) (triggers : MergedTriggers) : MergedTrigger seq =
    match findTriggersTime entity time triggers with
    | None -> Seq.empty
    | Some timeTriggers -> Array.toSeq timeTriggers.OnInsert

let findMergedTriggersUpdate (entity : ResolvedEntityRef) (time : TriggerTime) (fields : FieldName seq) (triggers : MergedTriggers) : MergedTrigger seq =
    match findTriggersTime entity time triggers with
    | None -> Seq.empty
    | Some timeTriggers ->
        seq {
            match Map.tryFind MUFAll timeTriggers.OnUpdateFields with
            | None -> ()
            | Some triggers -> yield! triggers
            yield!
                seq {
                    for field in fields do
                        match Map.tryFind (MUFField field) timeTriggers.OnUpdateFields with
                        | None -> ()
                        | Some triggers -> yield! triggers
                } |> Seq.distinctBy (fun trigger -> (trigger.Schema, trigger.Name))
        } |> Seq.sortBy (fun trigger -> trigger.Priority)

let findMergedTriggersDelete (entity : ResolvedEntityRef) (time : TriggerTime) (triggers : MergedTriggers) : MergedTrigger seq =
    match findTriggersTime entity time triggers with
    | None -> Seq.empty
    | Some timeTriggers -> Array.toSeq timeTriggers.OnDelete

let emptyMergedTriggerTime =
    { OnInsert = [||]
      OnUpdateFields = Map.empty
      OnDelete = [||]
    }

let emptyMergedTriggersEntity : MergedTriggersEntity =
    { Before = emptyMergedTriggerTime
      After = emptyMergedTriggerTime
    }

let emptyMergedTriggersSchema : MergedTriggersSchema =
    { Entities = Map.empty
    }

let emptyMergedTriggers : MergedTriggers =
    { Schemas = Map.empty
    }

let private mergeSortedTriggers (a : MergedTrigger[]) (b : MergedTrigger[]) : MergedTrigger[] =
    Seq.mergeSortedBy (fun trig -> (trig.Priority, trig.Inherited, trig.Schema, trig.Name)) a b |> Array.ofSeq

let private mergeTriggersTime (a : MergedTriggersTime) (b : MergedTriggersTime) : MergedTriggersTime =
    { OnInsert = mergeSortedTriggers a.OnInsert b.OnInsert
      OnUpdateFields = Map.unionWith mergeSortedTriggers a.OnUpdateFields b.OnUpdateFields
      OnDelete = mergeSortedTriggers a.OnDelete b.OnDelete
    }

let private mergeTriggersEntity (a : MergedTriggersEntity) (b : MergedTriggersEntity) : MergedTriggersEntity =
    { Before = mergeTriggersTime a.Before b.Before
      After = mergeTriggersTime a.After b.After
    }

let private mergeTriggersSchema (a : MergedTriggersSchema) (b : MergedTriggersSchema) : MergedTriggersSchema =
    { Entities = Map.unionWith mergeTriggersEntity a.Entities b.Entities
    }

let private mergeTriggersPair (a : MergedTriggers) (b : MergedTriggers) : MergedTriggers =
    { Schemas = Map.unionWith mergeTriggersSchema a.Schemas b.Schemas
    }

let private makeOneMergedTriggerEntity (schemaName : SchemaName) (name : TriggerName) (trigger : ResolvedTrigger) : MergedTriggersEntity option =
    if not (trigger.OnInsert && trigger.OnUpdateFields <> TUFSet Set.empty && trigger.OnDelete) then
        None
    else
        let merged =
            { Schema = schemaName
              Name = name
              Priority = trigger.Priority
              Inherited = None
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

let private markTriggerInherited (originalRef : ResolvedEntityRef) (trigger : MergedTrigger) : MergedTrigger =
    { trigger with Inherited = Some originalRef }

let private markTimeInherited (originalRef : ResolvedEntityRef) (time : MergedTriggersTime) : MergedTriggersTime =
    { OnInsert = Array.map (markTriggerInherited originalRef) time.OnInsert
      OnUpdateFields = Map.map (fun key -> Array.map (markTriggerInherited originalRef)) time.OnUpdateFields
      OnDelete = Array.map (markTriggerInherited originalRef) time.OnDelete
    }

let private markEntityInherited (originalRef : ResolvedEntityRef) (entityTriggers : MergedTriggersEntity) : MergedTriggersEntity =
    { Before = markTimeInherited originalRef entityTriggers.Before
      After = markTimeInherited originalRef entityTriggers.After
    }

type private TriggersMerger (layout : Layout) =
    let emitMergedTriggersEntity (schemaName : SchemaName) (triggerName : TriggerName) (triggerEntityRef : ResolvedEntityRef) (triggerEntity : ResolvedEntity) (trigger : ResolvedTrigger) =
        seq {
            match makeOneMergedTriggerEntity schemaName triggerName trigger with
            | None -> ()
            | Some merged ->
                yield (triggerEntityRef, merged)
                if not (Map.isEmpty triggerEntity.Children) then
                    let inheritedMerged = markEntityInherited triggerEntityRef merged
                    for KeyValue (childRef, childInfo) in triggerEntity.Children do
                        yield (childRef, inheritedMerged)
        }

    let emitTriggers (triggers : ResolvedTriggers) =
        seq {
            for KeyValue(schemaName, triggersDb) in triggers.Schemas do
                for KeyValue(triggerSchemaName, schema) in triggersDb.Schemas do
                    for KeyValue(triggerEntityName, entity) in schema.Entities do
                        let triggerRef = { Schema = triggerSchemaName; Name = triggerEntityName }
                        let triggerEntity = layout.FindEntity triggerRef |> Option.get
                        for KeyValue(triggerName, maybeTrigger) in entity.Triggers do
                            match maybeTrigger with
                            | Ok trigger -> yield! emitMergedTriggersEntity schemaName triggerName triggerRef triggerEntity trigger
                            | Error e -> ()
        }

    let mergeTriggers (triggers : ResolvedTriggers) : MergedTriggers =
        let schemas =
            emitTriggers triggers
            |> Map.ofSeqWith (fun ref attrs1 attrs2 -> mergeTriggersEntity attrs1 attrs2)
            |> Map.toSeq
            |> Seq.map (fun (ref, attrs) -> (ref.Schema, { Entities = Map.singleton ref.Name attrs }))
            |> Map.ofSeqWith (fun name -> mergeTriggersSchema)
        { Schemas = schemas }

    member this.MergeTriggers attrs = mergeTriggers attrs

let mergeTriggers (layout: Layout) (attrs : ResolvedTriggers) : MergedTriggers =
    let merger = TriggersMerger (layout)
    merger.MergeTriggers attrs