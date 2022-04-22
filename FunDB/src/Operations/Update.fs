module FunWithFlags.FunDB.Operations.Update

open System
open System.Reflection
open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Operations.Entity

type SystemUpdaterException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = SystemUpdaterException (message, null)

let inline private updateDifference (db : DbContext) (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (deleteFunc : 'k -> 'eobj -> unit) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
    let updateObject (name, newObject) =
        match Map.tryFind name existingObjects with
        | Some existingObject ->
            updateFunc name newObject existingObject
            (name, existingObject)
        | None ->
            let newExistingObject = createFunc name
            ignore <| db.Add(newExistingObject)
            updateFunc name newObject newExistingObject
            (name, newExistingObject)
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            deleteFunc name existingObject
    newObjects |> Map.toSeq |> Seq.map updateObject |> Map.ofSeq

let private getDbSet<'a when 'a :> DbContext> (defaultSchema : SchemaName) (db : 'a) (property : PropertyInfo) =
    let typ = property.PropertyType
    if not (typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<DbSet<_>>) then
        None
    else
        let entityType = typ.GetGenericArguments().[0]
        let metadata = db.Model.FindEntityType(entityType)
        let schemaName = metadata.GetSchema()
        let tableName = metadata.GetTableName()
        let schema =
            match schemaName with
            | null -> defaultSchema
            | name -> FunQLName name
        assert (not <| isNull tableName)
        let ref = { Schema = schema; Name = FunQLName tableName }
        Some (entityType, ref)

let private systemEntities =
    lazy (
        // We *need* to have a database provider, even if we only want to look at the model.
        // So we hack it together.
        let systemOptions =
            (DbContextOptionsBuilder<SystemContext> ())
                .UseNpgsql((null : Data.Common.DbConnection), fun opts -> ignore <| opts.UseNodaTime())
        use db = new SystemContext(systemOptions.Options)
        let ctxType = typeof<SystemContext>
        ctxType.GetProperties()
            |> Seq.mapMaybe (getDbSet funSchema db)
            |> dict
    )

type DeferredDeleteSet = Set<ResolvedEntityRef * RowId>

let getSystemEntityRef (typ : Type) =
    match systemEntities.Value.TryGetValue typ with
    | (true, ref) -> Some ref
    | (false, _) -> None

let private cascadeDeleteDeferred (filterEntities : ResolvedEntityRef -> bool) (layout : Layout) (connection : DatabaseTransaction) (deferredSet : DeferredDeleteSet) (cancellationToken : CancellationToken) =
    unitTask {
        let rec go (deferredSet : DeferredDeleteSet) =
            unitTask {
                let mutable deferredSet = deferredSet

                let deleteOne (entityRef : ResolvedEntityRef) (id : RowId) =
                    unitTask {
                        deferredSet <- Set.remove (entityRef, id) deferredSet
                        let! _ = deleteEntity connection.Connection.Query Map.empty layout None entityRef (RKId id) None cancellationToken
                        ()
                    }

                let (entityRef, id) = deferredSet |> Seq.first |> Option.get
                let! tree = getRelatedEntities connection.Connection.Query Map.empty layout None (fun entityRef rowId refFieldRef -> filterEntities refFieldRef.Entity) entityRef (RKId id) None cancellationToken
                do! iterReferencesUpwardsTask deleteOne (function | RDANoAction -> true | _ -> false) tree
                if not <| Set.isEmpty deferredSet then
                    do! go deferredSet
            }

        if not <| Set.isEmpty deferredSet then
            do! connection.DeferConstraints cancellationToken <| fun () -> task { do! go deferredSet }
    }

type SystemUpdater(db : SystemContext) =
    let mutable deletedObjects = Set.empty

    member this.DeleteObject (typ : Type, id : RowId) =
        let entityRef = getSystemEntityRef typ |> Option.get
        deletedObjects <- Set.add (entityRef, id) deletedObjects

    member inline this.DeleteObject (row : ^a) =
        let id = (^a : (member Id : int) row)
        this.DeleteObject(typeof< ^a >, id)

    member inline this.UpdateDifference (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
        updateDifference this.Context updateFunc createFunc (fun _ -> this.DeleteObject) newObjects existingObjects

    member inline this.UpdateRelatedDifference (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
        let createFunc name = raisef SystemUpdaterException "Object %O doesn't exist" name
        let deleteFunc name obj = raisef SystemUpdaterException "Refusing to delete object %O" name
        updateDifference this.Context updateFunc createFunc deleteFunc newObjects existingObjects

    member this.Context = db
    member this.DeletedObjects = deletedObjects

type UpdateResult =
    { DeferredDeletes : DeferredDeleteSet
      Changed : bool
    }

let updateResultIsEmpty (result : UpdateResult) =
    Set.isEmpty result.DeferredDeletes && not result.Changed

let unionUpdateResult (a : UpdateResult) (b : UpdateResult) =
    { DeferredDeletes = Set.union a.DeferredDeletes b.DeferredDeletes
      Changed = a.Changed || b.Changed
    }

let deleteDeferredFromUpdate (layout : Layout) (connection : DatabaseTransaction) (result : UpdateResult) (cancellationToken : CancellationToken) =
    cascadeDeleteDeferred (fun entityRef -> entityRef.Schema <> funSchema) layout connection result.DeferredDeletes cancellationToken

let private schemaEntityRef : ResolvedEntityRef = { Schema = funSchema; Name = FunQLName "schemas" }

let deleteSchemas (layout : Layout) (connection : DatabaseTransaction) (schemas : Set<SchemaName>) (cancellationToken : CancellationToken) =
    unitTask {
        let schemasArray = schemas |> Seq.map string |> Seq.toArray
        let! schemaIds =
            (query {
                for schema in connection.System.Schemas do
                    where (schemasArray.Contains(schema.Name))
                    select schema.Id
            }).ToArrayAsync(cancellationToken)
        let deletes = schemaIds |> Seq.map (fun id -> (schemaEntityRef, id)) |> Set.ofSeq
        do! cascadeDeleteDeferred (fun _ -> true) layout connection deletes cancellationToken
    }

// We first load existing rows and update/create new ones, then delay the second stage when we remove old rows.
// Otherwise an issue with removing a row without removing related rows may happen:
// 1. A different `update` function deletes a referenced row;
// 2. Now this `update` function will miss all rows which reference deleted row because EF Core uses `INNER JOIN`, and the deleted row, well, doesn't exist.
// For example: an entity is removed and we don't remove all default attributes with `field_entity` referencing this one.
let genericSystemUpdate<'updater when 'updater :> SystemUpdater> (db : SystemContext) (cancellationToken : CancellationToken) (getUpdater : unit -> Task<'updater>) : Task<UpdateResult> =
    task {
        let! _ = serializedSaveChangesAsync db cancellationToken
        let! updater = getUpdater ()
        let! changed = serializedSaveChangesAsync db cancellationToken
        return
            { DeferredDeletes = updater.DeletedObjects
              Changed = changed
            }
    }
