module FunWithFlags.FunDB.Schema

open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDBSchema.Schema

let getFieldsObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("Entities")
        .Include("Entities.ColumnFields")

let getLayoutObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    (getFieldsObjects schemas)
        .Include("Entities.ComputedFields")
        .Include("Entities.UniqueConstraints")
        .Include("Entities.CheckConstraints")
        .Include("Entities.Parent")
        .Include("Entities.Parent.Schema")

let getRolesObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("Roles")
        .Include("Roles.Parents")
        .Include("Roles.Parents.Parent")
        .Include("Roles.Parents.Parent.Schema")
        .Include("Roles.Entities")
        .Include("Roles.Entities.Entity")
        .Include("Roles.Entities.Entity.Schema")
        .Include("Roles.Entities.ColumnFields")

let getAttributesObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("FieldsAttributes")
        .Include("FieldsAttributes.FieldEntity")
        .Include("FieldsAttributes.FieldEntity.Schema")

let getUserViewsObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("UserViews")

let updateDifference (db : SystemContext) (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) : Map<'k, 'eobj> =
    let updateObject (name, newObject) =
        match Map.tryFind name existingObjects with
        | Some existingObject ->
            updateFunc name newObject existingObject
            (name, existingObject)
        | None ->
            let newExistingObject = createFunc name
            updateFunc name newObject newExistingObject
            (name, newExistingObject)
    let updatedObjects = newObjects |> Map.toSeq |> Seq.map updateObject |> Map.ofSeq
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            ignore <| db.Remove(existingObject)
    updatedObjects