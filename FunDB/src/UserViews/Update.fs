module FunWithFlags.FunDB.UserViews.Update

open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDBSchema.System

type private UserViewsUpdater (db : SystemContext) =
    let updateUserView (uv : SourceUserView) (existingUv : UserView) : unit =
        existingUv.AllowBroken <- uv.AllowBroken
        existingUv.Query <- uv.Query

    let updateUserViewsSchema (schema : SourceUserViewsSchema) (existingSchema : Schema) : unit =
        match schema.GeneratorScript with
        | Some src ->
            existingSchema.UserViewGeneratorScript <- src.Script
            existingSchema.UserViewGeneratorScriptAllowBroken <- src.AllowBroken
        | None ->
            existingSchema.UserViewGeneratorScript <- null

        let oldUserViewsMap =
            existingSchema.UserViews |> Seq.map (fun uv -> (FunQLName uv.Name, uv)) |> Map.ofSeq

        let updateFunc _ = updateUserView
        let createFunc (FunQLName name) =
            let newUv =
                UserView (
                    Name = name
                )
            existingSchema.UserViews.Add(newUv)
            newUv
        ignore <| updateDifference db updateFunc createFunc schema.UserViews oldUserViewsMap


    let updateSchemas (schemas : Map<SchemaName, SourceUserViewsSchema>) (oldSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateUserViewsSchema
        let createFunc name = failwith <| sprintf "Schema %O doesn't exist" name
        ignore <| updateDifference db updateFunc createFunc schemas oldSchemas

    member this.UpdateSchemas = updateSchemas

let updateUserViews (db : SystemContext) (uvs : SourceUserViews) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let! _ = db.SaveChangesAsync(cancellationToken)

        let currentSchemas = db.GetUserViewsObjects ()

        // We don't touch in any way schemas not in layout.
        let wantedSchemas = uvs.Schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
        let! schemasList = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
        let schemasMap =
            schemasList
            |> Seq.map (fun schema -> (FunQLName schema.Name, schema)) |> Map.ofSeq

        let updater = UserViewsUpdater(db)
        updater.UpdateSchemas uvs.Schemas schemasMap
        let! changedEntries = db.SaveChangesAsync(cancellationToken)
        return changedEntries > 0
    }

let markBrokenUserViews (db : SystemContext) (uvs : ErroredUserViews) (cancellationToken : CancellationToken) : Task<unit> =
    task {
        let currentSchemas = db.GetUserViewsObjects ()

        let wantedSchemas = uvs |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
        let! schemasMap =
            currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)

        for schema in schemasMap do
            let errors = Map.find (FunQLName schema.Name) uvs
            match errors with
            | Ok schemaErrors ->
                for uv in schema.UserViews do
                    if Map.containsKey (FunQLName uv.Name) schemaErrors then
                        uv.AllowBroken <- true
            | Error (SETGenerator _) ->
                schema.UserViewGeneratorScriptAllowBroken <- true

        let! _ = db.SaveChangesAsync(cancellationToken)
        return ()
    }