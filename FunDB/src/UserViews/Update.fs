module FunWithFlags.FunDB.UserViews.Update

open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Quotations
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Operations.Update
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDBSchema.System

type private UserViewsUpdater (db : SystemContext) as this =
    inherit SystemUpdater(db)

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
        ignore <| this.UpdateDifference updateFunc createFunc schema.UserViews oldUserViewsMap

    let updateSchemas (schemas : Map<SchemaName, SourceUserViewsSchema>) (existingSchemas : Map<SchemaName, Schema>) =
        let updateFunc _ = updateUserViewsSchema
        this.UpdateRelatedDifference updateFunc schemas existingSchemas

    member this.UpdateSchemas schemas existingSchemas = updateSchemas schemas existingSchemas

let updateUserViews (db : SystemContext) (uvs : SourceUserViews) (cancellationToken : CancellationToken) : Task<UpdateResult> =
    genericSystemUpdate db cancellationToken <| fun () ->
        task {
            let currentSchemas = db.GetUserViewsObjects ()

            // We don't touch in any way schemas not in layout.
            let wantedSchemas = uvs.Schemas |> Map.toSeq |> Seq.map (fun (FunQLName name, schema) -> name) |> Seq.toArray
            let! schemasList = currentSchemas.AsTracking().Where(fun schema -> wantedSchemas.Contains(schema.Name)).ToListAsync(cancellationToken)
            let schemasMap =
                schemasList
                |> Seq.map (fun schema -> (FunQLName schema.Name, schema)) |> Map.ofSeq

            let updater = UserViewsUpdater(db)
            ignore <| updater.UpdateSchemas uvs.Schemas schemasMap
            return updater
        }

type private UserViewErrorRef = ERGenerator of SchemaName
                              | ERUserView of ResolvedUserViewRef

let private findBrokenUserViewsSchema (schemaName : SchemaName) (schema : ErroredUserViewsSchema) : UserViewErrorRef seq =
    seq {
        match schema with
        | UEGenerator e -> yield ERGenerator schemaName
        | UEUserViews schema ->
            for KeyValue(uvName, uv) in schema do
                yield ERUserView { Schema = schemaName; Name = uvName }
    }

let private findBrokenUserViews (uvs : ErroredUserViews) : UserViewErrorRef seq =
    seq {
        for KeyValue(schemaName, schema) in uvs do
            yield! findBrokenUserViewsSchema schemaName schema
    }

let private checkUserViewName (ref : ResolvedUserViewRef) : Expr<UserView -> bool> =
    let checkSchema = checkSchemaName ref.Schema
    let uvName = string ref.Name
    <@ fun uv -> (%checkSchema) uv.Schema && uv.Name = uvName @>

let markBrokenUserViews (db : SystemContext) (uvs : ErroredUserViews) (cancellationToken : CancellationToken) : Task =
    unitTask {
        let broken = findBrokenUserViews uvs
        let schemaChecks = broken |> Seq.mapMaybe (function ERGenerator ref -> Some ref | _ -> None) |> Seq.map checkSchemaName
        do! genericMarkBroken db.Schemas schemaChecks <@ fun x -> Schema(UserViewGeneratorScriptAllowBroken = true) @> cancellationToken
        let uvChecks = broken |> Seq.mapMaybe (function ERUserView ref -> Some ref | _ -> None) |> Seq.map checkUserViewName
        do! genericMarkBroken db.UserViews uvChecks <@ fun x -> UserView(AllowBroken = true) @> cancellationToken
    }
