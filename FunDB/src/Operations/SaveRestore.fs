module FunWithFlags.FunDB.Operations.SaveRestore

open System.Threading.Tasks
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Schema
open FunWithFlags.FunDB.UserViews.Update

type SaveSchemaErrorInfo =
    | SENotFound
    with
        member this.Message =
            match this with
            | SENotFound -> "Schema not found"

type SaveSchemaException (info : SaveSchemaErrorInfo) =
    inherit Exception(info.Message)

    member this.Info = info

type SchemaDump =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceEntity>
      [<JsonProperty(Required=Required.DisallowNull)>]
      roles : Map<RoleName, SourceRole>
      [<JsonProperty(Required=Required.DisallowNull)>]
      userViews : Map<UserViewName, SourceUserView>
    }

let saveSchema (db : SystemContext) (name : SchemaName) : Task<SchemaDump> =
    task {
        let! entitiesData = buildSchemaLayout db
        let! rolesData = buildSchemaPermissions db
        let! userViewsData = buildSchemaUserViews db

        let findOrFail m =
            match Map.tryFind name m with
            | None -> raise <| SaveSchemaException SENotFound
            | Some v -> v
        let entities = findOrFail entitiesData.schemas
        let roles = findOrFail rolesData.schemas
        let userViews = findOrFail userViewsData.schemas
        return
            { entities = entities.entities
              roles = roles.roles
              userViews = userViews.userViews
            }
    }

let restoreSchema (db : SystemContext) (name : SchemaName) (dump : SchemaDump) : Task<bool> =
    task {
        let newLayout = { schemas = Map.singleton name { entities = dump.entities } } : SourceLayout
        let newPerms = { schemas = Map.singleton name { roles = dump.roles } } : SourcePermissions
        let newUserViews = { schemas = Map.singleton name { userViews = dump.userViews } } : SourceUserViews

        let! updated1 = updateLayout db newLayout
        let! updated2 = updatePermissions db newPerms
        let! updated3 = updateUserViews db newUserViews

        return updated1 || updated2 || updated3
    }