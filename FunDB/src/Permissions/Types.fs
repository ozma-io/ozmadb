module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.AST

type UserName = string

let rootUserName : UserName = "root"

type AllowedEntity =
    { fields : Set<FieldName>
    }

type AllowedSchema =
    { entities : Map<EntityName, AllowedEntity>
    }

type AllowedDatabase =
    { schemas : Map<SchemaName, AllowedSchema>
      systemEntities : Map<EntityName, AllowedEntity>
    } with
        member this.FindEntity (entity : EntityRef) =
            match entity.schema with
                | None -> Map.tryFind entity.name this.systemEntities
                | Some schemaName ->
                    match Map.tryFind schemaName this.schemas with
                        | None -> None
                        | Some schema -> Map.tryFind entity.name schema.entities