module FunWithFlags.FunDB.Permissions.Types

open FunWithFlags.FunDB.FunQL.AST

type UserName = string

type AllowedEntity =
    { fields : Set<FieldName>
    }

type AllowedSchema =
    { entities : Map<EntityName, AllowedEntity>
    }

type AllowedDatabase =
    { schemas : Map<SchemaName, AllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities