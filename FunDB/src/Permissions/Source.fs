module FunWithFlags.FunDB.Permissions.Source

open Newtonsoft.Json

open FunWithFlags.FunDB.FunQL.AST

type SourceAllowedField =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      change: bool
      select : string option
    }

type SourceAllowedEntity =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      allowBroken : bool
      check : string option
      [<JsonProperty(Required=Required.DisallowNull)>]
      insert : bool
      select : string option
      update : string option
      delete : string option
      [<JsonProperty(Required=Required.DisallowNull)>]
      fields : Map<FieldName, SourceAllowedField>
    }

type SourceAllowedSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceAllowedEntity>
    }

type SourceAllowedDatabase =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceAllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities

type SourceRole =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      parents : Set<ResolvedEntityRef>
      permissions : SourceAllowedDatabase
    }

type SourcePermissionsSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      roles : Map<RoleName, SourceRole>
    }

let emptySourcePermissionsSchema : SourcePermissionsSchema =
    { roles = Map.empty }

type SourcePermissions =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourcePermissionsSchema>
    }

let emptySourcePermissions : SourcePermissions =
    { schemas = Map.empty }