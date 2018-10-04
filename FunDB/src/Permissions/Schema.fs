module FunWithFlags.FunDB.Permissions.Schema

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Layout.Types

let private makeAllowedEntity (entity : ResolvedEntity) : AllowedEntity =
    { fields = Set.union (Map.keysSet entity.columnFields) (Map.keysSet entity.computedFields)
    }

let private makeAllowedSchema (schema : ResolvedSchema) : AllowedSchema =
    { entities = Map.map (fun name -> makeAllowedEntity) schema.entities
    }

let buildAllowedDatabase (layout : Layout) : AllowedDatabase =
    { schemas = Map.map (fun name -> makeAllowedSchema) layout.schemas
      systemEntities = Map.map (fun name -> makeAllowedEntity) layout.systemEntities
    }