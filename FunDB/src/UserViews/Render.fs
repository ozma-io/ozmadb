module FunWithFlags.FunDB.UserViews.Render

open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types

let private renderUserView = function
    | Ok uv ->
        { query = uv.resolved.ToFunQLString()
          allowBroken = uv.allowBroken
        }
    | Error e -> e.source

let renderUserViewsSchema (schema : UserViewsSchema) : SourceUserViewsSchema =
    { userViews = Map.map (fun name -> renderUserView) schema.userViews
      generatorScript = None
    }

let renderUserViews (uvs : UserViews) : SourceUserViews =
    { schemas = Map.map (fun schemaName -> renderUserViewsSchema) uvs.schemas
    }