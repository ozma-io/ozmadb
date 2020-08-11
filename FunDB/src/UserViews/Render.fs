module FunWithFlags.FunDB.UserViews.Render

open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types

let private renderUserView = function
    | Ok uv ->
        { Query = uv.resolved.ToFunQLString()
          AllowBroken = uv.allowBroken
        }
    | Error e -> e.source

let renderUserViewsSchema (schema : UserViewsSchema) : SourceUserViewsSchema =
    { UserViews = Map.map (fun name -> renderUserView) schema.userViews
      GeneratorScript = None
    }

let renderUserViews (uvs : UserViews) : SourceUserViews =
    { Schemas = Map.map (fun schemaName -> renderUserViewsSchema) uvs.schemas
    }