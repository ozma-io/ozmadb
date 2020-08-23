module FunWithFlags.FunDB.UserViews.Render

open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Types

let private renderUserView : Result<ResolvedUserView, UserViewError> -> SourceUserView = function
    | Ok uv ->
        { Query = uv.Resolved.ToFunQLString()
          AllowBroken = uv.AllowBroken
        } : SourceUserView
    | Error e -> e.Source

let renderUserViewsSchema (schema : UserViewsSchema) : SourceUserViewsSchema =
    { UserViews = Map.map (fun name -> renderUserView) schema.UserViews
      GeneratorScript = schema.GeneratorScript
    }

let renderUserViews (uvs : UserViews) : SourceUserViews =
    let renderOne = function
    | Ok schema -> renderUserViewsSchema schema
    | Error e -> e.Source

    { Schemas = Map.map (fun schemaName -> renderOne) uvs.Schemas
    }