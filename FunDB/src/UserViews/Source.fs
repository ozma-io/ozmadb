module FunWithFlags.FunDB.UserViews.Source

open Newtonsoft.Json

open FunWithFlags.FunDB.FunQL.AST

type SourceUserView =
    { allowBroken : bool
      query : string
    }

type SourceUserViewsSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      userViews : Map<UserViewName, SourceUserView>
    }

let emptySourceUserViewsSchema : SourceUserViewsSchema =
    { userViews = Map.empty }

type SourceUserViews =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceUserViewsSchema>
    }

let emptySourceUserViews : SourceUserViews =
    { schemas = Map.empty }