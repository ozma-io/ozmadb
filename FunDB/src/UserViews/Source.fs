module FunWithFlags.FunDB.UserViews.Source

open Newtonsoft.Json

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST

type SourceUserView =
    { allowBroken : bool
      query : string
    }

type SourceUserViewsSchema =
    { userViews : Map<UserViewName, SourceUserView>
      generatorScript : string option
    }

type SourceUserViews =
    { schemas : Map<SchemaName, SourceUserViewsSchema>
    }

let emptySourceUserViewsSchema : SourceUserViewsSchema =
    { userViews = Map.empty
      generatorScript = None
    }

let emptySourceUserViews : SourceUserViews =
    { schemas = Map.empty }