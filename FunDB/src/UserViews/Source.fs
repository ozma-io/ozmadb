module FunWithFlags.FunDB.UserViews.Source

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST

type SourceUserView =
    { AllowBroken : bool
      Query : string
    }

type SourceUserViewsGeneratorScript =
  { Script : string
    AllowBroken : bool
  }

type SourceUserViewsSchema =
    { UserViews : Map<UserViewName, SourceUserView>
      GeneratorScript : SourceUserViewsGeneratorScript option
    }

type SourceUserViews =
    { Schemas : Map<SchemaName, SourceUserViewsSchema>
    }

let emptySourceUserViewsSchema : SourceUserViewsSchema =
    { UserViews = Map.empty
      GeneratorScript = None
    }

let emptySourceUserViews : SourceUserViews =
    { Schemas = Map.empty }