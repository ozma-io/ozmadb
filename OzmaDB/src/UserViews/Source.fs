module OzmaDB.UserViews.Source

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST

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
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | Some schema -> Map.tryFind ref.Name schema.UserViews
            | _ -> None

let emptySourceUserViewsSchema : SourceUserViewsSchema =
    { UserViews = Map.empty
      GeneratorScript = None
    }

let emptySourceUserViews : SourceUserViews =
    { Schemas = Map.empty }