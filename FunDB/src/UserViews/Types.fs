module FunWithFlags.FunDB.UserViews.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
module SQL = FunWithFlags.FunDB.SQL.AST

type ViewColumnName = FunQLName

type UserViewRef = FunWithFlags.FunDB.FunQL.AST.UserViewRef
type ResolvedUserViewRef = FunWithFlags.FunDB.FunQL.AST.ResolvedUserViewRef

[<NoEquality; NoComparison>]
type ResolvedUserView =
    { Resolved : ResolvedViewExpr
      AllowBroken : bool
      Compiled : CompiledViewExpr
    }

[<NoEquality; NoComparison>]
type UserViewsSchema =
    { UserViews : Map<UserViewName, Result<ResolvedUserView, exn>>
      GeneratorScript : SourceUserViewsGeneratorScript option
    }

[<NoEquality; NoComparison>]
type UserViewsSchemaError =
  { Source : SourceUserViewsSchema
    Error : exn
  }

[<NoEquality; NoComparison>]
type UserViews =
    { Schemas : Map<SchemaName, Result<UserViewsSchema, UserViewsSchemaError>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.name schema.UserViews
            | _ -> None

type ErroredUserViewsSchema =
    | UEGenerator of exn
    | UEUserViews of Map<UserViewName, exn>

type ErroredUserViews = Map<SchemaName, ErroredUserViewsSchema>

let unionErroredUserViews (a : ErroredUserViews) (b : ErroredUserViews) : ErroredUserViews =
    let mergeOne a b =
        match (a, b) with
        | (UEUserViews schema1, UEUserViews schema2) -> UEUserViews (Map.unionUnique schema1 schema2)
        | (UEGenerator e, UEUserViews _) -> UEGenerator e
        | (UEUserViews _, UEGenerator e) -> UEGenerator e
        | _ -> failwith "Cannot merge different error types"
    Map.unionWith (fun name -> mergeOne) a b