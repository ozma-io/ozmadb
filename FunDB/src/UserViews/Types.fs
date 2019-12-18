module FunWithFlags.FunDB.UserViews.Types

open FunWithFlags.FunDB.Utils
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
    { resolved : ResolvedViewExpr
      allowBroken : bool
      compiled : CompiledViewExpr
    }

[<NoEquality; NoComparison>]
type UserViewError =
  { source : SourceUserView
    error : exn
  }

[<NoEquality; NoComparison>]
type UserViewsSchema =
    { userViews : Map<UserViewName, Result<ResolvedUserView, UserViewError>>
    }

[<NoEquality; NoComparison>]
type UserViews =
    { schemas : Map<SchemaName, UserViewsSchema>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.userViews

type ErroredUserViewsSchema = Map<UserViewName, exn>
type ErroredUserViews = Map<SchemaName, ErroredUserViewsSchema>

let unionErroredUserViews (a : ErroredUserViews) (b : ErroredUserViews) =
    Map.unionWith (fun name -> Map.unionUnique) a b