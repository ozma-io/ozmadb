module FunWithFlags.FunDB.UserViews.Types

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Objects.Types
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
    { UserViews : Map<UserViewName, PossiblyBroken<ResolvedUserView>>
    }

[<NoEquality; NoComparison>]
type UserViews =
    { Schemas : Map<SchemaName, PossiblyBroken<UserViewsSchema>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.Name schema.UserViews
            | _ -> None