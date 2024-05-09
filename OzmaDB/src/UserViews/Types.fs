module OzmaDB.UserViews.Types

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.Objects.Types
module SQL = OzmaDB.SQL.AST

type ViewColumnName = OzmaQLName

type UserViewRef = OzmaDB.OzmaQL.AST.UserViewRef
type ResolvedUserViewRef = OzmaDB.OzmaQL.AST.ResolvedUserViewRef

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