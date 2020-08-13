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
type UserViewError =
  { Source : SourceUserView
    Error : exn
  }

[<NoEquality; NoComparison>]
type UserViewsSchema =
    { UserViews : Map<UserViewName, Result<ResolvedUserView, UserViewError>>
      GeneratorScript : SourceUserViewsGeneratorScript option
    }

type UserViewsSchemaErrorType = SETGenerator of exn

[<NoEquality; NoComparison>]
type UserViewsSchemaError =
  { Source : SourceUserViewsSchema
    Error : UserViewsSchemaErrorType
  }

[<NoEquality; NoComparison>]
type UserViews =
    { Schemas : Map<SchemaName, Result<UserViewsSchema, UserViewsSchemaError>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.name schema.UserViews
            | _ -> None

type ErroredUserViewsSchema = Map<UserViewName, exn>
type ErroredUserViews = Map<SchemaName, Result<ErroredUserViewsSchema, UserViewsSchemaErrorType>>

let unionErroredUserViews (a : ErroredUserViews) (b : ErroredUserViews) =
    let mergeOne a b =
        match (a, b) with
        | (Ok schema1, Ok schema2) -> Ok (Map.unionUnique schema1 schema2)
        | (Error e, Ok _) -> Error e
        | (Ok _, Error e) -> Error e
        | _ -> failwith "Cannot merge different error types"
    Map.unionWith (fun name -> mergeOne) a b