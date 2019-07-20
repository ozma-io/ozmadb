module FunWithFlags.FunDB.UserViews.Types

open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type ViewColumnName = FunQLName

type UserViewRef = FunWithFlags.FunDB.FunQL.AST.UserViewRef
type ResolvedUserViewRef = FunWithFlags.FunDB.FunQL.AST.ResolvedUserViewRef

[<NoComparison>]
type MainField =
    { name : FieldName
      field : ResolvedColumnField
    }

[<NoComparison>]
type UserViewColumn =
    { name : ViewColumnName
      attributeTypes : ExecutedAttributeTypes
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : SQL.SimpleValueType
      punType : SQL.SimpleValueType option
      mainField : MainField option
    }

[<NoComparison>]
type UVDomainField =
    { ref : ResolvedFieldRef
      field : ResolvedColumnField option
      idColumn : EntityName
    }

type UVDomain = Map<FieldName, UVDomainField>
type UVDomains = Map<GlobalDomainId, UVDomain>

[<NoComparison>]
type UserViewInfo =
    { attributeTypes : ExecutedAttributeTypes
      rowAttributeTypes : ExecutedAttributeTypes
      domains : UVDomains
      mainEntity : ResolvedEntityRef option
      columns : UserViewColumn[]
    }

[<NoComparison>]
type PureAttributes =
    { attributes : ExecutedAttributeMap
      columnAttributes : ExecutedAttributeMap[]
    }

[<NoComparison>]
type ResolvedUserView =
    { resolved : ResolvedViewExpr
      allowBroken : bool
      compiled : CompiledViewExpr
      info : UserViewInfo
      pureAttributes : PureAttributes
    }

[<NoComparison>]
type UserViewError =
  { source : SourceUserView
    error : exn
  }

[<NoComparison>]
type UserViewsSchema =
    { userViews : Map<UserViewName, Result<ResolvedUserView, UserViewError>>
    }

[<NoComparison>]
type UserViews =
    { schemas : Map<SchemaName, UserViewsSchema>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.userViews

type ErroredUserViewsSchema = Map<UserViewName, exn>
type ErroredUserViews = Map<SchemaName, ErroredUserViewsSchema>