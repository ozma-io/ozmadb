module FunWithFlags.FunDB.API.Types

open System
open System.IO
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq
open NetJs

open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.SaveRestore
module SQL = FunWithFlags.FunDB.SQL.DDL

[<NoEquality; NoComparison>]
type CachedContext =
    { Layout : Layout
      UserViews : PrefetchedUserViews
      Permissions : Permissions
      DefaultAttrs : MergedDefaultAttributes
      Triggers : MergedTriggers
      SystemViews : SourceUserViews
      UserMeta : SQL.DatabaseMeta
    }

type IContext =
    inherit IDisposable
    inherit IAsyncDisposable

    abstract Transaction : DatabaseTransaction with get
    abstract State : CachedContext with get
    abstract Preload : Preload with get
    abstract TransactionTime : DateTime with get
    abstract LoggerFactory : ILoggerFactory with get
    abstract CancellationToken : CancellationToken with get
    abstract Isolate : Isolate

    abstract member ScheduleMigration : unit -> unit
    abstract member Commit : unit -> Task<unit>
    abstract member GetAnonymousView : string -> Task<PrefetchedUserView>
    abstract member ResolveAnonymousView : SchemaName option -> string -> Task<PrefetchedUserView>
    abstract member WriteEvent : EventEntry -> ValueTask

[<NoEquality; NoComparison>]
type RoleType =
    | RTRoot
    | RTRole of ResolvedRole

[<NoEquality; NoComparison>]
type RequestUser =
    { Type : RoleType
      Name : UserName
      Language : string
    }

type RawArguments = Map<string, JToken>

type IRequestContext =
    abstract Context : IContext with get
    abstract User : RequestUser with get
    abstract GlobalArguments : Map<ArgumentName, FieldValue> with get

    abstract member WriteEvent : (EventEntry -> unit) -> ValueTask
    abstract member WriteEventSync : (EventEntry -> unit) -> unit

[<SerializeAsObject("error")>]
type UserViewErrorInfo =
    | [<CaseName("not_found")>] UVENotFound
    | [<CaseName("access_denied")>] UVEAccessDenied
    | [<CaseName("resolution")>] UVEResolution of Details : string
    | [<CaseName("execution")>] UVEExecution of Details : string
    | [<CaseName("arguments")>] UVEArguments of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | UVENotFound -> "User view not found"
            | UVEAccessDenied -> "User view access denied"
            | UVEResolution msg -> "User view typecheck failed"
            | UVEExecution msg -> "User view execution failed"
            | UVEArguments msg -> "Invalid user view arguments"

[<SerializeAsObject("type")>]
type UserViewSource =
    | [<CaseName("anonymous")>] UVAnonymous of Query : string
    | [<CaseName("named")>] UVNamed of Ref : ResolvedUserViewRef

[<NoEquality; NoComparison>]
type UserViewEntriesResult =
    { Info : UserViewInfo
      Result : ExecutedViewExpr
    }

[<NoEquality; NoComparison>]
type UserViewInfoResult =
    { Info : UserViewInfo
      PureAttributes : ExecutedAttributeMap
      PureColumnAttributes : ExecutedAttributeMap array
    }
 
 type IUserViewsAPI =
    abstract member GetUserViewInfo : UserViewSource -> bool -> Task<Result<UserViewInfoResult, UserViewErrorInfo>>
    abstract member GetUserView : UserViewSource -> RawArguments -> bool -> Task<Result<UserViewEntriesResult, UserViewErrorInfo>>

[<SerializeAsObject("error")>]
type EntityErrorInfo =
    | [<CaseName("not_found")>] EENotFound
    | [<CaseName("access_denied")>] EEAccessDenied
    | [<CaseName("arguments")>] EEArguments of Details : string
    | [<CaseName("execution")>] EEExecution of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | EENotFound -> "Entity not found"
            | EEAccessDenied -> "Entity access denied"
            | EEArguments msg -> "Invalid operation arguments"
            | EEExecution msg -> "Operation execution failed"

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseName("insert")>] TInsertEntity of Entity : ResolvedEntityRef * Entries : RawArguments
    | [<CaseName("update")>] TUpdateEntity of Entity : ResolvedEntityRef * Id : int * Entries : RawArguments
    | [<CaseName("delete")>] TDeleteEntity of Entity : ResolvedEntityRef * Id : int

[<SerializeAsObject("type")>]
type TransactionOpResult =
    | [<CaseName("insert")>] TRInsertEntity of Id : int
    | [<CaseName("update")>] TRUpdateEntity
    | [<CaseName("delete")>] TRDeleteEntity

[<NoEquality; NoComparison>]
type Transaction =
    { Operations : TransactionOp[]
    }

[<NoEquality; NoComparison>]
type TransactionResult =
    { Results : TransactionOpResult[]
    }

type TransactionError =
    { Error : EntityErrorInfo
      Operation : int
    }

 type IEntitiesAPI =
    abstract member GetEntityInfo : ResolvedEntityRef -> Task<Result<SerializedEntity, EntityErrorInfo>>
    abstract member InsertEntity : ResolvedEntityRef -> RawArguments -> Task<Result<int, EntityErrorInfo>>
    abstract member UpdateEntity : ResolvedEntityRef -> int -> RawArguments -> Task<Result<unit, EntityErrorInfo>>
    abstract member DeleteEntity : ResolvedEntityRef -> int -> Task<Result<unit, EntityErrorInfo>>
    abstract member RunTransaction : Transaction -> Task<Result<TransactionResult, TransactionError>>

[<SerializeAsObject("error")>]
type SaveErrorInfo =
    | [<CaseName("access_denied")>] RSEAccessDenied
    | [<CaseName("not_found")>] RSENotFound
    with
        [<DataMember>]
        member this.Message =
            match this with
            | RSEAccessDenied -> "Dump access denied"
            | RSENotFound -> "Schema not found"

[<SerializeAsObject("error")>]
type RestoreErrorInfo =
    | [<CaseName("access_denied")>] RREAccessDenied
    | [<CaseName("preloaded")>] RREPreloaded
    | [<CaseName("invalid_format")>] RREInvalidFormat of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | RREAccessDenied -> "Restore access denied"
            | RREPreloaded -> "Cannot restore preloaded schemas"
            | RREInvalidFormat msg -> "Invalid data format"

type ISaveRestoreAPI =
    abstract member SaveSchema : SchemaName -> Task<Result<SchemaDump, SaveErrorInfo>>
    abstract member SaveZipSchema : SchemaName -> Task<Result<Stream, SaveErrorInfo>>
    abstract member RestoreSchema : SchemaName -> SchemaDump -> Task<Result<unit, RestoreErrorInfo>>
    abstract member RestoreZipSchema : SchemaName -> Stream -> Task<Result<unit, RestoreErrorInfo>>

type IFunDBAPI =
    abstract member Request : IRequestContext
    abstract member UserViews : IUserViewsAPI
    abstract member Entities : IEntitiesAPI
    abstract member SaveRestore : ISaveRestoreAPI