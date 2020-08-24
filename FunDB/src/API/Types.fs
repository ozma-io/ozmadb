module rec FunWithFlags.FunDB.API.Types

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
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.SaveRestore
open FunWithFlags.FunDB.Operations.Entity
module SQL = FunWithFlags.FunDB.SQL.DDL

type RawArguments = Map<string, JToken>

type ArgsTriggerResult =
    | ATTouched of RawArguments
    | ATUntouched
    | ATCancelled

type ITriggerScript =
    abstract member RunInsertTriggerBefore : ResolvedEntityRef -> EntityArguments -> CancellationToken -> Task<ArgsTriggerResult>
    abstract member RunUpdateTriggerBefore : ResolvedEntityRef -> int -> EntityArguments -> CancellationToken -> Task<ArgsTriggerResult>
    abstract member RunDeleteTriggerBefore : ResolvedEntityRef -> int -> CancellationToken -> Task<bool>

    abstract member RunInsertTriggerAfter : ResolvedEntityRef -> int -> EntityArguments -> CancellationToken -> Task
    abstract member RunUpdateTriggerAfter : ResolvedEntityRef -> int -> EntityArguments -> CancellationToken -> Task
    abstract member RunDeleteTriggerAfter : ResolvedEntityRef -> CancellationToken -> Task

type IContext =
    inherit IDisposable
    inherit IAsyncDisposable

    abstract member Transaction : DatabaseTransaction with get
    abstract member Preload : Preload with get
    abstract member TransactionTime : DateTime with get
    abstract member LoggerFactory : ILoggerFactory with get
    abstract member CancellationToken : CancellationToken with get
    abstract member Isolate : Isolate

    abstract member Layout : Layout
    abstract member UserViews : PrefetchedUserViews
    abstract member Permissions : Permissions
    abstract member DefaultAttrs : MergedDefaultAttributes
    abstract member Triggers : MergedTriggers

    abstract member ScheduleMigration : unit -> unit
    abstract member Commit : unit -> Task
    abstract member GetAnonymousView : string -> Task<PrefetchedUserView>
    abstract member ResolveAnonymousView : SchemaName option -> string -> Task<PrefetchedUserView>
    abstract member WriteEvent : EventEntry -> unit
    abstract member SetAPI : IFunDBAPI -> unit
    abstract member FindTrigger : TriggerRef -> ITriggerScript option

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

[<SerializeAsObject("type")>]
type EventSource =
    | [<CaseName("api")>] ESAPI
    | [<CaseName("trigger", InnerObject=true)>] ESTrigger of TriggerRef

type IRequestContext =
    abstract Context : IContext with get
    abstract User : RequestUser with get
    abstract GlobalArguments : Map<ArgumentName, FieldValue> with get
    abstract Source : EventSource with get

    abstract member WriteEvent : (EventEntry -> unit) -> unit
    abstract member WriteEventSync : (EventEntry -> unit) -> unit
    abstract member RunWithSource : EventSource -> (unit -> Task<'a>) -> Task<'a>

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
    | [<CaseName("exception")>] EEException of Details : string
    | [<CaseName("trigger")>] EETrigger of Schema : SchemaName * Name : TriggerName * Inner : EntityErrorInfo
    with
        [<DataMember>]
        member this.Message =
            match this with
            | EENotFound -> "Entity not found"
            | EEAccessDenied -> "Entity access denied"
            | EEArguments msg -> "Invalid operation arguments"
            | EEExecution msg -> "Operation execution failed"
            | EEException msg -> "Exception during running user code"
            | EETrigger (schema, name, inner) -> "Error while running trigger"

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseName("insert")>] TInsertEntity of Entity : ResolvedEntityRef * Entries : RawArguments
    | [<CaseName("update")>] TUpdateEntity of Entity : ResolvedEntityRef * Id : int * Entries : RawArguments
    | [<CaseName("delete")>] TDeleteEntity of Entity : ResolvedEntityRef * Id : int

[<SerializeAsObject("type")>]
type TransactionOpResult =
    | [<CaseName("insert")>] TRInsertEntity of Id : int option
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
    abstract member InsertEntity : ResolvedEntityRef -> RawArguments -> Task<Result<int option, EntityErrorInfo>>
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

let dummyFunDBAPI =
    { new IFunDBAPI with
          member this.Request = failwith "Attempted to access dummy API"
          member this.UserViews = failwith "Attempted to access dummy API"
          member this.Entities = failwith "Attempted to access dummy API"
          member this.SaveRestore = failwith "Attempted to access dummy API"
    }