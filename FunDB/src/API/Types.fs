module rec FunWithFlags.FunDB.API.Types

open System
open System.IO
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq
open NodaTime

open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.UserViews.DryRun
open FunWithFlags.FunDB.Actions.Types
open FunWithFlags.FunDB.Actions.Run
open FunWithFlags.FunDB.Triggers.Types
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Triggers.Run
open FunWithFlags.FunDB.JavaScript.Runtime
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.Operations.Preload
open FunWithFlags.FunDB.Operations.SaveRestore
open FunWithFlags.FunDB.Operations.Domain
open FunWithFlags.FunDB.Operations.Entity
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
module SQL = FunWithFlags.FunDB.SQL.Query

type IAPIError =
    abstract member Message : string

[<SerializeAsObject("error")>]
type GenericErrorInfo =
    | [<CaseName("quotaExceeded")>] GEQuotaExceeded of Details : string
    | [<CaseName("commit")>] GECommit of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | GEQuotaExceeded msg -> sprintf  "Quota exceeded: %s" msg
            | GECommit msg -> sprintf "Error during commit: %s" msg

        interface IAPIError with
            member this.Message = this.Message

type IContext =
    inherit IDisposable
    inherit IAsyncDisposable

    abstract member Transaction : DatabaseTransaction
    abstract member Preload : Preload
    abstract member TransactionId : int
    abstract member TransactionTime : Instant
    abstract member LoggerFactory : ILoggerFactory
    abstract member Runtime : IJSRuntime

    abstract member Layout : Layout
    abstract member UserViews : PrefetchedUserViews
    abstract member Permissions : Permissions
    abstract member DefaultAttrs : MergedDefaultAttributes
    abstract member Triggers : MergedTriggers
    abstract member Domains : LayoutDomains

    abstract member CancellationToken : CancellationToken with get, set
    // Cancellation token that is used to commit all changes.
    // Expected to be cancelled at the same time or after the primary cancellation token.
    // Its purpose is to allow long migrations without bumping max request time for users.
    abstract member CommitCancellationToken : CancellationToken with get, set

    abstract member SetForceAllowBroken : unit -> unit
    abstract member ScheduleMigration : unit -> unit
    abstract member ScheduleBeforeCommit : string -> (Layout -> Task<Result<unit, GenericErrorInfo>>) -> unit
    abstract member Commit : unit -> Task<Result<unit, GenericErrorInfo>>
    abstract member CheckIntegrity : unit -> Task
    abstract member GetAnonymousView : bool -> string -> Task<PrefetchedUserView>
    abstract member GetAnonymousCommand : bool -> string -> Task<CompiledCommandExpr>
    abstract member ResolveAnonymousView : bool -> SchemaName option -> string -> Task<PrefetchedUserView>
    abstract member WriteEvent : EventEntry -> unit
    abstract member SetAPI : IFunDBAPI -> unit
    abstract member FindAction : ActionRef -> Result<ActionScript, exn> option
    abstract member FindTrigger : TriggerRef -> PreparedTrigger option

[<NoEquality; NoComparison>]
type RoleInfo =
    { Role : ResolvedRole option
      Ref : ResolvedRoleRef option
      CanRead : bool
    }

[<NoEquality; NoComparison>]
type RoleType =
    | RTRoot
    | RTRole of RoleInfo
    with
        member this.IsRoot =
            match this with
            | RTRoot -> true
            | _ -> false

type RequestQuota =
    { MaxSize : int option // MiB
      MaxUsers : int option
    }

[<NoEquality; NoComparison>]
type RequestUser =
    { Id : int option
      Type : RoleType
      Name : UserName
    }

[<NoEquality; NoComparison>]
type RequestUserInfo =
    { Saved : RequestUser
      Effective : RequestUser
      Language : string
    }

[<SerializeAsObject("type")>]
type EventSource =
    | [<CaseName("api")>] ESAPI
    | [<CaseName("trigger", Type=CaseSerialization.InnerObject)>] ESTrigger of TriggerRef
    | [<CaseName("action", Type=CaseSerialization.InnerObject)>] ESAction of ActionRef
    with
        override this.ToString () =
            match this with
            | ESAPI -> "API"
            | ESTrigger trig -> sprintf "(trigger %O)" trig
            | ESAction ref -> sprintf "(action %O)" ref

type IRequestContext =
    abstract Context : IContext with get
    abstract User : RequestUserInfo with get
    abstract GlobalArguments : LocalArgumentsMap with get
    abstract Source : EventSource with get
    abstract Quota : RequestQuota with get

    abstract member WriteEvent : (EventEntry -> unit) -> unit
    abstract member WriteEventSync : (EventEntry -> unit) -> Task
    abstract member RunWithSource : EventSource -> (unit -> Task<'a>) -> Task<'a>
    abstract member PretendRole : ResolvedRoleRef -> (unit -> Task<'a>) -> Task<'a>
    abstract member PretendRoot : (unit -> Task<'a>) -> Task<'a>
    abstract member PretendUser : UserName -> (unit -> Task<'a>) -> Task<'a>
    abstract member IsPrivileged : bool

[<SerializeAsObject("error")>]
type UserViewErrorInfo =
    | [<CaseName("notFound")>] UVENotFound
    | [<CaseName("accessDenied")>] UVEAccessDenied
    | [<CaseName("compilation")>] UVECompilation of Details : string
    | [<CaseName("execution")>] UVEExecution of Details : string
    | [<CaseName("arguments")>] UVEArguments of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | UVENotFound -> "User view not found"
            | UVEAccessDenied -> "User view access denied"
            | UVECompilation msg -> sprintf  "User view compilation failed: %s" msg
            | UVEExecution msg -> sprintf "User view execution failed: %s" msg
            | UVEArguments msg -> sprintf "Invalid user view arguments: %s" msg

        interface IAPIError with
            member this.Message = this.Message

[<SerializeAsObject("type")>]
type UserViewSource =
    | [<CaseName("anonymous")>] UVAnonymous of Query : string
    | [<CaseName("named")>] UVNamed of Ref : ResolvedUserViewRef
    with
        override this.ToString () =
            match this with
            | UVNamed ref -> string ref
            | UVAnonymous query -> sprintf "(anonymous: %s)" query

[<NoEquality; NoComparison>]
type ExecutedViewExpr =
    { Attributes : ExecutedAttributesMap
      ColumnAttributes : ExecutedAttributesMap[]
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
      Rows : ExecutedRow[]
    }

[<NoEquality; NoComparison>]
type UserViewEntriesResult =
    { Info : UserViewInfo
      Result : ExecutedViewExpr
    }

[<NoEquality; NoComparison>]
type UserViewInfoResult =
    { Info : UserViewInfo
      ConstAttributes : ExecutedAttributesMap
      ConstColumnAttributes : ExecutedAttributesMap[]
      ConstArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
    }

type UserViewFlags =
    { ForceRecompile : bool
      NoAttributes : bool
      NoTracking : bool
      NoPuns : bool
    }

let emptyUserViewFlags =
    { ForceRecompile = false
      NoAttributes = false
      NoTracking = false
      NoPuns = false
    } : UserViewFlags

type IUserViewsAPI =
    abstract member GetUserViewInfo : UserViewSource -> UserViewFlags -> Task<Result<UserViewInfoResult, UserViewErrorInfo>>
    abstract member GetUserViewExplain :  UserViewSource -> RawArguments option -> SourceQueryChunk -> UserViewFlags -> SQL.ExplainOptions -> Task<Result<ExplainedViewExpr, UserViewErrorInfo>>
    abstract member GetUserView : UserViewSource -> RawArguments -> SourceQueryChunk -> UserViewFlags -> Task<Result<UserViewEntriesResult, UserViewErrorInfo>>

[<SerializeAsObject("error")>]
type EntityErrorInfo =
    | [<CaseName("notFound")>] EENotFound
    | [<CaseName("frozen")>] EEFrozen
    | [<CaseName("accessDenied")>] EEAccessDenied
    | [<CaseName("arguments")>] EEArguments of Details : string
    | [<CaseName("compilation")>] EECompilation of Details : string
    | [<CaseName("execution")>] EEExecution of Details : string
    | [<CaseName("exception")>] EEException of Details : string * UserData : JToken option
    | [<CaseName("trigger")>] EETrigger of Schema : SchemaName * Name : TriggerName * Inner : EntityErrorInfo
    with
        [<DataMember>]
        member this.Message =
            match this with
            | EENotFound -> "Entity not found"
            | EEFrozen -> "Entity is frozen"
            | EEAccessDenied -> "Entity access denied"
            | EECompilation msg -> sprintf "Command compilation failed: %s" msg
            | EEArguments msg -> sprintf "Invalid operation arguments: %s" msg
            | EEExecution msg -> sprintf "Operation execution failed: %s" msg
            | EEException (msg, userData) -> msg
            | EETrigger (schema, name, inner) -> sprintf "Error while running trigger %O.%O: %s" schema name inner.Message

        interface IAPIError with
            member this.Message = this.Message

[<SerializeAsObject("type")>]
type RawRowKey =
    | [<CaseName("primary", Type=CaseSerialization.InnerValue)>] RRKPrimary of RowId
    | [<CaseName(null)>] RRKAlt of Alt : ConstraintName * Keys : RawArguments

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseName("insert")>] TInsertEntity of Entity : ResolvedEntityRef * Entries : RawArguments
    | [<CaseName("update")>] TUpdateEntity of Entity : ResolvedEntityRef * Id : RawRowKey * Entries : RawArguments
    | [<CaseName("delete")>] TDeleteEntity of Entity : ResolvedEntityRef * Id : RawRowKey
    | [<CaseName("recursiveDelete")>] TRecursiveDeleteEntity of Entity : ResolvedEntityRef * Id : RawRowKey
    | [<CaseName("command")>] TCommand of Command : string * Arguments : RawArguments

[<SerializeAsObject("type")>]
type TransactionOpResult =
    | [<CaseName("insert")>] TRInsertEntity of Id : RowId option
    | [<CaseName("update")>] TRUpdateEntity of Id : RowId
    | [<CaseName("delete")>] TRDeleteEntity
    | [<CaseName("recursiveDelete")>] TRRecursiveDeleteEntity of Deleted : ReferencesTree
    | [<CaseName("command")>] TRCommand

[<NoEquality; NoComparison>]
type Transaction =
    { Operations : TransactionOp[]
    }

type TransactionError =
    { Operation : int
      Inner : EntityErrorInfo
    } with
        [<DataMember>]
        member this.Error = "transaction"

        [<DataMember>]
        member this.Message = this.Inner.Message

        interface IAPIError with
            member this.Message = this.Message

[<NoEquality; NoComparison>]
type TransactionResult =
    { Results : TransactionOpResult[]
    }

 type IEntitiesAPI =
    abstract member GetEntityInfo : ResolvedEntityRef -> Task<Result<SerializedEntity, EntityErrorInfo>>
    abstract member InsertEntities : ResolvedEntityRef -> RawArguments seq -> Task<Result<(RowId option)[], TransactionError>>
    abstract member UpdateEntity : ResolvedEntityRef -> RawRowKey -> RawArguments -> Task<Result<RowId, EntityErrorInfo>>
    abstract member DeleteEntity : ResolvedEntityRef -> RawRowKey -> Task<Result<unit, EntityErrorInfo>>
    abstract member GetRelatedEntities : ResolvedEntityRef -> RawRowKey -> Task<Result<ReferencesTree, EntityErrorInfo>>
    abstract member RecursiveDeleteEntity : ResolvedEntityRef -> RawRowKey -> Task<Result<ReferencesTree, EntityErrorInfo>>
    abstract member RunCommand : string -> RawArguments -> Task<Result<unit, EntityErrorInfo>>
    abstract member RunTransaction : Transaction -> Task<Result<TransactionResult, TransactionError>>
    abstract member DeferConstraints : (unit -> Task<'a>) -> Task<Result<'a, EntityErrorInfo>>

[<SerializeAsObject("error")>]
type SaveErrorInfo =
    | [<CaseName("accessDenied")>] RSEAccessDenied
    | [<CaseName("notFound")>] RSENotFound
    with
        [<DataMember>]
        member this.Message =
            match this with
            | RSEAccessDenied -> "Dump access denied"
            | RSENotFound -> "Schema not found"

        interface IAPIError with
            member this.Message = this.Message

[<SerializeAsObject("error")>]
type RestoreErrorInfo =
    | [<CaseName("accessDenied")>] RREAccessDenied
    | [<CaseName("preloaded")>] RREPreloaded
    | [<CaseName("invalidFormat")>] RREInvalidFormat of Details : string
    | [<CaseName("consistency")>] RREConsistency of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | RREAccessDenied -> "Restore access denied"
            | RREPreloaded -> "Cannot restore preloaded schemas"
            | RREInvalidFormat msg -> sprintf "Invalid data format: %s" msg
            | RREConsistency msg -> sprintf "Inconsistent dump: %s" msg

        interface IAPIError with
            member this.Message = this.Message

type SaveSchemas =
    | SSNames of SchemaName[]
    | SSAll
    | SSNonPreloaded

type ISaveRestoreAPI =
    abstract member SaveSchemas : SaveSchemas -> Task<Result<Map<SchemaName, SchemaDump>, SaveErrorInfo>>
    abstract member SaveZipSchemas : SaveSchemas -> Task<Result<Stream, SaveErrorInfo>>
    abstract member RestoreSchemas : Map<SchemaName, SchemaDump> -> bool -> Task<Result<unit, RestoreErrorInfo>>
    abstract member RestoreZipSchemas : Stream -> bool -> Task<Result<unit, RestoreErrorInfo>>

[<NoEquality; NoComparison>]
type ActionResult =
    { Result : JObject option
    }

[<SerializeAsObject("error")>]
type ActionErrorInfo =
    | [<CaseName("notFound")>] AENotFound
    | [<CaseName("compilation")>] AECompilation of Details : string
    | [<CaseName("exception")>] AEException of Details : string * UserData : JToken option
    with
        [<DataMember>]
        member this.Message =
            match this with
            | AENotFound -> "Action not found"
            | AECompilation msg -> sprintf "Action compilation failed: %s" msg
            | AEException (msg, userData) -> msg

        interface IAPIError with
            member this.Message = this.Message

type IActionsAPI =
    abstract member RunAction : ActionRef -> JObject -> Task<Result<ActionResult, ActionErrorInfo>>

[<NoEquality; NoComparison>]
type UserPermissions =
    { IsRoot : bool
    }

type IPermissionsAPI =
    abstract member UserPermissions : UserPermissions

[<NoEquality; NoComparison>]
type DomainValuesResult =
    { Values : DomainValue[]
      PunType : SQL.SimpleValueType
      Hash : string
    }

[<SerializeAsObject("error")>]
type DomainErrorInfo =
    | [<CaseName("notFound")>] DENotFound
    | [<CaseName("accessDenied")>] DEAccessDenied
    | [<CaseName("arguments")>] DEArguments of Details : string
    | [<CaseName("execution")>] DEExecution of Details : string
    with
        [<DataMember>]
        member this.Message =
            match this with
            | DENotFound -> "Field not found"
            | DEAccessDenied -> "Field access denied"
            | DEArguments msg -> sprintf "Invalid operation arguments: %s" msg
            | DEExecution msg -> sprintf "Operation execution failed: %s" msg

        interface IAPIError with
            member this.Message = this.Message

type DomainFlags =
    { ForceRecompile : bool
    }

let emptyDomainFlags =
    { ForceRecompile = false
    } : DomainFlags

type IDomainsAPI =
    abstract member GetDomainValues : ResolvedFieldRef -> int option -> SourceQueryChunk -> DomainFlags -> Task<Result<DomainValuesResult, DomainErrorInfo>>
    abstract member GetDomainExplain : ResolvedFieldRef -> int option -> SourceQueryChunk -> DomainFlags -> SQL.ExplainOptions -> Task<Result<ExplainedQuery, DomainErrorInfo>>

type IFunDBAPI =
    abstract member Request : IRequestContext
    abstract member UserViews : IUserViewsAPI
    abstract member Entities : IEntitiesAPI
    abstract member SaveRestore : ISaveRestoreAPI
    abstract member Actions : IActionsAPI
    abstract member Permissions : IPermissionsAPI
    abstract member Domains : IDomainsAPI

let dummyFunDBAPI =
    { new IFunDBAPI with
          member this.Request = failwith "Attempted to access dummy API"
          member this.UserViews = failwith "Attempted to access dummy API"
          member this.Entities = failwith "Attempted to access dummy API"
          member this.SaveRestore = failwith "Attempted to access dummy API"
          member this.Actions = failwith "Attempted to access dummy API"
          member this.Permissions = failwith "Attempted to access dummy API"
          member this.Domains = failwith "Attempted to access dummy API"
    }

let getReadRole = function
    | RTRoot -> None
    | RTRole role when role.CanRead -> None
    | RTRole role -> role.Role

let getWriteRole = function
    | RTRoot -> None
    | RTRole role -> Some (Option.defaultValue emptyResolvedRole role.Role)
