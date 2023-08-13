module rec FunWithFlags.FunDB.API.Types

open System
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open NodaTime
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunDB.Exception
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
open FunWithFlags.FunDB.Operations.Command
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
module SQL = FunWithFlags.FunDB.SQL.Query

[<SerializeAsObject("error")>]
type GenericErrorInfo =
    | [<CaseKey("quotaExceeded", IgnoreFields=[|"Details"|])>] GEQuotaExceeded of Details : string
    | [<CaseKey("commit")>] GECommit of Inner : IErrorDetails
    | [<CaseKey("migrationConflict")>] GEMigrationConflict
    | [<CaseKey("migration", IgnoreFields=[|"Details"|])>] GEMigration of Details : string
    | [<CaseKey("other", IgnoreFields=[|"Details"|])>] GEOther of Details : string
    with
        member this.LogMessage =
            match this with
            | GEQuotaExceeded details -> sprintf  "Quota exceeded: %s" details
            | GEMigrationConflict -> "Another migration is in progress"
            | GEMigration details -> sprintf "Failed to validate or perform the migration: %s" details
            | GECommit inner -> inner.LogMessage
            | GEOther details -> details

        [<DataMember>]
        member this.Message =
            match this with
            | GECommit inner -> inner.Message
            | _ -> this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | GEQuotaExceeded details -> 403
            | GEMigrationConflict -> 503
            | GEMigration details -> 422
            | GECommit inner -> inner.HTTPResponseCode
            | GEOther details -> 500

        member this.ShouldLog =
            match this with
            | GEQuotaExceeded details -> false
            | GEMigrationConflict -> false
            | GEMigration details -> false
            | GECommit inner -> inner.ShouldLog
            | GEOther details -> false

        static member private LookupKey = prepareLookupCaseKey<GenericErrorInfo>
        member this.Error =
            GenericErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

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
    | [<CaseKey("api")>] ESAPI
    | [<CaseKey("trigger", Type=CaseSerialization.InnerObject)>] ESTrigger of TriggerRef
    | [<CaseKey("action", Type=CaseSerialization.InnerObject)>] ESAction of ActionRef
    with
        override this.ToString () =
            match this with
            | ESAPI -> "API"
            | ESTrigger trig -> sprintf "(trigger %O)" trig
            | ESAction ref -> sprintf "(action %O)" ref

type [<JsonConverter(typeof<PretendRoleConverter>)>] PretendRole =
    | PRRole of ResolvedRoleRef
    | PRRoot

and PretendRoleConverter () =
    inherit JsonConverter<PretendRole> ()

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : PretendRole =
        match reader.TokenType with
        | JsonToken.String ->
            match reader.ReadAsString() with
            | "root" -> PRRoot
            | _ -> raise <| JsonSerializationException("Invalid value for PretendRole")
        | _ ->
            let ref = serializer.Deserialize<ResolvedRoleRef>(reader)
            PRRole ref

    override this.WriteJson (writer : JsonWriter, value : PretendRole, serializer : JsonSerializer) : unit =
        match value with
        | PRRole role -> serializer.Serialize(writer, role)
        | PRRoot -> writer.WriteValue "root"

type PretendRoleRequest =
    { AsRole : PretendRole
    } with
        interface ILoggableResponse with
            member this.ShouldLog = false

type PretendUserRequest =
    { AsUser : UserName
    }

type PretendErrorInfo =
    | PEUserNotFound
    | PERoleNotFound
    | PEAccessDenied
    | PENoUserRole
    with
        member this.LogMessage =
            match this with
            | PEUserNotFound -> "User not found"
            | PERoleNotFound -> "Role not found"
            | PEAccessDenied -> "Access denied"
            | PENoUserRole -> "User has no role"

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.ShouldLog =
            match this with
            | PEUserNotFound -> false
            | PERoleNotFound -> false
            | PEAccessDenied -> true
            | PENoUserRole -> false

        member this.HTTPResponseCode =
            match this with
            | PEUserNotFound -> 404
            | PERoleNotFound -> 404
            | PEAccessDenied -> 403
            | PENoUserRole -> 422

        static member private LookupKey = prepareLookupCaseKey<PretendErrorInfo>
        member this.Error =
            PretendErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.LogMessage = this.LogMessage
            member this.Message = this.Message
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

type IRequestContext =
    abstract Context : IContext with get
    abstract User : RequestUserInfo with get
    abstract GlobalArguments : LocalArgumentsMap with get
    abstract Source : EventSource with get
    abstract Quota : RequestQuota with get

    abstract member WriteEvent : (EventEntry -> unit) -> unit
    abstract member WriteEventSync : (EventEntry -> unit) -> Task
    abstract member RunWithSource : EventSource -> (unit -> Task<'a>) -> Task<'a>
    abstract member PretendRole : PretendRoleRequest -> (unit -> Task<'a>) -> Task<Result<'a, PretendErrorInfo>>
    abstract member PretendUser : PretendUserRequest -> (unit -> Task<'a>) -> Task<Result<'a, PretendErrorInfo>>
    abstract member IsPrivileged : bool

[<SerializeAsObject("error")>]
type UserViewErrorInfo =
    | [<CaseKey("accessDenied", IgnoreFields=[|"Details"|])>] UVEAccessDenied of Details : string
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] UVERequest of Details : string
    | [<CaseKey("other", IgnoreFields=[|"Details"|])>] UVEOther of Details : string
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] UVEExecution of FunQLExecutionError
    with
        member this.LogMessage =
            match this with
            | UVEAccessDenied msg -> sprintf  "User view access denied: %s" msg
            | UVERequest msg -> msg
            | UVEOther msg -> msg
            | UVEExecution inner -> sprintf "User view execution failed: %s" inner.LogMessage

        [<DataMember>]
        member this.Message =
            match this with
            | UVEAccessDenied msg -> "User view access denied"
            | UVEExecution inner -> sprintf "User view execution failed: %s" inner.Message
            | _ -> this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | UVEAccessDenied msg -> 403
            | UVERequest msg -> 422
            | UVEOther msg -> 500
            | UVEExecution inner -> inner.HTTPResponseCode

        member this.ShouldLog =
            match this with
            | UVEAccessDenied msg -> true
            | UVERequest msg -> false
            | UVEOther msg -> false
            | UVEExecution inner -> inner.ShouldLog

        static member private LookupKey = prepareLookupCaseKey<UserViewErrorInfo>
        member this.Error =
            match this with
            | UVEExecution inner -> inner.Error
            | _ -> UserViewErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

[<SerializeAsObject("type")>]
type UserViewSource =
    | [<CaseKey("anonymous")>] UVAnonymous of Query : string
    | [<CaseKey("named", Type=CaseSerialization.InnerObject)>] UVNamed of ResolvedUserViewRef
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
type UserViewEntriesResponse =
    { Info : UserViewInfo
      Result : ExecutedViewExpr
    } with
        interface ILoggableResponse with
            member this.ShouldLog = false

[<NoEquality; NoComparison>]
type UserViewInfoResponse =
    { Info : UserViewInfo
      ConstAttributes : ExecutedAttributesMap
      ConstColumnAttributes : ExecutedAttributesMap[]
      ConstArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
    } with
        interface ILoggableResponse with
            member this.ShouldLog = false

type UserViewFlags =
    { [<DataMember(EmitDefaultValue = false)>]
      ForceRecompile : bool
      [<DataMember(EmitDefaultValue = false)>]
      NoAttributes : bool
      [<DataMember(EmitDefaultValue = false)>]
      NoTracking : bool
      [<DataMember(EmitDefaultValue = false)>]
      NoPuns : bool
    }

let emptyUserViewFlags =
    { ForceRecompile = false
      NoAttributes = false
      NoTracking = false
      NoPuns = false
    } : UserViewFlags

type UserViewInfoRequest =
    { Source : UserViewSource
      [<DataMember(EmitDefaultValue = false)>]
      Flags : UserViewFlags option
    }

type UserViewExplainRequest =
    { Source : UserViewSource
      [<DataMember(EmitDefaultValue = false)>]
      Args : RawArguments option
      [<DataMember(EmitDefaultValue = false)>]
      Chunk : SourceQueryChunk option
      [<DataMember(EmitDefaultValue = false)>]
      Flags : UserViewFlags option
      [<DataMember(EmitDefaultValue = false)>]
      ExplainFlags : SQL.ExplainOptions option
    }

type UserViewRequest =
    { Source : UserViewSource
      [<DataMember(EmitDefaultValue = false)>]
      Args : RawArguments
      [<DataMember(EmitDefaultValue = false)>]
      Chunk : SourceQueryChunk option
      [<DataMember(EmitDefaultValue = false)>]
      Flags : UserViewFlags option
    }

type IUserViewsAPI =
    abstract member GetUserViewInfo : UserViewInfoRequest -> Task<Result<UserViewInfoResponse, UserViewErrorInfo>>
    abstract member GetUserViewExplain :  UserViewExplainRequest -> Task<Result<ExplainedViewExpr, UserViewErrorInfo>>
    abstract member GetUserView : UserViewRequest -> Task<Result<UserViewEntriesResponse, UserViewErrorInfo>>

[<SerializeAsObject("error")>]
type EntityErrorInfo =
    | [<CaseKey("exception", IgnoreFields=[|"Details"|])>] EEException of Details : string * UserData : JToken option
    | [<CaseKey("trigger")>] EETrigger of Schema : SchemaName * Name : TriggerName * Inner : IErrorDetails
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] EEOperation of EntityOperationError
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] EECommand of CommandError
    with
        member this.LogMessage =
            match this with
            | EEException (msg, data) -> msg
            | EETrigger (schema, name, inner) -> sprintf "Error while running trigger %O.%O: %s" schema name inner.Message
            | EEOperation err -> err.LogMessage
            | EECommand err -> err.LogMessage

        [<DataMember>]
        member this.Message =
            match this with
            | EEOperation err -> err.Message
            | EECommand err -> err.Message
            | _ -> this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | EEException (details, data) -> 500
            | EETrigger (schema, name, inner) -> 500
            | EEOperation inner -> inner.HTTPResponseCode
            | EECommand inner -> inner.HTTPResponseCode

        member this.ShouldLog =
            match this with
            | EEException (details, data) -> Option.isNone data
            // Better to log any errors from triggers.
            | EETrigger (schema, name, inner) -> inner.ShouldLog
            | EEOperation inner -> inner.ShouldLog
            | EECommand inner -> inner.ShouldLog

        static member private LookupKey = prepareLookupCaseKey<EntityErrorInfo>
        member this.Error =
            match this with
            | EEOperation inner -> inner.Error
            | EECommand inner -> inner.Error
            | _ -> EntityErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

[<SerializeAsObject("type")>]
type RawRowKey =
    | [<CaseKey("primary", Type=CaseSerialization.InnerValue)>] RRKPrimary of RowId
    | [<CaseKey(null)>] RRKAlt of Alt : ConstraintName * Keys : RawArguments

[<NoEquality; NoComparison>]
type InsertEntityRequest =
    { Entity : ResolvedEntityRef
      mutable Fields : RawArguments
    } with
        [<System.ComponentModel.DefaultValue(null)>]
        [<DataMember>]
        [<JsonProperty(Required=Required.DisallowNull, DefaultValueHandling=DefaultValueHandling.Ignore)>]
        member private this.Entries with set fields =
            this.Fields <- fields

[<NoEquality; NoComparison>]
type UpdateEntryRequest =
    { Entity : ResolvedEntityRef
      Id : RawRowKey
      mutable Fields : RawArguments
    } with
        [<System.ComponentModel.DefaultValue(null)>]
        [<DataMember>]
        [<JsonProperty(Required=Required.DisallowNull, DefaultValueHandling=DefaultValueHandling.Ignore)>]
        member private this.Entries with set fields =
            this.Fields <- fields

[<NoEquality; NoComparison>]
type DeleteEntryRequest =
    { Entity : ResolvedEntityRef
      Id : RawRowKey
    }

[<NoEquality; NoComparison>]
type CommandRequest =
    { Command : string
      [<DataMember(EmitDefaultValue = false)>]
      Args : RawArguments
    }

[<SerializeAsObject("type")>]
[<NoEquality; NoComparison>]
type TransactionOp =
    | [<CaseKey("insert", Type=CaseSerialization.InnerObject)>] TInsertEntity of InsertEntityRequest
    | [<CaseKey("update", Type=CaseSerialization.InnerObject)>] TUpdateEntity of UpdateEntryRequest
    | [<CaseKey("delete", Type=CaseSerialization.InnerObject)>] TDeleteEntity of DeleteEntryRequest
    | [<CaseKey("recursiveDelete", Type=CaseSerialization.InnerObject)>] TRecursiveDeleteEntity of DeleteEntryRequest
    | [<CaseKey("command", Type=CaseSerialization.InnerObject)>] TCommand of CommandRequest

type InsertEntityResponse =
    { Id : RowId option
    } with
        interface ILoggableResponse with
            member this.ShouldLog = true

type UpdateEntityResponse =
    { Id : RowId
    } with
        interface ILoggableResponse with
            member this.ShouldLog = true

[<SerializeAsObject("type")>]
type TransactionOpResponse =
    | [<CaseKey("insert", Type=CaseSerialization.InnerObject)>] TRInsertEntity of InsertEntityResponse
    | [<CaseKey("update", Type=CaseSerialization.InnerObject)>] TRUpdateEntity of UpdateEntityResponse
    | [<CaseKey("delete", Type=CaseSerialization.InnerObject)>] TRDeleteEntity
    | [<CaseKey("recursiveDelete", Type=CaseSerialization.InnerObject)>] TRRecursiveDeleteEntity of Deleted : ReferencesTree
    | [<CaseKey("command", Type=CaseSerialization.InnerObject)>] TRCommand

[<NoEquality; NoComparison>]
type TransactionRequest =
    { Operations : TransactionOp[]
    }

[<NoEquality; NoComparison>]
type TransactionErrorInfo =
    { Operation : int
      Inner : IErrorDetails
    } with
        member this.LogMessage = this.Inner.LogMessage

        [<DataMember>]
        member this.Message = this.Inner.Message

        member this.HTTPResponseCode = this.Inner.HTTPResponseCode

        member this.ShouldLog = this.Inner.ShouldLog

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        [<DataMember>]
        member this.Error = "transaction"

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

[<NoEquality; NoComparison>]
type TransactionResponse =
    { Results : TransactionOpResponse[]
    } with
        interface ILoggableResponse with
            member this.ShouldLog = true

[<NoEquality; NoComparison>]
type GetEntityInfoRequest =
    { Entity : ResolvedEntityRef   
    }

[<NoEquality; NoComparison>]
type GetRelatedEntriesRequest =
    { Entity : ResolvedEntityRef
      Id : RawRowKey
    }

[<NoEquality; NoComparison>]
type InsertEntriesRequest =
    { Entity : ResolvedEntityRef
      Entries : RawArguments seq
    }

type InsertEntitiesResponse =
    { Entries : InsertEntityResponse[]
    }

 type IEntitiesAPI =
    abstract member GetEntityInfo : GetEntityInfoRequest -> Task<Result<SerializedEntity, EntityErrorInfo>>
    abstract member InsertEntries : InsertEntriesRequest -> Task<Result<InsertEntitiesResponse, TransactionErrorInfo>>
    abstract member UpdateEntry : UpdateEntryRequest -> Task<Result<UpdateEntityResponse, EntityErrorInfo>>
    abstract member DeleteEntry : DeleteEntryRequest -> Task<Result<unit, EntityErrorInfo>>
    abstract member GetRelatedEntries : GetRelatedEntriesRequest -> Task<Result<ReferencesTree, EntityErrorInfo>>
    abstract member RecursiveDeleteEntry : DeleteEntryRequest -> Task<Result<ReferencesTree, EntityErrorInfo>>
    abstract member RunCommand : CommandRequest -> Task<Result<unit, EntityErrorInfo>>
    abstract member RunTransaction : TransactionRequest -> Task<Result<TransactionResponse, TransactionErrorInfo>>
    abstract member DeferConstraints : (unit -> Task<'a>) -> Task<Result<'a, EntityErrorInfo>>

[<SerializeAsObject("error")>]
type SaveErrorInfo =
    | [<CaseKey("accessDenied")>] RSEAccessDenied
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] RSERequest of Details : string
    with
        member this.LogMessage =
            match this with
            | RSEAccessDenied -> "Dump access denied"
            | RSERequest msg -> msg

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | RSEAccessDenied -> 403
            | RSERequest msg -> 422

        member this.ShouldLog =
            match this with
            | RSEAccessDenied -> true
            | RSERequest msg -> false

        static member private LookupKey = prepareLookupCaseKey<SaveErrorInfo>
        member this.Error =
            SaveErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

[<SerializeAsObject("error")>]
type RestoreErrorInfo =
    | [<CaseKey("accessDenied")>] RREAccessDenied
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] RRERequest of Details : string
    with
        member this.LogMessage =
            match this with
            | RREAccessDenied -> "Restore access denied"
            | RRERequest msg -> msg

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | RREAccessDenied -> 403
            | RRERequest msg -> 422

        member this.ShouldLog =
            match this with
            | RREAccessDenied -> true
            | RRERequest msg -> false

        static member private LookupKey = prepareLookupCaseKey<RestoreErrorInfo>
        member this.Error =
            RestoreErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

type [<JsonConverter(typeof<SaveSchemasConverter>)>] SaveSchemas =
    | SSNames of SchemaName[]
    | SSAll
    | SSNonPreloaded

and SaveSchemasConverter () =
    inherit JsonConverter<SaveSchemas> ()

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : SaveSchemas =
        match reader.TokenType with
        | JsonToken.String ->
            match reader.ReadAsString() with
            | "all" -> SSAll
            | "nonPreloaded" -> SSNonPreloaded
            | _ -> raise <| JsonSerializationException("Invalid value for SaveSchemas")
        | _ ->
            let names = serializer.Deserialize<SchemaName[]>(reader)
            SSNames names

    override this.WriteJson (writer : JsonWriter, value : SaveSchemas, serializer : JsonSerializer) : unit =
        match value with
        | SSNames names -> serializer.Serialize(writer, names)
        | SSAll -> writer.WriteValue "all"
        | SSNonPreloaded -> writer.WriteValue "nonPreloaded"

[<NoEquality; NoComparison>]
type SaveSchemasRequest =
    { Schemas : SaveSchemas
    }

[<NoEquality; NoComparison>]
type SaveSchemasResponse =
    { Schemas : Map<SchemaName, SchemaDump>
    } with
        interface ILoggableResponse with
            member this.ShouldLog = false

[<NoEquality; NoComparison>]
type RestoreSchemasFlags =
    { [<DataMember(EmitDefaultValue = false)>]
      DropOthers : bool
    }

let emptyRestoreSchemasFlags =
    { DropOthers = false
    } : RestoreSchemasFlags

[<NoEquality; NoComparison>]
type RestoreSchemasRequest =
    { Schemas : Map<SchemaName, SchemaDump>
      [<DataMember(EmitDefaultValue = false)>]
      Flags : RestoreSchemasFlags option
    }

[<NoEquality; NoComparison>]
type RestoreStreamSchemasRequest =
    { [<DataMember(EmitDefaultValue = false)>]
      Flags : RestoreSchemasFlags option
    }

type ISaveRestoreAPI =
    abstract member SaveSchemas : SaveSchemasRequest -> Task<Result<SaveSchemasResponse, SaveErrorInfo>>
    abstract member RestoreSchemas : RestoreSchemasRequest -> Task<Result<unit, RestoreErrorInfo>>

[<NoEquality; NoComparison>]
type RunActionRequest =
    { Action : ActionRef
      [<DataMember(EmitDefaultValue = false)>]
      Args : JObject option
    }

[<NoEquality; NoComparison>]
type ActionResponse =
    { Result : JObject option
    } with
    interface ILoggableResponse with
        member this.ShouldLog = false

[<SerializeAsObject("error")>]
[<NoEquality; NoComparison>]
type ActionErrorInfo =
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] AERequest of Details : string
    | [<CaseKey("exception", IgnoreFields=[|"Details"|])>] AEException of Details : string * UserData : JToken option
    | [<CaseKey("other", IgnoreFields=[|"Details"|])>] AEOther of Details : string
    with
        member this.LogMessage =
            match this with
            | AERequest msg -> msg
            | AEException (msg, userData) -> msg
            | AEOther msg -> msg

        [<DataMember>]
        member this.Message = this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | AERequest msg -> 412
            | AEException (details, userData) -> 500
            | AEOther msg -> 500

        member this.ShouldLog =
            match this with
            | AERequest msg -> false
            | AEException (details, data) -> Option.isNone data
            | AEOther details -> false

        static member private LookupKey = prepareLookupCaseKey<ActionErrorInfo>
        member this.Error =
            ActionErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

type IActionsAPI =
    abstract member RunAction : RunActionRequest -> Task<Result<ActionResponse, ActionErrorInfo>>

[<NoEquality; NoComparison>]
type UserPermissions =
    { IsRoot : bool
    }

type IPermissionsAPI =
    abstract member UserPermissions : UserPermissions

[<NoEquality; NoComparison>]
type DomainValuesResponse =
    { Values : DomainValue[]
      PunType : SQL.SimpleValueType
      Hash : string
    } with
        interface ILoggableResponse with
            member this.ShouldLog = false

[<SerializeAsObject("error")>]
type DomainErrorInfo =
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] DERequest of Details : string
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] DEDomain of DomainError
    with
        member this.LogMessage =
            match this with
            | DERequest msg -> msg
            | DEDomain inner -> inner.LogMessage

        [<DataMember>]
        member this.Message =
            match this with
            | DEDomain inner -> inner.Message
            | _ -> this.LogMessage

        member this.HTTPResponseCode =
            match this with
            | DERequest details -> 422
            | DEDomain inner -> inner.HTTPResponseCode

        member this.ShouldLog =
            match this with
            | DERequest details -> false
            | DEDomain inner -> inner.ShouldLog

        static member private LookupKey = prepareLookupCaseKey<DomainErrorInfo>
        member this.Error =
            match this with
            | DEDomain inner -> inner.Error
            | _ -> DomainErrorInfo.LookupKey this |> Option.get

        interface ILoggableResponse with
            member this.ShouldLog = this.ShouldLog

        interface IErrorDetails with
            member this.Message = this.Message
            member this.LogMessage = this.LogMessage
            member this.HTTPResponseCode = this.HTTPResponseCode
            member this.Error = this.Error

type DomainFlags =
    { ForceRecompile : bool
    }

let emptyDomainFlags =
    { ForceRecompile = false
    } : DomainFlags

type GetDomainValuesRequest =
    { Field : ResolvedFieldRef
      [<DataMember(EmitDefaultValue = false)>]
      Id : RawRowKey option
      [<DataMember(EmitDefaultValue = false)>]
      Chunk : SourceQueryChunk option
      [<DataMember(EmitDefaultValue = false)>]
      Flags : DomainFlags option
    }

type GetDomainExplainRequest =
    { Field : ResolvedFieldRef
      [<DataMember(EmitDefaultValue = false)>]
      Id : RawRowKey option
      [<DataMember(EmitDefaultValue = false)>]
      Chunk : SourceQueryChunk option
      [<DataMember(EmitDefaultValue = false)>]
      Flags : DomainFlags option
      [<DataMember(EmitDefaultValue = false)>]
      ExplainFlags : SQL.ExplainOptions option
    }

type IDomainsAPI =
    abstract member GetDomainValues : GetDomainValuesRequest -> Task<Result<DomainValuesResponse, DomainErrorInfo>>
    abstract member GetDomainExplain : GetDomainExplainRequest -> Task<Result<ExplainedQuery, DomainErrorInfo>>

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

type EmptyRequest =
    new () = { }

let emptyRequest = EmptyRequest()

let getReadRole = function
    | RTRoot -> None
    | RTRole role when role.CanRead -> None
    | RTRole role -> role.Role

let getWriteRole = function
    | RTRoot -> None
    | RTRole role -> Some (Option.defaultValue emptyResolvedRole role.Role)

let logAPIResponse<'Request, 'Response when 'Response :> ILoggableResponse>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (response : 'Response) =
    task {
        if response.ShouldLog then
            do! rctx.WriteEventSync (fun event ->
                event.Type <- name
                event.Request <- JsonConvert.SerializeObject request
                event.Response <- JsonConvert.SerializeObject response
            )
    }

let logAPISuccess<'Request>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request) =
    rctx.WriteEventSync (fun event ->
        event.Type <- name
        event.Request <- JsonConvert.SerializeObject request
        event.Response <- "{}"
    )

let logAPIError<'Request, 'Error when 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (error : 'Error) =
    if error.ShouldLog then
        rctx.WriteEvent (fun event ->
            let json = JObject.FromObject error
            json.["error"] <- error.LogMessage
            event.Type <- name
            event.Request <- JsonConvert.SerializeObject request
            event.Error <- json.ToString()
        )

let logAPIResult<'Request, 'Response, 'Error when 'Response :> ILoggableResponse and 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (result : Result<'Response, 'Error>) =
    unitTask {
        match result with
        | Ok response -> do! logAPIResponse rctx name request response
        | Error error -> logAPIError rctx name request error
    }

let logUnitAPIResult<'Request, 'Error when 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (result : Result<unit, 'Error>) =
    unitTask {
        match result with
        | Ok () -> do! logAPISuccess rctx name request
        | Error error -> logAPIError rctx name request error
    }


let logAPIIfError<'Request, 'Response, 'Error when 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (result : Result<'Response, 'Error>) =
    unitTask {
        match result with
        | Ok response -> ()
        | Error error -> logAPIError rctx name request error
    }

let inline wrapAPIResult<'Request, 'Response, 'Error when 'Response :> ILoggableResponse and 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (resultTask : Task<Result<'Response, 'Error>>)
        : Task<Result<'Response, 'Error>> =
    task {
        let! result = resultTask
        do! logAPIResult rctx name request result
        return result
    }

let inline wrapUnitAPIResult<'Request, 'Error when 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (resultTask : Task<Result<unit, 'Error>>)
        : Task<Result<unit, 'Error>> =
    task {
        let! result = resultTask
        do! logUnitAPIResult rctx name request result
        return result
    }

let inline wrapAPIError<'Request, 'Response, 'Error when 'Error :> IErrorDetails>
        (rctx : IRequestContext)
        (name : string)
        (request : 'Request)
        (resultTask : Task<Result<'Response, 'Error>>)
        : Task<Result<'Response, 'Error>> =
    task {
        let! result = resultTask
        do! logAPIIfError rctx name request result
        return result
    }