module FunWithFlags.FunDB.Operations.Command

open System.Threading
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine
open System.Runtime.Serialization

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

[<SerializeAsObject("error")>]
type CommandError =
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] CEExecution of FunQLExecutionError
    | [<CaseKey("request", IgnoreFields=[|"Details"|])>] CERequest of Details : string
    | [<CaseKey("accessDenied", IgnoreFields=[|"Details"|])>] CEAccessDenied of Details : string
    | [<CaseKey("other", IgnoreFields=[|"Details"|])>] CEOther of Details : string
    with
    member this.LogMessage =
        match this with
        | CEExecution e -> e.LogMessage
        | CERequest msg -> msg
        | CEAccessDenied details -> details
        | CEOther details -> details

    [<DataMember>]
    member this.Message =
        match this with
        | CEExecution e -> e.Message
        | CEAccessDenied details -> "Access denied"
        | _ -> this.LogMessage

    member this.HTTPResponseCode =
        match this with
        | CEExecution e -> e.HTTPResponseCode
        | CERequest details -> 422
        | CEAccessDenied details -> 403
        | CEOther details -> 500

    member this.ShouldLog =
        match this with
        | CEExecution e -> e.ShouldLog
        | CERequest details -> false
        | CEAccessDenied details -> true
        | CEOther details -> false

    static member private LookupKey = prepareLookupCaseKey<CommandError>
    member this.Error =
        match this with
        | CEExecution e -> e.Error
        | _ -> CommandError.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog

    interface IErrorDetails with
        member this.Message = this.Message
        member this.LogMessage = this.LogMessage
        member this.HTTPResponseCode = this.HTTPResponseCode
        member this.Error = this.Error

type CommandException (details : CommandError, innerException : exn) =
    inherit UserException(details.Message, innerException, true)
    new (details : CommandError) = CommandException (details, null)

    member this.Details = details

let resolveCommand (layout : Layout) (isPrivileged : bool) (rawCommand : string) : ResolvedCommandExpr =
    let parsed =
        match parse tokenizeFunQL commandExpr rawCommand with
        | Error msg -> raise <| CommandException (CERequest <| sprintf "Parse error: %s" msg)
        | Ok rawExpr -> rawExpr
    let callbacks = resolveCallbacks layout
    let resolved =
        try
            resolveCommandExpr callbacks { emptyExprResolutionFlags with Privileged = isPrivileged } parsed
        with
        | :? QueryResolveException as e when e.IsUserException ->
            raise <| CommandException (CERequest (fullUserMessage e), e)
    resolved

type private TriggerType =
    | TTInsert
    | TTUpdate of Set<FieldName>
    | TTDelete

// TODO: Not complete: doesn't look for nested DML statements.
// FunQL, though, doesn't support them as of now.
let private getTriggeredEntities = function
    | SQL.DESelect sel -> Seq.empty
    | SQL.DEInsert insert ->
        let tableInfo = insert.Table.Extra :?> RealEntityAnnotation
        Seq.singleton (TTInsert, tableInfo.RealEntity)
    | SQL.DEUpdate update ->
        let tableInfo = update.Table.Extra :?> RealEntityAnnotation

        let getField (name : SQL.UpdateColumnName) =
            let ann = name.Extra :?> RealFieldAnnotation
            ann.Name

        let getFields = function
            | SQL.UAESet (name, expr) -> Seq.singleton <| getField name
            | SQL.UAESelect (cols, select) -> Seq.map getField cols

        let fields = update.Assignments |> Seq.collect getFields |> Set.ofSeq
        Seq.singleton (TTUpdate fields, tableInfo.RealEntity)
    | SQL.DEDelete delete ->
        let tableInfo = delete.Table.Extra :?> RealEntityAnnotation
        Seq.singleton (TTDelete, tableInfo.RealEntity)

let private hasTriggers (typ : TriggerType) (triggers : MergedTriggersTime) =
    match typ with
    | TTInsert -> not <| Array.isEmpty triggers.OnInsert
    | TTUpdate fields ->
        let inline checkFields () = Seq.exists (fun field -> Map.containsKey (MUFField field) triggers.OnUpdateFields) fields
        not (Set.isEmpty fields) && (Map.containsKey MUFAll triggers.OnUpdateFields || checkFields ())
    | TTDelete -> not <| Array.isEmpty triggers.OnDelete

let executeCommand
        (connection : QueryConnection)
        (triggers : MergedTriggers)
        (globalArgs : LocalArgumentsMap)
        (layout : Layout)
        (applyRole : ResolvedRole option)
        (cmdExpr : CompiledCommandExpr)
        (comments : string option)
        (rawArgs : RawArguments)
        (cancellationToken : CancellationToken) : Task =
    unitTask {
        for (typ, entityRef) in getTriggeredEntities cmdExpr.Command.Expression do
            match triggers.FindEntity entityRef with
            | Some entityTriggers when hasTriggers typ entityTriggers.Before || hasTriggers typ entityTriggers.After ->
                raise <| CommandException(CEOther "Mass modification on entities with triggers is not supported")
            | _ -> ()

        let arguments =
            try
                convertQueryArguments globalArgs Map.empty rawArgs cmdExpr.Command.Arguments
            with
            | :? ArgumentCheckException as e ->
                raise <| CommandException (CEExecution (UVEArgument e.Details), e)
        let query =
            match applyRole with
            | None -> cmdExpr.Command
            | Some role ->
                let appliedDb =
                    try
                        applyPermissions layout role cmdExpr.UsedDatabase
                    with
                    | :? PermissionsApplyException as e when e.IsUserException ->
                        raise <| CommandException (CEAccessDenied (fullUserMessage e), e)
                for KeyValue(rootRef, allowedEntity) in appliedDb do
                    match allowedEntity.Check with
                    | Some FUnfiltered
                    | None -> ()
                    | Some (FFiltered filter) ->
                        raise <| CommandException(CEOther "Check restrictions are not supported for commands")
                applyRoleDataExpr layout appliedDb cmdExpr.Command

        let prefix = SQL.convertComments comments
        try
            do! setPragmas connection cmdExpr.Pragmas cancellationToken
            let! affected = connection.ExecuteNonQuery (prefix + SQL.toSQLString query.Expression) (prepareArguments cmdExpr.Command.Arguments arguments) cancellationToken
            do! unsetPragmas connection cmdExpr.Pragmas cancellationToken
            ()
        with
        | :? QueryExecutionException as e ->
            return raise <| CommandException(CEExecution <| convertQueryExecutionException layout e)
        ()
    }
