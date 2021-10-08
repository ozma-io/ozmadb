module FunWithFlags.FunDB.Operations.Command

open System.Threading
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
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

type CommandResolveException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        CommandResolveException (message, innerException, isUserException innerException)

    new (message : string) = CommandResolveException (message, null, true)

type CommandArgumentsException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        CommandArgumentsException (message, innerException, isUserException innerException)

    new (message : string) = CommandArgumentsException (message, null, true)

type CommandDeniedException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        CommandDeniedException (message, innerException, isUserException innerException)

    new (message : string) = CommandDeniedException (message, null, true)

type CommandExecutionException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        CommandExecutionException (message, innerException, isUserException innerException)

    new (message : string) = CommandExecutionException (message, null, true)

let resolveCommand (layout : Layout) (isPrivileged : bool) (rawCommand : string) : ResolvedCommandExpr =
    let parsed =
        match parse tokenizeFunQL commandExpr rawCommand with
        | Error msg -> raisef CommandResolveException "Parse error: %s" msg
        | Ok rawExpr -> rawExpr
    let resolved =
        try
            resolveCommandExpr layout { emptyExprResolutionFlags with Privileged = isPrivileged } parsed
        with
        | :? ViewResolveException as e -> raisefWithInner CommandResolveException e "Resolve error"
    resolved

type private TriggerType =
    | TTInsert
    | TTUpdate of Set<FieldName>
    | TTDelete

let private getTriggeredEntity = function
    | SQL.DESelect sel -> None
    | SQL.DEInsert insert ->
        let tableInfo = insert.Table.Extra :?> RealEntityAnnotation
        Some (TTInsert, tableInfo.RealEntity)
    | SQL.DEUpdate update ->
        let tableInfo = update.Table.Extra :?> RealEntityAnnotation
        let fields = update.Columns |> Map.values |> Seq.map (fun (extra, col) -> (extra :?> RealFieldAnnotation).Name) |> Set.ofSeq
        Some (TTUpdate fields, tableInfo.RealEntity)
    | SQL.DEDelete delete ->
        let tableInfo = delete.Table.Extra :?> RealEntityAnnotation
        Some (TTDelete, tableInfo.RealEntity)

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
        (role : ResolvedRole option)
        (cmdExpr : CompiledCommandExpr)
        (comments : string option)
        (rawArgs : RawArguments)
        (cancellationToken : CancellationToken) : Task =
    unitTask {
        let triggersATrigger =
            match getTriggeredEntity cmdExpr.Command.Expression with
            | None -> false
            | Some (typ, entityRef) ->
                match triggers.FindEntity entityRef with
                | None -> false
                | Some entityTriggers -> hasTriggers typ entityTriggers.Before || hasTriggers typ entityTriggers.After
        if triggersATrigger then
            raisef CommandExecutionException "Mass modification on entities with triggers is not supported"

        let arguments =
            try
                convertQueryArguments globalArgs Map.empty rawArgs cmdExpr.Command.Arguments
            with
            | :? ArgumentCheckException as e ->
                raisefUserWithInner CommandArgumentsException e ""
        let query =
            match role with
            | None -> cmdExpr.Command
            | Some role ->
                let appliedDb =
                    try
                        applyPermissions layout role cmdExpr.UsedDatabase
                    with
                    | :? PermissionsApplyException as e -> raisefWithInner CommandDeniedException e ""
                applyRoleDataExpr layout appliedDb cmdExpr.Command

        let prefix = SQL.convertComments comments
        try
            do! setPragmas connection cmdExpr.Pragmas cancellationToken
            let! affected = connection.ExecuteNonQuery (prefix + SQL.toSQLString query.Expression) (prepareArguments cmdExpr.Command.Arguments arguments) cancellationToken
            do! unsetPragmas connection cmdExpr.Pragmas cancellationToken
            ()
        with
        | :? QueryException as e -> raisefUserWithInner CommandExecutionException e ""
        ()
    }
