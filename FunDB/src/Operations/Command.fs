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

type CommandResolveException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        CommandResolveException (message, innerException, isUserException innerException)

    new (message : string) = CommandResolveException (message, null, true)

type CommandArgumentsException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        CommandArgumentsException (message, innerException, isUserException innerException)

    new (message : string) = CommandArgumentsException (message, null, true)

type CommandDeniedException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        CommandDeniedException (message, innerException, isUserException innerException)

    new (message : string) = CommandDeniedException (message, null, true)

type CommandExecutionException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        CommandExecutionException (message, innerException, isUserException innerException)

    new (message : string) = CommandExecutionException (message, null, true)

let resolveCommand (layout : Layout) (isPrivileged : bool) (rawCommand : string) : ResolvedCommandExpr =
    let parsed =
        match parse tokenizeFunQL commandExpr rawCommand with
        | Error msg -> raisef CommandResolveException "Parse error: %s" msg
        | Ok rawExpr -> rawExpr
    let callbacks = resolveCallbacks layout
    let resolved =
        try
            resolveCommandExpr callbacks { emptyExprResolutionFlags with Privileged = isPrivileged } parsed
        with
        | :? ViewResolveException as e -> raisefWithInner CommandResolveException e "Resolve error"
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
                raisef CommandExecutionException "Mass modification on entities with triggers is not supported"
            | _ -> ()

        let arguments =
            try
                convertQueryArguments globalArgs Map.empty rawArgs cmdExpr.Command.Arguments
            with
            | :? ArgumentCheckException as e ->
                raisefUserWithInner CommandArgumentsException e ""
        let query =
            match applyRole with
            | None -> cmdExpr.Command
            | Some role ->
                let appliedDb =
                    try
                        applyPermissions layout role cmdExpr.UsedDatabase
                    with
                    | :? PermissionsApplyException as e -> raisefWithInner CommandDeniedException e ""
                for KeyValue(rootRef, allowedEntity) in appliedDb do
                    match allowedEntity.Check with
                    | Some FUnfiltered
                    | None -> ()
                    | Some (FFiltered filter) -> raisef CommandExecutionException "Check restrictions are not supported for commands"
                applyRoleDataExpr layout appliedDb cmdExpr.Command

        let prefix = SQL.convertComments comments
        try
            do! setPragmas connection cmdExpr.Pragmas cancellationToken
            let! affected = connection.ExecuteNonQuery (prefix + SQL.toSQLString query.Expression) (prepareArguments cmdExpr.Command.Arguments arguments) cancellationToken
            do! unsetPragmas connection cmdExpr.Pragmas cancellationToken
            ()
        with
        | :? QueryExecutionException as e -> raisefUserWithInner CommandExecutionException e ""
        ()
    }
