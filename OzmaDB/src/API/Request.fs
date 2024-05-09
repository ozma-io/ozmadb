module OzmaDB.API.Request

open NodaTime
open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.Affine

open OzmaDBSchema.System
open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Permissions.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.API.Types

type DatabaseAccessDeniedException (message : string, innerException : exn) =
    inherit UserException(message, innerException, true)

    new (message : string) = DatabaseAccessDeniedException (message, null)

type RequestStackOverflowException (source : EventSource, innerException : exn) =
    inherit UserException("Request stack overflow", innerException, true)

    new (source : EventSource) = RequestStackOverflowException (source, null)

    member this.Source = source

[<NoEquality; NoComparison>]
type RequestParams =
    { Context : IContext
      UserName : UserName
      IsRoot : bool
      CanRead : bool
      Language : string
      Quota : RequestQuota
    }

type private RequestArguments =
    { CanRead : bool
      Quota : RequestQuota
    }

let private maxSourceDepth = 16

type private FetchedUser =
    { Id : int
      IsRoot : bool
      Role : ResolvedRoleRef option
    }

let private fetchUser (system : SystemContext) (userName : UserName) (allowDisabled : bool) (cancellationToken : CancellationToken) : Task<FetchedUser option> =
    task {
        let lowerUserName = userName.ToLowerInvariant()
        let userQuery =
            system.Users
                .Include("Role")
                .Include("Role.Schema")
                .Where(fun x -> x.Name.ToLower() = lowerUserName)
        let userQuery =
            if allowDisabled then
                userQuery
            else
                userQuery.Where(fun x -> x.IsEnabled)
        let! user = userQuery.FirstOrDefaultAsync(cancellationToken)
        if isNull user then
            return None
        else
            let role =
                if isNull user.Role then
                    None
                else
                    Some { Schema = OzmaQLName user.Role.Schema.Name; Name = OzmaQLName user.Role.Name }
            let ret =
                { Id = user.Id
                  IsRoot = user.IsRoot
                  Role = role
                }
            return Some ret
    }

type RequestContext private (ctx : IContext, initialUserInfo : RequestUserInfo, opts : RequestArguments, logger : ILogger) =
    let getGlobalArguments (user : RequestUserInfo) =
        let userIdValue =
            match user.Effective.Id with
            | None -> FNull
            | Some id -> FInt id
        // Should be in sync with globalArgumentTypes
        let globalArguments =
            [ (OzmaQLName "lang", FString user.Language)
              (OzmaQLName "user", FString user.Effective.Name)
              (OzmaQLName "user_id", userIdValue)
              (OzmaQLName "transaction_time", FDateTime ctx.TransactionTime)
              (OzmaQLName "transaction_id", FInt ctx.TransactionId)
            ] |> Map.ofList
        assert (Map.keysSet globalArgumentTypes = Map.keysSet globalArguments)
        globalArguments

    let mutable currentUser = initialUserInfo
    let mutable globalArguments = getGlobalArguments initialUserInfo

    let setCurrentUser (newUser : RequestUser) (f : unit -> Task<'a>) : Task<'a> =
        task {
            let oldUser = currentUser
            let oldArguments = globalArguments
            currentUser <- { currentUser with Effective = newUser }
            globalArguments <- getGlobalArguments currentUser
            try
                return! f ()
            finally
                globalArguments <- oldArguments
                currentUser <- oldUser
        }

    let mutable source = ESAPI
    let mutable sourceDepth = 0

    let makeEvent (addDetails : EventEntry -> unit) =
        let event =
            EventEntry (
                TransactionTimestamp = ctx.TransactionTime,
                TransactionId = ctx.TransactionId,
                Timestamp = SystemClock.Instance.GetCurrentInstant(),
                UserName = currentUser.Saved.Name,
                Source = JsonConvert.SerializeObject(source)
            )
        addDetails event
        event

    let isPrivileged () =
        match currentUser.Saved.Type with
        | RTRoot -> true
        | _ ->
            match source with
            | ESAction _ | ESTrigger _ -> true
            | ESAPI -> false

    static member Create (opts : RequestParams) : Task<RequestContext> =
        task {
            let ctx = opts.Context
            let logger = ctx.LoggerFactory.CreateLogger<RequestContext>()
            let! maybeUser = fetchUser ctx.Transaction.System opts.UserName false ctx.CancellationToken
            let roleType =
                if opts.IsRoot then
                    RTRoot
                else
                    match maybeUser with
                    | None when opts.CanRead -> RTRole { Role = None; Ref = None; CanRead = true }
                    | None ->
                        logger.LogError("User {user} was not found in the users table", opts.UserName)
                        raisef DatabaseAccessDeniedException ""
                    | Some user when user.IsRoot -> RTRoot
                    | Some user ->
                        match user.Role with
                        | None ->
                            if opts.CanRead then
                                RTRole { Role = None; Ref = None; CanRead = true }
                            else
                                logger.LogError("User {user} has no role set", opts.UserName)
                                raisef DatabaseAccessDeniedException ""
                        | Some roleRef ->
                            match ctx.Permissions.Find roleRef |> Option.get with
                            | Ok role -> RTRole { Role = Some role; Ref = Some roleRef; CanRead = opts.CanRead }
                            | Error _ when opts.CanRead -> RTRole { Role = None; Ref = Some roleRef; CanRead = true }
                            | Error e ->
                                logger.LogError(e.Error, "Role for user {user} is broken", opts.UserName)
                                raisef DatabaseAccessDeniedException ""
            let userId = maybeUser |> Option.map (fun u -> u.Id)
            let initialUser =
                { Id = userId
                  Type = roleType
                  Name = opts.UserName
                }
            let userInfo =
                { Saved = initialUser
                  Effective = initialUser
                  Language = opts.Language
                }
            let args =
                { CanRead = opts.CanRead
                  Quota = opts.Quota
                }
            return RequestContext(ctx, userInfo, args, logger)
        }

    member this.User = currentUser
    member this.Context = ctx
    member this.GlobalArguments = globalArguments

    member this.WriteEvent (addDetails : EventEntry -> unit) =
        ctx.WriteEvent (makeEvent addDetails)

    member this.WriteEventSync (addDetails : EventEntry -> unit) : Task =
        ignore <| ctx.Transaction.System.Events.Add(makeEvent addDetails)
        ctx.Transaction.SystemSaveChangesAsync(this.Context.CancellationToken) :> Task

    member this.RunWithSource (newSource : EventSource) (func : unit -> Task<'a>) : Task<'a> =
        task {
            if sourceDepth >= maxSourceDepth then
                raise <| RequestStackOverflowException(newSource)
            let oldSource = source
            source <- newSource
            sourceDepth <- sourceDepth + 1
            logger.LogInformation("Entering {source}, depth {depth}", newSource, sourceDepth)
            try
                try
                    return! func ()
                with
                | :? RequestStackOverflowException as e ->
                    return raise <| RequestStackOverflowException(oldSource, e)
            finally
                sourceDepth <- sourceDepth - 1
                source <- oldSource
                logger.LogInformation("Leaving {source}", newSource)
        }

    member this.PretendUser (req : PretendUserRequest) (func : unit -> Task<'a>) : Task<Result<'a, PretendErrorInfo>> =
        task {
            if not <| isPrivileged () then
                return Error PEAccessDenied
            else
                let! maybeUser = fetchUser ctx.Transaction.System req.AsUser true ctx.CancellationToken
                let maybeRoleType =
                    match maybeUser with
                        | None -> Error PEUserNotFound
                        | Some user when user.IsRoot -> Ok RTRoot
                        | Some user ->
                            match user.Role with
                            | None ->
                                if opts.CanRead then
                                    Ok <| RTRole { Role = None; Ref = None; CanRead = true }
                                else
                                    Error PENoUserRole
                            | Some roleRef ->
                                match ctx.Permissions.Find roleRef |> Option.get with
                                | Ok role -> Ok <| RTRole { Role = Some role; Ref = Some roleRef; CanRead = opts.CanRead }
                                | Error e ->
                                    logger.LogError(e.Error, "Role for user {user} is broken", req.AsUser)
                                    Error PENoUserRole
                match maybeRoleType with
                | Error e -> return Error e
                | Ok roleType ->
                    let user = Option.get maybeUser
                    let newUser =
                        { Id = Some user.Id
                          Name = req.AsUser
                          Type = roleType
                        }
                    let! ret = setCurrentUser newUser func
                    return Ok ret
        }

    member this.PretendRole (req : PretendRoleRequest) (func : unit -> Task<'a>) : Task<Result<'a, PretendErrorInfo>> =
        task {
            if not <| isPrivileged () then
                return Error PEAccessDenied
            else
                let maybeEffectiveRole =
                    match req.AsRole with
                    | PRRoot -> Ok RTRoot
                    | PRRole roleRef ->
                        match ctx.Permissions.Find roleRef with
                        | None -> Error PENoUserRole
                        | Some (Ok role) -> Ok (RTRole { Role = Some role; Ref = Some roleRef; CanRead = opts.CanRead })
                        | Some (Error e) ->
                            logger.LogError(e.Error, "Role {role} is broken", roleRef)
                            Error PENoUserRole
                match maybeEffectiveRole with
                | Error e -> return Error e
                | Ok effectiveRole ->
                    let newUser = { currentUser.Effective with Type = effectiveRole }
                    let! ret = setCurrentUser newUser func
                    return Ok ret
        }

    member this.IsPrivileged =
        match source with
        | ESAPI -> false
        | ESTrigger _ -> true
        | ESAction _ -> true

    interface IRequestContext with
        member this.User = currentUser
        member this.Context = ctx
        member this.GlobalArguments = globalArguments
        member this.Source = source
        member this.Quota = opts.Quota

        member this.WriteEvent addDetails = this.WriteEvent addDetails
        member this.WriteEventSync addDetails = this.WriteEventSync addDetails
        member this.RunWithSource newSource func = this.RunWithSource newSource func
        member this.PretendRole req func = this.PretendRole req func
        member this.PretendUser req func = this.PretendUser req func
        member this.IsPrivileged = this.IsPrivileged
