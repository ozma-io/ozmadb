module FunWithFlags.FunDB.API.Request

open System
open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunDBSchema.System
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.API.Types

type RequestErrorInfo =
    | REUserNotFound
    | RENoRole
    with
        member this.Message =
            match this with
            | REUserNotFound -> "User not found"
            | RENoRole -> "Access denied"

type RequestException (info : RequestErrorInfo) =
    inherit Exception(info.Message)

    member this.Info = info

[<NoEquality; NoComparison>]
type RequestParams =
    { Context : IContext
      UserName : UserName
      IsRoot : bool
      CanRead : bool
      Language : string
    }

type private RequestArguments =
    { CanRead : bool
    }

let private maxSourceDepth = 16

type private FetchedUser =
    { Id : int
      IsRoot : bool
      Role : ResolvedRoleRef option
    }

let private fetchUser (system : SystemContext) (userName : UserName) (cancellationToken : CancellationToken) : Task<FetchedUser option> =
    task {
        // FIXME: No index for this.
        let lowerUserName = userName.ToLower()
        let! user =
            system.Users
                .Include("Role")
                .Include("Role.Schema")
                .Where(fun x -> x.Name.ToLower() = lowerUserName && x.IsEnabled)
                .FirstOrDefaultAsync(cancellationToken)
        if isNull user then
            return None
        else
            let role =
                if isNull user.Role then
                    None
                else
                    Some { Schema = FunQLName user.Role.Schema.Name; Name = FunQLName user.Role.Name }
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
            [ (FunQLName "lang", FString user.Language)
              (FunQLName "user", FString user.Effective.Name)
              (FunQLName "user_id", userIdValue)
              (FunQLName "transaction_time", FDateTime ctx.TransactionTime)
              (FunQLName "transaction_id", FInt ctx.TransactionId)
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
                TransactionTimestamp = ctx.TransactionTime.ToDateTime(),
                TransactionId = ctx.TransactionId,
                Timestamp = DateTime.UtcNow,
                UserName = currentUser.Saved.Name,
                Source = JsonConvert.SerializeObject(source)
            )
        addDetails event
        event

    let checkRoot () =
        match currentUser.Saved.Type with
        | RTRoot -> ()
        | _ ->
            logger.LogError("This feature is only available for root users, not for {}", currentUser.Effective.Name)
            raise <| RequestException RENoRole

    static member Create (opts : RequestParams) : Task<RequestContext> =
        task {
            let ctx = opts.Context
            let logger = ctx.LoggerFactory.CreateLogger<RequestContext>()
            let! maybeUser = fetchUser ctx.Transaction.System opts.UserName ctx.CancellationToken
            let roleType =
                if opts.IsRoot then
                    RTRoot
                else
                    match maybeUser with
                    | None when opts.CanRead -> RTRole { Role = None; CanRead = true }
                    | None ->
                        logger.LogError("User {} not found in users table", opts.UserName)
                        raise <| RequestException REUserNotFound
                    | Some user when user.IsRoot -> RTRoot
                    | Some user ->
                        match user.Role with
                        | None ->
                            if opts.CanRead then
                                RTRole { Role = None; CanRead = true }
                            else
                                logger.LogError("User {} has no role set", opts.UserName)
                                raise <| RequestException RENoRole
                        | Some roleRef ->
                            match ctx.Permissions.Find roleRef |> Option.get with
                            | Ok role -> RTRole { Role = Some role; CanRead = opts.CanRead }
                            | Error _ when opts.CanRead -> RTRole { Role = None; CanRead = true }
                            | Error e ->
                                logger.LogError(e, "Role for user {} is broken", opts.UserName)
                                raise <| RequestException RENoRole
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
                }
            return RequestContext(ctx, userInfo, args, logger)
        }

    member this.User = currentUser
    member this.Context = ctx
    member this.GlobalArguments = globalArguments

    member this.WriteEvent (addDetails : EventEntry -> unit) =
        ctx.WriteEvent (makeEvent addDetails)

    member this.WriteEventSync (addDetails : EventEntry -> unit) =
        ignore <| ctx.Transaction.System.Events.Add(makeEvent addDetails)

    member this.RunWithSource (newSource : EventSource) (func : unit -> Task<'a>) : Task<'a> =
        task {
            if sourceDepth >= maxSourceDepth then
                raisef StackOverflowException "Stack depth exceeded"
            let oldSource = source
            source <- newSource
            sourceDepth <- sourceDepth + 1
            try
                return! func ()
            finally
                sourceDepth <- sourceDepth - 1
                source <- oldSource
        }

    member this.PretendUser (userName : UserName) (func : unit -> Task<'a>) : Task<'a> =
        task {
            checkRoot ()
            let! maybeUser = fetchUser ctx.Transaction.System userName ctx.CancellationToken
            let roleType =
                match maybeUser with
                    | None ->
                        logger.LogError("User {} not found in users table", userName)
                        raise <| RequestException REUserNotFound
                    | Some user when user.IsRoot -> RTRoot
                        | Some user ->
                            match user.Role with
                            | None ->
                                if opts.CanRead then
                                    RTRole { Role = None; CanRead = true }
                                else
                                    logger.LogError("User {} has no role set", userName)
                                    raise <| RequestException RENoRole
                            | Some roleRef ->
                                match ctx.Permissions.Find roleRef |> Option.get with
                                | Ok role -> RTRole { Role = Some role; CanRead = opts.CanRead }
                                | Error e ->
                                    logger.LogError(e, "Role for user {} is broken", userName)
                                    raise <| RequestException RENoRole
            let newUser =
                { Id = maybeUser |> Option.map (fun u -> u.Id)
                  Name = userName
                  Type = roleType
                }
            return! setCurrentUser newUser func
        }

    member this.PretendRole (newRole : ResolvedEntityRef) (func : unit -> Task<'a>) : Task<'a> =
        checkRoot ()
        let effectiveRole =
            match ctx.Permissions.Find newRole with
            | Some (Ok role) -> RTRole { Role = Some role; CanRead = opts.CanRead }
            | Some (Error e) ->
                logger.LogError(e, "Role {} is broken", newRole)
                raise <| RequestException RENoRole
            | None ->
                raise <| RequestException RENoRole
        let newUser = { currentUser.Effective with Type = effectiveRole }
        setCurrentUser newUser func

    interface IRequestContext with
        member this.User = currentUser
        member this.Context = ctx
        member this.GlobalArguments = globalArguments
        member this.Source = source

        member this.WriteEvent addDetails = this.WriteEvent addDetails
        member this.WriteEventSync addDetails = this.WriteEventSync addDetails
        member this.RunWithSource newSource func = this.RunWithSource newSource func
        member this.PretendRole newRole func = this.PretendRole newRole func
        member this.PretendUser newUserName func = this.PretendUser newUserName func