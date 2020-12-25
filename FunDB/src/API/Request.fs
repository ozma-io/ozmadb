module FunWithFlags.FunDB.API.Request

open System
open System.Threading
open System.Threading.Tasks
open NpgsqlTypes
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json
open Newtonsoft.Json.Linq
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
      Source : EventSource
    }

let private maxSourceDepth = 16

type RequestContext private (opts : RequestParams, userId : int option, roleType : RoleType) =
    let ctx = opts.Context
    let userIdValue =
        match userId with
        | None -> FNull
        | Some id -> FInt id
    // Should be in sync with globalArgumentTypes
    let globalArguments =
        [ (FunQLName "lang", FString opts.Language)
          (FunQLName "user", FString opts.UserName)
          (FunQLName "user_id", userIdValue)
          (FunQLName "transaction_time", FDateTime ctx.TransactionTime)
          (FunQLName "transaction_id", FInt ctx.TransactionId)
        ] |> Map.ofList
    do
        assert (Map.keysSet globalArgumentTypes = Map.keysSet globalArguments)

    let user =
        { Type = roleType
          Name = opts.UserName
          Language = opts.Language
        }

    let mutable source = opts.Source
    let mutable sourceDepth = 0

    let makeEvent (addDetails : EventEntry -> unit) =
        let event =
            EventEntry (
                TransactionTimestamp = ctx.TransactionTime.ToDateTime(),
                TransactionId = ctx.TransactionId,
                Timestamp = DateTime.UtcNow,
                UserName = opts.UserName,
                Source = JsonConvert.SerializeObject(source)
            )
        addDetails event
        event

    static member Create (opts : RequestParams) : Task<RequestContext> =
        task {
            let ctx = opts.Context
            let lowerUserName = opts.UserName.ToLowerInvariant()
            // FIXME: SLOW!
            let! rawUsers =
                ctx.Transaction.System.Users
                    .Include("Role")
                    .Include("Role.Schema")
                    .ToListAsync(ctx.CancellationToken)
            let rawUser = rawUsers |> Seq.filter (fun user -> user.Name.ToLowerInvariant() = lowerUserName) |> Seq.first
            let userId = rawUser |> Option.map (fun u -> u.Id)
            let roleType =
                if opts.IsRoot then
                    RTRoot
                else
                    match rawUser with
                    | None -> raise <| RequestException REUserNotFound
                    | Some user when user.IsRoot -> RTRoot
                    | Some user when isNull user.Role ->
                        if opts.CanRead then
                            RTRole { Role = None; CanRead = true }
                        else
                            raise <| RequestException RENoRole
                    | Some user ->
                        match ctx.Permissions.Find { schema = FunQLName user.Role.Schema.Name; name = FunQLName user.Role.Name } |> Option.get with
                        | Ok role -> RTRole { Role = Some role; CanRead = opts.CanRead }
                        | Error _ when opts.CanRead -> RTRole { Role = None; CanRead = true }
                        | Error e -> raise <| RequestException RENoRole
            return RequestContext(opts, userId, roleType)
        }

    member this.User = user
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

    interface IRequestContext with
        member this.User = user
        member this.Context = ctx
        member this.GlobalArguments = globalArguments
        member this.Source = source

        member this.WriteEvent addDetails = this.WriteEvent addDetails
        member this.WriteEventSync addDetails = this.WriteEventSync addDetails
        member this.RunWithSource newSource func = this.RunWithSource newSource func