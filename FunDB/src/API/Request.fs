module FunWithFlags.FunDB.API.Request

open System
open System.Threading
open System.Threading.Tasks
open NpgsqlTypes
open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq
open FSharp.Control.Tasks.V2.ContextInsensitive

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
      Language : string
    }

type RequestContext private (opts : RequestParams, userId : int option, roleType : RoleType) =
    let ctx = opts.Context
    let userIdValue =
        match userId with
        | None -> FNull
        | Some id -> FInt id
    let globalArguments =
        [ (FunQLName "lang", FString opts.Language)
          (FunQLName "user", FString opts.UserName)
          (FunQLName "user_id", userIdValue)
          (FunQLName "transaction_time", FDateTime <| NpgsqlDateTime ctx.TransactionTime)
        ] |> Map.ofList
    do
        assert (globalArgumentTypes |> Map.toSeq |> Seq.forall (fun (name, _) -> Map.containsKey name globalArguments))

    let user =
        { Type = roleType
          Name = opts.UserName
          Language = opts.Language
        }

    let makeEvent (addDetails : EventEntry -> unit) =
        let event =
            EventEntry (
                TransactionTimestamp = ctx.TransactionTime,
                Timestamp = DateTime.UtcNow,
                UserName = opts.UserName
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
                    | Some user when isNull user.Role -> raise <| RequestException RENoRole
                    | Some user ->
                        match ctx.State.Permissions.Find { schema = FunQLName user.Role.Schema.Name; name = FunQLName user.Role.Name } |> Option.get with
                        | Ok role -> RTRole role
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

    interface IRequestContext with
        member this.User = user
        member this.Context = ctx
        member this.GlobalArguments = globalArguments

        member this.WriteEvent addDetails = this.WriteEvent addDetails
        member this.WriteEventSync addDetails = this.WriteEventSync addDetails