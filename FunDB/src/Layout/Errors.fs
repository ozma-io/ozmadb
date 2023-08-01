module FunWithFlags.FunDB.Layout.Errors

open Npgsql
open System.Runtime.Serialization

open FunWithFlags.FunUtils.Serialization
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.AST

[<SerializeAsObject("error")>]
type IntegrityQueryError =
    | [<CaseKey("foreignKey")>] LQEForeignKey of Field : ResolvedFieldRef
    with
    member this.LogMessage =
        match this with
        | LQEForeignKey ref -> sprintf "Foreign key %O is violated" ref

    [<DataMember>]
    member this.Message = this.LogMessage

    member this.HTTPResponseCode = 422

    member this.ShouldLog = false

    static member private LookupKey = prepareLookupCaseKey<IntegrityQueryError>
    member this.Error =
        IntegrityQueryError.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog

    interface IErrorDetails with
        member this.LogMessage = this.LogMessage
        member this.Message = this.Message
        member this.HTTPResponseCode = this.HTTPResponseCode
        member this.Error = this.Error

let extractIntegrityQueryError (layout : Layout) (e : PostgresException) : IntegrityQueryError option =
    match e.SqlState with
    | "23503" ->
        match Map.tryFind (FunQLName e.SchemaName) layout.Schemas with
        | None -> None
        | Some schema ->
            match Map.tryFind (SQL.SQLName e.ConstraintName) schema.ForeignConstraintNames with
            | None -> None
            | Some ref -> Some <| LQEForeignKey ref
    | _ -> None