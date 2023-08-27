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
    | [<CaseKey("checkConstraint")>] LQECheckConstraint of Constraint : ResolvedConstraintRef
    | [<CaseKey("uniqueConstraint")>] LQEUniqueConstraint of Constraint : ResolvedConstraintRef
    with
    member this.LogMessage =
        match this with
        | LQEForeignKey ref -> sprintf "Foreign key %O is violated" ref
        | LQECheckConstraint ref -> sprintf "Check constraint %O is violated" ref
        | LQEUniqueConstraint ref -> sprintf "Unique constraint %O is violated" ref

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

let inline private findNameInLayout
        (layout : Layout)
        (schemaName : string)
        (objectName : string)
        ([<InlineIfLambda>] getSchemaObjects : ResolvedSchema -> Map<SQL.SQLName, 'a>)
        ([<InlineIfLambda>] convert : 'a -> 'b) : 'b option =
    match Map.tryFind (FunQLName schemaName) layout.Schemas with
    | None -> None
    | Some schema ->
        match Map.tryFind (SQL.SQLName objectName) (getSchemaObjects schema) with
        | None -> None
        | Some object -> Some <| convert object

let extractIntegrityQueryError (layout : Layout) (e : PostgresException) : IntegrityQueryError option =
    match e.SqlState with
    | "23503" ->
        findNameInLayout
            layout
            e.SchemaName
            e.ConstraintName
            (fun schema -> schema.ForeignConstraintNames)
            LQEForeignKey
    | "23514" ->
        findNameInLayout
            layout
            e.SchemaName
            e.ConstraintName
            (fun schema -> schema.CheckConstraintNames)
            LQECheckConstraint
    | "23505" ->
        findNameInLayout
            layout
            e.SchemaName
            e.ConstraintName
            (fun schema -> schema.UniqueConstraintNames)
            LQEUniqueConstraint
    | _ -> None