module OzmaDB.Layout.Errors

open Npgsql
open System.Runtime.Serialization

open OzmaDB.OzmaUtils.Serialization
open OzmaDB.OzmaQL.AST
open OzmaDB.Exception
open OzmaDB.Layout.Types
module SQL = OzmaDB.SQL.AST

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
    member this.Details = Map.empty

    static member private LookupKey = prepareLookupCaseKey<IntegrityQueryError>
    member this.Error =
        IntegrityQueryError.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details

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
    match Map.tryFind (OzmaQLName schemaName) layout.Schemas with
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