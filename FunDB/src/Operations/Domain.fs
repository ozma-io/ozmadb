module FunWithFlags.FunDB.Operations.Domain

open System.Threading
open System.Runtime.Serialization
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

type DomainExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = DomainExecutionException (message, null)

type DomainDeniedException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = DomainDeniedException (message, null)

let private findDomainColumn = function
    | col when col = domainValueName -> Some sqlDomainValueName
    | col when col = domainPunName -> Some sqlDomainPunName
    | _ -> None

type DomainValue =
    { Value : SQL.Value
      [<DataMember(EmitDefaultValue = false)>]
      Pun : SQL.Value option
    }

let private convertDomainValue (values : SQL.Value[]) =
    assert (Array.length values = 2)
    { Value = values.[0]
      Pun = Some values.[1]
    }

let getDomainValues (connection : QueryConnection) (layout : Layout) (domain : DomainExpr) (role : ResolvedRole option) (arguments : ArgumentValuesMap) (chunk : SourceQueryChunk) (cancellationToken : CancellationToken) : Task<DomainValue[]> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                try
                    applyRoleQueryExpr layout r domain.UsedSchemas domain.Query
                with
                | :? PermissionsViewException as e -> raisefWithInner DomainDeniedException e.InnerException "%s" e.Message
        let resolvedChunk = genericResolveChunk findDomainColumn chunk
        let (argValues, query) = queryExprChunk resolvedChunk query

        try
            let arguments = Map.union arguments (Map.mapKeys PLocal argValues)
            return! connection.ExecuteQuery (query.Expression.ToSQLString()) (prepareArguments query.Arguments arguments) cancellationToken <| fun result ->
                result.Rows |> Seq.map convertDomainValue |> Seq.toArray |> Task.result
        with
            | :? QueryException as ex ->
                return raisefWithInner DomainExecutionException ex.InnerException "%s" ex.Message
    }