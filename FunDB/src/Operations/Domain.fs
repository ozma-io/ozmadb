module FunWithFlags.FunDB.Operations.Domain

open System.Threading
open System.Runtime.Serialization
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

type DomainExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = DomainExecutionException (message, null)

type DomainDeniedException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = DomainDeniedException (message, null)

let private domainColumns =
    Map.ofList
        [ (domainValueName, sqlDomainValueName)
          (domainPunName, sqlDomainPunName)
        ]

type DomainValue =
    { Value : SQL.Value
      [<DataMember(EmitDefaultValue = false)>]
      Pun : SQL.Value option
    }

[<NoEquality; NoComparison>]
type DomainValues =
    { Values : DomainValue[]
      PunType : SQL.SimpleValueType
    }

let private convertDomainValue (values : SQL.Value[]) =
    assert (Array.length values = 2)
    { Value = values.[0]
      Pun = Some values.[1]
    }

let getDomainValues (connection : QueryConnection) (layout : Layout) (domain : DomainExpr) (comments : string option) (role : ResolvedRole option) (arguments : ArgumentValuesMap) (chunk : SourceQueryChunk) (cancellationToken : CancellationToken) : Task<DomainValues> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                try
                    applyRoleQueryExpr layout r domain.UsedSchemas domain.Query
                with
                | :? PermissionsApplyException as e -> raisefWithInner DomainDeniedException e ""
        let resolvedChunk = genericResolveChunk layout domainColumns chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query

        try
            let arguments = Map.union arguments (Map.mapKeys PLocal argValues)
            return! connection.ExecuteQuery (convertComments comments + query.Expression.ToSQLString()) (prepareArguments query.Arguments arguments) cancellationToken <| fun result ->
                let values = result.Rows |> Seq.map convertDomainValue |> Seq.toArray
                let (_, punType) = result.Columns.[1]
                let ret =
                    { Values = values
                      PunType = punType
                    }
                Task.result ret
        with
            | :? QueryException as ex ->
                return raisefWithInner DomainExecutionException ex.InnerException "%s" ex.Message
    }

let explainDomainValues (connection : QueryConnection) (layout : Layout) (domain : DomainExpr) (role : ResolvedRole option) (maybeArguments : ArgumentValuesMap option) (chunk : SourceQueryChunk) (explainOpts : ExplainOptions) (cancellationToken : CancellationToken) : Task<ExplainedQuery> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                try
                    applyRoleQueryExpr layout r domain.UsedSchemas domain.Query
                with
                | :? PermissionsApplyException as e -> raisefWithInner DomainDeniedException e ""
        let resolvedChunk = genericResolveChunk layout domainColumns chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query
        let arguments = Option.defaultWith (fun () -> query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg.FieldType)) maybeArguments

        try
            let arguments = Map.union arguments (Map.mapKeys PLocal argValues)
            let compiledArgs = prepareArguments query.Arguments arguments
            let! explanation = runExplainQuery connection query.Expression compiledArgs explainOpts cancellationToken
            return { Query = string query.Expression; Explanation = explanation }
        with
            | :? QueryException as ex ->
                return raisefWithInner DomainExecutionException ex.InnerException "%s" ex.Message
    }