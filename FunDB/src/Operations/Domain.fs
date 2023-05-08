module FunWithFlags.FunDB.Operations.Domain

open System.Linq
open System.Threading
open System.Runtime.Serialization
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Permissions.View
open FunWithFlags.FunDB.Permissions.Apply
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

[<SerializeAsObject("error")>]
type DomainError =
    | [<CaseKey(null, Type=CaseSerialization.InnerObject)>] DEExecution of FunQLExecutionError
    | [<CaseKey("accessDenied", IgnoreFields=[|"Details"|])>] DEAccessDenied of Details : string
    with
    member this.LogMessage =
        match this with
        | DEExecution e -> e.LogMessage
        | DEAccessDenied details -> details

    member this.Message =
        match this with
        | DEExecution e -> e.Message
        | DEAccessDenied details -> "Access denied"

    member this.HTTPResponseCode =
        match this with
        | DEExecution e -> e.HTTPResponseCode
        | DEAccessDenied details -> 403

    member this.ShouldLog =
        match this with
        | DEExecution e -> e.ShouldLog
        | DEAccessDenied details -> true

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog

    interface IErrorDetails with
        member this.Message = this.Message
        member this.LogMessage = this.LogMessage
        member this.HTTPResponseCode = this.HTTPResponseCode

type DomainRequestException (details : DomainError, innerException : exn) =
    inherit UserException(details.LogMessage, innerException, true)

    new (details : DomainError) = DomainRequestException (details, null)

    member this.Details = details

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
                let appliedDb =
                    try
                        applyPermissions layout r domain.UsedDatabase
                    with
                    | :? PermissionsApplyException as e when e.IsUserException ->
                        raise <| DomainRequestException(DEAccessDenied (fullUserMessage e), e)
                applyRoleSelectExpr layout appliedDb domain.Query
        let resolvedChunk = genericResolveChunk layout domainColumns chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query

        try
            let arguments = Map.unionUnique arguments (Map.mapKeys PLocal argValues)
            return! connection.ExecuteQuery (convertComments comments + query.Expression.ToSQLString()) (prepareArguments query.Arguments arguments) cancellationToken <| fun columns result ->
                task {
                    let! values = result.Select(convertDomainValue).ToArrayAsync(cancellationToken)
                    let (_, punType) = columns.[1]
                    let ret =
                        { Values = values
                          PunType = punType
                        }
                    return ret
                }
        with
            | :? QueryExecutionException as e ->
                return raise <| DomainRequestException(DEExecution <| convertQueryExecutionException layout e)
    }

let explainDomainValues (connection : QueryConnection) (layout : Layout) (domain : DomainExpr) (role : ResolvedRole option) (maybeArguments : ArgumentValuesMap option) (chunk : SourceQueryChunk) (explainOpts : ExplainOptions) (cancellationToken : CancellationToken) : Task<ExplainedQuery> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                let appliedDb =
                    try
                        applyPermissions layout r domain.UsedDatabase
                    with
                    | :? PermissionsApplyException as e ->
                        raise <| DomainRequestException(DEAccessDenied (fullUserMessage e), e)
                applyRoleSelectExpr layout appliedDb domain.Query
        let resolvedChunk = genericResolveChunk layout domainColumns chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query
        let arguments = Option.defaultWith (fun () -> query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg)) maybeArguments

        try
            let arguments = Map.unionUnique arguments (Map.mapKeys PLocal argValues)
            let compiledArgs = prepareArguments query.Arguments arguments
            let! explanation = runExplainQuery connection query.Expression compiledArgs explainOpts cancellationToken
            return { Query = string query.Expression; Parameters = compiledArgs; Explanation = explanation }
        with
            | :? QueryExecutionException as e ->
                return raise <| DomainRequestException(DEExecution <| convertQueryExecutionException layout e)
    }
