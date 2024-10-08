module OzmaDB.Operations.Domain

open System.Linq
open System.Threading
open System.Runtime.Serialization
open System.Threading.Tasks
open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Chunk
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Query
open OzmaDB.Layout.Types
open OzmaDB.Layout.Domain
open OzmaDB.Permissions.Types
open OzmaDB.Permissions.View
open OzmaDB.Permissions.Apply
open OzmaDB.SQL.Query
open OzmaDB.SQL.Utils

module SQL = OzmaDB.SQL.AST

[<SerializeAsObject("error")>]
type DomainError =
    | [<CaseKey(null, Type = CaseSerialization.InnerObject)>] DEExecution of OzmaQLExecutionError
    | [<CaseKey("accessDenied", IgnoreFields = [| "Details" |])>] DEAccessDenied of Details: string

    member this.LogMessage =
        match this with
        | DEExecution e -> e.LogMessage
        | DEAccessDenied details -> details

    [<DataMember>]
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

    member this.Details =
        match this with
        | DEExecution e -> e.Details
        | _ -> Map.empty

    static member private LookupKey = prepareLookupCaseKey<DomainError>

    member this.Error =
        match this with
        | DEExecution e -> e.Error
        | _ -> DomainError.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details

    interface IErrorDetails with
        member this.Message = this.Message
        member this.LogMessage = this.LogMessage
        member this.HTTPResponseCode = this.HTTPResponseCode
        member this.Error = this.Error

type DomainRequestException(details: DomainError, innerException: exn) =
    inherit UserException(details.LogMessage, innerException, true)

    new(details: DomainError) = DomainRequestException(details, null)

    member this.Details = details

let private domainColumns (expr: DomainExpr) : ChunkColumnsMap =
    let valueChunkColumn =
        { SQLName = sqlDomainValueName
          ValueType = sqlFunIdType }
        : ChunkColumn

    let punChunkColumn =
        { SQLName = sqlDomainPunName
          ValueType = expr.PunValueType }
        : ChunkColumn

    Map.ofList [ (domainValueName, valueChunkColumn); (domainPunName, punChunkColumn) ]

type DomainValue =
    { Value: SQL.Value
      [<DataMember(EmitDefaultValue = false)>]
      Pun: SQL.Value option }

[<NoEquality; NoComparison>]
type DomainValues =
    { Values: DomainValue[]
      PunType: SQL.SimpleValueType }

let private convertDomainValue (values: SQL.Value[]) =
    assert (Array.length values = 2)

    { Value = values.[0]
      Pun = Some values.[1] }

let getDomainValues
    (connection: QueryConnection)
    (layout: Layout)
    (domain: DomainExpr)
    (comments: string option)
    (role: ResolvedRole option)
    (arguments: ArgumentValuesMap)
    (chunk: SourceQueryChunk)
    (cancellationToken: CancellationToken)
    : Task<DomainValues> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                let appliedDb =
                    try
                        applyPermissions layout r domain.UsedDatabase
                    with :? PermissionsApplyException as e when e.IsUserException ->
                        raise <| DomainRequestException(DEAccessDenied(fullUserMessage e), e)

                applyRoleSelectExpr layout appliedDb domain.Query

        let resolvedChunk = genericResolveChunk layout (domainColumns domain) chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query

        try
            let arguments = Map.unionUnique arguments (Map.mapKeys PLocal argValues)

            return!
                connection.ExecuteQuery
                    (convertComments comments + query.Expression.ToSQLString())
                    (prepareArguments query.Arguments arguments)
                    cancellationToken
                <| fun columns result ->
                    task {
                        let! values = result.Select(convertDomainValue).ToArrayAsync(cancellationToken)
                        let (_, punType) = columns.[1]
                        let ret = { Values = values; PunType = punType }
                        return ret
                    }
        with :? QueryExecutionException as e ->
            return
                raise
                <| DomainRequestException(DEExecution <| convertQueryExecutionException layout e)
    }

let explainDomainValues
    (connection: QueryConnection)
    (layout: Layout)
    (domain: DomainExpr)
    (role: ResolvedRole option)
    (maybeArguments: ArgumentValuesMap option)
    (chunk: SourceQueryChunk)
    (explainOpts: ExplainOptions)
    (cancellationToken: CancellationToken)
    : Task<ExplainedQuery> =
    task {
        let query =
            match role with
            | None -> domain.Query
            | Some r ->
                let appliedDb =
                    try
                        applyPermissions layout r domain.UsedDatabase
                    with :? PermissionsApplyException as e ->
                        raise <| DomainRequestException(DEAccessDenied(fullUserMessage e), e)

                applyRoleSelectExpr layout appliedDb domain.Query

        let resolvedChunk = genericResolveChunk layout (domainColumns domain) chunk
        let (argValues, query) = queryExprChunk layout resolvedChunk query

        let arguments =
            Option.defaultWith
                (fun () -> query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg))
                maybeArguments

        try
            let arguments = Map.unionUnique arguments (Map.mapKeys PLocal argValues)
            let compiledArgs = prepareArguments query.Arguments arguments
            let! explanation = runExplainQuery connection query.Expression compiledArgs explainOpts cancellationToken

            return
                { Query = string query.Expression
                  Parameters = compiledArgs
                  Explanation = explanation }
        with :? QueryExecutionException as e ->
            return
                raise
                <| DomainRequestException(DEExecution <| convertQueryExecutionException layout e)
    }
