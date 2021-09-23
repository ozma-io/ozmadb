module FunWithFlags.FunDB.API.Domains

open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.Operations.Domain
open FunWithFlags.FunDB.Layout.Domain
open FunWithFlags.FunDB.API.Types
module SQL = FunWithFlags.FunDB.SQL.Query

let private domainComments (ref : ResolvedFieldRef) (role : RoleType) (rowId : int option) (argValues : LocalArgumentsMap) (chunk : SourceQueryChunk) =
    let refStr = sprintf "domain for %O" ref
    let rowIdStr =
        match rowId with
        | None -> ""
        | Some id -> sprintf ", row id %i" id
    let argsStr = sprintf ", args %s" (JsonConvert.SerializeObject argValues)
    let chunkStr = sprintf ", chunk %s" (JsonConvert.SerializeObject chunk)
    let roleStr =
        match role with
        | RTRoot -> ""
        | RTRole role when role.CanRead -> ""
        | RTRole role -> sprintf ", role %O" role.Ref
    String.concat "" [refStr; rowIdStr; argsStr; chunkStr; roleStr]

let private canExplain : RoleType -> bool = function
    | RTRoot -> true
    | RTRole role -> role.CanRead

type DomainsAPI (api : IFunDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<DomainsAPI>()

    member this.GetDomainValues (fieldRef : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) =
        task {
            match findDomainForField ctx.Layout fieldRef ctx.Domains with
            | None -> return Error DENotFound
            | Some domain ->
                try
                    let argValues = Map.mapKeys PGlobal rctx.GlobalArguments
                    let (expr, argValues) =
                        match (rowId, domain.RowSpecific) with
                        | (Some id, Some rowSpecific) -> (rowSpecific, Map.add (PLocal funId) (FInt id) argValues)
                        | _ -> (domain.Generic, argValues)
                    let role = getReadRole rctx.User.Effective.Type
                    let comment = domainComments fieldRef rctx.User.Effective.Type rowId rctx.GlobalArguments chunk
                    let! ret = getDomainValues ctx.Transaction.Connection.Query ctx.Layout expr (Some comment) role argValues chunk ctx.CancellationToken
                    return Ok
                        { Values = ret.Values
                          PunType = ret.PunType
                          Hash = expr.Hash
                        }
                with
                | :? ChunkException as ex when ex.IsUserException ->
                    return Error <| DEArguments (exceptionString ex)
                | :? DomainExecutionException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Failed to get domain values")
                    let str = exceptionString ex
                    return Error (DEExecution str)
                | :? DomainDeniedException as ex when ex.IsUserException ->
                    logger.LogError(ex, "Access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getDomainValues"
                        event.SchemaName <- fieldRef.Entity.Schema.ToString()
                        event.EntityName <- fieldRef.Entity.Name.ToString()
                        event.FieldName <- fieldRef.Name.ToString()
                        event.Error <- "access_denied"
                        event.Details <- exceptionString ex
                    )
                    return Error DEAccessDenied
        }

    member this.GetDomainExplain (fieldRef : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) (explainOpts : SQL.ExplainOptions) =
        task {
            if not (canExplain rctx.User.Saved.Type) then
                logger.LogError("Explain access denied")
                rctx.WriteEvent (fun event ->
                    event.Type <- "getDomainExplain"
                    event.Error <- "access_denied"
                )
                return Error DEAccessDenied
            else
                match findDomainForField ctx.Layout fieldRef ctx.Domains with
                | None -> return Error DENotFound
                | Some domain ->
                    try
                        let argValues = Map.mapKeys PGlobal rctx.GlobalArguments
                        let (expr, argValues) =
                            match (rowId, domain.RowSpecific) with
                            | (Some id, Some rowSpecific) -> (rowSpecific, Map.add (PLocal funId) (FInt id) argValues)
                            | _ -> (domain.Generic, argValues)
                        let role = getReadRole rctx.User.Effective.Type
                        let! ret = explainDomainValues ctx.Transaction.Connection.Query ctx.Layout expr role (Some argValues) chunk explainOpts ctx.CancellationToken
                        return Ok ret
                    with
                    | :? ChunkException as ex when ex.IsUserException ->
                        return Error <| DEArguments (exceptionString ex)
                    | :? DomainExecutionException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Failed to get domain explain")
                        let str = exceptionString ex
                        return Error (DEExecution str)
                    | :? DomainDeniedException as ex when ex.IsUserException ->
                        logger.LogError(ex, "Access denied")
                        rctx.WriteEvent (fun event ->
                            event.Type <- "getDomainExplain"
                            event.SchemaName <- fieldRef.Entity.Schema.ToString()
                            event.EntityName <- fieldRef.Entity.Name.ToString()
                            event.FieldName <- fieldRef.Name.ToString()
                            event.Error <- "access_denied"
                            event.Details <- exceptionString ex
                        )
                        return Error DEAccessDenied
        }

    interface IDomainsAPI with
        member this.GetDomainValues fieldRef rowId chunk = this.GetDomainValues fieldRef rowId chunk
        member this.GetDomainExplain fieldRef rowId chunk explainOpts = this.GetDomainExplain fieldRef rowId chunk explainOpts