module FunWithFlags.FunDB.API.Domains

open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Chunk
open FunWithFlags.FunDB.Operations.Domain
open FunWithFlags.FunDB.API.Types

type DomainsAPI (rctx : IRequestContext) =
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<DomainsAPI>()

    member this.GetDomainValues (fieldRef : ResolvedFieldRef) (rowId : int option) (chunk : SourceQueryChunk) =
        task {
            match ctx.Domains.FindField fieldRef with
            | None -> return Error DENotFound
            | Some domain ->
                try
                    let argValues = Map.mapKeys PGlobal rctx.GlobalArguments
                    let (expr, argValues) =
                        match (rowId, domain.RowSpecific) with
                        | (Some id, Some rowSpecific) -> (rowSpecific, Map.add (PLocal funId) (FInt id) argValues)
                        | _ -> (domain.Generic, argValues)
                    let! ret = getDomainValues ctx.Transaction.Connection.Query ctx.Layout expr (getReadRole rctx.User.Type) argValues chunk ctx.CancellationToken
                    return Ok
                        { Values = ret.Values
                          PunType = ret.PunType
                          Hash = expr.Hash
                        }
                with
                | :? ChunkException as ex ->
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getDomainValues"
                        event.SchemaName <- fieldRef.entity.schema.ToString()
                        event.EntityName <- fieldRef.entity.name.ToString()
                        event.FieldName <- fieldRef.name.ToString()
                        event.Error <- "arguments"
                        event.Details <- exceptionString ex
                    )
                    return Error <| DEArguments (exceptionString ex)
                | :? DomainExecutionException as ex ->
                    logger.LogError(ex, "Failed to get domain values")
                    let str = exceptionString ex
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getDomainValues"
                        event.SchemaName <- fieldRef.entity.schema.ToString()
                        event.EntityName <- fieldRef.entity.name.ToString()
                        event.FieldName <- fieldRef.name.ToString()
                        event.Error <- "execution"
                        event.Details <- str
                    )
                    return Error (DEExecution str)
                | :? DomainDeniedException as ex ->
                    logger.LogError(ex, "Access denied")
                    rctx.WriteEvent (fun event ->
                        event.Type <- "getDomainValues"
                        event.SchemaName <- fieldRef.entity.schema.ToString()
                        event.EntityName <- fieldRef.entity.name.ToString()
                        event.FieldName <- fieldRef.name.ToString()
                        event.Error <- "access_denied"
                        event.Details <- exceptionString ex
                    )
                    return Error DEAccessDenied
        }

    interface IDomainsAPI with
        member this.GetDomainValues fieldRef rowId chunk = this.GetDomainValues fieldRef rowId chunk