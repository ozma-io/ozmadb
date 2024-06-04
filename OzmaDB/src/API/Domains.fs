module OzmaDB.API.Domains

open FSharp.Control.Tasks.Affine
open Microsoft.Extensions.Logging
open System.Threading.Tasks
open Newtonsoft.Json

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Chunk
open OzmaDB.OzmaQL.Query
open OzmaDB.Operations.Domain
open OzmaDB.Layout.Domain
open OzmaDB.API.Types
module SQL = OzmaDB.SQL.Query

let private domainComments (ref : ResolvedFieldRef) (role : RoleType) (rowId : RawRowKey option) (argValues : LocalArgumentsMap) (chunk : SourceQueryChunk) =
    let refStr = sprintf "domain for %O" ref
    let rowIdStr =
        match rowId with
        | None -> ""
        | Some id -> sprintf ", row id %s" (JsonConvert.SerializeObject id)
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

type DomainsAPI (api : IOzmaDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<DomainsAPI>()

    let getDomain (flags : DomainFlags) (fieldRef : ResolvedFieldRef) : FieldDomain option =
        if flags.ForceRecompile then
            buildSingleLayoutDomain ctx.Layout fieldRef
        else
            findDomainForField ctx.Layout fieldRef ctx.Domains

    member this.GetDomainValues (req : GetDomainValuesRequest) : Task<Result<DomainValuesResponse, DomainErrorInfo>> =
        wrapAPIResult rctx logger "getDomainValues" req <| fun () -> task {
            let flags = Option.defaultValue emptyDomainFlags req.Flags
            match getDomain flags req.Field with
            | None ->
                let msg = sprintf "Field %O not found" req.Field
                return Error (DERequest msg)
            | Some domain ->
                try
                    let argValues = Map.mapKeys PGlobal rctx.GlobalArguments
                    let (expr, argValues) =
                        match (req.Id, domain.RowSpecific) with
                        | (Some (RRKPrimary id), Some rowSpecific) -> (rowSpecific, Map.add (PLocal funId) (FInt id) argValues)
                        | (Some (RRKAlt (constrName, args)), Some rowSpecific) -> failwith "Not implemented"
                        | _ -> (domain.Generic, argValues)
                    let role = getReadRole rctx.User.Effective.Type
                    let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
                    let comment = domainComments req.Field rctx.User.Effective.Type req.Id rctx.GlobalArguments chunk
                    let! ret = getDomainValues ctx.Transaction.Connection.Query ctx.Layout expr (Some comment) role argValues chunk ctx.CancellationToken
                    return Ok
                        { Values = ret.Values
                          PunType = ret.PunType
                          Hash = expr.Hash
                        }
                with
                | :? ChunkException as e when e.IsUserException ->
                    let msg = sprintf "Chunk spec error: %s" (fullUserMessage e)
                    return Error <| DERequest msg
                | :? DomainRequestException as e when e.IsUserException ->
                    logger.LogError(e, "Failed to get domain values")
                    return Error (DEDomain e.Details)
        }

    member this.GetDomainExplain (req : GetDomainExplainRequest) : Task<Result<ExplainedQuery, DomainErrorInfo>> =
        wrapAPIResult rctx logger "getDomainExplain" req <| fun () -> task {
            if not (canExplain rctx.User.Saved.Type) then
                logger.LogError("Explain access denied")
                return Error (DEDomain (DEAccessDenied "Explain access denied"))
            else
                let flags = Option.defaultValue emptyDomainFlags req.Flags
                match getDomain flags req.Field with
                | None ->
                    let msg = sprintf "Field %O not found" req.Field
                    return Error <| DERequest msg
                | Some domain ->
                    try
                        let argValues = Map.mapKeys PGlobal rctx.GlobalArguments
                        let (expr, argValues) =
                            match (req.Id, domain.RowSpecific) with
                            | (Some (RRKPrimary id), Some rowSpecific) -> (rowSpecific, Map.add (PLocal funId) (FInt id) argValues)
                            | _ -> (domain.Generic, argValues)
                        let role = getReadRole rctx.User.Effective.Type
                        let chunk = Option.defaultValue emptySourceQueryChunk req.Chunk
                        let explainFlags = Option.defaultValue SQL.defaultExplainOptions req.ExplainFlags
                        let! ret = explainDomainValues ctx.Transaction.Connection.Query ctx.Layout expr role (Some argValues) chunk explainFlags ctx.CancellationToken
                        return Ok ret
                    with
                    | :? ChunkException as e when e.IsUserException ->
                        let msg = sprintf "Chunk spec error: %s" (fullUserMessage e)
                        return Error <| DERequest msg
                    | :? DomainRequestException as e when e.IsUserException ->
                        logger.LogError(e, "Failed to get domain query explain")
                        return Error (DEDomain e.Details)
        }

    interface IDomainsAPI with
        member this.GetDomainValues req = this.GetDomainValues req
        member this.GetDomainExplain req = this.GetDomainExplain req