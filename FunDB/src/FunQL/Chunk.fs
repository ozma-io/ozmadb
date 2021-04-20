module FunWithFlags.FunDB.FunQL.Chunk

open Newtonsoft.Json.Linq
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Chunk

type ChunkException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ChunkException (message, null)

type SourceChunkArgument =
    { Type : string
      Value : JToken
    }

type SourceChunkWhere =
    { Arguments : Map<ArgumentName, SourceChunkArgument>
      Expression : string
    }

type SourceQueryChunk =
    { Offset : int option
      Limit : int option
      Where : SourceChunkWhere option
    }

type ResolvedChunkExpr = FieldExpr<EntityRef, ValueRef<FieldName>>

type ResolvedChunkWhere =
    { Arguments : Map<ArgumentName, ResolvedFieldType * FieldValue>
      ColumnNames : Map<FieldName, SQL.ColumnName>
      Expression : ResolvedChunkExpr
    }

type ResolvedQueryChunk =
    { Offset : int option
      Limit : int option
      Where : ResolvedChunkWhere option
    }

let emptyQueryChunk =
    { Offset = None
      Limit = None
      Where = None
    } : SourceQueryChunk

let private limitOffsetArgument =
    { ArgType = FTType (FETScalar SFTInt)
      Optional = false
    } : ResolvedArgument

let private addAnonymousInt (int : int option) (argValues : LocalArgumentsMap) (arguments : QueryArguments) : LocalArgumentsMap * QueryArguments * SQL.ValueExpr option =
    match int with
    | None -> (argValues, arguments, None)
    | Some offset ->
        let (argId, arg, arguments) = addAnonymousArgument limitOffsetArgument arguments
        let argValues = Map.add arg (FInt offset) argValues
        let sqlPlaceholder = Some <| SQL.VEPlaceholder argId
        (argValues, arguments, sqlPlaceholder)

let private compileWhereExpr (initialArguments : QueryArguments) (localArguments : Map<ArgumentName, PlaceholderId>) (columns : Map<FieldName, SQL.ColumnName>) (expr : ResolvedChunkExpr) =
    let mutable arguments = initialArguments

    let compileLinkedRef ctx (ref : ValueRef<FieldName>) : SQL.ValueExpr =
        match ref with
        | VRColumn name -> SQL.VEColumn { table = None; name = Map.find name columns }
        | VRPlaceholder (PGlobal name as arg) ->
            let (argId, newArguments) = addArgument arg globalArgumentTypes.[name] arguments
            arguments <- newArguments
            SQL.VEPlaceholder argId
        | VRPlaceholder (PLocal name) ->
            SQL.VEPlaceholder (localArguments.[name])
    let compileSubSelectExpr select = failwith "Impossible SELECT sub-expression"

    let ret = genericCompileFieldExpr { Schemas = Map.empty } compileLinkedRef compileSubSelectExpr expr
    (arguments, ret)

let private genericResolveWhere (findColumn : FieldName -> SQL.ColumnName option) (chunk : SourceChunkWhere) : ResolvedChunkWhere =
    let parsedExpr =
        match parse tokenizeFunQL fieldExpr chunk.Expression with
        | Ok r -> r
        | Error msg -> raisef ChunkException "Error parsing chunk restriction: %s" msg

    let resolveArgument (arg : SourceChunkArgument) : ResolvedFieldType * FieldValue =
        let parsedType =
            match parse tokenizeFunQL fieldType arg.Type with
            | Ok r -> r
            | Error msg -> raisef ChunkException "Error parsing argument type: %s" msg
        let resolvedType =
            match parsedType with
            | FTReference ref -> raisef ChunkException "References in chunk restrictions are not supported: %O" ref
            | FTEnum vals -> FTEnum vals
            | FTType t -> FTType t
        let resolvedValue =
            match parseValueFromJson resolvedType true arg.Value with
            | Some v -> v
            | None -> raisef ChunkException "Couldn't parse value %O to type %O" arg.Value resolvedType
        (resolvedType, resolvedValue)

    let resolvedArguments = Map.map (fun name -> resolveArgument) chunk.Arguments

    let mutable columnNames = Map.empty

    let resolveExprReference : LinkedFieldRef -> ValueRef<FieldName> = function
        | { Ref = VRColumn { entity = None; name = name }; Path = [||] } ->
            match findColumn name with
            | None -> raisef ChunkException "Field not found in chunk restriction: %O" name
            | Some columnName ->
                columnNames <- Map.add name columnName columnNames
                VRColumn name
        | { Ref = VRPlaceholder (PGlobal name as arg); Path = [||] } ->
            if not <| Map.containsKey name globalArgumentTypes then
                raisef ChunkException "Unknown global argument in chunk restriction: %O" ref
            VRPlaceholder arg
        | { Ref = VRPlaceholder (PLocal name as arg); Path = [||] } ->
            if not <| Map.containsKey name resolvedArguments then
                raisef ChunkException "Unknown local argument in chunk restriction: %O" ref
            VRPlaceholder arg
        | ref -> raisef ChunkException "Invalid reference in chunk restriction: %O" ref

    let resolveQuery query = raisef ChunkException "Unexpected sub-expression in chunk restriction"
    let resolveAggr aggr = raisef ChunkException "Unexpected aggregate expression in chunk restriction"
    let resolveSubEntity ctx fieldRef subEntity = raisef ChunkException "Unexpected aggregate expression in chunk restriction"
    let mapper =
        { idFieldExprMapper resolveExprReference resolveQuery with
              Aggregate = resolveAggr
              SubEntity = resolveSubEntity
        }
    let resolvedExpr = mapFieldExpr mapper parsedExpr

    { Arguments = resolvedArguments
      ColumnNames = columnNames
      Expression = resolvedExpr
    }

let genericResolveChunk (findColumn : FieldName -> SQL.ColumnName option) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    { Offset = chunk.Offset
      Limit = chunk.Limit
      Where = Option.map (genericResolveWhere findColumn) chunk.Where
    }

let resolveViewExprChunk (viewExpr : CompiledViewExpr) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    let getOneColumn = function
        | (CTColumn name, columnName) -> Some (name, columnName)
        | _ -> None
    let columnsMap = viewExpr.Columns |> Seq.mapMaybe getOneColumn |> Map.ofSeq
    genericResolveChunk (fun name -> Map.tryFind name columnsMap) chunk

let queryExprChunk (chunk : ResolvedQueryChunk) (query : Query<SQL.SelectExpr>) : LocalArgumentsMap * Query<SQL.SelectExpr> =
    let argValues = Map.empty : LocalArgumentsMap
    let arguments = query.Arguments
    let (argValues, arguments, sqlOffset) = addAnonymousInt chunk.Offset argValues arguments
    let (argValues, arguments, sqlLimit) = addAnonymousInt chunk.Limit argValues arguments
    let (argValues, arguments, sqlWhere) =
        match chunk.Where with
        | None -> (argValues, arguments, None)
        | Some where ->
            let foldArgs (argValues, arguments, localArguments) (name, (argType, argValue)) =
                let argumentInfo =
                    { ArgType = argType
                      Optional = false
                    } : ResolvedArgument
                let (argId, arg, arguments) = addAnonymousArgument argumentInfo arguments
                let localArguments = Map.add name argId localArguments
                let argValues = Map.add arg argValue argValues
                (argValues, arguments, localArguments)
            let (argValues, arguments, localArguments) = Seq.fold foldArgs (argValues, arguments, Map.empty) (Map.toSeq where.Arguments)
            let (arguments, compiled) = compileWhereExpr arguments localArguments where.ColumnNames where.Expression
            (argValues, arguments, Some compiled)

    let sqlChunk =
        { Offset = sqlOffset
          Limit = sqlLimit
          Where = sqlWhere
        } : SQL.QueryChunk
    let queryExpr = SQL.selectExprChunk sqlChunk query.Expression
    let res =
        { Arguments = arguments
          Expression = queryExpr
        } : Query<SQL.SelectExpr>
    (argValues, res)