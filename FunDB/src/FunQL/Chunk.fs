module FunWithFlags.FunDB.FunQL.Chunk

open Newtonsoft.Json.Linq
open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.UsedReferences
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

type ColumnNamesMap = Map<FieldName, SQL.ColumnName>

type ResolvedChunkWhere =
    { Arguments : ResolvedArgumentsMap
      ArgumentValues : Map<ArgumentName, FieldValue>
      ColumnNames : ColumnNamesMap
      Expression : ResolvedFieldExpr
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

let private limitOffsetArgument = requiredArgument <| FTScalar SFTInt

let private maybeAddAnonymousInt (int : int option) (argValues : LocalArgumentsMap) (arguments : QueryArguments) : LocalArgumentsMap * QueryArguments * SQL.ValueExpr option =
    match int with
    | None -> (argValues, arguments, None)
    | Some offset ->
        let (argInfo, arg, arguments) = addAnonymousArgument limitOffsetArgument arguments
        let argValues = Map.add arg (FInt offset) argValues
        let sqlPlaceholder = Some <| SQL.VEPlaceholder argInfo.PlaceholderId
        (argValues, arguments, sqlPlaceholder)

// `chunkArguments` is a map from anonymous argument name to a real argument name.
let private compileWhereExpr (layout : Layout) (initialArguments : QueryArguments) (chunkArguments : Map<ArgumentName, ArgumentName>) (expr : ResolvedFieldExpr) =
    // A small hack: remake compiled arguments map so that it only contains chunk arguments.
    let convertArgument (name : Placeholder) =
        match name with
        | PGlobal glob -> Some name
        | PLocal loc ->
            match Map.tryFind loc chunkArguments with
            | None -> None
            | Some realName -> Some (PLocal realName)
    let compileExpr (args : QueryArguments) =
        let (info, expr) = compileSingleFieldExpr layout emptyExprCompilationFlags args expr
        (info.Arguments, expr)
    modifyArgumentsInNamespace convertArgument compileExpr initialArguments

let private chunkFromEntityId = -1

let private genericResolveWhere (layout : Layout) (namesMap : ColumnNamesMap) (chunk : SourceChunkWhere) : ResolvedChunkWhere =
    let parsedExpr =
        match parse tokenizeFunQL fieldExpr chunk.Expression with
        | Ok r -> r
        | Error msg -> raisef ChunkException "Error parsing chunk restriction: %s" msg

    let parseChunkArgument (arg : SourceChunkArgument) : ParsedArgument =
        match parse tokenizeFunQL fieldType arg.Type with
        | Ok r -> requiredArgument r
        | Error msg -> raisef ChunkException "Error parsing argument type: %s" msg
    let parsedArguments = Map.map (fun name -> parseChunkArgument) chunk.Arguments

    let makeCustomMapping (name : FieldName) (colName : SQL.ColumnName) =
        let info =
            { Bound = None
              ForceSQLName = Some colName
            } : CustomFromField
        let ref = { Entity = None; Name = name } : FieldRef
        (ref, info)
    let customMapping = Map.mapWithKeys makeCustomMapping namesMap : CustomFromMapping

    let (localArguments, resolvedExpr) =
        try
            resolveSingleFieldExpr layout parsedArguments chunkFromEntityId emptyExprResolutionFlags (SFCustom customMapping) parsedExpr
        with
        | :? ViewResolveException as e -> raisefWithInner ChunkException e ""
    let (info, usedArguments) = fieldExprUsedReferences layout resolvedExpr
    if not info.IsLocal then
        raisef ChunkException "Expression is required to be local"
    if info.HasAggregates then
        raisef ChunkException "Aggregate functions are not supported here"

    let resolveChunkValue (name : ArgumentName) (arg : SourceChunkArgument) : FieldValue =
        let argument = Map.find (PLocal name)  localArguments
        match parseValueFromJson argument.ArgType true arg.Value with
        | Some v -> v
        | None -> raisef ChunkException "Couldn't parse value %O to type %O" arg.Value argument.ArgType

    let argValues = Map.map resolveChunkValue chunk.Arguments

    { Arguments = localArguments
      ArgumentValues = argValues
      ColumnNames = namesMap
      Expression = resolvedExpr
    }

let genericResolveChunk (layout : Layout) (namesMap : ColumnNamesMap) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    { Offset = chunk.Offset
      Limit = chunk.Limit
      Where = Option.map (genericResolveWhere layout namesMap) chunk.Where
    }

let resolveViewExprChunk (layout : Layout) (viewExpr : CompiledViewExpr) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    let getOneColumn = function
        | (CTColumn name, columnName) -> Some (name, columnName)
        | _ -> None
    let columnsMap = viewExpr.Columns |> Seq.mapMaybe getOneColumn |> Map.ofSeq
    genericResolveChunk layout columnsMap chunk

let queryExprChunk (layout : Layout) (chunk : ResolvedQueryChunk) (query : Query<SQL.SelectExpr>) : LocalArgumentsMap * Query<SQL.SelectExpr> =
    let argValues = Map.empty : LocalArgumentsMap
    let arguments = query.Arguments
    let (argValues, arguments, sqlOffset) = maybeAddAnonymousInt chunk.Offset argValues arguments
    let (argValues, arguments, sqlLimit) = maybeAddAnonymousInt chunk.Limit argValues arguments
    let (argValues, arguments, sqlWhere) =
        match chunk.Where with
        | None -> (argValues, arguments, None)
        | Some where ->
            let foldArgs (argValues, arguments, chunkArguments) (name, argValue) =
                let argumentInfo = Map.find (PLocal name) where.Arguments
                let (argId, anonName, arguments) = addAnonymousArgument argumentInfo arguments
                let chunkArguments = Map.add anonName name chunkArguments
                let argValues = Map.add anonName argValue argValues
                (argValues, arguments, chunkArguments)
            let (argValues, arguments, chunkArguments) = Seq.fold foldArgs (argValues, arguments, Map.empty) (Map.toSeq where.ArgumentValues)
            let (arguments, compiled) = compileWhereExpr layout arguments chunkArguments where.Expression
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