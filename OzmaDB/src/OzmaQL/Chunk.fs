module OzmaDB.OzmaQL.Chunk

open Newtonsoft.Json.Linq
open System.Runtime.Serialization
open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Parsing
open OzmaDB.OzmaQL.Lex
open OzmaDB.OzmaQL.Parse
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Optimize
module SQL = OzmaDB.SQL.Utils
module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Chunk

type ChunkException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ChunkException (message, innerException, isUserException innerException)

    new (message : string) = ChunkException (message, null, true)

type SourceChunkArgument =
    { Type : string
      Value : JToken
    }

type SourceChunkWhere =
    { Arguments : Map<ArgumentName, SourceChunkArgument>
      Expression : string
    }

type SourceQueryChunk =
    { [<DataMember(EmitDefaultValue = false)>]
      Offset : int option
      [<DataMember(EmitDefaultValue = false)>]
      Limit : int option
      [<DataMember(EmitDefaultValue = false)>]
      Where : SourceChunkWhere option
      [<DataMember(EmitDefaultValue = false)>]
      Search : string option
    }

let emptySourceQueryChunk : SourceQueryChunk =
    { Offset = None
      Limit = None
      Where = None
      Search = None
    }

type ChunkColumn =
    { SQLName : SQL.ColumnName
      ValueType : SQL.SimpleValueType
    }

type ChunkColumnsMap = Map<FieldName, ChunkColumn>

type ResolvedChunkWhere =
    { Arguments : ResolvedArgumentsMap
      ArgumentValues : Map<ArgumentName, FieldValue>
      Expression : ResolvedFieldExpr
    }

let unionResolvedChunkWhere (a : ResolvedChunkWhere) (b : ResolvedChunkWhere) =
    { Arguments = OrderedMap.unionUnique a.Arguments b.Arguments
      ArgumentValues = Map.unionUnique a.ArgumentValues b.ArgumentValues
      Expression = FEAnd (a.Expression, b.Expression)
    }

type ResolvedQueryChunk =
    { Offset : int option
      Limit : int option
      Where : ResolvedChunkWhere option
    }

let private limitOffsetArgument = requiredArgument <| FTScalar SFTInt

let private maybeAddAnonymousInt (int : int option) (argValues : LocalArgumentsMap) (arguments : QueryArguments) : LocalArgumentsMap * QueryArguments * SQL.ValueExpr option =
    match int with
    | None -> (argValues, arguments, None)
    | Some offset ->
        let (argInfo, arg, arguments) = addAnonymousArgument limitOffsetArgument arguments
        let argValues = Map.add arg (FInt offset) argValues
        let sqlArgumentRef = Some <| SQL.VEPlaceholder argInfo.PlaceholderId
        (argValues, arguments, sqlArgumentRef)

// `chunkArguments` is a map from anonymous argument name to a real argument name.
let private compileWhereExpr (layout : Layout) (initialArguments : QueryArguments) (chunkArguments : Map<ArgumentName, ArgumentName>) (expr : ResolvedFieldExpr) =
    // A small hack: remake compiled arguments map so that it only contains chunk arguments.
    let convertArgument (name : ArgumentRef) =
        match name with
        | PGlobal glob -> Some name
        | PLocal loc ->
            match Map.tryFind loc chunkArguments with
            | None -> None
            | Some realName -> Some (PLocal realName)
    let compileExpr (args : QueryArguments) =
        let (info, expr) = compileSingleFieldExpr defaultCompilationFlags layout emptyExprCompilationFlags args expr
        (info.Arguments, expr)
    modifyArgumentsInNamespace convertArgument compileExpr initialArguments

let private genericResolveWhere (layout : Layout) (columns : ChunkColumnsMap) (chunk : SourceChunkWhere) : ResolvedChunkWhere =
    let parsedExpr =
        match parse tokenizeOzmaQL fieldExpr chunk.Expression with
        | Ok r -> r
        | Error msg -> raisef ChunkException "Error parsing chunk restriction: %s" msg

    let parseChunkArgument (arg : SourceChunkArgument) : ParsedArgument =
        match parse tokenizeOzmaQL fieldType arg.Type with
        | Ok r -> requiredArgument r
        | Error msg -> raisef ChunkException "Error parsing argument type: %s" msg
    let parsedArguments = Map.map (fun name -> parseChunkArgument) chunk.Arguments |> OrderedMap.ofMap

    let makeCustomMapping (name : FieldName) (col : ChunkColumn) =
        let info =
            { customFromField CFUnbound with
                ForceSQLName = Some col.SQLName
            } : CustomFromField
        let ref = { Entity = None; Name = name } : FieldRef
        (ref, info)
    let customMapping = Map.mapWithKeys makeCustomMapping columns : CustomFromMapping

    let callbacks = resolveCallbacks layout
    let (info, resolvedExpr) =
        try
            resolveSingleFieldExpr callbacks parsedArguments emptyExprResolutionFlags (SFCustom customMapping) parsedExpr
        with
        | :? QueryResolveException as e -> raisefWithInner ChunkException e ""
    if not <| exprIsLocal info.Flags then
        raisef ChunkException "Expression is required to be local"
    if info.Flags.HasAggregates then
        raisef ChunkException "Aggregate functions are not supported here"

    let resolveChunkValue (name : ArgumentName) (arg : SourceChunkArgument) : FieldValue =
        let argument = OrderedMap.find (PLocal name) info.LocalArguments
        match parseValueFromJson argument.ArgType arg.Value with
        | Some v -> v
        | None -> raisef ChunkException "Couldn't parse value %O to type %O" arg.Value argument.ArgType

    let argValues = Map.map resolveChunkValue chunk.Arguments

    { Arguments = info.LocalArguments
      ArgumentValues = argValues
      Expression = resolvedExpr
    }

type private SearchWord =
    { Index : int
      String : string
      Integer : int option
    }

let private convertWord (index : int) (word : string) =
    { Index = index
      String = word
      Integer = Parsing.tryIntInvariant word
    }

let private searchToWhere (columns : ChunkColumnsMap) (search : string) : ResolvedChunkWhere =
    let mutable arguments : ResolvedArgumentsMap = OrderedMap.empty
    let mutable argumentValues : Map<ArgumentName, FieldValue> = Map.empty

    let getLikeStringArgument (word : SearchWord) : ResolvedFieldExpr =
        let argName = OzmaQLName <| sprintf "__search_%d_like" word.Index
        if not <| Map.containsKey argName argumentValues then
            let argValue = sprintf "%%%s%%" (SQL.escapeSqlLike word.String)
            let argValue = FString argValue
            arguments <- OrderedMap.add (PLocal argName) (argument (FTScalar SFTString) false) arguments
            argumentValues <- Map.add argName argValue argumentValues
        PLocal argName |> VRArgument |> resolvedRefFieldExpr

    let getIntegerArgument (word : SearchWord) : ResolvedFieldExpr option =
        match word.Integer with
        | None -> None
        | Some int ->
            let argName = OzmaQLName <| sprintf "__search_%d_int" word.Index
            if not <| Map.containsKey argName argumentValues then
                let argValue = FInt int
                arguments <- OrderedMap.add (PLocal argName) (argument (FTScalar SFTInt) false) arguments
                argumentValues <- Map.add argName argValue argumentValues
            PLocal argName |> VRArgument |> resolvedRefFieldExpr |> Some

    let getColumnSearchOp (KeyValue(name : FieldName, info : ChunkColumn)) =
        let columnRef =
            lazy (
                ({ Entity = None; Name = name } : FieldRef)
                |> VRColumn
                |> resolvedRefFieldExpr
            )
        match info.ValueType with
        | SQL.VTScalar SQL.STString ->
            Some (fun (word : SearchWord) ->
                let arg = getLikeStringArgument word
                Some <| FEBinaryOp (columnRef.Value, BOILike, arg)
            )
        | SQL.VTScalar SQL.STBigInt
        | SQL.VTScalar SQL.STInt ->
            Some (fun (word : SearchWord) ->
                match getIntegerArgument word with
                | None -> None
                | Some arg ->
                    Some <| FEBinaryOp (columnRef.Value, BOEq, arg)
            )
        | _ -> None

    let words =
        search.Split(' ')
        |> Seq.filter (fun s -> s <> "")
        |> Seq.mapi convertWord
        |> Seq.cache
    let searchOps = columns |> Seq.mapMaybe getColumnSearchOp |> Seq.cache

    let appearsInColumn (word : SearchWord) =
        searchOps
            |> Seq.mapMaybe (fun op -> Option.map optimizeFieldExpr (op word))
            |> orFieldExprs

    let optimized =
        words
        |> Seq.map appearsInColumn
        |> andFieldExprs
    { Arguments = arguments
      ArgumentValues = argumentValues
      Expression = optimized.ToFieldExpr()
    }

let genericResolveChunk (layout : Layout) (columns : ChunkColumnsMap) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    let searchExpr =
        Option.map (searchToWhere columns) chunk.Search
    let where = Option.map (genericResolveWhere layout columns) chunk.Where
    let searchExpr =
        Option.unionWith unionResolvedChunkWhere searchExpr where
    { Offset = chunk.Offset
      Limit = chunk.Limit
      Where = searchExpr
    }

let resolveViewExprChunk (layout : Layout) (viewExpr : CompiledViewExpr) (chunk : SourceQueryChunk) : ResolvedQueryChunk =
    let getOneColumn (info : CompiledColumnInfo) =
        match info.Info.ValueType with
        | None -> None
        | Some valueType ->
            let maybeColName =
                match info.Type with
                | CTColumn name -> Some name
                | CTMeta CMMainId -> Some <| OzmaQLName "__main_id"
                | CTColumnMeta (OzmaQLName col, CCPun) -> Some (OzmaQLName <| sprintf "__pun__%s" col)
                | _ -> None
            match maybeColName with
            | None -> None
            | Some colName ->
                let chunkCol =
                    { SQLName = info.Name
                      ValueType = valueType
                    }
                Some (colName, chunkCol)
    let columns = viewExpr.Columns |> Seq.mapMaybe getOneColumn |> Map.ofSeq
    genericResolveChunk layout columns chunk

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
                let argumentInfo = OrderedMap.find (PLocal name) where.Arguments
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
