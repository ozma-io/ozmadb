module FunWithFlags.FunDB.FunQL.Arguments

open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type ArgumentCheckException (message : string) =
    inherit Exception(message)

type PlaceholderId = int

[<NoEquality; NoComparison>]
type CompiledArgument =
    { PlaceholderId : PlaceholderId
      FieldType : ArgumentFieldType
      DbType : SQL.DBValueType
      optional : bool
    }

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>
type CompiledArgumentsMap = Map<Placeholder, CompiledArgument>
type ArgumentValues = Map<Placeholder, FieldValue>

[<NoEquality; NoComparison>]
type QueryArguments =
    { Types : CompiledArgumentsMap
      LastPlaceholderId : PlaceholderId
    }

[<NoEquality; NoComparison>]
type Query<'e when 'e :> ISQLString> =
    { Expression : 'e
      Arguments : QueryArguments
    }

let private compileScalarType : ScalarFieldType -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate
    | SFTInterval -> SQL.STInterval
    | SFTJson -> SQL.STJson
    | SFTUserViewRef -> SQL.STJson

let compileFieldExprType : FieldExprType -> SQL.SimpleValueType = function
    | FETScalar stype -> SQL.VTScalar <| compileScalarType stype
    | FETArray stype -> SQL.VTArray <| compileScalarType stype

let compileFieldType : FieldType<_, _> -> SQL.SimpleValueType = function
    | FTType fetype -> compileFieldExprType fetype
    | FTReference (ent, restriction) -> SQL.VTScalar SQL.STInt
    | FTEnum vals -> SQL.VTScalar SQL.STString

let private compileArgument (placeholderId : PlaceholderId) (arg : ResolvedArgument) : CompiledArgument =
    { PlaceholderId = placeholderId
      FieldType = arg.argType
      DbType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType arg.argType)
      optional = arg.optional
    }

let emptyArguments =
    { Types = Map.empty
      LastPlaceholderId = 0
    }

let addArgument (name : Placeholder) (arg : ResolvedArgument) (args : QueryArguments) : PlaceholderId * QueryArguments =
    match Map.tryFind name args.Types with
    | Some oldArg -> (oldArg.PlaceholderId, args)
    | None ->
        let nextArg = args.LastPlaceholderId + 1
        let ret =
            { Types = Map.add name (compileArgument nextArg arg) args.Types
              LastPlaceholderId = nextArg
            }
        (nextArg, ret)

let mergeArguments (a : QueryArguments) (b : QueryArguments) : QueryArguments =
    { Types = Map.unionUnique a.Types b.Types
      LastPlaceholderId = max a.LastPlaceholderId b.LastPlaceholderId
    }

let compileArguments (args : ResolvedArgumentsMap) : QueryArguments =
    let mutable lastPlaceholderId = 0

    let convertArgument name arg =
        lastPlaceholderId <- lastPlaceholderId + 1
        compileArgument lastPlaceholderId arg

    let arguments = args |> Map.map convertArgument
    { Types = arguments
      LastPlaceholderId = lastPlaceholderId
    }

let private typecheckArgument (fieldType : FieldType<_, _>) (value : FieldValue) : unit =
    match fieldType with
    | FTEnum vals ->
        match value with
        | FString str when Set.contains str vals -> ()
        | FNull -> ()
        | _ -> raisef ArgumentCheckException "Argument is not from allowed values of a enum: %O" value
    // Most casting/typechecking will be done by database or Npgsql
    | _ -> ()

let compileArray (vals : 'a array) : SQL.ValueArray<'a> = Array.map SQL.AVValue vals

let compileFieldValueSingle : FieldValue -> SQL.Value = function
    | FInt i -> SQL.VInt i
    | FDecimal d -> SQL.VDecimal d
    | FString s -> SQL.VString s
    | FBool b -> SQL.VBool b
    | FDateTime dt -> SQL.VDateTime dt
    | FDate d -> SQL.VDate d
    | FInterval int -> SQL.VInterval int
    | FJson j -> SQL.VJson j
    | FUserViewRef r -> SQL.VJson <| JToken.FromObject(r)
    | FIntArray vals -> SQL.VIntArray (compileArray vals)
    | FDecimalArray vals -> SQL.VDecimalArray (compileArray vals)
    | FStringArray vals -> SQL.VStringArray (compileArray vals)
    | FBoolArray vals -> SQL.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> SQL.VDateTimeArray (compileArray vals)
    | FDateArray vals -> SQL.VDateArray (compileArray vals)
    | FIntervalArray vals -> SQL.VIntervalArray (compileArray vals)
    | FJsonArray vals -> SQL.VJsonArray (compileArray vals)
    | FUserViewRefArray vals -> SQL.VJsonArray (vals |> Array.map (fun x -> JToken.FromObject(x)) |> compileArray)
    | FNull -> SQL.VNull

let prepareArguments (args : QueryArguments) (values : ArgumentValues) : ExprParameters =
    let makeParameter (name : Placeholder) (mapping : CompiledArgument) =
        let (notFound, value) =
            match Map.tryFind name values with
            | None ->
                if mapping.optional then
                    (true, FNull)
                else
                    raisef ArgumentCheckException "Argument not found: %O" name
            | Some value -> (false, value)
        if not (mapping.optional && notFound) then
            typecheckArgument mapping.FieldType value
        (mapping.PlaceholderId, compileFieldValueSingle value)
    args.Types |> Map.mapWithKeys makeParameter