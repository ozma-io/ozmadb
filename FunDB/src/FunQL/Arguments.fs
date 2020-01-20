module FunWithFlags.FunDB.FunQL.Arguments

open Newtonsoft.Json.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type ArgumentCheckException (message : string) =
    inherit Exception(message)

type PlaceholderId = int

[<NoEquality; NoComparison>]
type CompiledArgument =
    { placeholderId : PlaceholderId
      fieldType : ArgumentFieldType
      valueType : SQL.SimpleValueType
      optional : bool
    }

type ResolvedArgumentsMap = Map<Placeholder, ResolvedArgument>
type CompiledArgumentsMap = Map<Placeholder, CompiledArgument>
type ArgumentValues = Map<Placeholder, FieldValue>

[<NoEquality; NoComparison>]
type QueryArguments =
    { types : CompiledArgumentsMap
      lastPlaceholderId : PlaceholderId
    }

[<NoEquality; NoComparison>]
type Query<'e when 'e :> ISQLString> =
    { expression : 'e
      arguments : QueryArguments
    }

let private compileScalarType : ScalarFieldType -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate
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
    { placeholderId = placeholderId
      fieldType = arg.argType
      valueType = compileFieldType arg.argType
      optional = arg.optional
    }

let emptyArguments =
    { types = Map.empty
      lastPlaceholderId = 0
    }

let addArgument (name : Placeholder) (arg : ResolvedArgument) (args : QueryArguments) : PlaceholderId * QueryArguments =
    match Map.tryFind name args.types with
    | Some oldArg -> (oldArg.placeholderId, args)
    | None ->
        let nextArg = args.lastPlaceholderId + 1
        let ret =
            { types = Map.add name (compileArgument nextArg arg) args.types
              lastPlaceholderId = nextArg
            }
        (nextArg, ret)

let mergeArguments (a : QueryArguments) (b : QueryArguments) : QueryArguments =
    { types = Map.unionUnique a.types b.types
      lastPlaceholderId = max a.lastPlaceholderId b.lastPlaceholderId
    }

let compileArguments (args : ResolvedArgumentsMap) : QueryArguments =
    let mutable lastPlaceholderId = 0

    let convertArgument name arg =
        lastPlaceholderId <- lastPlaceholderId + 1
        compileArgument lastPlaceholderId arg

    let arguments = args |> Map.map convertArgument
    { types = arguments
      lastPlaceholderId = lastPlaceholderId
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
    | FJson j -> SQL.VJson j
    | FUserViewRef r -> SQL.VJson <| JToken.FromObject(r)
    | FIntArray vals -> SQL.VIntArray (compileArray vals)
    | FDecimalArray vals -> SQL.VDecimalArray (compileArray vals)
    | FStringArray vals -> SQL.VStringArray (compileArray vals)
    | FBoolArray vals -> SQL.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> SQL.VDateTimeArray (compileArray vals)
    | FDateArray vals -> SQL.VDateArray (compileArray vals)
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
            typecheckArgument mapping.fieldType value
        (mapping.placeholderId, (mapping.valueType, compileFieldValueSingle value))
    args.types |> Map.mapWithKeys makeParameter