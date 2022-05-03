module FunWithFlags.FunDB.FunQL.Arguments

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Json
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type ArgumentCheckException (message : string) =
    inherit UserException(message)

type PlaceholderId = int

[<NoEquality; NoComparison>]
type CompiledArgument =
    { PlaceholderId : PlaceholderId
      FieldType : ResolvedFieldType
      DbType : SQL.DBValueType
      Optional : bool
      DefaultValue : FieldValue option
      Attributes : ResolvedBoundAttributesMap
    }

type RawArguments = Map<string, JToken>

type CompiledArgumentsMap = Map<ArgumentRef, CompiledArgument>
type ArgumentValuesMap = Map<ArgumentRef, FieldValue>

[<NoEquality; NoComparison>]
type QueryArguments =
    { Types : CompiledArgumentsMap
      NextPlaceholderId : PlaceholderId
      NextAnonymousId : int
    }

[<NoEquality; NoComparison>]
type Query<'e when 'e :> ISQLString> =
    { Expression : 'e
      Arguments : QueryArguments
    }

let compileArray (vals : 'a array) : SQL.ValueArray<'a> = Array.map SQL.AVValue vals

let compileFieldValue : FieldValue -> SQL.Value = function
    | FInt i -> SQL.VInt i
    | FDecimal d -> SQL.VDecimal d
    | FString s -> SQL.VString s
    | FBool b -> SQL.VBool b
    | FDateTime dt -> SQL.VDateTime dt
    | FDate d -> SQL.VDate d
    | FInterval int -> SQL.VInterval int
    | FJson j -> SQL.VJson j
    | FUuid u -> SQL.VUuid u
    | FUserViewRef r -> JToken.FromObject(r) |> ComparableJToken |> SQL.VJson
    | FIntArray vals -> SQL.VIntArray (compileArray vals)
    | FDecimalArray vals -> SQL.VDecimalArray (compileArray vals)
    | FStringArray vals -> SQL.VStringArray (compileArray vals)
    | FBoolArray vals -> SQL.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> SQL.VDateTimeArray (compileArray vals)
    | FDateArray vals -> SQL.VDateArray (compileArray vals)
    | FIntervalArray vals -> SQL.VIntervalArray (compileArray vals)
    | FJsonArray vals -> SQL.VJsonArray (compileArray vals)
    | FUserViewRefArray vals -> SQL.VJsonArray (vals |> Array.map (fun x -> JToken.FromObject(x) |> ComparableJToken) |> compileArray)
    | FUuidArray vals -> SQL.VUuidArray (compileArray vals)
    | FNull -> SQL.VNull

let private compileArgument (placeholderId : PlaceholderId) (arg : ResolvedArgument) : CompiledArgument =
    { PlaceholderId = placeholderId
      FieldType = arg.ArgType
      DbType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType arg.ArgType)
      Optional = arg.Optional
      DefaultValue = arg.DefaultValue
      Attributes = arg.Attributes
    }

let emptyArguments =
    { Types = Map.empty
      NextPlaceholderId = 0
      NextAnonymousId = 0
    }

let addArgument (name : ArgumentRef) (arg : ResolvedArgument) (args : QueryArguments) : CompiledArgument * QueryArguments =
    match Map.tryFind name args.Types with
    | Some oldArg -> (oldArg, args)
    | None ->
        let argId = args.NextPlaceholderId
        let newArg = compileArgument argId arg
        let ret =
            { args with
                  Types = Map.add name newArg args.Types
                  NextPlaceholderId = argId + 1
            }
        (newArg, ret)

let addAnonymousArgument (arg : ResolvedArgument) (args : QueryArguments) : CompiledArgument * ArgumentName * QueryArguments =
    let argId = args.NextPlaceholderId
    let name = sprintf "__%i" args.NextAnonymousId |> FunQLName
    let newArg = compileArgument argId arg
    let args =
        { args with
              Types = Map.add (PLocal name) newArg args.Types
              NextPlaceholderId = argId + 1
              NextAnonymousId = args.NextAnonymousId + 1
        }
    (newArg, name, args)

let mergeArguments (a : QueryArguments) (b : QueryArguments) : QueryArguments =
    { Types = Map.unionUnique a.Types b.Types
      NextPlaceholderId = max a.NextPlaceholderId b.NextPlaceholderId
      NextAnonymousId = max a.NextAnonymousId b.NextAnonymousId
    }

let compileArguments (args : ResolvedArgumentsMap) : QueryArguments =
    let mutable nextPlaceholderId = 0

    let convertArgument name arg =
        let ret = compileArgument nextPlaceholderId arg
        nextPlaceholderId <- nextPlaceholderId + 1
        ret

    let arguments = args |> OrderedMap.toMap |> Map.map convertArgument
    { Types = arguments
      NextPlaceholderId = nextPlaceholderId
      NextAnonymousId = 0
    }

let private typecheckArgument (fieldType : FieldType<_>) (value : FieldValue) : unit =
    match fieldType with
    | FTScalar (SFTEnum vals) ->
        match value with
        | FString str when OrderedSet.contains str vals -> ()
        | FNull -> ()
        | _ -> raisef ArgumentCheckException "Argument is not from allowed values of a enum: %O" value
    | FTArray (SFTEnum vals) ->
        match value with
        | FStringArray strs when Seq.forall (fun str -> OrderedSet.contains str vals) strs -> ()
        | FNull -> ()
        | _ -> raisef ArgumentCheckException "Argument is not from allowed values of a enum: %O" value
    // Most casting/typechecking will be done by database or Npgsql
    | _ -> ()

let prepareArguments (args : QueryArguments) (values : ArgumentValuesMap) : ExprParameters =
    let makeParameter (name : ArgumentRef) (mapping : CompiledArgument) =
        let (notFound, value) =
            match Map.tryFind name values with
            | None ->
                let defValue =
                    match mapping.DefaultValue with
                    | Some def -> def
                    | None when mapping.Optional -> FNull
                    | None -> raisef ArgumentCheckException "Argument not found: %O" name
                (true, defValue)
            | Some value -> (false, value)
        if not (mapping.Optional && notFound) then
            typecheckArgument mapping.FieldType value
        (mapping.PlaceholderId, compileFieldValue value)
    args.Types |> Map.mapWithKeys makeParameter

// Used to compile expressions with some arguments hidden or renamed, and insert changed arguments back.
let modifyArgumentsInNamespace (changeNames : ArgumentRef -> ArgumentRef option) (modify : QueryArguments -> QueryArguments * 'a) (arguments : QueryArguments) : QueryArguments * 'a =
    let convertArgument (name : ArgumentRef) (arg : CompiledArgument) =
        match changeNames name with
        | Some newName -> Some (newName, arg)
        | None -> None
    let nsArgumentsMap = Map.mapWithKeysMaybe convertArgument arguments.Types

    let nsArguments = { arguments with Types = nsArgumentsMap }
    let (newNsArguments, ret) = modify nsArguments

    let maybeInsertArgument (currMap : CompiledArgumentsMap) (name : ArgumentRef, arg : CompiledArgument) =
        if arg.PlaceholderId >= arguments.NextPlaceholderId then
            assert (not <| Map.containsKey name currMap)
            Map.add name arg currMap
        else
            currMap
    let newArgumentTypes = Seq.fold maybeInsertArgument arguments.Types (Map.toSeq newNsArguments.Types)

    let newArguments = { newNsArguments with Types = newArgumentTypes }
    (newArguments, ret)

// We don't check that all required arguments are provided; it happens later.
let convertQueryArguments (globalArgValues : LocalArgumentsMap) (extraArgValues : ArgumentValuesMap) (rawArgValues : RawArguments) (arguments : QueryArguments) : ArgumentValuesMap =
    let findArgument (name : ArgumentRef) (arg : CompiledArgument) =
        match Map.tryFind name extraArgValues with
        | Some arg -> Some arg
        | None ->
            match name with
            | PLocal (FunQLName lname) ->
                match Map.tryFind lname rawArgValues with
                | Some argStr ->
                    match parseValueFromJson arg.FieldType arg.Optional argStr with
                    | None ->  raisef ArgumentCheckException "Cannot convert argument %O to type %O" name arg.FieldType
                    | Some arg -> Some arg
                | None -> None
            | PGlobal gname -> Some <| Map.find gname globalArgValues
    arguments.Types |> Map.mapMaybe findArgument

let convertEntityArguments (entity : ResolvedEntity) (rawArgs : RawArguments) : LocalArgumentsMap =
    let getValue (fieldName : FieldName) (field : ResolvedColumnField) =
        match Map.tryFind (string fieldName) rawArgs with
        | None -> None
        | Some value ->
            match parseValueFromJson field.FieldType field.IsNullable value with
            | None -> raisef ArgumentCheckException "Cannot convert argument %O to type %O" fieldName field.FieldType
            | Some arg -> Some arg
    entity.ColumnFields |> Map.mapMaybe getValue
