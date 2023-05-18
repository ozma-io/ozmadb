module FunWithFlags.FunDB.SQL.Typecheck

open FSharpPlus

open Microsoft.FSharp.Reflection
open FunWithFlags.FunUtils.Serialization.Utils
open FunWithFlags.FunUtils

open FunWithFlags.FunDB.SQL.AST

// Most of this information is manually taken from pg_catalog. FunDB doesn't support working with arbitrary user-(re)defined operators and functions.

type TypeCategoryName = char

// `bool` signifies if the type is preferred.
type TypeCategoryMap = Map<SimpleValueType, bool>

type TypeCategoriesMap = Map<TypeCategoryName, TypeCategoryMap>

type TypeCategory =
    { Category : TypeCategoryName
      IsPreferred : bool
    }

type ReverseTypeCategoriesMap = Map<SimpleValueType, TypeCategory>

// We expect there are no implicit conversions between arrays and scalars: this is important for inference algorithm.
type HoledValueType =
    | HTScalar of SimpleType option
    | HTArray of SimpleType option
    | HTAny

type HoleArgument = SimpleValueType

type FunctionSignaturesMap = Map<HoledValueType[], HoledValueType>

type UnaryOperatorSignaturesMap = Map<HoledValueType, HoledValueType>

type BinaryOperatorSignaturesMap = Map<HoledValueType * HoledValueType, HoledValueType>

type ImplicitCastsMap = Map<SimpleType, Set<SimpleType>>

let sqlSimpleTypes = unionCases typeof<SimpleType> |> Seq.map (fun case -> FSharpValue.MakeUnion(case.Info, [||]) :?> SimpleType) |> Set.ofSeq

// SELECT * FROM pg_catalog.pg_type WHERE typcategory = 'D';
let sqlTypeCategories : TypeCategoriesMap =
    Map.ofList
        [ ('N', Map.ofList [(VTScalar STDecimal, true); (VTScalar STInt, false); (VTScalar STRegclass, true); (VTScalar STBigInt, false)])
          ('D', Map.ofList [(VTScalar STDateTime, true); (VTScalar STLocalDateTime, false); (VTScalar STDate, false)])
          ('I', Map.ofList [(VTScalar STInterval, true)])
          ('S', Map.ofList [(VTScalar STString, true)])
          ('U', Map.ofList [(VTScalar STUuid, false); (VTScalar STJson, false)])
          ('B', Map.ofList [(VTScalar STBool, true)])
          ('A', sqlSimpleTypes |> Seq.map (fun typ -> (VTArray typ, false)) |> Map.ofSeq)
        ]

let private checkAllCategories () =
    let sqlAllTypes = Seq.append (Seq.map VTScalar sqlSimpleTypes) (Seq.map VTArray sqlSimpleTypes) |> Set.ofSeq
    let foundTypes = sqlTypeCategories |> Map.values |> Seq.collect Map.keys |> Set.ofSeq
    foundTypes = sqlAllTypes

assert checkAllCategories ()

let sqlReverseTypeCategories : ReverseTypeCategoriesMap =
    sqlTypeCategories
    |> Map.toSeq
    |> Seq.collect (fun (cat, typs) -> typs |> Map.toSeq |> Seq.map (fun (typ, preferred) -> (typ, { IsPreferred = preferred; Category = cat })))
    |> Map.ofSeq

let sqlPreferredTypes : Set<SimpleValueType> =
    sqlTypeCategories |> Map.values |> Seq.collect (Map.toSeq >> Seq.mapMaybe (fun (typ, preferred) -> if preferred then Some typ else None)) |> Set.ofSeq

(* Helpful query:
     SELECT
       pg_cast.*,
       srctype.typname AS sourcename,
       targettype.typname AS targetname
     FROM pg_catalog.pg_cast
     LEFT JOIN pg_catalog.pg_type AS srctype ON castsource = srctype.oid
     LEFT JOIN pg_catalog.pg_type AS targettype ON casttarget = targettype.oid
     WHERE castcontext = 'i'
     AND castsource <> casttarget;
*)
let sqlImplicitCasts : ImplicitCastsMap =
    Map.ofList
        [ (STInt, Set.ofList [STDecimal; STBigInt])
          (STBigInt, Set.ofList [STDecimal])
          (STDate, Set.ofList [STDateTime; STLocalDateTime])
          (STLocalDateTime, Set.ofList [STDateTime])
        ]

let tryImplicitCasts (wanted : SimpleType) (given : SimpleType) =
    match Map.tryFind given sqlImplicitCasts with
    | None -> false
    | Some newGiven -> newGiven |> Seq.contains wanted

let tryImplicitValueCasts (wanted : SimpleValueType) (given : SimpleValueType) =
    match (wanted, given) with
    | (VTScalar wantedTyp, VTScalar givenTyp) -> tryImplicitCasts wantedTyp givenTyp
    | (VTArray wantedTyp, VTArray givenTyp) -> tryImplicitCasts wantedTyp givenTyp
    | _ -> false

let canAcceptType (wanted : SimpleValueType) (given : SimpleValueType) =
    if wanted = given then
        true
    else
        tryImplicitValueCasts wanted given

let private specializeHoledType (pl : HoleArgument) (holedTyp : HoledValueType) : SimpleValueType option =
    match holedTyp with
    | HTAny -> Some pl
    | HTScalar None ->
        match pl with
        | VTScalar typ -> Some (VTScalar typ)
        | VTArray typ -> None
    | HTArray None ->
        match pl with
        | VTScalar typ -> Some (VTArray typ)
        | VTArray typ -> None
    | HTScalar (Some typ) -> Some (VTScalar typ)
    | HTArray (Some typ) -> Some (VTArray typ)

let private getHoledType (holedTyp : HoledValueType) : SimpleValueType =
    match holedTyp with
    | HTScalar (Some typ) -> VTScalar typ
    | HTArray (Some typ) -> VTArray typ
    | _ -> failwith "Unexpected hole in type"

let private inferHole (holedTyp : HoledValueType) (comparedTyp : SimpleValueType) : Result<HoleArgument option, unit> =
    match holedTyp with
    | HTAny -> Ok (Some comparedTyp)
    | HTScalar None ->
        match comparedTyp with
        | VTScalar typ -> Ok (Some (VTScalar typ))
        | VTArray typ -> Error ()
    | HTArray None ->
        match comparedTyp with
        | VTArray typ -> Ok (Some (VTScalar typ))
        | VTScalar typ -> Error ()
    | HTScalar (Some typ)
    | HTArray (Some typ) -> Ok None

let private funScalarsToSignatures (signs : (SimpleType list * SimpleType) seq) : FunctionSignaturesMap =
    signs |> Seq.map (fun (args, ret) -> (List.map (Some >> HTScalar) args |> Array.ofList, HTScalar (Some ret))) |> Map.ofSeq

let private funScalarIdSignatures (signs : SimpleType seq) : FunctionSignaturesMap =
    signs |> Seq.map (fun typ -> ([typ], typ)) |> funScalarsToSignatures

let private toCharSignatures : FunctionSignaturesMap =
    [STDateTime; STLocalDateTime; STInterval; STInt; STDecimal] |> Seq.map (fun typ -> ([typ; STString], STString)) |> funScalarsToSignatures

let sqlKnownFunctions : Map<FunctionName, FunctionSignaturesMap> =
    Map.ofList
        [ // Common
          (SQLName "to_char", toCharSignatures)
          // Numbers
          (SQLName "abs", funScalarIdSignatures [STInt; STDecimal])
          (SQLName "round", funScalarIdSignatures [STInt; STDecimal])
          // Strings
          (SQLName "upper", funScalarIdSignatures [STString])
          (SQLName "lower", funScalarIdSignatures [STString])
          (SQLName "length", funScalarsToSignatures [([STString], STInt)])
          (SQLName "substr", funScalarsToSignatures [([STString; STInt], STString); ([STString; STInt; STInt], STString)])
          (SQLName "ltrim", funScalarsToSignatures [([STString], STString); ([STString; STString], STString)])
          (SQLName "rtrim", funScalarsToSignatures [([STString], STString); ([STString; STString], STString)])
          (SQLName "btrim", funScalarsToSignatures [([STString], STString); ([STString; STString], STString)])
          (SQLName "split_part", funScalarsToSignatures [([STString; STString; STInt], STString)])
          (SQLName "replace", funScalarsToSignatures [([STString; STString; STString], STString)])
          (SQLName "left", funScalarsToSignatures [([STString; STInt], STString)])
          (SQLName "strpos", funScalarsToSignatures [([STString; STString], STInt)])
          // Dates
          (SQLName "age", funScalarsToSignatures [([STDateTime; STDateTime], STInterval); ([STDateTime], STInterval)])
          (SQLName "date_part", funScalarsToSignatures [([STString; STDateTime], STDecimal); ([STString; STInterval], STDecimal)])
          (SQLName "date_trunc", funScalarsToSignatures [([STString; STLocalDateTime], STLocalDateTime); ([STString; STDateTime], STDateTime); ([STString; STInterval], STInterval)])
          (SQLName "isfinite", funScalarsToSignatures [([STDate], STBool); ([STDateTime], STBool); ([STDate], STBool)])
        ]

let private binScalarsToSignatures (signs : ((SimpleType * SimpleType) * SimpleType) seq) : BinaryOperatorSignaturesMap =
    signs |> Seq.map (fun ((a, b), ret) -> ((HTScalar (Some a), HTScalar (Some b)), HTScalar (Some ret))) |> Map.ofSeq

let private symmetricOpSignatures (signatures : ((SimpleType * SimpleType) * SimpleType) seq) : BinaryOperatorSignaturesMap =
    Seq.append (signatures |> Seq.map (fun ((a, b), ret) -> ((a, b), ret))) (signatures |> Seq.map (fun ((a, b), ret) -> ((b, a), ret)))
    |> binScalarsToSignatures

let private compareOverloads =
    let same = Map.ofList [((HTAny, HTAny), HTScalar (Some STBool))]
    let notsame =
        [ (STDate, STDateTime)
          (STDate, STLocalDateTime)
          (STDateTime, STLocalDateTime)
        ] |> Seq.map (fun pair -> (pair, STBool)) |> symmetricOpSignatures
    Map.unionUnique same notsame

let private concatOverloads =
    Map.ofList
        [ ((HTArray None, HTArray None), HTArray None)
          ((HTArray None, HTScalar None), HTArray None)
          ((HTScalar None, HTArray None), HTArray None)
          ((HTScalar (Some STString), HTScalar None), HTScalar (Some STString))
          ((HTScalar None, HTScalar (Some STString)), HTScalar (Some STString))
        ]

let private likeOverloads =
    [ ((STString, STString), STBool)
    ] |> binScalarsToSignatures

let private plusOverloads =
    [ ((STInt, STInt), STInt)
      ((STDecimal, STDecimal), STDecimal)
      ((STDate, STInterval), STDate)
      ((STDate, STInt), STDate)
      ((STInterval, STInterval), STInterval)
      ((STDateTime, STInterval), STDateTime)
      ((STLocalDateTime, STInterval), STLocalDateTime)
    ] |> symmetricOpSignatures

let private minusOverloads =
    [ ((STInt, STInt), STInt)
      ((STDecimal, STDecimal), STDecimal)
      ((STDate, STDate), STInt)
      ((STDate, STInt), STDate)
      ((STDate, STInterval), STDateTime)
      ((STDateTime, STInterval), STDateTime)
      ((STInterval, STInterval), STInterval)
      ((STDateTime, STDateTime), STInterval)
      ((STLocalDateTime, STLocalDateTime), STInterval)
    ] |> binScalarsToSignatures

let private multiplyOverloads =
    [ ((STInt, STInt), STInt)
      ((STDecimal, STDecimal), STDecimal)
      ((STInterval, STDecimal), STInterval)
    ] |> symmetricOpSignatures

let private divideOverloads =
    [ ((STInt, STInt), STInt)
      ((STDecimal, STDecimal), STDecimal)
      ((STInterval, STDecimal), STInterval)
    ] |> binScalarsToSignatures

let private jsonArrowOverloads =
    [ ((STJson, STString), STJson)
      ((STJson, STInt), STJson)
    ] |> binScalarsToSignatures

let private jsonTextArrowOverloads =
    [ ((STJson, STString), STString)
      ((STJson, STInt), STString)
    ] |> binScalarsToSignatures

let binaryOperatorSignature = function
    | BOLess
    | BOLessEq
    | BOGreater
    | BOGreaterEq
    | BOEq
    | BONotEq -> compareOverloads
    | BOConcat -> concatOverloads
    | BOLike
    | BOILike
    | BONotLike
    | BONotILike
    | BOMatchRegex
    | BOMatchRegexCI
    | BONotMatchRegex
    | BONotMatchRegexCI -> likeOverloads
    | BOPlus -> plusOverloads
    | BOMinus -> minusOverloads
    | BOMultiply -> multiplyOverloads
    | BODivide -> divideOverloads
    | BOJsonArrow -> jsonArrowOverloads
    | BOJsonTextArrow -> jsonTextArrowOverloads

let private expandHoledSignatures (signature : HoledValueType[]) (ret : HoledValueType) (typs : SimpleValueType option[]) : (SimpleValueType[] * SimpleValueType) seq =
    let addArgument pls (hole, mtyp) =
        let typ = Option.defaultValue (VTScalar STString) mtyp
        match inferHole hole typ with
        | Error () -> None
        | Ok None -> Some pls
        | Ok (Some pl) -> Some (Set.add pl pls)
    match Seq.zip signature typs |> Seq.foldOption addArgument Set.empty with
    | None -> Seq.empty
    | Some pls when Set.isEmpty pls ->
        Seq.singleton (Array.map getHoledType signature, getHoledType ret)
    | Some pls ->
        let specializeType pl =
            match Seq.traverseOption (specializeHoledType pl) signature with
            | None -> None
            | Some args ->
                match specializeHoledType pl ret with
                | None -> None
                | Some rtyp -> Some (Seq.toArray args, rtyp)
        pls |> Seq.mapMaybe specializeType

let rec private trySignature (inexactNum : int) (notPreferredNum : int) (compared : (SimpleValueType * SimpleValueType option) seq) : (int * int) option =
    match compared with
    | Seq.Snoc ((sigTyp, margTyp), rest) ->
        match margTyp with
        | Some argTyp when argTyp = sigTyp -> trySignature inexactNum notPreferredNum rest
        | Some argTyp when tryImplicitValueCasts sigTyp argTyp ->
            let notPreferredNum =
                if Set.contains sigTyp sqlPreferredTypes then
                    notPreferredNum
                else
                    notPreferredNum + 1
            trySignature (inexactNum + 1) notPreferredNum rest
        | Some argTyp ->
            None
        | None ->
            let notPreferredNum =
                if Set.contains sigTyp sqlPreferredTypes then
                    notPreferredNum
                else
                    notPreferredNum + 1
            trySignature (inexactNum + 1) notPreferredNum rest
    | _ -> Some (inexactNum, notPreferredNum)

// Implements steps 3.e and 3.f from https://www.postgresql.org/docs/current/typeconv-oper.html
let private filterOverloadsByUnknowns (considered : (SimpleValueType[] * SimpleValueType)[]) (args : SimpleValueType option[]) =
    if Array.length considered = 1 then
        Some considered.[0]
    else
        let allUnknowns = args |> Seq.mapiMaybe (fun i arg -> if Option.isNone arg then Some i else None) |> Seq.toArray
        if Array.isEmpty allUnknowns then
            None
        else
            // 3.e
            let checkUnknownByOthers i =
                let stringSignatures = considered |> Seq.mapiMaybe (fun signI (sign, ret) -> if sign.[i] = VTScalar STString then Some signI else None) |> Set.ofSeq
                if not <| Set.isEmpty stringSignatures then
                    stringSignatures
                else
                    let (firstSign, firstRet) = considered.[0]
                    let firstCat = Map.find firstSign.[i] sqlReverseTypeCategories
                    let checkCategory preferred (i, (sign : SimpleValueType[], ret)) =
                        let cat = Map.find sign.[i] sqlReverseTypeCategories
                        if cat.Category = firstCat.Category then
                            if cat.IsPreferred then
                                Some <| Set.add i preferred
                            else
                                Some preferred
                        else
                            None
                    match considered |> Seq.indexed |> Seq.foldOption checkCategory Set.empty with
                    | None -> Set.ofSeq (seq { 0..considered.Length - 1 })
                    | Some preferredSigns when Set.isEmpty preferredSigns -> Set.ofSeq (seq { 0..considered.Length - 1 })
                    | Some preferredSigns -> preferredSigns

            let otherUnknownSets = allUnknowns |> Seq.map checkUnknownByOthers |> Seq.fold Set.intersect Set.empty
            let considered =
                if Set.isEmpty otherUnknownSets then
                    considered
                else
                    considered |> Seq.filteri (fun i _ -> Set.contains i otherUnknownSets) |> Seq.toArray
            if Array.length considered = 1 then
                Some considered.[0]
            else
                // 3.f
                match args |> Seq.catMaybes |> Seq.first with
                | None -> None
                | Some firstTyp ->
                    if args |> Seq.forall (function | None -> true; | Some otherTyp -> otherTyp = firstTyp) |> not then
                        None
                    else
                        let checkFilledTypeSignature (sign : SimpleValueType[], ret) =
                            allUnknowns |> Seq.forall (fun i -> canAcceptType sign.[i] firstTyp)
                        let considered = considered |> Seq.filter checkFilledTypeSignature |> Seq.toArray
                        if Array.length considered = 1 then
                            Some considered.[0]
                        else
                            None

// https://www.postgresql.org/docs/current/typeconv-func.html
let findFunctionOverloads (signatures : FunctionSignaturesMap) (args : SimpleValueType option[]) : (SimpleValueType[] * SimpleValueType) option =
    let expandOne (sign, ret) =
        if Array.length sign <> Array.length args then
            Seq.empty
        else
            expandHoledSignatures sign ret args
    let expandedSignatures = signatures |> Map.toSeq |> Seq.collect expandOne

    let sortedSignatures =
        expandedSignatures
        |> Seq.mapMaybe (fun (sign, ret) -> trySignature 0 0 (Seq.zip sign args) |> Option.map (fun key -> (key, (sign, ret))))
        |> Seq.sortBy fst
        |> Seq.toArray

    if Array.isEmpty sortedSignatures then
        None
    else
        let ((minErrors, minInexact) as minKey, firstFunc) = sortedSignatures.[0]
        if minErrors = 0 then
            Some firstFunc
        else
            let considered = sortedSignatures |> Seq.takeWhile (fun (key, pair) -> key = minKey) |> Seq.map snd |> Seq.toArray
            filterOverloadsByUnknowns considered args

// https://www.postgresql.org/docs/current/typeconv-oper.html
let findBinaryOpOverloads (signatures : BinaryOperatorSignaturesMap) (a : SimpleValueType option) (b : SimpleValueType option) : ((SimpleValueType * SimpleValueType) * SimpleValueType) option =
    let convertBack (sign : SimpleValueType[], ret) =
        ((sign.[0], sign.[1]), ret)

    let argsArray = [|a; b|]

    let expandOne ((sa, sb), ret) =
        expandHoledSignatures [|sa; sb|] ret argsArray
    let expandedSignatures = signatures |> Map.toSeq |> Seq.collect expandOne

    let sortedSignatures =
        expandedSignatures
        |> Seq.mapMaybe (fun (sign, ret) -> trySignature 0 0 (Seq.zip sign argsArray) |> Option.map (fun key -> (key, (sign, ret))))
        |> Seq.sortBy fst
        |> Seq.toArray
    if Array.isEmpty sortedSignatures then
        None
    else
        let ((minErrors, minInexact) as minKey, firstFunc) = sortedSignatures.[0]
        if minErrors = 0 then
            Some <| convertBack firstFunc
        else
            let considered = sortedSignatures |> Seq.takeWhile (fun (key, pair) -> key = minKey) |> Seq.map snd |> Seq.toArray
            // 2.a
            let checkArgs =
                match (a, b) with
                | (Some _, Some _)
                | (None, None) -> None
                | (Some one, None)
                | (None, Some one) -> Some [|one; one|]
            let checkFound = checkArgs |> Option.bind (fun cargs -> considered |> Seq.tryFind (fun (sign, ret) -> sign = cargs))
            match checkFound with
            | Some foundFunc -> Some <| convertBack foundFunc
            | None -> filterOverloadsByUnknowns considered argsArray |> Option.map convertBack

// https://www.postgresql.org/docs/current/typeconv-union-case.html
let unionTypes (args : (SimpleValueType option) seq) : SimpleValueType option =
    let notUnknownArgs = args |> Seq.catMaybes |> Seq.toArray
    if Array.isEmpty notUnknownArgs then
        Some <| VTScalar STString
    else
        let checkCandidateType candidateTyp argTyp =
            if (Map.find candidateTyp sqlReverseTypeCategories).IsPreferred then
                Error candidateTyp
            else
                if candidateTyp = argTyp then
                    Ok candidateTyp
                else if tryImplicitValueCasts argTyp candidateTyp && not (tryImplicitValueCasts candidateTyp argTyp) then
                    Ok argTyp
                else
                    Ok candidateTyp

        let firstTyp = notUnknownArgs.[0]
        let candidateTyp = Seq.foldResult checkCandidateType firstTyp notUnknownArgs |> Result.either id id
        if notUnknownArgs |> Seq.forall (fun typ -> canAcceptType candidateTyp typ) then
            Some candidateTyp
        else
            None

let valueSimpleType : Value -> SimpleValueType option = function
    | VInt i -> Some <| VTScalar STInt
    | VBigInt i -> Some <| VTScalar STBigInt
    | VDecimal d -> Some <| VTScalar STDecimal
    | VString s -> Some <| VTScalar STString
    | VBool b -> Some <| VTScalar STBool
    | VDateTime dt -> Some <| VTScalar STDateTime
    | VLocalDateTime dt -> Some <| VTScalar STLocalDateTime
    | VDate dt -> Some <| VTScalar STDate
    | VInterval int -> Some <| VTScalar STInterval
    | VRegclass rc -> Some <| VTScalar STRegclass
    | VJson j -> Some <| VTScalar STJson
    | VUuid u -> Some <| VTScalar STUuid
    | VIntArray vals -> Some <| VTArray STInt
    | VBigIntArray vals -> Some <| VTArray STBigInt
    | VDecimalArray vals -> Some <| VTArray STDecimal
    | VStringArray vals -> Some <| VTArray STString
    | VBoolArray vals -> Some <| VTArray STBool
    | VDateTimeArray vals -> Some <| VTArray STDateTime
    | VLocalDateTimeArray vals -> Some <| VTArray STLocalDateTime
    | VDateArray vals -> Some <| VTArray STDate
    | VIntervalArray vals -> Some <| VTArray STInterval
    | VRegclassArray vals -> Some <| VTArray STRegclass
    | VJsonArray vals -> Some <| VTArray STJson
    | VUuidArray vals -> Some <| VTArray STUuid
    | VNull -> None
