module FunWithFlags.FunDB.FunQL.Typecheck

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Typecheck

type ViewTypecheckException (message : string, innerException : exn) =
    inherit Exception(message, innerException)

    new (message : string) = ViewTypecheckException (message, null)

let compileScalarType : ScalarFieldType -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate
    | SFTInterval -> SQL.STInterval
    | SFTJson -> SQL.STJson
    | SFTUserViewRef -> SQL.STJson
    | SFTUuid -> SQL.STUuid

let compileFieldExprType : FieldExprType -> SQL.SimpleValueType = function
    | FETScalar stype -> SQL.VTScalar <| compileScalarType stype
    | FETArray stype -> SQL.VTArray <| compileScalarType stype

let compileFieldType : FieldType<_> -> SQL.SimpleValueType = function
    | FTType fetype -> compileFieldExprType fetype
    | FTReference ent -> SQL.VTScalar SQL.STInt
    | FTEnum vals -> SQL.VTScalar SQL.STString

let decompileScalarType : SQL.SimpleType -> ScalarFieldType = function
    | SQL.STInt -> SFTInt
    | SQL.STBigInt -> failwith "Unexpected bigint encountered"
    | SQL.STRegclass -> failwith "Unexpected regclass encountered"
    | SQL.STDecimal -> SFTDecimal
    | SQL.STString -> SFTString
    | SQL.STBool -> SFTBool
    | SQL.STDateTime -> SFTDateTime
    | SQL.STDate -> SFTDate
    | SQL.STInterval -> SFTInterval
    | SQL.STJson -> SFTJson
    | SQL.STUuid -> SFTUuid

let decompileFieldExprType : SQL.SimpleValueType -> FieldExprType = function
    | SQL.VTScalar typ -> FETScalar <| decompileScalarType typ
    | SQL.VTArray typ -> FETArray <| decompileScalarType typ

let fieldValueType : FieldValue -> ResolvedFieldType option = function
    | FInt _ -> Some <| FTType (FETScalar SFTInt)
    | FDecimal _ -> Some <| FTType (FETScalar SFTDecimal)
    | FString _ -> Some <| FTType (FETScalar SFTString)
    | FBool _ -> Some <| FTType (FETScalar SFTBool)
    | FDateTime _-> Some <| FTType (FETScalar SFTDateTime)
    | FDate _ -> Some <| FTType (FETScalar SFTDate)
    | FInterval _ -> Some <| FTType (FETScalar SFTInterval)
    | FJson _ -> Some <| FTType (FETScalar SFTJson)
    | FUserViewRef _ -> Some <| FTType (FETScalar SFTUserViewRef)
    | FUuid _ -> Some <| FTType (FETScalar SFTUuid)
    | FIntArray _ -> Some <| FTType (FETArray SFTInt)
    | FDecimalArray _ -> Some <| FTType (FETArray SFTDecimal)
    | FStringArray _ -> Some <| FTType (FETArray SFTString)
    | FBoolArray _ -> Some <| FTType (FETArray SFTBool)
    | FDateTimeArray _ -> Some <| FTType (FETArray SFTDateTime)
    | FDateArray _ -> Some <| FTType (FETArray SFTDate)
    | FIntervalArray _ -> Some <| FTType (FETArray SFTInterval)
    | FJsonArray _ -> Some <| FTType (FETArray SFTJson)
    | FUserViewRefArray _ -> Some <| FTType (FETArray SFTUserViewRef)
    | FUuidArray _ -> Some <| FTType (FETArray SFTUuid)
    | FNull -> None

let private eraseFieldType : FieldType<'e> -> FieldExprType = function
    | FTReference ref -> FETScalar SFTInt
    | FTEnum vals -> FETScalar SFTString
    | FTType typ -> typ

let compileBinaryOp = function
    | BOLess -> SQL.BOLess
    | BOLessEq -> SQL.BOLessEq
    | BOGreater -> SQL.BOGreater
    | BOGreaterEq -> SQL.BOGreaterEq
    | BOEq -> SQL.BOEq
    | BONotEq -> SQL.BONotEq
    | BOConcat -> SQL.BOConcat
    | BOLike -> SQL.BOLike
    | BOILike -> SQL.BOILike
    | BONotLike -> SQL.BONotLike
    | BONotILike -> SQL.BONotILike
    | BOMatchRegex -> SQL.BOMatchRegex
    | BOMatchRegexCI -> SQL.BOMatchRegexCI
    | BONotMatchRegex -> SQL.BONotMatchRegex
    | BONotMatchRegexCI -> SQL.BONotMatchRegexCI
    | BOPlus -> SQL.BOPlus
    | BOMinus -> SQL.BOMinus
    | BOMultiply -> SQL.BOMultiply
    | BODivide -> SQL.BODivide
    | BOJsonArrow -> SQL.BOJsonArrow
    | BOJsonTextArrow -> SQL.BOJsonTextArrow

let unionTypes (args : (ResolvedFieldType option) seq) : ResolvedFieldType option =
    let sqlArgs = args |> Seq.map (Option.map compileFieldType)
    SQL.unionTypes sqlArgs |> Option.map (decompileFieldExprType >> FTType)

let private checkFunc (name : FunctionName) (args : (ResolvedFieldType option) seq) : ResolvedFieldType =
    try
        match Map.find name allowedFunctions with
        | FRFunction name ->
            let overloads = Map.find name SQL.sqlKnownFunctions
            let sqlArgs = args |> Seq.map (Option.map compileFieldType) |> Seq.toArray
            match SQL.findFunctionOverloads overloads sqlArgs with
            | None -> raisef ViewTypecheckException "Couldn't deduce function overload"
            | Some (typs, ret) -> FTType <| decompileFieldExprType ret
        | FRSpecial SQL.SFLeast
        | FRSpecial SQL.SFGreatest
        | FRSpecial SQL.SFCoalesce ->
            match unionTypes args with
            | Some ret -> ret
            | None -> raisef ViewTypecheckException "Cannot unify values of different types"
    with
    | :? ViewTypecheckException as e -> raisefWithInner ViewTypecheckException e "In function call %O(%O)" name args

let private checkBinaryOp (op : BinaryOperator) (a : ResolvedFieldType option) (b : ResolvedFieldType option) : ResolvedFieldType =
    let overloads = SQL.binaryOperatorSignature (compileBinaryOp op)
    match SQL.findBinaryOpOverloads overloads (Option.map compileFieldType a) (Option.map compileFieldType b) with
    | None -> raisef ViewTypecheckException "Couldn't deduce operator overload for %O %O %O" a op b
    | Some (typs, ret) -> FTType <| decompileFieldExprType ret

let private scalarJson = FTType (FETScalar SFTJson)

let private scalarString = FTType (FETScalar SFTString)

let private scalarBool = FTType (FETScalar SFTBool)

type private Typechecker (layout : ILayoutBits) =
    let isSubtype (wanted : ResolvedFieldType) (maybeGiven : ResolvedFieldType option) : bool =
        match maybeGiven with
        | None -> true
        | Some given ->
            match (wanted, given) with
            | (FTReference wantedRef, FTReference givenRef) when wantedRef = givenRef -> true
            | (FTReference wantedRef, FTReference givenRef) ->
                let wantedEntity = layout.FindEntity wantedRef |> Option.get
                Map.containsKey givenRef wantedEntity.Children
            | (FTEnum wantedVals, FTEnum givenVals) -> Set.isEmpty (Set.difference givenVals wantedVals)
            | (FTType (FETScalar SFTInt), FTReference _) -> true
            | (FTType (FETScalar SFTString), FTEnum _) -> true
            | (FTType a, FTType b) when a = b -> true
            | (FTType a, FTType b) -> SQL.tryImplicitCasts (compileFieldExprType a) (compileFieldExprType b)
            | _ -> false

    let rec typecheckFieldRef (linked : LinkedBoundFieldRef) : ResolvedFieldType option =
        match linked.Ref.Ref with
        | VRPlaceholder arg -> failwith "Not implemented"
        | VRColumn col ->
            let fieldInfo = ObjectMap.findType<FieldMeta> linked.Extra
            let boundInfo = Option.get fieldInfo.Bound
            let fieldRef =
                if Array.isEmpty linked.Ref.Path then
                    boundInfo.Ref
                else
                    let entityRef = Array.last boundInfo.Path
                    let arrow = Array.last linked.Ref.Path
                    { Entity = entityRef; Name = arrow.Name }
            let entity = layout.FindEntity fieldRef.Entity |> Option.get
            let field = entity.FindField fieldRef.Name |> Option.get
            match field.Field with
            | RId -> Some <| FTReference fieldRef.Entity
            | RSubEntity -> Some scalarJson
            | RColumnField col -> Some col.FieldType
            | RComputedField comp -> comp.Type

    and typecheckBinaryLogical (a : ResolvedFieldExpr) (b : ResolvedFieldExpr) : ResolvedFieldType =
        let ta = typecheckFieldExpr a
        if not <| isSubtype scalarBool ta then
            raisef ViewTypecheckException "Boolean expected, %O found" ta
        let tb = typecheckFieldExpr b
        if not <| isSubtype scalarBool tb then
            raisef ViewTypecheckException "Boolean expected, %O found" tb
        scalarBool

    and typecheckAnyLogical (a : ResolvedFieldExpr) : ResolvedFieldType =
        let inner = typecheckFieldExpr a
        FTType (FETScalar SFTBool)

    and typecheckEqLogical (a : ResolvedFieldExpr) (b : ResolvedFieldExpr) : ResolvedFieldType =
        let ta = typecheckFieldExpr a
        let tb =  typecheckFieldExpr b
        FTType (FETScalar SFTBool)

    and typecheckInValues (e : ResolvedFieldExpr) (vals :ResolvedFieldExpr[]) : ResolvedFieldType =
        let typ = typecheckFieldExpr e
        for v in vals do
            let vt = typecheckFieldExpr v
            ignore <| checkBinaryOp BOEq typ vt
        FTType (FETScalar SFTBool)

    and typecheckLike (e : ResolvedFieldExpr) (pat : ResolvedFieldExpr) : ResolvedFieldType =
        let te = typecheckFieldExpr e
        let tpat = typecheckFieldExpr pat
        ignore <| checkBinaryOp BOLike te tpat
        scalarBool

    and typecheckInArray (e : ResolvedFieldExpr) (op : BinaryOperator) (arr : ResolvedFieldExpr) : ResolvedFieldType =
        let te = typecheckFieldExpr e
        match typecheckFieldExpr arr |> Option.map eraseFieldType with
        | None -> ()
        | Some (FETArray atyp) -> ignore <| checkBinaryOp op te (atyp |> FETScalar |> FTType |> Some)
        | Some (FETScalar ftyp) -> raisef ViewTypecheckException "Array expected, %O found" ftyp
        scalarBool

    and typecheckCase (es : (ResolvedFieldExpr * ResolvedFieldExpr)[]) (els : ResolvedFieldExpr option) : ResolvedFieldType =
        for (check, expr) in es do
            let checkt = typecheckFieldExpr check
            if not <| isSubtype scalarBool checkt then
                raisef ViewTypecheckException "Boolean expected, %O found" checkt

        let allVals = Seq.append (Seq.map snd es) (Option.toSeq els) |> Seq.map typecheckFieldExpr
        match unionTypes allVals with
        | None -> raisef ViewTypecheckException "Couldn't unify CASE argument types"
        | Some typ -> typ

    and typecheckFieldExpr : ResolvedFieldExpr -> ResolvedFieldType option = function
        | FEValue value -> fieldValueType value
        | FERef r -> typecheckFieldRef r
        | FENot e ->
            match typecheckFieldExpr e with
            | Some (FTType (FETScalar SFTBool)) as typ -> typ
            | _ -> None
        | FEAnd (a, b) -> Some <| typecheckBinaryLogical a b
        | FEOr (a, b) -> Some <| typecheckBinaryLogical a b
        | FEBinaryOp (a, op, b) ->
            let ta = typecheckFieldExpr a
            let tb = typecheckFieldExpr b
            Some <| checkBinaryOp op ta tb
        | FEAll (e, op, arr) -> Some <| typecheckInArray e op arr
        | FEAny (e, op, arr) -> Some <| typecheckInArray e op arr
        | FESimilarTo (e, pat) -> Some <| typecheckLike e pat
        | FENotSimilarTo (e, pat) -> Some <| typecheckLike e pat
        | FEDistinct (a, b) -> Some <| typecheckEqLogical a b
        | FENotDistinct (a, b) -> Some <| typecheckEqLogical a b
        | FEIn (e, vals) -> Some <| typecheckInValues e vals
        | FENotIn (e, vals) -> Some <| typecheckInValues e vals
        | FEInQuery (e, query) -> failwith "Not implemented"
        | FENotInQuery (e, query) -> failwith "Not implemented"
        | FECast (e, typ) ->
            // We always expect cast to succeed; worst case it's impossible, whatever.
            Some (FTType typ)
        | FEIsNull e -> Some <| typecheckAnyLogical e
        | FEIsNotNull e -> Some <| typecheckAnyLogical e
        | FECase (es, els) -> Some <| typecheckCase es els
        | FEJsonArray vals ->
            for v in vals do
                ignore <| typecheckFieldExpr v
            Some <| FTType (FETScalar SFTJson)
        | FEJsonObject obj ->
            for KeyValue(name, v) in obj do
                ignore <| typecheckFieldExpr v
            Some <| FTType (FETScalar SFTJson)
        | FEFunc (name, args) ->
            let argTypes = Seq.map typecheckFieldExpr args
            Some <| checkFunc name argTypes
        | FEAggFunc (name, args) -> failwith "Not implemented"
        | FESubquery query -> failwith "Not implemented"
        | FEInheritedFrom (f, nam) -> Some (FTType (FETScalar SFTBool))
        | FEOfType (f, nam) -> Some (FTType (FETScalar SFTBool))

    member this.TypecheckFieldExpr expr = typecheckFieldExpr expr

let typecheckFieldExpr (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ResolvedFieldType option =
    let resolver = Typechecker layout
    resolver.TypecheckFieldExpr expr