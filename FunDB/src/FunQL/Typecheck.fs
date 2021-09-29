module FunWithFlags.FunDB.FunQL.Typecheck

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Typecheck

type ViewTypecheckException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        ViewTypecheckException (message, innerException, isUserException innerException)

    new (message : string) = ViewTypecheckException (message, null, true)

let decompileScalarType : SQL.SimpleType -> ScalarFieldType<_> = function
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

let decompileFieldType : SQL.SimpleValueType -> FieldType<_> = function
    | SQL.VTScalar typ -> FTScalar <| decompileScalarType typ
    | SQL.VTArray typ -> FTArray <| decompileScalarType typ

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
    SQL.unionTypes sqlArgs |> Option.map decompileFieldType

let private checkFunc (name : FunctionName) (args : (ResolvedFieldType option) seq) : ResolvedFieldType =
    try
        match Map.find name allowedFunctions with
        | FRFunction name ->
            let overloads = Map.find name SQL.sqlKnownFunctions
            let sqlArgs = args |> Seq.map (Option.map compileFieldType) |> Seq.toArray
            match SQL.findFunctionOverloads overloads sqlArgs with
            | None -> raisef ViewTypecheckException "Couldn't deduce function overload"
            | Some (typs, ret) -> decompileFieldType ret
        | FRSpecial SQL.SFLeast
        | FRSpecial SQL.SFGreatest
        | FRSpecial SQL.SFCoalesce ->
            match unionTypes args with
            | Some ret -> ret
            | None -> raisef ViewTypecheckException "Cannot unify values of different types"
    with
    | e -> raisefWithInner ViewTypecheckException e "In function call %O%O" name (Seq.toList args)

let private checkBinaryOp (op : BinaryOperator) (a : ResolvedFieldType option) (b : ResolvedFieldType option) : ResolvedFieldType =
    let overloads = SQL.binaryOperatorSignature (compileBinaryOp op)
    match SQL.findBinaryOpOverloads overloads (Option.map compileFieldType a) (Option.map compileFieldType b) with
    | None -> raisef ViewTypecheckException "Couldn't deduce operator overload for %O %O %O" a op b
    | Some (typs, ret) -> decompileFieldType ret

let private scalarJson = FTScalar SFTJson

let private scalarBool = FTScalar SFTBool

type private Typechecker (layout : ILayoutBits) =
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
            | RId -> Some (FTScalar <| SFTReference fieldRef.Entity)
            | RSubEntity -> Some scalarJson
            | RColumnField col -> Some col.FieldType
            | RComputedField comp -> comp.Type

    and typecheckBinaryLogical (a : ResolvedFieldExpr) (b : ResolvedFieldExpr) : ResolvedFieldType =
        let ta = typecheckFieldExpr a
        if not <| isMaybeSubtype layout scalarBool ta then
            raisef ViewTypecheckException "Boolean expected, %O found" ta
        let tb = typecheckFieldExpr b
        if not <| isMaybeSubtype layout scalarBool tb then
            raisef ViewTypecheckException "Boolean expected, %O found" tb
        scalarBool

    and typecheckAnyLogical (a : ResolvedFieldExpr) : ResolvedFieldType =
        let inner = typecheckFieldExpr a
        FTScalar SFTBool

    and typecheckEqLogical (a : ResolvedFieldExpr) (b : ResolvedFieldExpr) : ResolvedFieldType =
        let ta = typecheckFieldExpr a
        let tb =  typecheckFieldExpr b
        FTScalar SFTBool

    and typecheckInValues (e : ResolvedFieldExpr) (vals :ResolvedFieldExpr[]) : ResolvedFieldType =
        let typ = typecheckFieldExpr e
        for v in vals do
            let vt = typecheckFieldExpr v
            ignore <| checkBinaryOp BOEq typ vt
        FTScalar SFTBool

    and typecheckLike (e : ResolvedFieldExpr) (pat : ResolvedFieldExpr) : ResolvedFieldType =
        let te = typecheckFieldExpr e
        let tpat = typecheckFieldExpr pat
        ignore <| checkBinaryOp BOLike te tpat
        scalarBool

    and typecheckInArray (e : ResolvedFieldExpr) (op : BinaryOperator) (arr : ResolvedFieldExpr) : ResolvedFieldType =
        let te = typecheckFieldExpr e
        match typecheckFieldExpr arr with
        | None -> ()
        | Some (FTArray atyp) -> ignore <| checkBinaryOp op te (atyp |> FTScalar |> Some)
        | Some (FTScalar ftyp) -> raisef ViewTypecheckException "Array expected, %O found" ftyp
        scalarBool

    and typecheckCase (es : (ResolvedFieldExpr * ResolvedFieldExpr)[]) (els : ResolvedFieldExpr option) : ResolvedFieldType =
        for (check, expr) in es do
            let checkt = typecheckFieldExpr check
            if not <| isMaybeSubtype layout scalarBool checkt then
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
            | Some (FTScalar SFTBool) as typ -> typ
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
            Some (mapFieldType (tryResolveEntityRef >> Option.get) typ)
        | FEIsNull e -> Some <| typecheckAnyLogical e
        | FEIsNotNull e -> Some <| typecheckAnyLogical e
        | FECase (es, els) -> Some <| typecheckCase es els
        | FEJsonArray vals ->
            for v in vals do
                ignore <| typecheckFieldExpr v
            Some <| FTScalar SFTJson
        | FEJsonObject obj ->
            for KeyValue(name, v) in obj do
                ignore <| typecheckFieldExpr v
            Some <| FTScalar SFTJson
        | FEFunc (name, args) ->
            let argTypes = Seq.map typecheckFieldExpr args
            Some <| checkFunc name argTypes
        | FEAggFunc (name, args) -> failwith "Not implemented"
        | FESubquery query -> failwith "Not implemented"
        | FEInheritedFrom (f, nam) -> Some (FTScalar SFTBool)
        | FEOfType (f, nam) -> Some (FTScalar SFTBool)

    member this.TypecheckFieldExpr expr = typecheckFieldExpr expr

let typecheckFieldExpr (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ResolvedFieldType option =
    let resolver = Typechecker layout
    resolver.TypecheckFieldExpr expr