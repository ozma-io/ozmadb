module FunWithFlags.FunDB.FunQL.Typecheck

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Typecheck

type ViewTypecheckException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ViewTypecheckException (message, innerException, isUserException innerException)

    new (message : string) = ViewTypecheckException (message, null, true)

let private optionTypeToString : ResolvedFieldType option -> string = function
    | Some t -> string t
    | None -> "<unknown>"

type FunctionRepr =
    | FRFunction of SQL.SQLName
    | FRSpecial of SQL.SpecialFunction

let allowedFunctions : Map<FunctionName, FunctionRepr> =
    Map.ofList
        [ // Common
          (FunQLName "to_char", FRFunction <| SQL.SQLName "to_char")
          // Numbers
          (FunQLName "abs", FRFunction <| SQL.SQLName "abs")
          (FunQLName "round", FRFunction <| SQL.SQLName "round")
          // Strings
          (FunQLName "upper", FRFunction <| SQL.SQLName "upper")
          (FunQLName "lower", FRFunction <| SQL.SQLName "lower")
          (FunQLName "length", FRFunction <| SQL.SQLName "length")
          (FunQLName "substr", FRFunction <| SQL.SQLName "substr")
          (FunQLName "ltrim", FRFunction <| SQL.SQLName "ltrim")
          (FunQLName "rtrim", FRFunction <| SQL.SQLName "rtrim")
          (FunQLName "btrim", FRFunction <| SQL.SQLName "btrim")
          (FunQLName "split_part", FRFunction <| SQL.SQLName "split_part")
          (FunQLName "replace", FRFunction <| SQL.SQLName "replace")
          (FunQLName "left", FRFunction <| SQL.SQLName "left")
          (FunQLName "strpos", FRFunction <| SQL.SQLName "strpos")
          // Dates
          (FunQLName "age", FRFunction <| SQL.SQLName "age")
          (FunQLName "date_part", FRFunction <| SQL.SQLName "date_part")
          (FunQLName "date_trunc", FRFunction <| SQL.SQLName "date_trunc")
          (FunQLName "isfinite", FRFunction <| SQL.SQLName "isfinite")
          // Special
          (FunQLName "coalesce", FRSpecial SQL.SFCoalesce)
          (FunQLName "least", FRSpecial SQL.SFLeast)
          (FunQLName "greatest", FRSpecial SQL.SFGreatest)
        ]

let private checkAllowedFunctions () =
    allowedFunctions
    |> Map.values |> Seq.mapMaybe (function | FRFunction name -> Some name; | _ -> None)
    |> Seq.forall (fun name -> Map.containsKey name SQL.sqlKnownFunctions)

assert checkAllowedFunctions ()

let private checkFunc (name : FunctionName) (args : (ResolvedFieldType option) seq) : ResolvedFieldType =
    try
        match Map.tryFind name allowedFunctions with
        | Some (FRFunction name) ->
            let overloads = Map.find name SQL.sqlKnownFunctions
            let sqlArgs = args |> Seq.map (Option.map compileFieldType) |> Seq.toArray
            match SQL.findFunctionOverloads overloads sqlArgs with
            | None -> raisef ViewTypecheckException "Couldn't deduce function overload"
            | Some (typs, ret) -> decompileFieldType ret
        | Some (FRSpecial SQL.SFLeast)
        | Some (FRSpecial SQL.SFGreatest)
        | Some (FRSpecial SQL.SFCoalesce) ->
            match unionTypes args with
            | Some ret -> ret
            | None -> raisef ViewTypecheckException "Cannot unify values of different types"
        | None -> raisef ViewTypecheckException "Unknown function"
    with
    | e -> raisefWithInner ViewTypecheckException e "In function call %O(%s)" name (args |> Seq.map optionTypeToString |> String.concat ", ")

let private checkBinaryOp (op : BinaryOperator) (a : ResolvedFieldType option) (b : ResolvedFieldType option) : ResolvedFieldType =
    let overloads = SQL.binaryOperatorSignature (compileBinaryOp op)
    match SQL.findBinaryOpOverloads overloads (Option.map compileFieldType a) (Option.map compileFieldType b) with
    | None -> raisef ViewTypecheckException "Couldn't deduce operator overload for %s %O %s" (optionTypeToString a) op (optionTypeToString b)
    | Some (typs, ret) -> decompileFieldType ret

let private scalarJson = FTScalar SFTJson

let private scalarBool = FTScalar SFTBool

let resolvedRefType (layout : ILayoutBits) (linked : LinkedBoundFieldRef) : ResolvedFieldType option =
    let fieldInfo = ObjectMap.findType<FieldRefMeta> linked.Extra
    if Array.isEmpty linked.Ref.Path then
        fieldInfo.Type
    else
        let lastField = Array.last linked.Ref.Path
        let lastEntity = Array.last fieldInfo.Path
        let entity = layout.FindEntity lastEntity |> Option.get
        let field = entity.FindField lastField.Name |> Option.get
        resolvedFieldType field.Field

type private Typechecker (layout : ILayoutBits) =
    let rec typecheckBinaryLogical (a : ResolvedFieldExpr) (b : ResolvedFieldExpr) : ResolvedFieldType =
        let ta = typecheckFieldExpr a
        if not <| isMaybeSubtype layout scalarBool ta then
            raisef ViewTypecheckException "Boolean expected, %O found" (optionTypeToString ta)
        let tb = typecheckFieldExpr b
        if not <| isMaybeSubtype layout scalarBool tb then
            raisef ViewTypecheckException "Boolean expected, %O found" (optionTypeToString tb)
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
                raisef ViewTypecheckException "Boolean expected, %s found" (optionTypeToString checkt)

        let allVals = Seq.append (Seq.map snd es) (Option.toSeq els) |> Seq.map typecheckFieldExpr
        match unionTypes allVals with
        | None -> raisef ViewTypecheckException "Couldn't unify CASE result types"
        | Some typ -> typ

    and typecheckMatch (expr : ResolvedFieldExpr) (es : (ResolvedFieldExpr * ResolvedFieldExpr)[]) (els : ResolvedFieldExpr option) : ResolvedFieldType =
        let allConds = Seq.append (Seq.singleton expr) (Seq.map fst es) |> Seq.map typecheckFieldExpr
        match unionTypes allConds with
        | None -> raisef ViewTypecheckException "Couldn't unify MATCH condition types"
        | Some ctyp -> ()

        let allVals = Seq.append (Seq.map snd es) (Option.toSeq els) |> Seq.map typecheckFieldExpr
        match unionTypes allVals with
        | None -> raisef ViewTypecheckException "Couldn't unify MATCH result types"
        | Some typ -> typ

    and typecheckFieldExpr : ResolvedFieldExpr -> ResolvedFieldType option = function
        | FEValue value -> fieldValueType value
        | FERef r -> resolvedRefType layout r
        | FEEntityAttr (eref, attr) -> raisef ViewTypecheckException "Not implemented"
        | FEFieldAttr (fref, attr) -> raisef ViewTypecheckException "Not implemented"
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
        | FEInQuery (e, query) -> raisef ViewTypecheckException "Not implemented"
        | FENotInQuery (e, query) -> raisef ViewTypecheckException "Not implemented"
        | FECast (e, typ) ->
            // We always expect cast to succeed; worst case it's impossible, whatever.
            Some (mapFieldType getResolvedEntityRef typ)
        | FEIsNull e -> Some <| typecheckAnyLogical e
        | FEIsNotNull e -> Some <| typecheckAnyLogical e
        | FECase (es, els) -> Some <| typecheckCase es els
        | FEMatch (field, es, els) -> Some <| typecheckMatch field es els
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
        | FEAggFunc (name, args) -> raisef ViewTypecheckException "Not implemented"
        | FESubquery query -> raisef ViewTypecheckException "Not implemented"
        | FEInheritedFrom (f, nam) -> Some (FTScalar SFTBool)
        | FEOfType (f, nam) -> Some (FTScalar SFTBool)

    member this.TypecheckFieldExpr expr = typecheckFieldExpr expr

let typecheckFieldExpr (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ResolvedFieldType option =
    let resolver = Typechecker layout
    resolver.TypecheckFieldExpr expr
