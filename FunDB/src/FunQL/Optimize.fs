module FunWithFlags.FunDB.FunQL.Optimize

open System
open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

// Actually stringified expression, used for deduplication.
type ExpressionKey = string

type HashedFieldExprs<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName = Map<ExpressionKey, OptimizedFieldExpr<'e, 'f>>

and [<CustomEquality; NoComparison>] OptimizedFieldExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | OFEOr of HashedFieldExprs<'e, 'f>
    | OFEAnd of HashedFieldExprs<'e, 'f>
    | OFEExpr of StringComparable<FieldExpr<'e, 'f>>
    | OFENot of OptimizedFieldExpr<'e, 'f>
    | OFETrue
    | OFEFalse
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFieldExpr () =
            match this with
            | OFEOr vals -> vals |> Map.values |> Seq.map (fun v -> v.ToFieldExpr ()) |> Seq.fold1 (curry FEOr)
            | OFEAnd vals -> vals |> Map.values |> Seq.map (fun v -> v.ToFieldExpr ()) |> Seq.fold1 (curry FEAnd)
            | OFENot expr -> FENot (expr.ToFieldExpr ())
            | OFETrue -> FEValue (FBool true)
            | OFEFalse -> FEValue (FBool false)
            | OFEExpr e -> e.Value

        member this.ToFunQLString () =
            match this with
            | OFEOr vals -> vals |> Map.values |> Seq.map (fun v -> sprintf "(%s)" (v.ToFunQLString())) |> String.concat " OR "
            | OFEAnd vals -> vals |> Map.values |> Seq.map (fun v -> sprintf "(%s)" (v.ToFunQLString())) |> String.concat " AND "
            | OFENot expr -> sprintf "NOT (%s)" (expr.ToFunQLString())
            | OFETrue -> "TRUE"
            | OFEFalse -> "FALSE"
            | OFEExpr e -> e.String

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

        interface IEquatable<OptimizedFieldExpr<'e, 'f>> with
            member this.Equals other =
                match (this, other) with
                | (OFEOr ors1, OFEOr ors2) -> Map.keys ors1 = Map.keys ors2
                | (OFEAnd ands1, OFEAnd ands2) -> Map.keys ands1 = Map.keys ands2
                | (OFEExpr expr1, OFEExpr expr2) -> expr1 = expr2
                | (OFENot e1, OFENot e2) -> e1 = e2
                | (OFETrue, OFETrue)
                | (OFEFalse, OFEFalse) -> true
                | _ -> false

type ResolvedOptimizedFieldExpr = OptimizedFieldExpr<EntityRef, LinkedBoundFieldRef>

let orFieldExpr (a : OptimizedFieldExpr<'e, 'f>) (b : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match (a, b) with
    | (OFEOr ors1, OFEOr ors2) -> OFEOr (Map.union ors1 ors2)
    | (expr, OFETrue)
    | (OFETrue, expr) -> OFETrue
    | (expr, OFEFalse)
    | (OFEFalse, expr) -> expr
    | (OFEOr ors, expr)
    | (expr, OFEOr ors) -> OFEOr (Map.add (string expr) expr ors)
    | ((OFEAnd ands as andsExpr), expr)
    | (expr, (OFEAnd ands as andsExpr)) ->
        let andsStr = string andsExpr
        let exprStr = string expr
        if Map.containsKey exprStr ands then
            expr
        else
            Map.singleton andsStr andsExpr |> Map.add exprStr expr |> OFEOr
    | (expr1, expr2) ->
        let expr1Str = string expr1
        let expr2Str = string expr2
        if expr1Str = expr2Str then
            expr1
        else
            Map.singleton expr1Str expr1 |> Map.add expr2Str expr2 |> OFEOr

let andFieldExpr (a : OptimizedFieldExpr<'e, 'f>) (b : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match (a, b) with
    | (OFEAnd ands1, OFEAnd ands2) -> OFEAnd (Map.union ands1 ands2)
    | (expr, OFETrue)
    | (OFETrue, expr) -> expr
    | (expr, OFEFalse)
    | (OFEFalse, expr) -> OFEFalse
    | (OFEAnd ands, expr)
    | (expr, OFEAnd ands) -> OFEAnd (Map.add (string expr) expr ands)
    | ((OFEOr ors as orsExpr), expr)
    | (expr, (OFEOr ors as orsExpr)) ->
        let orsStr = string orsExpr
        let exprStr = string expr
        if Map.containsKey exprStr ors then
            orsExpr
        else
            Map.singleton orsStr orsExpr |> Map.add exprStr expr |> OFEAnd
    | (expr1, expr2) ->
        let expr1Str = string expr1
        let expr2Str = string expr2
        if expr1Str = expr2Str then
            expr1
        else
            Map.empty |> Map.add expr1Str expr1 |> Map.add expr2Str expr2 |> OFEAnd

let rec notFieldExpr (a : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match a with
    | OFETrue -> OFEFalse
    | OFEFalse -> OFETrue
    | OFENot expr -> expr
    | OFEAnd ands -> OFEOr (invertHashedExprs ands)
    | OFEOr ors -> OFEAnd (invertHashedExprs ors)
    | expr -> OFENot expr

and private invertHashedExprs exprs = exprs |> Map.values |> Seq.map (notFieldExpr >> (fun x -> (string x, x))) |> Map.ofSeq

let optimizeFieldValue : FieldValue -> OptimizedFieldExpr<'e, 'f> = function
    | FBool false -> OFEFalse
    | FBool true -> OFETrue
    | v -> FEValue v |> String.comparable |> OFEExpr

let optimizedIsFalse : OptimizedFieldExpr<'e, 'f> -> bool = function
    | OFEFalse -> true
    | _ -> false

let optimizedIsTrue : OptimizedFieldExpr<'e, 'f> -> bool = function
    | OFETrue -> true
    | _ -> false

let rec optimizeFieldExpr : FieldExpr<'e, 'f> -> OptimizedFieldExpr<'e, 'f> = function
    | FEOr (a, b) -> orFieldExpr (optimizeFieldExpr a) (optimizeFieldExpr b)
    | FEAnd (a, b) -> andFieldExpr (optimizeFieldExpr a) (optimizeFieldExpr b)
    | FENot expr -> notFieldExpr (optimizeFieldExpr expr)
    | FEValue v -> optimizeFieldValue v
    | expr -> OFEExpr (String.comparable expr)

let rec mapOptimizedFieldExpr (f : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>) (e : OptimizedFieldExpr<'e1, 'f1>) : OptimizedFieldExpr<'e2, 'f2> =
    match e with
    | OFEOr ors -> ors |> Map.values |> Seq.map (mapOptimizedFieldExpr f) |> Seq.fold1 orFieldExpr
    | OFEAnd ands -> ands |> Map.values |> Seq.map (mapOptimizedFieldExpr f) |> Seq.fold1 andFieldExpr
    | OFENot expr -> notFieldExpr (mapOptimizedFieldExpr f expr)
    | OFETrue -> OFETrue
    | OFEFalse -> OFEFalse
    | OFEExpr expr -> optimizeFieldExpr (f expr.Value)

let orFieldExprs (exprs : OptimizedFieldExpr<'e, 'f> seq) : OptimizedFieldExpr<'e, 'f> =
    let mutable result = OFEFalse
    let i = exprs.GetEnumerator()
    while not (optimizedIsTrue result) && i.MoveNext() do
        result <- orFieldExpr result i.Current
    result

let andFieldExprs (exprs : OptimizedFieldExpr<'e, 'f> seq) : OptimizedFieldExpr<'e, 'f> =
    let mutable result = OFETrue
    let i = exprs.GetEnumerator()
    while not (optimizedIsFalse result) && i.MoveNext() do
        result <- andFieldExpr result i.Current
    result
