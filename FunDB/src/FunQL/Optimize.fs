module FunWithFlags.FunDB.FunQL.Optimize

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

// Actually stringified expression, used for deduplication.
type ExpressionKey = string

[<NoComparison>]
type OptimizedFieldExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | OFEOr of Map<ExpressionKey, OptimizedFieldExpr<'e, 'f>>
    | OFEAnd of Map<ExpressionKey, OptimizedFieldExpr<'e, 'f>>
    | OFEExpr of FieldExpr<'e, 'f>
    | OFETrue
    | OFEFalse
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFieldExpr () =
            match this with
            | OFEOr vals -> vals |> Map.values |> Seq.map (fun v -> v.ToFieldExpr ()) |> Seq.fold1 (curry FEOr)
            | OFEAnd vals -> vals |> Map.values |> Seq.map (fun v -> v.ToFieldExpr ()) |> Seq.fold1 (curry FEAnd)
            | OFETrue -> FEValue (FBool true)
            | OFEFalse -> FEValue (FBool false)
            | OFEExpr e -> e

        member this.ToFunQLString () = this.ToFieldExpr().ToFunQLString()

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ResolvedOptimizedFieldExpr = OptimizedFieldExpr<ResolvedEntityRef, LinkedBoundFieldRef>

let orFieldExpr (a : OptimizedFieldExpr<'e, 'f>) (b : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match (a, b) with
    | (OFEOr ors1, OFEOr ors2) -> OFEOr (Map.union ors1 ors2)
    | (OFEOr ors, OFETrue)
    | (OFETrue, OFEOr ors) -> OFETrue
    | (expr, OFEFalse)
    | (OFEFalse, expr) -> expr
    | (OFEOr ors, expr)
    | (expr, OFEOr ors) -> OFEOr (Map.add (expr.ToFieldExpr().ToString()) expr ors)
    | (expr1, expr2) -> Map.empty |> Map.add (expr1.ToFieldExpr().ToString()) expr1 |> Map.add (expr2.ToFieldExpr().ToString()) expr2 |> OFEOr

let andFieldExpr (a : OptimizedFieldExpr<'e, 'f>) (b : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match (a, b) with
    | (OFEAnd ands1, OFEAnd ands2) -> OFEAnd (Map.union ands1 ands2)
    | (expr, OFETrue)
    | (OFETrue, expr) -> expr
    | (OFEAnd ands, OFEFalse)
    | (OFEFalse, OFEAnd ands) -> OFEFalse
    | (OFEAnd ands, expr)
    | (expr, OFEAnd ands) -> OFEAnd (Map.add (expr.ToFieldExpr().ToString()) expr ands)
    | (expr1, expr2) -> Map.empty |> Map.add (expr1.ToFieldExpr().ToString()) expr1 |> Map.add (expr2.ToFieldExpr().ToString()) expr2 |> OFEAnd

let optimizeFieldValue : FieldValue -> OptimizedFieldExpr<'e, 'f> = function
    | FBool false -> OFEFalse
    | FBool true -> OFETrue
    | v -> OFEExpr (FEValue v)

let rec optimizeFieldExpr : FieldExpr<'e, 'f> -> OptimizedFieldExpr<'e, 'f> = function
    | FEOr (a, b) -> orFieldExpr (optimizeFieldExpr a) (optimizeFieldExpr b)
    | FEAnd (a, b) -> andFieldExpr (optimizeFieldExpr a) (optimizeFieldExpr b)
    | FEValue v -> optimizeFieldValue v
    | expr -> OFEExpr expr

let rec mapOptimizedFieldExpr (mapper : FieldExprMapper<'e1, 'f1, 'e2, 'f2>) (e : OptimizedFieldExpr<'e1, 'f1>) : OptimizedFieldExpr<'e2, 'f2> =
    let mapInsides (ie : OptimizedFieldExpr<'e1, 'f1>) =
        let r = mapOptimizedFieldExpr mapper ie
        (r.ToFieldExpr().ToString(), r)

    match e with
    | OFEOr ors -> ors |> Map.values |> Seq.map mapInsides |> Map.ofSeq |> OFEOr
    | OFEAnd ors -> ors |> Map.values |> Seq.map mapInsides |> Map.ofSeq |> OFEAnd
    | OFETrue -> optimizeFieldValue (mapper.value (FBool true))
    | OFEFalse -> optimizeFieldValue (mapper.value (FBool false))
    | OFEExpr expr -> mapFieldExpr mapper expr |> OFEExpr