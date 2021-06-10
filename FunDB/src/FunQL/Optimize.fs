module FunWithFlags.FunDB.FunQL.Optimize

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST

// Actually stringified expression, used for deduplication.
type ExpressionKey = string

type HashedFieldExprs<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName = Map<ExpressionKey, OptimizedFieldExpr<'e, 'f>>

and [<NoEquality; NoComparison>] OptimizedFieldExpr<'e, 'f> when 'e :> IFunQLName and 'f :> IFunQLName =
    | OFEOr of HashedFieldExprs<'e, 'f>
    | OFEAnd of HashedFieldExprs<'e, 'f>
    | OFEExpr of StringComparable<FieldExpr<'e, 'f>>
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
            | OFEExpr e -> e.Value

        member this.ToFunQLString () =
            match this with
            | OFEOr vals -> vals |> Map.values |> Seq.map (fun v -> sprintf "(%s)" (v.ToFunQLString())) |> String.concat " OR "
            | OFEAnd vals -> vals |> Map.values |> Seq.map (fun v -> sprintf "(%s)" (v.ToFunQLString())) |> String.concat " AND "
            | OFETrue -> "TRUE"
            | OFEFalse -> "FALSE"
            | OFEExpr e -> e.String

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ResolvedOptimizedFieldExpr = OptimizedFieldExpr<EntityRef, LinkedBoundFieldRef>

let orFieldExpr (a : OptimizedFieldExpr<'e, 'f>) (b : OptimizedFieldExpr<'e, 'f>) : OptimizedFieldExpr<'e, 'f> =
    match (a, b) with
    | (OFEOr ors1, OFEOr ors2) -> OFEOr (Map.union ors1 ors2)
    | (expr, OFETrue)
    | (OFETrue, expr) -> OFETrue
    | (expr, OFEFalse)
    | (OFEFalse, expr) -> expr
    | (OFEOr ors, expr)
    | (expr, OFEOr ors) -> OFEOr (Map.add (expr.ToString()) expr ors)
    | (expr1, expr2) ->
        let expr1Str = expr1.ToString()
        let expr2Str = expr2.ToString()
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
    | (expr, OFEAnd ands) -> OFEAnd (Map.add (expr.ToString()) expr ands)
    | (expr1, expr2) ->
        let expr1Str = expr1.ToString()
        let expr2Str = expr2.ToString()
        if expr1Str = expr2Str then
            expr1
        else
            Map.empty |> Map.add expr1Str expr1 |> Map.add expr2Str expr2 |> OFEAnd

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
    | FEValue v -> optimizeFieldValue v
    | expr -> OFEExpr (String.comparable expr)

let rec mapOptimizedFieldExpr (f : FieldExpr<'e1, 'f1> -> FieldExpr<'e2, 'f2>) (e : OptimizedFieldExpr<'e1, 'f1>) : OptimizedFieldExpr<'e2, 'f2> =
    match e with
    | OFEOr ors -> ors |> Map.values |> Seq.map (mapOptimizedFieldExpr f) |> Seq.fold1 orFieldExpr
    | OFEAnd ors -> ors |> Map.values |> Seq.map (mapOptimizedFieldExpr f) |> Seq.fold1 andFieldExpr
    | OFETrue -> optimizeFieldExpr (f (FEValue (FBool true)))
    | OFEFalse -> optimizeFieldExpr (f (FEValue (FBool false)))
    | OFEExpr expr -> optimizeFieldExpr (f expr.Value)