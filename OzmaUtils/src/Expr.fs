[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Expr

open System
open System.Reflection
open System.Linq.Expressions
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Linq.RuntimeHelpers

// http://www.fssnip.net/bx/title/Expanding-quotations

/// The parameter 'vars' is an immutable map that assigns expressions to variables
/// (as we recursively process the tree, we replace all known variables)
let rec private expand' (vars: Map<Var, Expr>) (expr: Expr) : Expr =
    // First recursively process & replace variables
    let expanded =
        match expr with
        // If the variable has an assignment, then replace it with the expression
        | ExprShape.ShapeVar v when Map.containsKey v vars -> vars.[v]
        // Apply 'expand' recursively on all sub-expressions
        | ExprShape.ShapeVar v -> Expr.Var v
        | Patterns.Call(body, DerivedPatterns.MethodWithReflectedDefinition meth, args) ->
            let this =
                match body with
                | Some b -> Expr.Application(meth, b)
                | _ -> meth

            let res = Expr.Applications(this, [ for a in args -> [ a ] ])
            expand' vars res
        | ExprShape.ShapeLambda(v, expr) -> Expr.Lambda(v, expand' vars expr)
        | ExprShape.ShapeCombination(o, exprs) -> ExprShape.RebuildShapeCombination(o, List.map (expand' vars) exprs)

    // After expanding, try reducing the expression - we can replace 'let'
    // expressions and applications where the first argument is lambda
    match expanded with
    | Patterns.Application(ExprShape.ShapeLambda(v, body), assign)
    | Patterns.Let(v, assign, body) -> expand' (Map.add v (expand' vars assign) vars) body
    | _ -> expanded

let expand (expr: Expr<'a>) : Expr<'a> = Expr.Cast(expand' Map.empty expr)

let rec private assignmentToMemberBinding
    (obj: Var)
    (bindings: MemberBinding list)
    : Expr -> (MemberBinding list) option =
    function
    | Patterns.Sequential(next, Patterns.PropertySet(Some(Patterns.Var obj'), propertyInfo, [], expr)) when obj = obj' ->
        let bind =
            Expression.Bind(propertyInfo :> MemberInfo, LeafExpressionConverter.QuotationToExpression expr)
            :> MemberBinding

        assignmentToMemberBinding obj (bind :: bindings) next
    | Patterns.Value(null, t) when t = typeof<unit> -> Some bindings
    | e -> failwithf "Unexpected expression: %O" e

let toMemberInit (expr: Expr<'arg -> 'o>) : Expression<Func<'arg, 'o>> =
    match expr with
    | Patterns.Lambda(v,
                      Patterns.Let(obj,
                                   Patterns.NewObject(constr, args),
                                   Patterns.Sequential(assignments, Patterns.Var obj'))) when obj = obj' ->
        match assignmentToMemberBinding obj [] assignments with
        | Some members ->
            let newExpr =
                Expression.New(constr, Seq.map LeafExpressionConverter.QuotationToExpression args)

            let init = Expression.MemberInit(newExpr, members)
            let parExpr = Expression.Parameter(v.Type, v.Name)
            Expression.Lambda(init :> Expression, [| parExpr |]) :?> Expression<Func<'arg, 'o>>
        | None -> failwithf "Failed to convert assignments to member init: %O" assignments
    | e -> failwithf "Unexpected expression: %O" e

let toExpressionFunc (expr: Expr<'a -> 'b>) : Expression<Func<'a, 'b>> =
    LeafExpressionConverter.QuotationToLambdaExpression(expand <@ Func<_, _>(fun x -> (%expr) x) @>)
