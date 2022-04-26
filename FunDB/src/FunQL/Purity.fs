module FunWithFlags.FunDB.FunQL.Purity

open FunWithFlags.FunUtils

open FunWithFlags.FunDB.SQL.Purity
open FunWithFlags.FunDB.FunQL.AST

let private foldPuritySeq = Seq.foldOption (fun acc curr -> unionPurityOption (Some acc) curr) Pure

let rec private checkPureSubSelectTreeExpr : ResolvedSelectTreeExpr -> PurityStatus option = function
    | SSelect query -> checkPureSubSingleSelectExpr query
    | SValues vals -> vals |> Seq.concat |> Seq.map checkPureFieldExpr |> foldPuritySeq
    | SSetOp setOp -> None

and private checkPureSubQueryColumnResult (result : ResolvedQueryColumnResult) : PurityStatus option =
    seq {
        yield checkPureFieldExpr result.Result
        yield! result.Attributes |> Map.values |> Seq.map checkPureSubAttribute
    } |> foldPuritySeq

and private checkPureSubQueryResult : ResolvedQueryResult -> PurityStatus option = function
    | QRAll _ -> Some Pure
    | QRExpr e -> checkPureSubQueryColumnResult e

and private checkPureSubAttribute : ResolvedBoundAttribute -> PurityStatus option = function
    | BAExpr e -> checkPureFieldExpr e
    // Mappings in subexpressions are considered pure -- impurity will come from a reference.
    | BAMapping mapping -> Some Pure

and private checkPureSubFromExpr : ResolvedFromExpr -> PurityStatus option = function
    | FEntity _ ->
        // TODO: check is table is from CTE, then it might still be pure.
        None
    | FSubExpr subExpr -> checkPureSubSelectExpr subExpr.Select
    | FJoin join ->
        seq {
            yield checkPureSubFromExpr join.A
            yield checkPureSubFromExpr join.B
            yield checkPureFieldExpr join.Condition
        } |> foldPuritySeq

and private checkPureSubSingleSelectExpr (select : ResolvedSingleSelectExpr) : PurityStatus option =
    seq {
        yield! select.Results |> Seq.map checkPureSubQueryResult
        yield! select.From |> Option.toSeq |> Seq.map checkPureSubFromExpr
        yield! select.GroupBy |> Seq.map checkPureFieldExpr
        yield checkPureSubOrderLimitExpr select.OrderLimit
        yield! select.Where |> Option.toSeq |> Seq.map checkPureFieldExpr
    } |> foldPuritySeq

and private checkPureSubOrderLimitExpr (orderLimit : ResolvedOrderLimitClause) : PurityStatus option =
    seq {
        yield! orderLimit.Limit |> Option.toSeq |> Seq.map checkPureFieldExpr
        yield! orderLimit.Offset |> Option.toSeq |> Seq.map checkPureFieldExpr
        yield! orderLimit.OrderBy |> Seq.map (fun order -> checkPureFieldExpr order.Expr)
    } |> foldPuritySeq

and private checkPureSubCommonTableExprs (exprs : ResolvedCommonTableExprs) : PurityStatus option =
    exprs.Exprs |> Seq.map (fun (name, expr) -> checkPureSubSelectExpr expr.Expr) |> foldPuritySeq

and private checkPureSubDataExpr : ResolvedDataExpr -> PurityStatus option = function
    | DESelect sel -> checkPureSubSelectExpr sel
    | DEInsert _ -> None
    | DEUpdate _ -> None
    | DEDelete _ -> None

and private checkPureSubSelectExpr (select : ResolvedSelectExpr) : PurityStatus option =
    seq {
        yield! select.CTEs |> Option.toSeq |> Seq.map checkPureSubCommonTableExprs
        yield checkPureSubSelectTreeExpr select.Tree
    } |> foldPuritySeq

and checkPureFieldExpr (expr : ResolvedFieldExpr) : PurityStatus option =
    let mutable purity = Some Pure

    let foundReference (ref : LinkedBoundFieldRef) =
        if not <| Array.isEmpty ref.Ref.Path then
            purity <- None
        else
            match ref.Ref.Ref with
            | VRPlaceholder _ ->
                purity <- unionPurityOption purity (Some PureWithArgs)
            | VRColumn _ ->
                // TODO: check if column is in a sub-select, in which case it might still be pure.
                purity <- None

    let foundPlaceholder placeholder =
        purity <- unionPurityOption purity (Some PureWithArgs)

    let foundQuery query =
        purity <- unionPurityOption purity (checkPureSubSelectExpr query)

    iterFieldExpr
        { idFieldExprIter with
              FieldReference = foundReference
              Query = foundQuery
        }
        expr

    purity

let checkPureBoundAttribute : ResolvedBoundAttribute -> PurityStatus option = function
    | BAExpr e -> checkPureFieldExpr e
    | BAMapping _ -> None