module FunWithFlags.FunDB.SQL.Purity

open FunWithFlags.FunUtils

open FunWithFlags.FunDB.SQL.AST

type PurityStatus = Pure | PureWithArgs

let unionPurity (a : PurityStatus) (b : PurityStatus) : PurityStatus =
    match (a, b) with
    | (Pure, Pure) -> Pure
    | (PureWithArgs, _) -> PureWithArgs
    | (_, PureWithArgs) -> PureWithArgs

let unionPurityOption (a : PurityStatus option) (b : PurityStatus option) : PurityStatus option =
    match (a, b) with
    | (None, _)
    | (_, None) -> None
    | (Some a, Some b) -> Some (unionPurity a b)

let private foldPuritySeq = Seq.foldOption (fun acc curr -> unionPurityOption (Some acc) curr) Pure

let rec private checkPureSubSelectTreeExpr : SelectTreeExpr -> PurityStatus option = function
    | SSelect query -> checkPureSubSingleSelectExpr query
    | SValues vals -> vals |> Seq.concat |> Seq.map checkPureValueExpr |> foldPuritySeq
    | SSetOp setOp -> None

and private checkPureSubSelectedColumn : SelectedColumn -> PurityStatus option = function
    | SCAll from -> Some Pure
    | SCExpr (name, expr) -> checkPureValueExpr expr

and private checkPureSubFromExpr : FromExpr -> PurityStatus option = function
    | FTable _ ->
        // TODO: check is table is from CTE, then it might still be pure.
        None
    | FSubExpr subExpr -> checkPureSubSelectExpr subExpr.Select
    | FJoin join ->
        seq {
            yield checkPureSubFromExpr join.A
            yield checkPureSubFromExpr join.B
            yield checkPureValueExpr join.Condition
        } |> foldPuritySeq

and private checkPureSubSingleSelectExpr (select : SingleSelectExpr) : PurityStatus option =
    seq {
        yield! select.Columns |> Seq.map checkPureSubSelectedColumn
        yield! select.From |> Option.toSeq |> Seq.map checkPureSubFromExpr
        yield! select.GroupBy |> Seq.map checkPureValueExpr
        yield checkPureSubOrderLimitExpr select.OrderLimit
        yield! select.Where |> Option.toSeq |> Seq.map checkPureValueExpr
    } |> foldPuritySeq

and private checkPureSubOrderLimitExpr (orderLimit : OrderLimitClause) : PurityStatus option =
    seq {
        yield! orderLimit.Limit |> Option.toSeq |> Seq.map checkPureValueExpr
        yield! orderLimit.Offset |> Option.toSeq |> Seq.map checkPureValueExpr
        yield! orderLimit.OrderBy |> Seq.map (fun order -> checkPureValueExpr order.Expr)
    } |> foldPuritySeq

and private checkPureSubCommonTableExprs (exprs : CommonTableExprs) : PurityStatus option =
    exprs.Exprs |> Seq.map (fun (name, expr) -> checkPureSubDataExpr expr.Expr) |> foldPuritySeq

and private checkPureSubDataExpr : DataExpr -> PurityStatus option = function
    | DESelect sel -> checkPureSubSelectExpr sel
    | DEInsert _ -> None
    | DEUpdate _ -> None
    | DEDelete _ -> None

and private checkPureSubSelectExpr (select : SelectExpr) : PurityStatus option =
    seq {
        yield! select.CTEs |> Option.toSeq |> Seq.map checkPureSubCommonTableExprs
        yield checkPureSubSelectTreeExpr select.Tree
    } |> foldPuritySeq

and checkPureValueExpr (expr : ValueExpr) : PurityStatus option =
    let mutable purity = Some Pure

    let foundReference column =
        // TODO: check if column is in a sub-select, in which case it might still be pure.
        purity <- None

    let foundPlaceholder placeholder =
        purity <- unionPurityOption purity (Some PureWithArgs)

    let foundQuery query =
        purity <- unionPurityOption purity (checkPureSubSelectExpr query)

    iterValueExpr
        { idValueExprIter with
              ColumnReference = foundReference
              Placeholder = foundPlaceholder
              Query = foundQuery
        }
        expr

    purity
