module FunWithFlags.FunDB.SQL.Chunk

open FunWithFlags.FunDB.SQL.AST

type QueryChunk =
    { Offset : ValueExpr option
      Limit : ValueExpr option
      Where : ValueExpr option
    }

let emptyQueryChunk =
    { Offset = None
      Limit = None
      Where = None
    } : QueryChunk

let private applyToOrderLimit (chunk : QueryChunk) (orderLimit : OrderLimitClause) =
    let offsetExpr =
        match chunk.Offset with
        | None -> orderLimit.Offset
        | Some offset ->
            Some <|
                match orderLimit.Offset with
                | None -> offset
                | Some oldOffset ->
                    let safeOffset = VESpecialFunc (SFGreatest, [|VEValue (VInt 0); offset|])
                    VEBinaryOp (oldOffset, BOPlus, safeOffset)
    let limitExpr =
        match chunk.Limit with
        | None -> orderLimit.Limit
        | Some limit ->
            Some <|
                match orderLimit.Limit with
                | None -> limit
                | Some oldLimit ->
                    // If offset is used, we need to consider of it in limit too to prevent leaks.
                    let oldLimit =
                        match chunk.Offset with
                        | None -> oldLimit
                        | Some offset ->
                            let safeOffset = VESpecialFunc (SFGreatest, [|VEValue (VInt 0); offset|])
                            let newLimit = VEBinaryOp (oldLimit, BOMinus, safeOffset)
                            VESpecialFunc (SFGreatest, [|VEValue (VInt 0); newLimit|])
                    VESpecialFunc (SFLeast, [|oldLimit; limit|])
    { orderLimit with
          Offset = offsetExpr
          Limit = limitExpr
    }

type private ChunkApplier (chunk : QueryChunk) =
    let applyLimitSelectTreeExpr : SelectTreeExpr -> SelectTreeExpr = function
        | SSelect sel -> SSelect { sel with OrderLimit = applyToOrderLimit chunk sel.OrderLimit }
        | SValues _ as values ->
            // Outer select is the way.
            let fromAlias =
                { Name = SQLName "inner"
                  Columns = None
                }
            let innerSelect = selectExpr values
            let tableExpr = subSelectExpr fromAlias innerSelect
            let outerSelect =
                { emptySingleSelectExpr with
                    Columns = [| SCAll None |]
                    From = Some <| FTableExpr tableExpr
                    OrderLimit = applyToOrderLimit chunk emptyOrderLimitClause
                }
            SSelect outerSelect
        | SSetOp setOp ->
            SSetOp
                { setOp with
                      OrderLimit = applyToOrderLimit chunk setOp.OrderLimit
                }

    let applySelectExpr (select : SelectExpr) : SelectExpr =
        match chunk.Where with
        | None ->
            { select with
                  Tree = applyLimitSelectTreeExpr select.Tree
            }
        | Some where ->
            // We need to create an outer SELECT with restrictions and its own limits.
            let fromAlias =
                { Name = SQLName "inner"
                  Columns = None
                }
            let tableExpr = subSelectExpr fromAlias select
            let outerSelect =
                { emptySingleSelectExpr with
                      Columns = [| SCAll None |]
                      From = Some <| FTableExpr tableExpr
                      Where = Some where
                      OrderLimit = applyToOrderLimit chunk emptyOrderLimitClause
                }
            selectExpr (SSelect outerSelect)

    member this.ApplySelectExpr select = applySelectExpr select

let selectExprChunk (chunk : QueryChunk) (select : SelectExpr) : SelectExpr =
    let applier = ChunkApplier chunk
    applier.ApplySelectExpr select