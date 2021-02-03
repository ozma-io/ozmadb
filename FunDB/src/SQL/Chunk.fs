module FunWithFlags.FunDB.SQL.Chunk

open FunWithFlags.FunDB.SQL.AST

type QueryChunk =
    { Offset : ValueExpr option
      Limit : ValueExpr option
    }

let emptyQueryChunk =
    { Offset = None
      Limit = None
    } : QueryChunk

let private applyToOrderLimit (chunk : QueryChunk) (orderLimit : OrderLimitClause) =
    let offsetExpr =
        match chunk.Offset with
        | None -> orderLimit.Offset
        | Some offset ->
            Some <|
                match orderLimit.Offset with
                | Some oldOffset -> VEPlus (oldOffset, offset)
                | None -> offset
    let limitExpr =
        match chunk.Limit with
        | None -> orderLimit.Limit
        | Some limit ->
            Some <|
                match orderLimit.Limit with
                | Some oldLimit -> VEFunc(SQLName "least", [|oldLimit; limit|])
                | None -> limit
    { orderLimit with
          Offset = offsetExpr
          Limit = limitExpr
    }

type private ChunkApplier (chunk : QueryChunk) =
    let rec applySelectTreeExpr : SelectTreeExpr -> SelectTreeExpr = function
        | SSelect sel -> SSelect { sel with OrderLimit = applyToOrderLimit chunk sel.OrderLimit }
        | SValues values -> SValues values
        | SSetOp setOp ->
            SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = applySelectExpr setOp.A
                  B = applySelectExpr setOp.B
                  OrderLimit = setOp.OrderLimit
                }

    and applyCommonTableExpr (cte : CommonTableExpr) : CommonTableExpr =
        { Fields = cte.Fields
          Materialized = cte.Materialized
          Expr = applySelectExpr cte.Expr
        }

    and applyCommonTableExprs (ctes : CommonTableExprs) : CommonTableExprs =
        { Recursive = ctes.Recursive
          Exprs = Array.map (fun (name, expr) -> (name, applyCommonTableExpr expr)) ctes.Exprs
        }

    and applySelectExpr (select : SelectExpr) : SelectExpr =
        { CTEs = Option.map applyCommonTableExprs select.CTEs
          Tree = applySelectTreeExpr select.Tree
        }

    member this.ApplySelectExpr select = applySelectExpr select

let selectExprChunk (chunk : QueryChunk) (select : SelectExpr) : SelectExpr =
    let applier = ChunkApplier chunk
    applier.ApplySelectExpr select