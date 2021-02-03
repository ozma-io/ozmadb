module FunWithFlags.FunDB.FunQL.Chunk

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
module SQL = FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Chunk

type ViewChunk =
    { Offset : int option
      Limit : int option
    }

let emptyViewChunk =
    { Offset = None
      Limit = None
    } : ViewChunk

let private limitOffsetArgument =
    { ArgType = FTType (FETScalar SFTInt)
      Optional = false
    } : ResolvedArgument

let private addAnonymousInt (int : int option) (argValues : ArgumentValues) (arguments : QueryArguments) : ArgumentValues * QueryArguments * SQL.ValueExpr option =
    match int with
    | None -> (argValues, arguments, None)
    | Some offset ->
        let (argId, arg, arguments) = addAnonymousArgument limitOffsetArgument arguments
        let argValues = Map.add arg (FInt offset) argValues
        let sqlOffset = Some <| SQL.VEPlaceholder argId
        (argValues, arguments, sqlOffset)

let viewExprChunk (chunk : ViewChunk) (expr : CompiledViewExpr) : ArgumentValues * CompiledViewExpr =
    let argValues = Map.empty : ArgumentValues
    let arguments = expr.Query.Arguments
    let (argValues, arguments, sqlOffset) = addAnonymousInt chunk.Offset argValues arguments
    let (argValues, arguments, sqlLimit) = addAnonymousInt chunk.Limit argValues arguments
    let sqlChunk =
        { Offset = sqlOffset
          Limit = sqlLimit
        } : QueryChunk
    let queryExpr = selectExprChunk sqlChunk expr.Query.Expression
    let res =
        { expr with
              Query =
                  { Arguments = arguments
                    Expression = queryExpr
                  }
        }
    (argValues, res)