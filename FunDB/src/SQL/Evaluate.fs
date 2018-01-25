module internal FunWithFlags.FunDB.SQL.Evaluate

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST

let rec getPureValueExpr = function
    | VEValue(v) -> Some(VEValue(v))
    | VEColumn(c) -> None
    | VENot(a) -> Option.map VENot (getPureValueExpr a)
    | VEConcat(a, b) -> getPureValueExpr a |> Option.bind (fun aVal -> getPureValueExpr b |> Option.map (fun bVal -> VEConcat(aVal, bVal)))
    | VEEq(a, b) -> getPureValueExpr a |> Option.bind (fun aVal -> getPureValueExpr b |> Option.map (fun bVal -> VEEq(aVal, bVal)))
    | VEIn(a, arr) -> getPureValueExpr a |> Option.bind (fun aVal ->
                                                        let newArr = arr |> seqMapMaybe getPureValueExpr |> Array.ofSeq
                                                        if Array.length newArr = Array.length arr then
                                                            Some(VEIn(aVal, newArr))
                                                        else
                                                            None)
    | VEAnd(a, b) -> getPureValueExpr a |> Option.bind (fun aVal -> getPureValueExpr b |> Option.map (fun bVal -> VEAnd(aVal, bVal)))
    | VEFunc(name, args) -> None
    | VECast(a, typ) -> getPureValueExpr a |> Option.map (fun aVal -> VECast(aVal, typ))
