module FunWithFlags.FunDB.JavaScript.FunQL

open System
open NodaTime

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.JavaScript.AST
open FunWithFlags.FunDB.FunQL.AST

type JSCompileException (message : string) =
    inherit Exception(message)

// FIXME: may not take offset into consideration
let jsDateTime (dt : Instant) : JSExpr = JSNew (JSVar "Date", [| dt.ToUnixTimeMilliseconds() |> double |> JSNumber |> JSValue |])

let jsDate (dt : LocalDate) : JSExpr = JSNew (JSVar "Date", [| dt.Year; dt.Month; dt.Day |] |> Array.map (double >> JSNumber >> JSValue))

let jsFieldValue : FieldValue -> JSExpr = function
    | FInt i -> JSValue <| JSNumber (double i)
    | FDecimal d -> JSValue <| JSNumber (double d)
    | FString s -> JSValue <| JSString s
    | FBool b -> JSValue <| JSBool b
    | FDateTime dt -> failwith "Not implemented"
    | FDate dt -> failwith "Not implemented"
    | FInterval int -> failwith "Not implemented"
    | FJson j -> failwith "Not implemented"
    | FUserViewRef r -> failwith "Not implemented"
    | FUuid u -> failwith "Not implemented"
    | FIntArray ints -> JSArray <| Array.map (double >> JSNumber >> JSValue) ints
    | FDecimalArray decs -> JSArray <| Array.map (double >> JSNumber >> JSValue) decs
    | FStringArray strings -> JSArray <| Array.map (JSString >> JSValue) strings
    | FBoolArray bools -> JSArray <| Array.map (JSBool >> JSValue) bools
    | FDateTimeArray dts -> failwith "Not implemented"
    | FDateArray dts -> failwith "Not implemented"
    | FIntervalArray ints -> failwith "Not implemented"
    | FJsonArray jss -> failwith "Not implemented"
    | FUserViewRefArray refs -> failwith "Not implemented"
    | FUuidArray uuids -> failwith "Not implemented"
    | FNull -> JSValue JSNull

// Expects local variable `context`.
let jsLocalFieldExpr : LocalFieldExpr -> JSExpr =
    let rec go = function
        | FEValue v -> jsFieldValue v
        //| FERef col -> JSCall (JSObjectAccess (JSVar "context", "getColumn"), [| JSValue (JSString (col.ToString())) |])
        //| FEPlaceholder p -> raisef JSCompileException "Unexpected placeholder in local expression: %O" p
        | FENot e -> JSNot <| go e
        | FEAnd (a, b) -> JSAnd (go a, go b)
        | FEOr (a, b) -> JSOr (go a, go b)
        | FEBinaryOp (a, op, b) ->
            match op with
            | BOConcat -> JSPlus (go a, go b)
            | BOEq -> JSCall (JSObjectAccess (JSVar "context", "isEqual"), [| go a; go b |])
            | BONotEq -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isEqual"), [| go a; go b |])
            | BOLike -> JSCall (JSObjectAccess (JSVar "context", "isLike"), [| go a; go b |])
            | BONotLike -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isLike"), [| go a; go b |])
            | BOLess -> JSCall (JSObjectAccess (JSVar "context", "isLess"), [| go a; go b |])
            | BOLessEq -> JSCall (JSObjectAccess (JSVar "context", "isLessEq"), [| go a; go b |])
            | BOGreater -> JSCall (JSObjectAccess (JSVar "context", "isGreater"), [| go a; go b |])
            | BOGreaterEq -> JSCall (JSObjectAccess (JSVar "context", "isGreaterEq"), [| go a; go b |])
            | _ -> failwith "Not implemented"
        | FEIn (e, items) -> JSCall (JSObjectAccess (JSVar "context", "isIn"), [| go e; JSArray <| Array.map go items |])
        | FENotIn (e, items) -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isIn"), [| go e; JSArray <| Array.map go items |])
        | FECast (e, typ) ->
            let (isArray, scalarName) =
                match typ with
                | FTScalar st -> (false, st)
                | FTArray st -> (true, st)
            JSCall (JSObjectAccess (JSVar "context", "cast"), [| go e; JSValue (JSBool isArray); JSValue (JSString (scalarName.ToFunQLString())) |])
        | FEIsNull e -> JSStrictEq (go e, JSValue JSNull)
        | FEIsNotNull e -> JSStrictNotEq (go e, JSValue JSNull)
        | _ -> failwith "Not implemented"
    go