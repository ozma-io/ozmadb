module FunWithFlags.FunDB.JavaScript.FunQL

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.JavaScript.AST
open FunWithFlags.FunDB.FunQL.AST

type JSCompileException (message : string) =
    inherit Exception(message)

// FIXME: may not take offset into consideration
let jsDateTime (dt : DateTime) : JSExpr = JSNew (JSVar "Date", [| dt.Year; dt.Month; dt.Day; dt.Hour; dt.Minute; dt.Second; dt.Millisecond |] |> Array.map (double >> JSNumber >> JSValue))

let jsDate (dt : DateTime) : JSExpr = JSNew (JSVar "Date", [| dt.Year; dt.Month; dt.Day; dt.Hour |] |> Array.map (double >> JSNumber >> JSValue))

let jsFieldValue : FieldValue -> JSExpr = function
    | FInt i -> JSValue <| JSNumber (double i)
    | FDecimal d -> JSValue <| JSNumber (double d)
    | FString s -> JSValue <| JSString s
    | FBool b -> JSValue <| JSBool b
    | FDateTime dt -> jsDateTime dt
    | FDate dt -> jsDate dt
    | FJson j -> failwith "Not implemented"
    | FUserViewRef r -> failwith "Not implemented"
    | FIntArray ints -> JSArray <| Array.map (double >> JSNumber >> JSValue) ints
    | FDecimalArray decs -> JSArray <| Array.map (double >> JSNumber >> JSValue) decs
    | FStringArray strings -> JSArray <| Array.map (JSString >> JSValue) strings
    | FBoolArray bools -> JSArray <| Array.map (JSBool >> JSValue) bools
    | FDateTimeArray dts -> JSArray <| Array.map jsDateTime dts
    | FDateArray dts -> JSArray <| Array.map jsDate dts
    | FJsonArray jss -> failwith "Not implemented"
    | FUserViewRefArray refs -> failwith "Not implemented"
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
        | FEConcat (a, b) -> JSPlus (go a, go b)
        | FEEq (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isEqual"), [| go a; go b |])
        | FENotEq (a, b) -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isEqual"), [| go a; go b |])
        | FELike (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isLike"), [| go a; go b |])
        | FENotLike (a, b) -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isLike"), [| go a; go b |])
        | FELess (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isLess"), [| go a; go b |])
        | FELessEq (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isLessEq"), [| go a; go b |])
        | FEGreater (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isGreater"), [| go a; go b |])
        | FEGreaterEq (a, b) -> JSCall (JSObjectAccess (JSVar "context", "isGreaterEq"), [| go a; go b |])
        | FEIn (e, items) -> JSCall (JSObjectAccess (JSVar "context", "isIn"), [| go e; JSArray <| Array.map go items |])
        | FENotIn (e, items) -> JSNot <| JSCall (JSObjectAccess (JSVar "context", "isIn"), [| go e; JSArray <| Array.map go items |])
        | FEInQuery (e, query) -> failwith "Not implemented"
        | FENotInQuery (e, query) -> failwith "Not implemented"
        | FECast (e, typ) ->
            let (isArray, scalarName) =
                match typ with
                | FETScalar st -> (false, st)
                | FETArray st -> (true, st)
            JSCall (JSObjectAccess (JSVar "context", "cast"), [| go e; JSValue (JSBool isArray); JSValue (JSString (scalarName.ToFunQLString())) |])
        | FEIsNull e -> JSStrictEq (go e, JSValue JSNull)
        | FEIsNotNull e -> JSStrictNotEq (go e, JSValue JSNull)
        | FECase (es, els) -> failwith "Not implemented"
        | FECoalesce items -> JSCall (JSObjectAccess (JSVar "context", "coalesce"), Array.map go items)
        | FEJsonArray items -> failwith "Not implemented"
        | FEJsonObject obj -> failwith "Not implemented"
        | FEJsonArrow (a, b) -> failwith "Not implemented"
        | FEJsonTextArrow (a, b) -> failwith "Not implemented"
        | _ -> failwith "impossible"
    go