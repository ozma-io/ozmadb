module OzmaDB.JavaScript.AST

open OzmaDB.JavaScript.Utils

type JSValue =
    | JSNumber of double
    | JSString of string
    | JSBool of bool
    | JSNull
    | JSUndefined
    with
        override this.ToString () = this.ToJSString()

        member this.ToJSString () =
            match this with
            | JSNumber d -> renderJsNumber d
            | JSString s -> renderJsString s
            | JSBool b -> renderJsBool b
            | JSNull -> "null"
            | JSUndefined -> "undefined"

        interface IJSString with
            member this.ToJSString () = this.ToJSString()

type JSExpr =
    | JSValue of JSValue
    | JSVar of string
    | JSNew of JSExpr * JSExpr[]
    | JSCall of JSExpr * JSExpr[]
    | JSObjectAccess of JSExpr * string
    | JSArray of JSExpr[]
    | JSArrayAccess of JSExpr * JSExpr
    | JSNot of JSExpr
    | JSAnd of JSExpr * JSExpr
    | JSOr of JSExpr * JSExpr
    | JSPlus of JSExpr * JSExpr
    | JSStrictEq of JSExpr * JSExpr
    | JSStrictNotEq of JSExpr * JSExpr
    | JSLess of JSExpr * JSExpr
    | JSLessEq of JSExpr * JSExpr
    | JSGreater of JSExpr * JSExpr
    | JSGreaterEq of JSExpr * JSExpr
    | JSIn of JSExpr * JSExpr
    with
        override this.ToString () = this.ToJSString()

        member this.ToJSString () =
            match this with
            | JSValue v -> v.ToJSString()
            | JSVar name -> name
            | JSNew (constr, args) -> sprintf "new (%s)(%s)" (constr.ToJSString()) (args |> Array.map (fun x -> x.ToJSString()) |> String.concat ", ")
            | JSCall (func, args) -> sprintf "(%s)(%s)"(func.ToJSString()) (args |> Array.map (fun x -> x.ToJSString()) |> String.concat ", ")
            | JSObjectAccess (obj, memb) -> sprintf "(%s).%s" (obj.ToJSString()) memb
            | JSArray vals -> sprintf "[%s]" (vals |> Array.map (fun x -> x.ToJSString()) |> String.concat ", ")
            | JSArrayAccess (arr, elem) -> sprintf "(%s)[%s]" (arr.ToJSString()) (elem.ToJSString())
            | JSNot e -> sprintf "!(%s)" (e.ToJSString())
            | JSAnd (a, b) -> sprintf "(%s) && (%s)" (a.ToJSString()) (b.ToJSString())
            | JSOr (a, b) -> sprintf "(%s) || (%s)" (a.ToJSString()) (b.ToJSString())
            | JSPlus (a, b) -> sprintf "(%s) + (%s)" (a.ToJSString()) (b.ToJSString())
            | JSStrictEq (a, b) -> sprintf "(%s) === (%s)" (a.ToJSString()) (b.ToJSString())
            | JSStrictNotEq (a, b) -> sprintf "(%s) !== (%s)" (a.ToJSString()) (b.ToJSString())
            | JSLess (a, b) -> sprintf "(%s) < (%s)" (a.ToJSString()) (b.ToJSString())
            | JSLessEq (a, b) -> sprintf "(%s) <= (%s)" (a.ToJSString()) (b.ToJSString())
            | JSGreater (a, b) -> sprintf "(%s) > (%s)" (a.ToJSString()) (b.ToJSString())
            | JSGreaterEq (a, b) -> sprintf "(%s) >= (%s)" (a.ToJSString()) (b.ToJSString())
            | JSIn (key, dict) -> sprintf "(%s) in (%s)" (key.ToJSString()) (dict.ToJSString())

        interface IJSString with
            member this.ToJSString () = this.ToJSString()