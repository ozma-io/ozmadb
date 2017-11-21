namespace FunWithFlags.FunDB.SQL.Value

open FunWithFlags.FunDB.SQL.Utils

type ValueType =
    | VTInt
    | VTFloat
    | VTString
    | VTBool
    | VTObject
    with
        override this.ToString () =
            match this with
                | VTInt -> "int8"
                | VTFloat -> "float8"
                | VTString -> "text"
                | VTBool -> "bool"
                | VTObject -> "regclass"

type Value =
    | VInt of int
    | VFloat of double
    | VString of string
    | VBool of bool
    | VArray of Value array
    | VNull
    with
        override this.ToString () =
            match this with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> renderSqlString s
                | VBool(b) -> renderBool b
                | VArray(arr) -> arr |> Array.map (fun v -> v.ToString ()) |> String.concat ", " |> sprintf "(%s)"
                | VNull -> "NULL"

        member this.ToDisplayString () =
            match this with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> s
                | VBool(b) -> renderBool b
                | VArray(arr) -> arr |> Array.map (fun v -> v.ToDisplayString ()) |> String.concat ", " |> sprintf "(%s)"
                | VNull -> "NULL"

type ValueExpr<'c> =
    | WValue of Value
    | WColumn of 'c
    | WNot of ValueExpr<'c>
    | WConcat of ValueExpr<'c> * ValueExpr<'c>
    | WEq of ValueExpr<'c> * ValueExpr<'c>
    | WIn of ValueExpr<'c> * ValueExpr<'c>
    | WAnd of ValueExpr<'c> * ValueExpr<'c>
    | WFunc of string * ValueExpr<'c> array
    | WCast of ValueExpr<'c> * ValueType
