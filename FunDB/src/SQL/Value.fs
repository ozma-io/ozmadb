namespace FunWithFlags.FunDB.SQL.Value

open FunWithFlags.FunDB.SQL.Utils

type ValueType =
    | VTInt
    | VTFloat
    | VTString
    | VTBool

type Value =
    | VInt of int
    | VFloat of double
    | VString of string
    | VBool of bool
    | VNull
    with
        override this.ToString () =
            match this with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> renderSqlString s
                | VBool(b) -> renderBool b
                | VNull -> "NULL"

        member this.ToDisplayString () =
            match this with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> s
                | VBool(b) -> renderBool b
                | VNull -> "NULL"

type ValueExpr<'c> =
    | WValue of Value
    | WColumn of 'c
    | WEq of ValueExpr<'c> * ValueExpr<'c>
    | WAnd of ValueExpr<'c> * ValueExpr<'c>
