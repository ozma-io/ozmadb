module internal FunWithFlags.FunDB.SQL.Qualifier

open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL

let qualifyValue = function
    | VInt(i) -> VInt(i)
    | VFloat(f) -> VFloat(f)
    | VString(s) -> VString(s)
    | VBool(b) -> VBool(b)
    | VDateTime(dt) -> VDateTime(dt)
    | VDate(d) -> VDate(d)
    | VObject(ostr) ->
        let lexbuf = LexBuffer<char>.FromString ostr
        VObject(Parser.objectRef Lexer.tokenstream lexbuf)
    | VNull -> VNull

let rec qualifyValueExpr = function
    | VEValue(v) -> VEValue(qualifyValue v)
    | VEColumn(c) -> VEColumn(c)
    | VENot(a) -> VENot(qualifyValueExpr a)
    | VEConcat(a, b) -> VEConcat(qualifyValueExpr a, qualifyValueExpr b)
    | VEEq(a, b) -> VEEq(qualifyValueExpr a, qualifyValueExpr b)
    | VEIn(a, arr) -> VEIn(qualifyValueExpr a, Array.map qualifyValueExpr arr)
    | VEAnd(a, b) -> VEAnd(qualifyValueExpr a, qualifyValueExpr b)
    | VEFunc(name, args) -> VEFunc(name, Array.map qualifyValueExpr args)
    | VECast(a, typ) -> VECast(qualifyValueExpr a, typ)

let rec localizeParsedValueExpr = function
    | VEValue(v) -> VEValue(v)
    | VEColumn(c) ->
        match c.tableRef with
            | None -> VEColumn(LocalColumn(c.name))
            | Some(_) -> failwith "Local value expresison cannot use foreign tables"
    | VENot(a) -> VENot(localizeParsedValueExpr a)
    | VEConcat(a, b) -> VEConcat(localizeParsedValueExpr a, localizeParsedValueExpr b)
    | VEEq(a, b) -> VEEq(localizeParsedValueExpr a, localizeParsedValueExpr b)
    | VEIn(a, arr) -> VEIn(localizeParsedValueExpr a, Array.map localizeParsedValueExpr arr)
    | VEAnd(a, b) -> VEAnd(localizeParsedValueExpr a, localizeParsedValueExpr b)
    | VEFunc(name, args) -> VEFunc(name, Array.map localizeParsedValueExpr args)
    | VECast(a, typ) -> VECast(localizeParsedValueExpr a, typ)

let rec norefValueExpr = function
    | VEValue(v) -> VEValue(v)
    | VEColumn(c) -> failwith "Local value expresison cannot use foreign tables"
    | VENot(a) -> VENot(norefValueExpr a)
    | VEConcat(a, b) -> VEConcat(norefValueExpr a, norefValueExpr b)
    | VEEq(a, b) -> VEEq(norefValueExpr a, norefValueExpr b)
    | VEIn(a, arr) -> VEIn(norefValueExpr a, Array.map norefValueExpr arr)
    | VEAnd(a, b) -> VEAnd(norefValueExpr a, norefValueExpr b)
    | VEFunc(name, args) -> VEFunc(name, Array.map norefValueExpr args)
    | VECast(a, typ) -> VECast(norefValueExpr a, typ)
