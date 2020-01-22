module FunWithFlags.FunDB.Parsing

open FSharp.Text.Lexing

let newline (lexbuf: LexBuffer<_>) =
    lexbuf.StartPos <- lexbuf.StartPos.NextLine

let parse (tokenstream : LexBuffer<char> -> 'tok) (parser : (LexBuffer<char> -> 'tok) -> LexBuffer<char> -> 'a) (str : string) : Result<'a, string> =
    let lexbuf = LexBuffer<char>.FromString str
    try
        Ok (parser tokenstream lexbuf)
    with
    | Failure msg ->
        if lexbuf.StartPos.Line <> lexbuf.EndPos.Line then
            Error <| sprintf "Error at position %i.%i-%i.%i: %s" lexbuf.StartPos.Line lexbuf.StartPos.Column lexbuf.EndPos.Line lexbuf.EndPos.Column msg
        else
            Error <| sprintf "Error at line %i, position %i-%i, token %s: %s" lexbuf.StartPos.Line lexbuf.StartPos.Column lexbuf.EndPos.Column (LexBuffer<_>.LexemeString lexbuf) msg