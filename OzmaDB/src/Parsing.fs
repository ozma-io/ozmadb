module OzmaDB.Parsing

open FSharp.Text.Lexing

let newline (lexbuf: LexBuffer<_>) =
    lexbuf.EndPos <- lexbuf.EndPos.NextLine

type TokenStream<'tok> = LexBuffer<char> -> 'tok
type Parser<'tok, 'a> = (LexBuffer<char> -> 'tok) -> LexBuffer<char> -> 'a

let parse (tokenstream : TokenStream<'tok>) (parser : Parser<'tok, 'a>) (str : string) : Result<'a, string> =
    let lexbuf = LexBuffer<char>.FromString str
    try
        Ok (parser tokenstream lexbuf)
    with
    | Failure msg ->
        if lexbuf.StartPos.Line <> lexbuf.EndPos.Line then
            Error <| sprintf "Error at position %i.%i-%i.%i: %s" (lexbuf.StartPos.Line + 1) (lexbuf.StartPos.Column + 1) (lexbuf.EndPos.Line + 1) (lexbuf.EndPos.Column + 1) msg
        else
            Error <| sprintf "Error at line %i, position %i-%i, token %s: %s" (lexbuf.StartPos.Line + 1) (lexbuf.StartPos.Column + 1) (lexbuf.EndPos.Column + 1) (LexBuffer<_>.LexemeString lexbuf) msg