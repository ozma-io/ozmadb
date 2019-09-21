module FunWithFlags.FunDB.Parsing

open FSharp.Text.Lexing

let newline (lexbuf: LexBuffer<_>) =
    lexbuf.StartPos <- lexbuf.StartPos.NextLine

let parse (tokenstream : LexBuffer<char> -> 'tok) (parser : (LexBuffer<char> -> 'tok) -> LexBuffer<char> -> 'a) (str : string) : Result<'a, string> =
    let lexbuf = LexBuffer<char>.FromString str
    try
        Ok (parser tokenstream lexbuf)
    with
    | Failure msg -> Error msg
