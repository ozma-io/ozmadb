module FunWithFlags.FunDB.Parsing

open System.Text
open Microsoft.FSharp.Text.Lexing

module Helpers =
    let newline (lexbuf: LexBuffer<_>) =
        lexbuf.StartPos <- lexbuf.StartPos.NextLine

    let escapedString (quoteChar: char) (startPos : int) (lexbuf: LexBuffer<_>) : string =
        let strBuilder = StringBuilder lexbuf.LexemeLength
        let rec parseChar i =
            if i < lexbuf.LexemeLength - 1 then
                match lexbuf.LexemeChar i with
                | '\\' ->
                    let sp = lexbuf.LexemeChar(i + 1)
                    ignore <|
                        match sp with
                        | '\\' -> strBuilder.Append('\\')
                        | 't' -> strBuilder.Append('\t')
                        | 'r' -> strBuilder.Append('\r')
                        | 'n' -> strBuilder.Append('\n')
                        | _ ->
                            if sp = quoteChar
                            then strBuilder.Append(quoteChar)
                            else failwith <| sprintf "Invalid escape sequence: \\%c" sp
                    parseChar (i + 2)
                | c when c = quoteChar ->
                    let sp = lexbuf.LexemeChar(i + 1)
                    if sp = quoteChar then
                        ignore <| strBuilder.Append(sp)
                        parseChar (i + 2)
                    else failwith "Unexpected quote character"
                | c ->
                    ignore <| strBuilder.Append(c)
                    parseChar (i + 1)

        parseChar (startPos + 1) // Skip double quotes
        strBuilder.ToString()

let parse (tokenstream : LexBuffer<char> -> 'tok) (parser : (LexBuffer<char> -> 'tok) -> LexBuffer<char> -> 'a) (str : string) : Result<'a, string> =
    let lexbuf = LexBuffer<char>.FromString str
    try
        Ok (parser tokenstream lexbuf)
    with
    | Failure msg -> Error msg
