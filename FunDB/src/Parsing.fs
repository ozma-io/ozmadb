module internal FunWithFlags.FunDB.Parsing

open System.Text
open Microsoft.FSharp.Text.Lexing

let newline (lexbuf: LexBuffer<_>) =
    lexbuf.StartPos <- lexbuf.StartPos.NextLine

let escapedString (quoteChar: char) (lexbuf: LexBuffer<_>) =
    let strBuilder = new StringBuilder(lexbuf.LexemeLength)
    let rec parseChar i =
        if i < lexbuf.LexemeLength - 1 then
            let c = lexbuf.LexemeChar i
            if c = '\\' then
                let sp = lexbuf.LexemeChar (i + 1)
                ignore <|
                    match sp with
                        | '\\' -> strBuilder.Append '\\'
                        | 't' -> strBuilder.Append '\t'
                        | 'r' -> strBuilder.Append '\r'
                        | 'n' -> strBuilder.Append '\n'
                        | _ -> if sp = quoteChar
                               then strBuilder.Append quoteChar
                               else failwith (sprintf "Invalid escape sequence: \\%c" sp)
                parseChar (i + 2)
            else
                ignore <| strBuilder.Append c
                parseChar (i + 1)

    parseChar 1 // Skip double quotes
    strBuilder.ToString()
